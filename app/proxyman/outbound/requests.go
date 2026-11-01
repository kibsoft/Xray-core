package outbound

import (
	"context"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"sync"
)

type activeRequest struct {
	cancel context.CancelFunc
	link   *transport.Link
	once   sync.Once
}

// Raw transports also need retirement: cancel a stalled dial/handshake and
// close the user stream so applications can retry through the new route.
func (h *Handler) trackRequest(ctx context.Context, link *transport.Link) (context.Context, func()) {
	if session.SkipOutboundErrorReportFromContext(ctx) {
		return ctx, func() {}
	}
	ctx, cancel := context.WithCancel(ctx)
	r := &activeRequest{cancel: cancel, link: link}
	h.requestMu.Lock()
	if h.requests == nil {
		h.requests = make(map[*activeRequest]struct{})
	}
	h.requests[r] = struct{}{}
	h.requestMu.Unlock()
	return ctx, func() {
		h.requestMu.Lock()
		delete(h.requests, r)
		h.requestMu.Unlock()
		cancel()
	}
}

func (h *Handler) resetRequests() {
	h.requestMu.Lock()
	requests := make([]*activeRequest, 0, len(h.requests))
	for r := range h.requests {
		requests = append(requests, r)
	}
	h.requestMu.Unlock()
	for _, r := range requests {
		r.once.Do(func() {
			r.cancel()
			common.Interrupt(r.link.Reader)
			common.Interrupt(r.link.Writer)
		})
	}
}
