package outbound

import (
	"context"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
	"testing"
)

func TestNetworkResetCancelsOnlyThisNodesRawRequests(t *testing.T) {
	h, other := &Handler{}, &Handler{}
	r, w := pipe.New()
	r2, w2 := pipe.New()
	ctx, release := h.trackRequest(context.Background(), &transport.Link{Reader: r, Writer: w})
	defer release()
	ctx2, release2 := other.trackRequest(context.Background(), &transport.Link{Reader: r2, Writer: w2})
	defer release2()
	h.ResetNetworkConnections()
	if ctx.Err() != context.Canceled {
		t.Fatal("raw handshake was left running")
	}
	if ctx2.Err() != nil {
		t.Fatal("healthy sibling was interrupted")
	}
	h.ResetNetworkConnections()
}

func TestInternalHealthCheckIsNotRetiredWithUserRequests(t *testing.T) {
	h := &Handler{}
	r, w := pipe.New()
	ctx, release := h.trackRequest(session.ContextWithSkipOutboundErrorReport(context.Background()), &transport.Link{Reader: r, Writer: w})
	defer release()
	h.ResetNetworkConnections()
	if ctx.Err() != nil || len(h.requests) != 0 {
		t.Fatal("internal confirmation must retain its own deadline")
	}
}
