package outbound

import (
	"context"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport"
)

type (
	outboundRequestStart struct{}
	trafficObserver      interface {
		WatchOutboundTraffic(string) bool
		ReportOutboundTraffic(string, bool)
	}
)

func (h *Handler) watchTraffic(ctx context.Context, link *transport.Link) *transport.Link {
	if session.SkipOutboundErrorReportFromContext(ctx) {
		return link
	}
	paths := session.OutboundsFromContext(ctx)
	if len(paths) == 0 || paths[len(paths)-1].Target.Network != net.Network_TCP {
		return link
	}
	o, ok := h.outboundErrorObserver().(trafficObserver)
	if !ok || !o.WatchOutboundTraffic(h.tag) {
		return link
	}
	started, _ := ctx.Value(outboundRequestStart{}).(time.Time)
	report := func(downlink bool) {
		if observer, ok := o.(interface{ ReportOutboundTrafficSince(string, bool, time.Time) }); ok {
			observer.ReportOutboundTrafficSince(h.tag, downlink, started)
		} else {
			o.ReportOutboundTraffic(h.tag, downlink)
		}
	}
	// A connect request can stall inside dial/handshake before Process reads
	// its first payload. Arm here as well, including non-mux transports.
	report(false)
	return &transport.Link{
		Reader: &trafficReader{Reader: link.Reader, report: func() { report(false) }},
		Writer: &trafficWriter{Writer: link.Writer, report: func() { report(true) }},
	}
}

type trafficReader struct {
	buf.Reader
	report func()
}

func (r *trafficReader) ReadMultiBuffer() (buf.MultiBuffer, error) {
	b, err := r.Reader.ReadMultiBuffer()
	if !b.IsEmpty() {
		r.report()
	}
	return b, err
}

func (r *trafficReader) ReadMultiBufferTimeout(timeout time.Duration) (buf.MultiBuffer, error) {
	timed, ok := r.Reader.(buf.TimeoutReader)
	if !ok {
		return nil, buf.ErrNotTimeoutReader
	}
	b, err := timed.ReadMultiBufferTimeout(timeout)
	if !b.IsEmpty() {
		r.report()
	}
	return b, err
}
func (r *trafficReader) Close() error { return common.Close(r.Reader) }
func (r *trafficReader) Interrupt()   { common.Interrupt(r.Reader) }

type trafficWriter struct {
	buf.Writer
	report func()
}

// Kernel splice bypasses WriteMultiBuffer and cannot report live progress.
// Observed connections use buffered copy; unrelated traffic retains splice.
func (w *trafficWriter) TracksOutboundTraffic() bool { return true }

func (w *trafficWriter) WriteMultiBuffer(b buf.MultiBuffer) error {
	hasData := !b.IsEmpty()
	err := w.Writer.WriteMultiBuffer(b)
	if err == nil && hasData {
		w.report()
	}
	return err
}
func (w *trafficWriter) Close() error { return common.Close(w.Writer) }
func (w *trafficWriter) Interrupt()   { common.Interrupt(w.Writer) }
