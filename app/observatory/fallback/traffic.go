package fallback

import (
	"context"
	goerrors "errors"
	"io"
	"strings"
	"time"
)

const (
	errorBurstWindow = 2 * time.Second
	errorBurstCount  = 3
)

// tagTraffic tracks user sessions on one outbound tag. Observatory probes
// never increment inflight (they set SkipOutboundErrorReport).
type tagTraffic struct {
	inflight    int
	lastSuccess time.Time
	noiseTimes  []time.Time
}

func (o *Observer) ensureTrafficLocked(tag string) *tagTraffic {
	if o.traffic == nil {
		o.traffic = make(map[string]*tagTraffic)
	}
	t := o.traffic[tag]
	if t == nil {
		t = &tagTraffic{}
		o.traffic[tag] = t
	}
	return t
}

func (o *Observer) ReportOutboundSessionStart(tag string) {
	o.trafficMu.Lock()
	defer o.trafficMu.Unlock()
	t := o.ensureTrafficLocked(tag)
	t.inflight++
}

func (o *Observer) ReportOutboundSessionEnd(tag string, err error) {
	o.trafficMu.Lock()
	defer o.trafficMu.Unlock()
	t := o.ensureTrafficLocked(tag)
	if t.inflight > 0 {
		t.inflight--
	}
	if isOutboundSuccess(err) {
		t.lastSuccess = time.Now()
		t.noiseTimes = nil
	}
}

func (o *Observer) inflightFor(tag string) int {
	o.trafficMu.Lock()
	defer o.trafficMu.Unlock()
	if o.traffic == nil {
		return 0
	}
	t := o.traffic[tag]
	if t == nil {
		return 0
	}
	return t.inflight
}

func isOutboundSuccess(err error) bool {
	if err == nil {
		return true
	}
	return goerrors.Is(err, io.EOF)
}

func isNoiseError(err error) bool {
	if err == nil {
		return false
	}
	if goerrors.Is(err, io.ErrClosedPipe) || goerrors.Is(err, context.Canceled) {
		return true
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "closed pipe") || strings.Contains(msg, "context canceled") {
		return true
	}
	return isLocalLoopbackAbort(err)
}

// isLocalLoopbackAbort is a client abort of the local SOCKS/inbound socket
// (both endpoints on loopback). A reset from the node/CDN uses a public
// remote address and remains a hard failure.
func isLocalLoopbackAbort(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	loopback := strings.Count(msg, "127.0.0.1") + strings.Count(msg, "[::1]")
	if loopback < 2 {
		return false
	}
	return strings.Contains(msg, "forcibly closed") ||
		strings.Contains(msg, "wsarecv") ||
		strings.Contains(msg, "connection reset")
}

// shouldSkipNoiseFollowUp is true when a closed-pipe / canceled error should
// not start a probe. Parallel sessions or a recent success mean the node is
// still carrying traffic. Isolated noise is counted toward a short burst;
// only inflight==0 (this session still counted, so inflight<=1) plus N noise
// errors with no success in the window may probe.
func (o *Observer) shouldSkipNoiseFollowUp(tag string) bool {
	o.trafficMu.Lock()
	defer o.trafficMu.Unlock()
	t := o.ensureTrafficLocked(tag)
	now := time.Now()
	if t.inflight > 1 {
		return true
	}
	if !t.lastSuccess.IsZero() && now.Sub(t.lastSuccess) < errorBurstWindow {
		return true
	}
	t.pruneNoise(now)
	t.noiseTimes = append(t.noiseTimes, now)
	return len(t.noiseTimes) < errorBurstCount
}

func (t *tagTraffic) pruneNoise(now time.Time) {
	cutoff := now.Add(-errorBurstWindow)
	kept := t.noiseTimes[:0]
	for _, ts := range t.noiseTimes {
		if ts.After(cutoff) {
			kept = append(kept, ts)
		}
	}
	t.noiseTimes = kept
}
