package fallback

import (
	"context"
	"errors"
	vnet "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
	"sync/atomic"
	"testing"
	"time"
)

func TestUnansweredTrafficDetectsFailureWithoutTransportError(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.ctx = context.Background()
	o.finished = done.New()
	o.ohm.(*mockOutboundManager).bySelector = map[string][]string{"mux": {"bgspb-mux"}}
	old := tagged.Dialer
	var calls atomic.Int32
	tagged.Dialer = func(context.Context, routing.Dispatcher, vnet.Destination, string) (vnet.Conn, error) {
		calls.Add(1)
		return nil, errors.New("blackholed transport")
	}
	t.Cleanup(func() { o.Close(); tagged.Dialer = old })
	if len(o.traffic) != 0 {
		t.Fatal("idle must not schedule health checks")
	}
	o.ReportOutboundTraffic("bgspb-mux", false)
	deadline := time.Now().Add(trafficStallTimeout + time.Second)
	for !o.isKnownDead("bgspb-mux") && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if !o.isKnownDead("bgspb-mux") {
		t.Fatal("missing transport error must not leave the failed node alive")
	}
	for o.isProbing() {
		time.Sleep(time.Millisecond)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected one confirmation, got %d", calls.Load())
	}
}

func TestDownlinkAndNetworkResetCancelStallSuspicion(t *testing.T) {
	o := newMuxObserverForFollowUp()
	t.Cleanup(o.stopTrafficWatchdogs)
	o.ReportOutboundTraffic("bgspb-mux", false)
	old := o.traffic["bgspb-mux"]
	pending := old.pendingSince
	o.ReportOutboundTraffic("bgspb-mux", true)
	if old.watchdog != nil || !old.pendingSince.IsZero() {
		t.Fatal("response must cancel suspicion")
	}
	o.checkStalledTraffic("bgspb-mux", old, pending)
	if o.isProbing() {
		t.Fatal("canceled timer must not start a probe")
	}
	o.ReportOutboundTraffic("bgspb-mux", false)
	pending = old.pendingSince
	o.stopTrafficWatchdogs()
	o.checkStalledTraffic("bgspb-mux", old, pending)
	if o.isProbing() {
		t.Fatal("old network timer must not start a probe")
	}
}

func TestHealthyCheckAbsorbsOldErrorsAndBacksOffSilentDestination(t *testing.T) {
	o := newMuxObserverForFollowUp()
	t.Cleanup(o.stopTrafficWatchdogs)
	old := time.Now().Add(-time.Second)
	o.noteHealthCheck("bgspb-mux", true)
	o.tryBeginProbe()
	for range 100 {
		o.ReportOutboundErrorSince("bgspb-mux", errors.New("late dial error"), old)
	}
	if o.pendingErrorTag != "" {
		t.Fatal("old errors queued more probes after a successful check")
	}
	o.endProbe()
	o.ReportOutboundTraffic("bgspb-mux", false)
	tr := o.traffic["bgspb-mux"]
	if tr.watchdogBackoff != healthyStallBackoff {
		t.Fatal("silent destination must back off after a healthy check")
	}
	o.ReportOutboundTraffic("bgspb-mux", true)
	if tr.watchdogBackoff != 0 {
		t.Fatal("real response must restore fast detection for a new failure")
	}
}

func TestErrorCooldownDoesNotCreateWaiters(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.lastErrorProbe = time.Now()
	o.config.ErrorProbeCooldown = int64(time.Hour)
	start := time.Now()
	for range 100 {
		o.probeAfterOutboundError("bgspb-mux")
	}
	if time.Since(start) > time.Second {
		t.Fatal("cooldown must absorb errors without sleeping")
	}
	if o.isProbing() || o.pendingErrorTag != "" {
		t.Fatal("cooldown must not leave queued checks")
	}
}

func TestClosedRequestsDoNotLeaveStallWatchdog(t *testing.T) {
	o := newMuxObserverForFollowUp()
	t.Cleanup(o.stopTrafficWatchdogs)
	o.ReportOutboundSessionStart("bgspb-mux")
	o.ReportOutboundTraffic("bgspb-mux", false)
	o.ReportOutboundSessionEnd("bgspb-mux", context.Canceled)
	tr := o.traffic["bgspb-mux"]
	if tr.watchdog != nil || !tr.pendingSince.IsZero() {
		t.Fatal("closed request must not generate a delayed ping")
	}
}

func TestOldNetworkCompletionCannotCancelNewRequestWatchdog(t *testing.T) {
	o := newMuxObserverForFollowUp()
	t.Cleanup(o.stopTrafficWatchdogs)
	old := time.Now().Add(-time.Second)
	o.stopTrafficWatchdogs()
	current := time.Now()
	o.ReportOutboundSessionStartSince("bgspb-mux", current)
	o.ReportOutboundTrafficSince("bgspb-mux", false, current)
	o.ReportOutboundSessionEndSince("bgspb-mux", context.Canceled, old)
	o.ReportOutboundTrafficSince("bgspb-mux", true, old)
	tr := o.traffic["bgspb-mux"]
	if tr.inflight != 1 || tr.watchdog == nil || tr.pendingSince.IsZero() {
		t.Fatal("old network completion cleared current traffic state")
	}
}
