package fallback

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
)

type networkTestMode struct {
	observer *Observer
	fallback atomic.Bool
}

func (m *networkTestMode) EnableFallbackMode()  { m.fallback.Store(true) }
func (m *networkTestMode) DisableFallbackMode() { m.fallback.Store(false) }
func (m *networkTestMode) IsFallbackMode() bool { return m.fallback.Load() }
func (m *networkTestMode) SyncFallbackMode() {
	report, _ := m.observer.GetObservation(context.Background())
	var primary, fallback *observatory.OutboundStatus
	for _, status := range report.(*observatory.ObservationResult).Status {
		if status.OutboundTag == "primary-1" {
			primary = status
		}
		if status.OutboundTag == "fallback-1" {
			fallback = status
		}
	}
	if primary == nil || primary.Alive {
		return
	}
	if fallback != nil && fallback.Alive && m.observer.AllowFallbackEntry() {
		m.EnableFallbackMode()
	}
}

func TestNetworkChangeCancelsOldChecksAndDiscardsOldHealth(t *testing.T) {
	o := &Observer{ctx: context.Background()}
	oldCtx, cancel := context.WithCancel(o.ctx)
	o.networkCtx, o.networkCancel = oldCtx, cancel
	o.networkEpoch.Store(1)
	old := probeResult{ProbeResult: observatory.ProbeResult{Alive: true}, epoch: 1}
	// A closed observer needs no goroutine; exercise invalidation with an empty selector.
	o.config = &Config{}
	o.finished = done.New()
	o.NotifyNetworkChanged()
	defer o.Close()
	select {
	case <-oldCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("old network checks were not cancelled")
	}
	o.updateStatusForResult("primary-1", &old)
	report, _ := o.GetObservation(o.ctx)
	if len(report.(*observatory.ObservationResult).Status) != 0 {
		t.Fatal("stale result repopulated new network health")
	}
}

func TestNetworkChangeEntersWhitelistWithinSevenSecondsWhileProbeSlotBusy(t *testing.T) {
	domestic := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer domestic.Close()
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer proxy.Close()
	oldDialer := tagged.Dialer
	tagged.Dialer = func(ctx context.Context, _ routing.Dispatcher, dest v2net.Destination, tag string) (v2net.Conn, error) {
		if tag == "fallback-1" {
			return (&net.Dialer{}).DialContext(ctx, "tcp", proxy.Listener.Addr().String())
		}
		if tag == "direct" && dest.Address.String() == "domestic.test" {
			return (&net.Dialer{}).DialContext(ctx, "tcp", domestic.Listener.Addr().String())
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	defer func() { tagged.Dialer = oldDialer }()
	o := &Observer{
		ctx: context.Background(), finished: done.New(),
		config: &Config{SubjectSelector: []string{"primary-"}, FallbackSubjectSelector: []string{"fallback-"},
			CanaryForeign: []string{"foreign.test:443"}, CanaryDomestic: []string{"domestic.test:443"},
			CanaryTimeout: int64(100 * time.Millisecond), CanaryConfirmRounds: 2, ProbeUrl: "http://probe.test/generate_204"},
		ohm: &mockOutboundManager{mockHandlerSelector: mockHandlerSelector{bySelector: map[string][]string{
			"primary-": {"primary-1"}, "fallback-": {"fallback-1"},
		}}},
		// Mimic an error-driven probe still using the old Wi-Fi network.
		probing: true,
	}
	mode := &networkTestMode{observer: o}
	o.modeCtrl = mode
	o.canary.state, o.canary.at = NetworkOpen, time.Now()
	defer o.Close()
	started := time.Now()
	o.NotifyNetworkChanged()
	deadline := time.NewTimer(7 * time.Second)
	defer deadline.Stop()
	for !mode.IsFallbackMode() || !o.NetworkChangeReady() {
		select {
		case <-deadline.C:
			t.Fatal("network change waited for the old probe slot or exceeded 7s")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if elapsed := time.Since(started); elapsed > 7*time.Second {
		t.Fatalf("whitelist entry took %s", elapsed)
	}
}
