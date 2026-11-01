package fallback

import (
	"context"
	"github.com/xtls/xray-core/app/observatory"
	gonet "net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
)

type recoveryTestController struct {
	mockModeController
	selected string
}

func TestNetworkChangeReadyWaitsForRouting(t *testing.T) {
	o := &Observer{config: &Config{CanaryForeign: []string{"foreign"}, CanaryDomestic: []string{"domestic"}}}
	for _, state := range []NetworkState{NetworkUnknown, NetworkOffline} {
		o.canary.state = state
		if o.NetworkChangeReady() {
			t.Fatal("unusable path must not publish a new VPN network")
		}
	}
	for _, state := range []NetworkState{NetworkOpen, NetworkWhitelist} {
		o.canary.state = state
		o.networkEpoch.Store(1)
		o.networkReadyEpoch.Store(0)
		if o.NetworkChangeReady() {
			t.Fatal("canary verdict alone must not publish before routing is ready")
		}
		o.networkReadyEpoch.Store(1)
		if !o.NetworkChangeReady() {
			t.Fatal("completed routing check must release the native handoff")
		}
	}
}

func (r *recoveryTestController) PrimaryRecoveryCandidates() []string {
	return []string{"bad-primary", "good-primary"}
}
func (r *recoveryTestController) RecoverPrimary(tag string) { r.selected = tag; r.fallback = false }

func TestRecoveryChoosesReachableDefaultPrimary(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer server.Close()
	old := tagged.Dialer
	tagged.Dialer = func(_ context.Context, _ routing.Dispatcher, _ v2net.Destination, tag string) (v2net.Conn, error) {
		if tag == "bad-primary" {
			return nil, context.DeadlineExceeded
		}
		return gonet.Dial("tcp", server.Listener.Addr().String())
	}
	t.Cleanup(func() { tagged.Dialer = old })
	controller := &recoveryTestController{mockModeController: mockModeController{fallback: true}}
	o := &Observer{ctx: context.Background(), config: &Config{ProbeUrl: server.URL, CanaryForeign: []string{"foreign"}, CanaryDomestic: []string{"domestic"}}, ohm: &mockOutboundManager{}, modeCtrl: controller, canary: canaryCache{state: NetworkOpen, at: time.Now()}}
	o.recoverPrimaries.Store(true)
	o.recoverPrimaryPool()
	if controller.selected != "good-primary" || controller.fallback || o.recoverPrimaries.Load() {
		t.Fatal("must leave WL on a confirmed reachable default node")
	}
	if !o.isKnownDead("bad-primary") || !o.isKnownAlive("good-primary") {
		t.Fatal("must retain recovery health, not erase it after switching")
	}
}

func TestNetworkChangeCancelsProbeAndDropsOldHealth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, server := gonet.Pipe()
	defer server.Close()
	defer client.Close()
	started := make(chan struct{})
	old := tagged.Dialer
	tagged.Dialer = func(context.Context, routing.Dispatcher, v2net.Destination, string) (v2net.Conn, error) {
		close(started)
		return client, nil
	}
	t.Cleanup(func() { tagged.Dialer = old })
	o := &Observer{ctx: context.Background(), networkCtx: ctx, config: &Config{ProbeUrl: "http://example.invalid/generate_204"}}
	finished := make(chan struct{})
	go func() { o.probeOutbounds([]string{"primary-1"}); close(finished) }()
	<-started
	o.networkEpoch.Add(1)
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("old-network HTTP probe must cancel without waiting for its 5s timeout")
	}
	if len(o.status) != 0 {
		t.Fatal("canceled old-network result must not mark the node dead")
	}
}

func TestKnownDeadPrimaryIgnoresLateErrorsWithCanary(t *testing.T) {
	o := &Observer{config: &Config{
		SubjectSelector: []string{"primary-"}, FallbackSubjectSelector: []string{"fallback-"},
		CanaryForeign: []string{"foreign"}, CanaryDomestic: []string{"domestic"},
	}, ohm: &mockOutboundManager{}, status: []*observatory.OutboundStatus{{OutboundTag: "primary-1", Alive: false}}}
	if !o.shouldSkipErrorFollowUp("primary-1") {
		t.Fatal("late mux errors must not re-probe an already dead primary")
	}
}

func TestPendingFallbackIsNotEligibleBeforeProbe(t *testing.T) {
	o := &Observer{config: &Config{FallbackSubjectSelector: []string{"fallback-"}}, ohm: &mockOutboundManager{}}
	o.markPendingFallbacks([]string{"fallback-1", "primary-1"})
	if len(o.status) != 1 || o.status[0].OutboundTag != "fallback-1" || o.status[0].Alive {
		t.Fatal("unprobed WL must be ineligible, without changing primary health")
	}
	o.updateStatusForResult("fallback-1", &probeResult{ProbeResult: observatory.ProbeResult{Alive: true}})
	o.markPendingFallbacks([]string{"fallback-1"})
	if !o.status[0].Alive {
		t.Fatal("confirmed healthy WL must stay eligible")
	}
}

func TestCanaryProbeSelection(t *testing.T) {
	for _, tc := range []struct {
		state NetworkState
		want  int
	}{
		{NetworkOpen, 0}, {NetworkOffline, 0}, {NetworkWhitelist, 1},
	} {
		t.Run(tc.state.String(), func(t *testing.T) {
			o := &Observer{config: &Config{
				SubjectSelector: []string{"primary-"}, FallbackSubjectSelector: []string{"fallback-"},
				CanaryForeign: []string{"foreign"}, CanaryDomestic: []string{"domestic"},
			}, ohm: &mockOutboundManager{}, canary: canaryCache{state: tc.state, at: time.Now()}}
			tags := o.tagsToProbeForNetwork()
			if len(tags) != tc.want {
				t.Fatalf("got probes %v, want %d", tags, tc.want)
			}
			if tc.want > 0 && tags[0] != "fallback-1" {
				t.Fatalf("must check only WL, got %v", tags)
			}
		})
	}
}

func TestOfflineFallbackDoesNotProbeServers(t *testing.T) {
	old := tagged.Dialer
	tagged.Dialer = func(context.Context, routing.Dispatcher, v2net.Destination, string) (v2net.Conn, error) {
		t.Fatal("offline must not dial a proxy")
		return nil, nil
	}
	t.Cleanup(func() { tagged.Dialer = old })
	o := &Observer{config: &Config{
		FallbackSubjectSelector: []string{"fallback-"},
		CanaryForeign:           []string{"foreign"}, CanaryDomestic: []string{"domestic"},
	}, ohm: &mockOutboundManager{}, modeCtrl: &mockModeController{fallback: true},
		canary: canaryCache{state: NetworkOffline, at: time.Now()}}
	o.fallbackCanaryTick()
}
