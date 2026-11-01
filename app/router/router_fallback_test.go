package router

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/geodata"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/outbound"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	"github.com/xtls/xray-core/testing/mocks"
)

type fallbackTestOutboundManager struct {
	outbound.Manager
	outbound.HandlerSelector
}

type pendingNetworkObservatory struct {
	mockObservatory
	ready bool
}

func (o *pendingNetworkObservatory) NetworkChangeReady() bool { return o.ready }

func TestRouter_NetworkChangeKeepsFallbackUntilPrimaryConfirmed(t *testing.T) {
	r := newTestFallbackRouter(t)
	obs := &pendingNetworkObservatory{mockObservatory: mockObservatory{
		status: []*observatory.OutboundStatus{{OutboundTag: "fallback-1", Alive: true}},
	}}
	r.observatory = obs
	r.EnableFallbackMode()
	r.SyncFallbackMode()
	if !r.fallbackMode.Load() {
		t.Fatal("unknown primary on a new network must not count as recovery")
	}
	obs.status = append(obs.status, &observatory.OutboundStatus{OutboundTag: "primary-1", Alive: true})
	r.SyncFallbackMode()
	if r.fallbackMode.Load() {
		t.Fatal("confirmed primary should leave fallback immediately")
	}
}

func TestRouter_FallbackModeRules(t *testing.T) {
	r := newTestFallbackRouter(t)
	r.EnableFallbackMode()

	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
		Target: net.TCPDestination(net.DomainAddress("bank.ru"), 443),
	}})
	route, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
	common.Must(err)
	if tag := route.GetOutboundTag(); tag != "direct" {
		t.Fatalf("expected direct, got %q", tag)
	}
}

func TestRouter_FallbackDefaultBalancer(t *testing.T) {
	r := newTestFallbackRouter(t)
	r.EnableFallbackMode()

	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
		Target: net.TCPDestination(net.DomainAddress("google.com"), 443),
	}})
	route, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
	common.Must(err)
	if tag := route.GetOutboundTag(); tag != "fallback-1" && tag != "fallback-2" {
		t.Fatalf("expected fallback outbound, got %q", tag)
	}
}

func TestRouter_PrimaryModeRules(t *testing.T) {
	r := newTestFallbackRouter(t)

	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
		Target: net.TCPDestination(net.DomainAddress("google.com"), 443),
	}})
	route, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
	common.Must(err)
	if tag := route.GetOutboundTag(); tag != "primary-1" && tag != "primary-2" {
		t.Fatalf("expected primary outbound, got %q", tag)
	}
}

func TestRouter_FallbackRuNotDirect(t *testing.T) {
	r := newTestFallbackRouter(t)
	r.EnableFallbackMode()

	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
		Target: net.TCPDestination(net.DomainAddress("yandex.ru"), 443),
	}})
	route, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
	common.Must(err)
	if tag := route.GetOutboundTag(); tag != "fallback-1" && tag != "fallback-2" {
		t.Fatalf("expected fallback outbound for unmatched domain, got %q", tag)
	}
}

func TestRouter_PickRouteRetryOnFallback(t *testing.T) {
	r := newTestFallbackRouter(t)
	obs := &mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "primary-1", Alive: false},
		{OutboundTag: "primary-2", Alive: false},
		{OutboundTag: "fallback-1", Alive: true},
	}}
	r.observatory = obs
	r.stickyStrategy.observatory = obs

	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
		Target: net.TCPDestination(net.DomainAddress("google.com"), 443),
	}})
	route, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
	common.Must(err)
	mode, err := r.GetRoutingMode()
	common.Must(err)
	if !mode {
		t.Fatal("expected fallback mode enabled after all primary dead")
	}
	if tag := route.GetOutboundTag(); tag != "fallback-1" && tag != "fallback-2" {
		t.Fatalf("expected fallback outbound after retry, got %q", tag)
	}
}

func TestRouter_TryEnterFallbackRequiresAliveFallback(t *testing.T) {
	r := newTestFallbackRouter(t)
	obs := &mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "primary-1", Alive: false},
		{OutboundTag: "primary-2", Alive: false},
		{OutboundTag: "fallback-1", Alive: false},
		{OutboundTag: "fallback-2", Alive: false},
	}}
	r.observatory = obs
	r.stickyStrategy.observatory = obs

	r.TryEnterFallbackMode()
	mode, err := r.GetRoutingMode()
	common.Must(err)
	if mode {
		t.Fatal("expected primary mode when fallback outbounds are dead")
	}
}

func TestRouter_SyncFallbackMode(t *testing.T) {
	r := newTestFallbackRouter(t)
	obs := &mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "primary-1", Alive: false},
		{OutboundTag: "primary-2", Alive: false},
		{OutboundTag: "fallback-1", Alive: true},
	}}
	r.observatory = obs

	r.SyncFallbackMode()
	mode, err := r.GetRoutingMode()
	common.Must(err)
	if !mode {
		t.Fatal("expected fallback mode when mux are dead and fallback is alive")
	}

	obs.status[0].Alive = true
	r.SyncFallbackMode()
	mode, err = r.GetRoutingMode()
	common.Must(err)
	if mode {
		t.Fatal("expected primary mode after a mux outbound recovered")
	}
}

func TestRouter_SyncFallbackLeavesWhenBothDead(t *testing.T) {
	r := newTestFallbackRouter(t)
	r.EnableFallbackMode()
	obs := &mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "primary-1", Alive: false},
		{OutboundTag: "primary-2", Alive: false},
		{OutboundTag: "fallback-1", Alive: false},
		{OutboundTag: "fallback-2", Alive: false},
	}}
	r.observatory = obs

	r.SyncFallbackMode()
	mode, err := r.GetRoutingMode()
	common.Must(err)
	if mode {
		t.Fatal("expected to leave fallback when fallback outbounds are also dead")
	}
	if len(obs.status) != 4 {
		t.Fatalf("expected Dead marks kept when both dead, got %v", obs.status)
	}
}

func TestRouter_RecoveryDisablesFallback(t *testing.T) {
	r := newTestFallbackRouter(t)
	r.observatory = &mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "primary-1", Alive: true},
		{OutboundTag: "primary-2", Alive: false},
	}}
	r.EnableFallbackMode()

	candidates, err := r.balancers["main-sticky"].SelectOutbounds()
	common.Must(err)
	if !r.hasAlivePrimary(candidates) {
		t.Fatal("expected at least one alive primary outbound")
	}
	r.DisableFallbackMode()
	mode, err := r.GetRoutingMode()
	common.Must(err)
	if mode {
		t.Fatal("expected primary mode after recovery")
	}
}

func TestRouter_GetRoutingMode(t *testing.T) {
	r := newTestFallbackRouter(t)
	mode, err := r.GetRoutingMode()
	if err != nil || mode {
		t.Fatalf("expected primary mode, got mode=%v err=%v", mode, err)
	}
	r.EnableFallbackMode()
	mode, err = r.GetRoutingMode()
	if err != nil || !mode {
		t.Fatalf("expected fallback mode, got mode=%v err=%v", mode, err)
	}
}

func TestRouter_SyncFallback_PrimaryRecoveredClearsObservation(t *testing.T) {
	r := newTestFallbackRouter(t)
	obs := &mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "primary-1", Alive: true},
		{OutboundTag: "fallback-1", Alive: true},
	}}
	r.observatory = obs

	r.EnableFallbackMode()
	r.SyncFallbackMode()
	if r.IsFallbackMode() {
		t.Fatal("expected primary mode after recovery")
	}
	if len(obs.status) != 0 {
		t.Fatalf("expected observation cleared on primary recovery, got %v", obs.status)
	}
}

func TestRouter_DisableFallbackMode_DoesNotClear(t *testing.T) {
	r := newTestFallbackRouter(t)
	obs := &mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "primary-1", Alive: false},
	}}
	r.observatory = obs

	r.EnableFallbackMode()
	r.DisableFallbackMode()
	if len(obs.status) != 1 {
		t.Fatal("expected DisableFallbackMode alone not to clear observation")
	}
}

func newTestFallbackRouter(t *testing.T) *Router {
	t.Helper()
	domainRules, err := geodata.ParseDomainRules([]string{"bank.ru"}, geodata.Domain_Substr)
	common.Must(err)

	config := &Config{
		FallbackBalancerTag: "fallback-random",
		Rule: []*RoutingRule{
			{
				TargetTag: &RoutingRule_BalancingTag{BalancingTag: "main-sticky"},
				Networks:  []net.Network{net.Network_TCP},
			},
		},
		FallbackRule: []*RoutingRule{
			{
				RuleTag:   "fb-bank",
				TargetTag: &RoutingRule_Tag{Tag: "direct"},
				Domain:    domainRules,
			},
		},
		BalancingRule: []*BalancingRule{
			{
				Tag:              "main-sticky",
				OutboundSelector: []string{"primary-"},
				Strategy:         "stickyrandom",
			},
			{
				Tag:              "fallback-random",
				OutboundSelector: []string{"fallback-"},
				Strategy:         "random",
			},
		},
	}

	mockCtl := gomock.NewController(t)
	t.Cleanup(mockCtl.Finish)

	mockDNS := mocks.NewDNSClient(mockCtl)
	mockOhm := mocks.NewOutboundManager(mockCtl)
	mockHs := mocks.NewOutboundHandlerSelector(mockCtl)

	mockHs.EXPECT().Select(gomock.Any()).AnyTimes().DoAndReturn(func(selectors []string) []string {
		if len(selectors) == 1 && selectors[0] == "primary-" {
			return []string{"primary-1", "primary-2"}
		}
		if len(selectors) == 1 && selectors[0] == "fallback-" {
			return []string{"fallback-1", "fallback-2"}
		}
		return nil
	})

	r := new(Router)
	common.Must(r.Init(context.Background(), config, mockDNS, &fallbackTestOutboundManager{
		Manager:         mockOhm,
		HandlerSelector: mockHs,
	}, nil))
	return r
}

type gatedObservatory struct {
	mockObservatory
	allow bool
}

type networkObservatory struct {
	mockObservatory
	state string
}

func (o *networkObservatory) NetworkRoutingState() string { return o.state }
func (o *networkObservatory) AllowFallbackEntry() bool    { return o.state == "whitelist" }

func TestRouter_NetworkVerdictControlsMode(t *testing.T) {
	r := newTestFallbackRouter(t)
	obs := &networkObservatory{state: "whitelist", mockObservatory: mockObservatory{status: []*observatory.OutboundStatus{
		// Stale primary Alive must not suppress a real whitelist verdict.
		{OutboundTag: "primary-1", Alive: true},
		{OutboundTag: "fallback-1", Alive: true},
	}}}
	r.observatory = obs
	r.SyncFallbackMode()
	if !r.IsFallbackMode() {
		t.Fatal("confirmed WL must enter fallback even with stale primary health")
	}
	obs.state = "offline"
	r.SyncFallbackMode()
	if !r.IsFallbackMode() {
		t.Fatal("offline must preserve the last routing mode")
	}
	obs.state = "open"
	obs.status[0].Alive = false
	r.SyncFallbackMode()
	if r.IsFallbackMode() {
		t.Fatal("open network must leave WL even when a primary is dead")
	}
	r.TryEnterFallbackMode()
	if r.IsFallbackMode() {
		t.Fatal("dead primaries must not enable WL on an open network")
	}
}

func TestDefaultStickyIsChosenByRuleOrder(t *testing.T) {
	r := newTestFallbackRouter(t)
	main := r.balancers["main-sticky"]
	r.balancers["google-only"] = &Balancer{strategy: NewStickyRandomStrategy()}
	rules := []*RoutingRule{
		{TargetTag: &RoutingRule_BalancingTag{BalancingTag: "google-only"}},
		{TargetTag: &RoutingRule_BalancingTag{BalancingTag: "main-sticky"}},
	}
	for i := 0; i < 50; i++ {
		r.wireStickyStrategies(rules)
		if r.stickyBalancerTag != "main-sticky" || r.stickyStrategy != main.strategy {
			t.Fatal("recovery must use default route, never Google-only map entry")
		}
	}
}

func TestRecoveryPinsHealthyPrimary(t *testing.T) {
	r := newTestFallbackRouter(t)
	obs := &networkObservatory{state: "open", mockObservatory: mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "primary-1", Alive: false}, {OutboundTag: "primary-2", Alive: true},
	}}}
	r.observatory = obs
	r.stickyStrategy.observatory = obs
	r.EnableFallbackMode()
	r.RecoverPrimary("primary-2")
	r.SyncFallbackMode()
	if r.IsFallbackMode() || r.CurrentStickyTag() != "primary-2" || len(obs.status) != 2 {
		t.Fatal("must retain confirmed health and selected node after recovery")
	}
}

func (g *gatedObservatory) AllowFallbackEntry() bool { return g.allow }

func TestRouter_FallbackEntryGate(t *testing.T) {
	status := []*observatory.OutboundStatus{
		{OutboundTag: "primary-1", Alive: false},
		{OutboundTag: "primary-2", Alive: false},
		{OutboundTag: "fallback-1", Alive: true},
	}
	r := newTestFallbackRouter(t)
	obs := &gatedObservatory{mockObservatory: mockObservatory{status: status}}
	r.observatory = obs
	r.stickyStrategy.observatory = obs

	// Open network: all primaries look dead but whitelist entry is vetoed.
	r.SyncFallbackMode()
	if mode, _ := r.GetRoutingMode(); mode {
		t.Fatal("fallback must not be entered when the gate vetoes it")
	}

	// Whitelist signature: entry is allowed.
	obs.allow = true
	r.SyncFallbackMode()
	if mode, _ := r.GetRoutingMode(); !mode {
		t.Fatal("fallback must be entered when the gate allows it")
	}

	// Once in fallback a later veto must not drop the mode.
	obs.allow = false
	r.SyncFallbackMode()
	if mode, _ := r.GetRoutingMode(); !mode {
		t.Fatal("fallback mode must persist while primaries stay dead")
	}
}
