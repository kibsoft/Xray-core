package fallback

import (
	"context"
	"io"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"golang.org/x/net/http2"
)

type mockHandlerSelector struct {
	lastSelectors []string
	bySelector    map[string][]string
}

func (m *mockHandlerSelector) Select(selectors []string) []string {
	m.lastSelectors = append([]string{}, selectors...)
	if m.bySelector != nil {
		var out []string
		for _, selector := range selectors {
			out = append(out, m.bySelector[selector]...)
		}
		return out
	}
	if len(selectors) == 1 && selectors[0] == "fallback-" {
		return []string{"fallback-1"}
	}
	return nil
}

type mockOutboundManager struct {
	mockHandlerSelector
}

func (m *mockOutboundManager) Type() interface{} { return nil }
func (m *mockOutboundManager) Start() error      { return nil }
func (m *mockOutboundManager) Close() error      { return nil }
func (m *mockOutboundManager) GetHandler(tag string) outbound.Handler {
	return nil
}
func (m *mockOutboundManager) GetDefaultHandler() outbound.Handler { return nil }
func (m *mockOutboundManager) AddHandler(context.Context, outbound.Handler) error {
	return nil
}
func (m *mockOutboundManager) RemoveHandler(context.Context, string) error {
	return nil
}
func (m *mockOutboundManager) ListHandlers(context.Context) []outbound.Handler {
	return nil
}

type mockModeController struct {
	fallback bool
}

func (m *mockModeController) EnableFallbackMode()  { m.fallback = true }
func (m *mockModeController) DisableFallbackMode() { m.fallback = false }
func (m *mockModeController) IsFallbackMode() bool { return m.fallback }

func TestActiveSelectors_PrimaryOnly(t *testing.T) {
	o := &Observer{
		config: &Config{
			SubjectSelector:         []string{"primary-"},
			FallbackSubjectSelector: []string{"fallback-"},
		},
		modeCtrl: &mockModeController{fallback: false},
	}
	got := o.activeSelectors()
	if len(got) != 1 || got[0] != "primary-" {
		t.Fatalf("expected primary selector only, got %v", got)
	}
}

func TestActiveSelectors_IncludesFallback(t *testing.T) {
	o := &Observer{
		config: &Config{
			SubjectSelector:         []string{"primary-"},
			FallbackSubjectSelector: []string{"fallback-"},
		},
		modeCtrl: &mockModeController{fallback: true},
	}
	got := o.activeSelectors()
	if len(got) != 2 {
		t.Fatalf("expected 2 selectors, got %v", got)
	}
}

func TestProbeFallback_SelectsFallbackOutbounds(t *testing.T) {
	ohm := &mockOutboundManager{}
	o := &Observer{
		config: &Config{
			FallbackSubjectSelector: []string{"fallback-"},
		},
		ohm: ohm,
	}
	got := o.fallbackOutboundTags()
	if len(got) != 1 || got[0] != "fallback-1" {
		t.Fatalf("expected fallback outbound tags, got %v", got)
	}
	if len(ohm.lastSelectors) != 1 || ohm.lastSelectors[0] != "fallback-" {
		t.Fatalf("expected fallback selector probe, got %v", ohm.lastSelectors)
	}
}

func TestProbeOnErrorDefaultEnabled(t *testing.T) {
	o := &Observer{config: &Config{}}
	if !o.probeOnErrorEnabled() {
		t.Fatal("expected probe on error to be enabled by default")
	}
	o.config.DisableProbeOnError = true
	if o.probeOnErrorEnabled() {
		t.Fatal("expected probe on error disabled")
	}
}

func TestUniqueTags(t *testing.T) {
	got := uniqueTags([]string{"a", "b", "a", "", "c"})
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Fatalf("unexpected unique tags: %v", got)
	}
}

func TestIdleProbeIntervalZeroDisablesPeriodic(t *testing.T) {
	o := &Observer{config: &Config{}}
	if o.idleProbeInterval() != 0 {
		t.Fatalf("expected 0 idle interval, got %s", o.idleProbeInterval())
	}
}

func TestTryStartErrorProbeSkipsWhileBusy(t *testing.T) {
	o := &Observer{config: &Config{ErrorProbeCooldown: int64(time.Second)}}
	if !o.tryBeginProbe() {
		t.Fatal("expected to begin probe")
	}
	if o.tryStartErrorProbe() {
		t.Fatal("error probe must wait while another probe is running")
	}
	if !o.lastErrorProbe.IsZero() {
		t.Fatal("cooldown must not start when the probe is skipped")
	}
	o.endProbe()
	if !o.tryStartErrorProbe() {
		t.Fatal("expected error probe to start after the busy probe ends")
	}
	o.endProbe()
	if o.tryStartErrorProbe() {
		t.Fatal("expected cooldown to block a second error probe")
	}
}

func newMuxObserverForFollowUp() *Observer {
	ohm := &mockOutboundManager{}
	ohm.bySelector = map[string][]string{
		"mux": {"bgspb-mux", "aespb-mux", "twspb-mux"},
		"wl":  {"wl-beget"},
	}
	return &Observer{
		config: &Config{
			SubjectSelector:         []string{"mux"},
			FallbackSubjectSelector: []string{"wl"},
		},
		ohm: ohm,
	}
}

func TestHasConfirmedAlivePrimaryExcept_SiblingAlive(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: true},
	}
	if !o.hasConfirmedAlivePrimaryExcept("bgspb-mux") {
		t.Fatal("expected another primary to be confirmed alive")
	}
	if !o.shouldSkipErrorFollowUp("bgspb-mux") {
		t.Fatal("must not sweep other outbounds when a sibling mux is alive")
	}
}

func TestHasConfirmedAlivePrimaryExcept_AllDeadNeedsFallback(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
	}
	if o.hasConfirmedAlivePrimaryExcept("bgspb-mux") {
		t.Fatal("no primary should be alive")
	}
	if o.shouldSkipErrorFollowUp("bgspb-mux") {
		t.Fatal("all primaries dead must still follow up")
	}
	got := o.primaryOutboundTagsExcept("bgspb-mux")
	if len(got) != 2 {
		t.Fatalf("expected remaining mux tags, got %v", got)
	}
}

func TestShouldSkipErrorFollowUp_UnknownSiblingsNeedProbe(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
	}
	if o.shouldSkipErrorFollowUp("bgspb-mux") {
		t.Fatal("unprobed siblings must be checked before skipping follow-up")
	}
}

func TestReportOutboundErrorIgnoredWhenProbing(t *testing.T) {
	o := newMuxObserverForFollowUp()
	if !o.tryBeginProbe() {
		t.Fatal("expected to begin probe")
	}
	o.ReportOutboundError("bgspb-mux", nil)
	if !o.lastErrorProbe.IsZero() {
		t.Fatal("probe-in-flight error report must not start an error probe")
	}
	o.endProbe()
}

func TestShouldSkipErrorFollowUp_FallbackErrorWhenPrimaryAlive(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "aespb-mux", Alive: true},
		{OutboundTag: "wl-beget", Alive: true},
	}
	if !o.shouldSkipErrorFollowUp("wl-beget") {
		t.Fatal("leftover whitelist errors must be ignored after a mux recovered")
	}
	o.ReportOutboundError("wl-beget", nil)
	if !o.lastErrorProbe.IsZero() {
		t.Fatal("whitelist error report must not start a probe while mux is alive")
	}
}

func TestShouldSkipErrorFollowUp_FallbackErrorWhenAllMuxDead(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
		{OutboundTag: "wl-beget", Alive: true},
	}
	if o.shouldSkipErrorFollowUp("wl-beget") {
		t.Fatal("whitelist errors must still be checked while all mux are dead")
	}
}

func TestPlanErrorFollowUp_FallbackAliveDoesNotWakeMux(t *testing.T) {
	o := newMuxObserverForFollowUp()
	plan := o.planErrorFollowUp("wl-beget", true)
	if plan.wake {
		t.Fatal("alive whitelist error must not wake mux recovery")
	}
	if len(plan.probePrimary) != 0 || len(plan.probeFallback) != 0 {
		t.Fatalf("alive whitelist must not sweep others, got %+v", plan)
	}
}

func TestPlanErrorFollowUp_FallbackDeadProbesSiblingFallbackOnly(t *testing.T) {
	ohm := &mockOutboundManager{}
	ohm.bySelector = map[string][]string{
		"mux": {"bgspb-mux"},
		"wl":  {"wl-beget", "wl-other"},
	}
	o := &Observer{
		config: &Config{
			SubjectSelector:         []string{"mux"},
			FallbackSubjectSelector: []string{"wl"},
		},
		ohm: ohm,
	}
	plan := o.planErrorFollowUp("wl-beget", false)
	if plan.wake {
		t.Fatal("dead whitelist must not wake mux recovery")
	}
	if len(plan.probePrimary) != 0 {
		t.Fatalf("dead whitelist must not sweep mux, got %v", plan.probePrimary)
	}
	if len(plan.probeFallback) != 1 || plan.probeFallback[0] != "wl-other" {
		t.Fatalf("expected sibling fallback only, got %v", plan.probeFallback)
	}
}

func TestPlanErrorFollowUp_MuxAllDeadStillProbesFallback(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
	}
	plan := o.planErrorFollowUp("bgspb-mux", false)
	if !plan.wake {
		t.Fatal("mux error follow-up should wake the recovery loop")
	}
	if len(plan.probePrimary) != 2 {
		t.Fatalf("expected remaining mux tags, got %v", plan.probePrimary)
	}
	if len(plan.probeFallback) != 1 || plan.probeFallback[0] != "wl-beget" {
		t.Fatalf("expected fallback probe, got %v", plan.probeFallback)
	}
}

func TestPlanErrorFollowUp_MuxDeadSweepsEvenWhenSiblingAlive(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: true},
		{OutboundTag: "twspb-mux", Alive: true},
	}
	plan := o.planErrorFollowUp("bgspb-mux", false)
	if !plan.wake {
		t.Fatal("mux error follow-up should wake the recovery loop")
	}
	if len(plan.probePrimary) != 2 {
		t.Fatalf("expected sibling sweep despite Alive marks, got %v", plan.probePrimary)
	}
	if len(plan.probeFallback) != 1 || plan.probeFallback[0] != "wl-beget" {
		t.Fatalf("expected wl probe to decide fallback entry, got %v", plan.probeFallback)
	}
}

func TestKnownDeadFallbackTags_OnlyConfirmedDead(t *testing.T) {
	ohm := &mockOutboundManager{}
	ohm.bySelector = map[string][]string{
		"mux": {"bgspb-mux"},
		"wl":  {"wl-beget", "wl-vk"},
	}
	o := &Observer{
		config: &Config{
			SubjectSelector:         []string{"mux"},
			FallbackSubjectSelector: []string{"wl"},
		},
		ohm: ohm,
		status: []*observatory.OutboundStatus{
			{OutboundTag: "wl-beget", Alive: false},
			{OutboundTag: "wl-vk", Alive: true},
		},
	}
	got := o.knownDeadFallbackTags()
	if len(got) != 1 || got[0] != "wl-beget" {
		t.Fatalf("expected only dead wl-beget, got %v", got)
	}
}

func TestKnownDeadFallbackTags_UnprobedNotRetried(t *testing.T) {
	ohm := &mockOutboundManager{}
	ohm.bySelector = map[string][]string{
		"mux": {"bgspb-mux"},
		"wl":  {"wl-beget", "wl-vk"},
	}
	o := &Observer{
		config: &Config{
			SubjectSelector:         []string{"mux"},
			FallbackSubjectSelector: []string{"wl"},
		},
		ohm: ohm,
		status: []*observatory.OutboundStatus{
			{OutboundTag: "wl-vk", Alive: true},
		},
	}
	if got := o.knownDeadFallbackTags(); len(got) != 0 {
		t.Fatalf("unprobed fallback tags must not be on the recovery tick, got %v", got)
	}
}

func TestPlanErrorFollowUp_MuxAllDeadSkipsFallbackWhenAlreadyInFallback(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.modeCtrl = &mockModeController{fallback: true}
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
		{OutboundTag: "wl-beget", Alive: true},
	}
	plan := o.planErrorFollowUp("bgspb-mux", false)
	if !plan.wake {
		t.Fatal("mux recovery should still wake the recovery loop")
	}
	if len(plan.probePrimary) != 2 {
		t.Fatalf("expected remaining mux tags, got %v", plan.probePrimary)
	}
	if len(plan.probeFallback) != 0 {
		t.Fatalf("already in fallback must not re-probe whitelist, got %v", plan.probeFallback)
	}
}

func TestPlanErrorFollowUp_MuxAllDeadSkipsFallbackWhenWhitelistAlreadyAlive(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
		{OutboundTag: "wl-beget", Alive: true},
	}
	plan := o.planErrorFollowUp("bgspb-mux", false)
	if len(plan.probeFallback) != 0 {
		t.Fatalf("confirmed-alive whitelist must not be probed again, got %v", plan.probeFallback)
	}
}

func TestIgnoreErrorsEmptyIgnoresNothing(t *testing.T) {
	o := &Observer{config: &Config{}}
	internal := http2.StreamError{StreamID: 1, Code: http2.ErrCodeInternal}
	abort := errors.New("write tcp 127.0.0.1:10808->127.0.0.1:1: wsasend: An established connection was aborted by the software in your host machine")
	if o.shouldIgnoreReportedError(internal) || o.shouldIgnoreReportedError(abort) {
		t.Fatal("empty ignoreErrors must not ignore probes")
	}
}

func TestIsHTTP2InternalError(t *testing.T) {
	if isHTTP2InternalError(nil) {
		t.Fatal("nil must not be treated as INTERNAL_ERROR")
	}
	if isHTTP2InternalError(errors.New("connectex: An attempt was made to access a socket in a way forbidden by its access permissions")) {
		t.Fatal("dial errors must still probe")
	}
	streamErr := http2.StreamError{StreamID: 1, Code: http2.ErrCodeInternal}
	if !isHTTP2InternalError(streamErr) {
		t.Fatal("typed StreamError INTERNAL_ERROR must be detected")
	}
	wrapped := errors.New("failed to process outbound traffic").Base(streamErr)
	if !isHTTP2InternalError(wrapped) {
		t.Fatal("wrapped StreamError INTERNAL_ERROR must be detected")
	}
	if isHTTP2InternalError(http2.StreamError{StreamID: 1, Code: http2.ErrCodeCancel}) {
		t.Fatal("CANCEL must not be treated as INTERNAL_ERROR")
	}
	if !isHTTP2InternalError(errors.New("stream error: stream ID 3; INTERNAL_ERROR; received from peer")) {
		t.Fatal("string-form H2 RST must be detected")
	}
}

func TestIsLocalAbortError(t *testing.T) {
	if isLocalAbortError(nil) {
		t.Fatal("nil must not be treated as local abort")
	}
	if isLocalAbortError(errors.New("i/o timeout")) {
		t.Fatal("timeout must not be treated as local abort")
	}
	abort := errors.New("write tcp 127.0.0.1:10808->127.0.0.1:1: wsasend: An established connection was aborted by the software in your host machine")
	if !isLocalAbortError(abort) {
		t.Fatal("wsasend abort must be detected")
	}
	if isLocalAbortError(syscall.Errno(10054)) {
		t.Fatal("connection reset must not be treated as local abort")
	}
	if !isLocalAbortError(wsaEConnAborted) {
		t.Fatal("WSAECONNABORTED errno must be detected")
	}
}

func TestShouldIgnoreReportedError(t *testing.T) {
	internal := http2.StreamError{StreamID: 1, Code: http2.ErrCodeInternal}
	abort := errors.New("wsasend: An established connection was aborted by the software in your host machine")
	timeout := errors.New("i/o timeout")

	o := &Observer{config: &Config{IgnoreErrors: []string{IgnoreErrorInternal}}}
	if !o.shouldIgnoreReportedError(internal) {
		t.Fatal("INTERNAL_ERROR must be ignored when listed")
	}
	if o.shouldIgnoreReportedError(abort) || o.shouldIgnoreReportedError(timeout) {
		t.Fatal("only listed kinds must be ignored")
	}

	o.config.IgnoreErrors = []string{IgnoreErrorWSASend}
	if !o.shouldIgnoreReportedError(abort) {
		t.Fatal("wsasend must be ignored when listed")
	}
	if o.shouldIgnoreReportedError(internal) {
		t.Fatal("INTERNAL_ERROR must still probe when not listed")
	}

	o.config.IgnoreErrors = []string{IgnoreErrorInternal, IgnoreErrorWSASend}
	if !o.shouldIgnoreReportedError(internal) || !o.shouldIgnoreReportedError(abort) {
		t.Fatal("both listed kinds must be ignored")
	}
}

func TestReportOutboundErrorIgnoresListedErrors(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.config.IgnoreErrors = []string{IgnoreErrorInternal, IgnoreErrorWSASend}
	o.ReportOutboundError("wl-beget", http2.StreamError{StreamID: 1, Code: http2.ErrCodeInternal})
	o.ReportOutboundError("wl-beget", errors.New("wsasend: An established connection was aborted by the software in your host machine"))
	if !o.lastErrorProbe.IsZero() {
		t.Fatal("listed ignoreErrors must not start a probe")
	}
}

func TestNormalizeIgnoreErrors(t *testing.T) {
	got, err := NormalizeIgnoreErrors([]string{"INTERNAL_ERROR", "localAbort", "wsasend"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != IgnoreErrorInternal || got[1] != IgnoreErrorWSASend {
		t.Fatalf("unexpected canonical ignoreErrors: %v", got)
	}
	if _, err := NormalizeIgnoreErrors([]string{"boom"}); err == nil {
		t.Fatal("unknown names must fail")
	}
}

func TestIsNoiseError(t *testing.T) {
	if isNoiseError(nil) || isNoiseError(io.EOF) {
		t.Fatal("nil and EOF must not be treated as noise")
	}
	if !isNoiseError(io.ErrClosedPipe) || !isNoiseError(context.Canceled) {
		t.Fatal("closed pipe and canceled must be noise")
	}
	wrapped := errors.New("failed to process outbound traffic").Base(io.ErrClosedPipe)
	if !isNoiseError(wrapped) {
		t.Fatal("wrapped closed pipe must be noise")
	}
	if isNoiseError(errors.New("connectex: An attempt was made to access a socket in a way forbidden by its access permissions")) {
		t.Fatal("connectex must be a hard failure")
	}
	loopbackReset := errors.New("read tcp 127.0.0.1:13735->127.0.0.1:54730: wsarecv: An existing connection was forcibly closed by the remote host.")
	if !isNoiseError(loopbackReset) {
		t.Fatal("local SOCKS reset must be noise")
	}
	remoteReset := errors.New("read tcp 192.168.1.2:50000->185.31.114.248:443: wsarecv: An existing connection was forcibly closed by the remote host.")
	if isNoiseError(remoteReset) {
		t.Fatal("reset from a public remote address must stay a hard failure")
	}
}

func TestIsOutboundSuccess(t *testing.T) {
	if !isOutboundSuccess(nil) || !isOutboundSuccess(io.EOF) {
		t.Fatal("nil and EOF must count as success")
	}
	if isOutboundSuccess(io.ErrClosedPipe) || isOutboundSuccess(context.Canceled) {
		t.Fatal("closed pipe and canceled must not count as success")
	}
}

func TestShouldStartErrorProbe_NoiseSkippedWhileInflight(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
		{OutboundTag: "wl-beget", Alive: true},
	}
	o.ReportOutboundSessionStart("wl-beget")
	o.ReportOutboundSessionStart("wl-beget")
	if o.inflightFor("wl-beget") != 2 {
		t.Fatalf("expected inflight 2, got %d", o.inflightFor("wl-beget"))
	}
	if o.shouldStartErrorProbe("wl-beget", io.ErrClosedPipe) {
		t.Fatal("closed-pipe must not probe while another session is in flight")
	}
}

func TestShouldStartErrorProbe_NoiseSkippedAfterRecentSuccess(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
		{OutboundTag: "wl-beget", Alive: true},
	}
	o.ReportOutboundSessionStart("wl-beget")
	o.ReportOutboundSessionEnd("wl-beget", io.EOF)
	o.ReportOutboundSessionStart("wl-beget")
	if o.shouldStartErrorProbe("wl-beget", io.ErrClosedPipe) {
		t.Fatal("closed-pipe must not probe while lastSuccess is inside the burst window")
	}
}

func TestShouldStartErrorProbe_IsolatedNoiseDoesNotProbe(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
		{OutboundTag: "wl-beget", Alive: true},
	}
	o.ReportOutboundSessionStart("wl-beget")
	if o.shouldStartErrorProbe("wl-beget", context.Canceled) {
		t.Fatal("a single canceled session must not probe")
	}
}

func TestShouldStartErrorProbe_NoiseBurstWithoutSuccessProbes(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
		{OutboundTag: "wl-beget", Alive: true},
	}
	for i := 0; i < errorBurstCount-1; i++ {
		o.ReportOutboundSessionStart("wl-beget")
		if o.shouldStartErrorProbe("wl-beget", io.ErrClosedPipe) {
			t.Fatalf("noise %d must not probe yet", i+1)
		}
		o.ReportOutboundSessionEnd("wl-beget", io.ErrClosedPipe)
	}
	o.ReportOutboundSessionStart("wl-beget")
	if !o.shouldStartErrorProbe("wl-beget", io.ErrClosedPipe) {
		t.Fatal("burst of closed-pipe with no success must probe")
	}
}

func TestShouldStartErrorProbe_LocalLoopbackResetSkippedWhileInflight(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
		{OutboundTag: "wl-beget", Alive: true},
	}
	o.ReportOutboundSessionStart("wl-beget")
	o.ReportOutboundSessionStart("wl-beget")
	loopbackReset := errors.New("read tcp 127.0.0.1:13735->127.0.0.1:54730: wsarecv: An existing connection was forcibly closed by the remote host.")
	if o.shouldStartErrorProbe("wl-beget", loopbackReset) {
		t.Fatal("local SOCKS reset must not probe while another session is in flight")
	}
}

func TestShouldStartErrorProbe_HardErrorProbesDespiteInflight(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
		{OutboundTag: "wl-beget", Alive: true},
	}
	o.ReportOutboundSessionStart("wl-beget")
	o.ReportOutboundSessionStart("wl-beget")
	hard := errors.New("connectex: An attempt was made to access a socket in a way forbidden by its access permissions")
	if !o.shouldStartErrorProbe("wl-beget", hard) {
		t.Fatal("hard dial failure must probe even when other sessions are in flight")
	}
}

func TestShouldStartErrorProbe_SiblingInflightDoesNotSkip(t *testing.T) {
	ohm := &mockOutboundManager{}
	ohm.bySelector = map[string][]string{
		"mux": {"bgspb-mux"},
		"wl":  {"wl-beget", "wl-vk"},
	}
	o := &Observer{
		config: &Config{
			SubjectSelector:         []string{"mux"},
			FallbackSubjectSelector: []string{"wl"},
		},
		ohm: ohm,
		status: []*observatory.OutboundStatus{
			{OutboundTag: "bgspb-mux", Alive: false},
			{OutboundTag: "wl-beget", Alive: true},
			{OutboundTag: "wl-vk", Alive: true},
		},
	}
	o.ReportOutboundSessionStart("wl-vk")
	o.ReportOutboundSessionEnd("wl-vk", nil)
	o.ReportOutboundSessionStart("wl-beget")
	hard := errors.New("i/o timeout")
	if !o.shouldStartErrorProbe("wl-beget", hard) {
		t.Fatal("sibling tag traffic must not suppress a hard error on this tag")
	}
	if o.shouldStartErrorProbe("wl-beget", io.ErrClosedPipe) {
		t.Fatal("isolated closed-pipe on wl-beget must still wait for a burst")
	}
}

func TestShouldStartErrorProbe_IgnoreErrorsStillWin(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.config.IgnoreErrors = []string{IgnoreErrorInternal, IgnoreErrorWSASend}
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: false},
		{OutboundTag: "twspb-mux", Alive: false},
		{OutboundTag: "wl-beget", Alive: true},
	}
	internal := http2.StreamError{StreamID: 1, Code: http2.ErrCodeInternal}
	abort := errors.New("wsasend: An established connection was aborted by the software in your host machine")
	if o.shouldStartErrorProbe("wl-beget", internal) || o.shouldStartErrorProbe("wl-beget", abort) {
		t.Fatal("listed ignoreErrors must not start a probe")
	}
}

func TestShouldStartErrorProbe_MuxFollowUpStillSkippedInFallback(t *testing.T) {
	o := newMuxObserverForFollowUp()
	o.modeCtrl = &mockModeController{fallback: true}
	o.status = []*observatory.OutboundStatus{
		{OutboundTag: "bgspb-mux", Alive: false},
		{OutboundTag: "aespb-mux", Alive: true},
		{OutboundTag: "twspb-mux", Alive: false},
	}
	hard := errors.New("connectex: forbidden")
	if o.shouldStartErrorProbe("bgspb-mux", hard) {
		t.Fatal("known-dead mux with a live sibling must not start an error probe")
	}
}

func TestStartupProbe_SkippedAfterClose(t *testing.T) {
	old := startupProbeDelay
	startupProbeDelay = 200 * time.Millisecond
	defer func() { startupProbeDelay = old }()

	ohm := &mockOutboundManager{
		mockHandlerSelector: mockHandlerSelector{
			bySelector: map[string][]string{
				"primary-":  {"primary-1"},
				"fallback-": {"fallback-1"},
			},
		},
	}
	o := &Observer{
		config: &Config{
			SubjectSelector:         []string{"primary-"},
			FallbackSubjectSelector: []string{"fallback-"},
		},
		ohm:      ohm,
		finished: done.New(),
		wake:     make(chan struct{}, 1),
	}
	if err := o.finished.Close(); err != nil {
		t.Fatal(err)
	}

	finished := make(chan struct{})
	go func() {
		o.startupProbe()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("startupProbe hung after Close")
	}

	if len(ohm.lastSelectors) != 0 {
		t.Fatalf("expected no outbound select after Close during startup delay, got %v", ohm.lastSelectors)
	}
	if o.isProbing() {
		t.Fatal("expected probing=false after Close skipped startup probe")
	}
}

func TestForgetStartupObservationIfPrimary_ClearsStatus(t *testing.T) {
	o := &Observer{
		modeCtrl: &mockModeController{fallback: false},
		status: []*observatory.OutboundStatus{
			{OutboundTag: "primary-1", Alive: true},
			{OutboundTag: "fallback-1", Alive: true},
		},
	}
	o.forgetStartupObservationIfPrimary()
	if len(o.status) != 0 {
		t.Fatalf("expected empty status in primary mode, got %v", o.status)
	}
}

func TestForgetStartupObservationIfPrimary_KeepsStatusInFallback(t *testing.T) {
	o := &Observer{
		modeCtrl: &mockModeController{fallback: true},
		status: []*observatory.OutboundStatus{
			{OutboundTag: "primary-1", Alive: false},
			{OutboundTag: "fallback-1", Alive: true},
		},
	}
	o.forgetStartupObservationIfPrimary()
	if len(o.status) != 2 {
		t.Fatalf("expected status retained in fallback mode, got %v", o.status)
	}
	if o.status[0].Alive || !o.status[1].Alive {
		t.Fatalf("expected primary dead and fallback alive retained, got %+v %+v", o.status[0], o.status[1])
	}
}

var _ routing.FallbackModeController = (*mockModeController)(nil)
