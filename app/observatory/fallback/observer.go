package fallback

import (
	"context"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/common/utils"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport/internet/tagged"
	"google.golang.org/protobuf/proto"
)

type Observer struct {
	config *Config
	ctx    context.Context

	statusLock sync.Mutex
	status     []*observatory.OutboundStatus

	finished *done.Instance

	ohm        outbound.Manager
	dispatcher routing.Dispatcher
	modeCtrl   routing.FallbackModeController

	probeMu         sync.Mutex
	probing         bool
	lastErrorProbe  time.Time
	pendingErrorTag string
	wake            chan struct{}

	trafficMu sync.Mutex
	traffic   map[string]*tagTraffic

	// canary holds the latest direct network verdict (open / whitelist / ...).
	canary canaryCache

	dwellMu       sync.Mutex
	fallbackSince time.Time
	lastFullProbe time.Time
}

// deadPrimaryRecheckInterval is how often known-dead primaries are re-probed
// in primary mode, only while the canary reports an open network.
const deadPrimaryRecheckInterval = 60 * time.Second

// fullProbeMinGap throttles router-requested primary+fallback sweeps.
const fullProbeMinGap = 3 * time.Second

// speculativeSweepDelay: if the canary has not answered "open" by then, start
// probing siblings and whitelist nodes without waiting for its verdict. An
// open network answers within a few hundred ms, so this rarely costs pings.
const speculativeSweepDelay = 600 * time.Millisecond

// whitelistStatusMaxAge is the age after which a whitelist node status is
// refreshed when a whitelist network is detected.
const whitelistStatusMaxAge = 60 * time.Second

// deadFallbackRecheckInterval throttles re-pinging dead whitelist nodes.
const deadFallbackRecheckInterval = 30 * time.Second

// recoveryPrimarySample is how many random primaries are probed when leaving
// fallback is considered; one alive primary is enough.
const recoveryPrimarySample = 3

// defaultStartupProbeDelay waits for the network path (e.g. LTE after VPN up)
// before the cold-start primary+fallback health check.
const defaultStartupProbeDelay = time.Second

// probeSlotPollInterval is the wait between tryBeginProbe attempts when
// startupProbe finds the slot busy (error-driven sweep after Xray restart).
const probeSlotPollInterval = 100 * time.Millisecond

// startupProbeDelay is the wait before startupProbe runs. Tests may shorten it.
var startupProbeDelay = defaultStartupProbeDelay

func (o *Observer) ProbeIntervalDuration() time.Duration {
	if o.config != nil && o.config.RecoveryProbeInterval != 0 {
		return time.Duration(o.config.RecoveryProbeInterval)
	}
	if o.config != nil && o.config.ProbeInterval != 0 {
		return time.Duration(o.config.ProbeInterval)
	}
	return 15 * time.Second
}

func (o *Observer) idleProbeInterval() time.Duration {
	if o.config == nil || o.config.ProbeInterval == 0 {
		return 0
	}
	return time.Duration(o.config.ProbeInterval)
}

func (o *Observer) recoveryProbeInterval() time.Duration {
	if o.config != nil && o.config.RecoveryProbeInterval != 0 {
		return time.Duration(o.config.RecoveryProbeInterval)
	}
	return 15 * time.Second
}

func (o *Observer) errorProbeCooldown() time.Duration {
	if o.config != nil && o.config.ErrorProbeCooldown != 0 {
		return time.Duration(o.config.ErrorProbeCooldown)
	}
	return 3 * time.Second
}

func (o *Observer) probeOnErrorEnabled() bool {
	if o.config == nil {
		return true
	}
	return !o.config.DisableProbeOnError
}

func (o *Observer) GetObservation(ctx context.Context) (proto.Message, error) {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	return &observatory.ObservationResult{Status: o.status}, nil
}

func (o *Observer) Type() interface{} {
	return extension.ObservatoryType()
}

func (o *Observer) Start() error {
	if o.config != nil && len(o.config.SubjectSelector) != 0 {
		o.finished = done.New()
		if o.wake == nil {
			o.wake = make(chan struct{}, 1)
		}
		go o.background()
		go o.startupProbe()
	}
	return nil
}

func (o *Observer) Close() error {
	if o.finished != nil {
		return o.finished.Close()
	}
	return nil
}

// startupProbe runs once after Start so cold-start can enter fallback without
// waiting for outbound dial retries to exhaust (~5x16s). If an error-driven
// probe already holds the slot (TUN still up after Xray restart), wait for it
// instead of skipping. Status is cleared only after this probe actually ran.
func (o *Observer) startupProbe() {
	if o.finished == nil {
		return
	}
	timer := time.NewTimer(startupProbeDelay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-o.finished.Wait():
		return
	}
	if o.finished.Done() {
		return
	}
	if !o.waitBeginProbe() {
		return
	}
	o.probeOutbounds(o.tagsToProbeForNetwork())
	o.syncFallbackMode()
	o.signalWake()
	o.forgetStartupObservationIfPrimary()
	o.endProbe()
}

// tagsToProbeForNetwork picks the outbounds a full check should ping. Without
// the canary this is primary+fallback. With it, whitelist FALLBACK nodes are
// pinged only when the network looks like a whitelist, and nothing is pinged
// while offline (dead marks would be stale once the network returns).
func (o *Observer) tagsToProbeForNetwork() []string {
	if !o.canaryEnabled() {
		return o.primaryAndFallbackTags()
	}
	switch o.CheckNetwork() {
	case NetworkWhitelist:
		return o.primaryAndFallbackTags()
	case NetworkOffline:
		return nil
	default:
		return o.primaryOutboundTags()
	}
}

func (o *Observer) noteFallbackDwell() {
	inFallback := o.inFallbackMode()
	o.dwellMu.Lock()
	defer o.dwellMu.Unlock()
	if !inFallback {
		o.fallbackSince = time.Time{}
	} else if o.fallbackSince.IsZero() {
		o.fallbackSince = time.Now()
	}
}

// dwellElapsed reports whether fallback mode has lasted long enough to
// consider leaving it.
func (o *Observer) dwellElapsed() bool {
	o.noteFallbackDwell()
	o.dwellMu.Lock()
	defer o.dwellMu.Unlock()
	return !o.fallbackSince.IsZero() && time.Since(o.fallbackSince) >= o.minFallbackDwell()
}

func (o *Observer) knownDeadPrimaryTags() []string {
	var dead []string
	for _, tag := range o.primaryOutboundTags() {
		if o.isKnownDead(tag) {
			dead = append(dead, tag)
		}
	}
	return dead
}

// randomSample returns up to n distinct random elements of tags.
func randomSample(tags []string, n int) []string {
	out := append([]string{}, tags...)
	rand.Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	if len(out) > n {
		out = out[:n]
	}
	return out
}

// forgetStartupObservationIfPrimary drops probe results after a cold-start
// decide when staying in primary mode, so later error follow-up treats
// siblings as unprobed again (Wi-Fi→LTE sweep). Status is kept in fallback
// mode so recovery does not treat unprobed primary as alive.
func (o *Observer) forgetStartupObservationIfPrimary() {
	if o.inFallbackMode() {
		return
	}
	o.ClearObservationStatus()
}

// ClearObservationStatus drops all probe results so outbounds are unprobed
// again. Used after cold-start primary decide and when leaving fallback mode.
func (o *Observer) ClearObservationStatus() {
	// With the canary enabled, whitelist node health is kept: dropping it makes
	// the balancer send traffic to dead wl nodes on the next fallback entry.
	// Stale entries are refreshed by age instead (see fallbackTagsToProbe).
	var keep []string
	if o.canaryEnabled() {
		keep = o.fallbackOutboundTags()
	}
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	var kept []*observatory.OutboundStatus
	for _, status := range o.status {
		if slices.Contains(keep, status.OutboundTag) {
			kept = append(kept, status)
		}
	}
	o.status = kept
}

// fallbackTagsToProbe returns whitelist outbounds that have never been probed,
// or whose last probe is older than maxAge. With deadOnly only dead ones are
// considered stale (alive nodes are watched by real traffic errors).
func (o *Observer) fallbackTagsToProbe(maxAge time.Duration, deadOnly bool) []string {
	tags := o.fallbackOutboundTags()
	if len(tags) == 0 {
		return nil
	}
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	now := time.Now().Unix()
	var out []string
	for _, tag := range tags {
		loc := o.findStatusLocationLockHolderOnly(tag)
		if loc == -1 {
			out = append(out, tag)
			continue
		}
		st := o.status[loc]
		if deadOnly && st.Alive {
			continue
		}
		if now-st.LastTryTime >= int64(maxAge/time.Second) {
			out = append(out, tag)
		}
	}
	return out
}

func (o *Observer) ProbeFallback() {
	outbounds := o.fallbackOutboundTags()
	if len(outbounds) == 0 {
		return
	}
	o.probeOutbounds(outbounds)
	o.syncFallbackMode()
}

func (o *Observer) ProbeNow(tags []string) {
	if len(tags) == 0 {
		return
	}
	o.probeOutbounds(tags)
	o.syncFallbackMode()
}

func (o *Observer) ProbePrimaryAndFallback() {
	if !o.tryBeginProbe() {
		return
	}
	defer o.endProbe()
	o.dwellMu.Lock()
	tooSoon := time.Since(o.lastFullProbe) < fullProbeMinGap
	if !tooSoon {
		o.lastFullProbe = time.Now()
	}
	o.dwellMu.Unlock()
	if tooSoon {
		return
	}
	o.probeOutbounds(o.tagsToProbeForNetwork())
	o.syncFallbackMode()
	o.signalWake()
}

func (o *Observer) shouldIgnoreReportedError(err error) bool {
	if o.config == nil || err == nil || len(o.config.IgnoreErrors) == 0 {
		return false
	}
	for _, kind := range o.config.IgnoreErrors {
		if errorMatchesIgnoreKind(kind, err) {
			return true
		}
	}
	return false
}

func (o *Observer) ReportOutboundError(tag string, err error) {
	if !o.shouldStartErrorProbe(tag, err) {
		return
	}
	go o.probeAfterOutboundError(tag)
}

// shouldStartErrorProbe applies ignoreErrors, then treats hard failures
// (dial/timeout/refused) as immediate probes and closed-pipe / canceled as
// noise gated by per-tag inflight and a short error burst.
func (o *Observer) shouldStartErrorProbe(tag string, err error) bool {
	if !o.probeOnErrorEnabled() {
		return false
	}
	if o.shouldIgnoreReportedError(err) {
		return false
	}
	if !o.isObservedTag(tag) {
		return false
	}
	if isNoiseError(err) && o.shouldSkipNoiseFollowUp(tag) {
		return false
	}
	if o.shouldSkipErrorFollowUp(tag) {
		return false
	}
	if o.notePendingIfProbing(tag) {
		return false
	}
	return true
}

func (o *Observer) probeAfterOutboundError(tag string) {
	deadline := time.Now().Add(8 * time.Second)
	for {
		if o.tryStartErrorProbe() {
			o.runErrorProbe(tag)
			return
		}
		if o.finished != nil && o.finished.Done() {
			return
		}
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// runPrimaryErrorProbeWithCanary handles an error on a primary node when the
// network canary is enabled. The canary runs in parallel with the probe:
//   - open network: the node gets one retry; if still dead it is just marked
//     dead (sticky fails over), with no sibling sweep and no whitelist pings;
//   - whitelist signature: siblings and whitelist nodes are probed so fallback
//     mode can be entered;
//   - offline: nothing is marked, a dead mark would be stale on return.
func (o *Observer) runPrimaryErrorProbeWithCanary(tag string) {
	stateCh := make(chan NetworkState, 1)
	go func() { stateCh <- o.CheckNetwork() }()
	resultCh := make(chan observatory.ProbeResult, 1)
	go func() { resultCh <- o.probe(tag) }()

	// Siblings and stale/unknown whitelist nodes are swept in parallel with
	// the canary once the network looks like a whitelist (or the canary is
	// suspiciously slow), so fallback entry does not wait canary + sweep.
	var sweepDone chan struct{}
	startSweep := func() {
		if sweepDone != nil {
			return
		}
		sweepDone = make(chan struct{})
		tags := uniqueTags(append(o.primaryOutboundTagsExcept(tag), o.fallbackTagsToProbe(whitelistStatusMaxAge, false)...))
		go func() {
			o.probeOutbounds(tags)
			close(sweepDone)
		}()
	}
	waitSweep := func() {
		if sweepDone == nil {
			return
		}
		if o.finished == nil {
			<-sweepDone
			return
		}
		select {
		case <-sweepDone:
		case <-o.finished.Wait():
		}
	}

	var state NetworkState
	timer := time.NewTimer(speculativeSweepDelay)
	select {
	case state = <-stateCh:
		timer.Stop()
		if state == NetworkWhitelist {
			startSweep()
		}
	case <-timer.C:
		startSweep()
		state = <-stateCh
	}
	result := <-resultCh
	if !result.Alive && state == NetworkOpen && o.sleepOrFinished(time.Second) {
		result = o.probe(tag)
	}
	waitSweep()
	if state == NetworkOffline {
		return
	}
	o.updateStatusForResult(tag, &result)
	o.syncFallbackMode()
	o.signalWake()
}

func (o *Observer) runErrorProbe(tag string) {
	defer o.endProbe()
	if o.canaryEnabled() && !o.isFallbackSubject(tag) {
		o.runPrimaryErrorProbeWithCanary(tag)
		return
	}
	result := o.probe(tag)
	o.updateStatusForResult(tag, &result)
	plan := o.planErrorFollowUp(tag, result.Alive)
	if len(plan.probePrimary) > 0 {
		o.probeOutbounds(plan.probePrimary)
	}
	if len(plan.probeFallback) > 0 {
		o.probeOutbounds(plan.probeFallback)
	}
	o.syncFallbackMode()
	if plan.wake {
		o.signalWake()
	}
}

// planErrorFollowUp decides extra probes after an error-driven check of tag.
// Fallback-subject errors never wake mux recovery: leftover XHTTP noise must
// not re-sweep primaries. Mux recovery stays on recoveryProbeInterval and
// does not re-ping whitelist once fallback is already usable.
func (o *Observer) planErrorFollowUp(tag string, alive bool) errorFollowUpPlan {
	if o.isFallbackSubject(tag) {
		plan := errorFollowUpPlan{}
		if !alive {
			plan.probeFallback = tagsExcept(o.fallbackOutboundTags(), tag)
		}
		return plan
	}
	plan := errorFollowUpPlan{wake: true}
	if !alive {
		// Always re-check siblings: stale Wi-Fi Alive must not block a later
		// Wi-Fi→LTE sweep. One dead node on a healthy network costs an extra
		// generate_204 round — acceptable for reliable network-switch failover.
		plan.probePrimary = o.primaryOutboundTagsExcept(tag)
		// Ping whitelist only when we still need it to decide whether to
		// enter fallback. Already in fallback, or a fallback node already
		// confirmed alive: mux recovery must not re-probe generate_204.
		if o.needsFallbackHealthCheck() {
			plan.probeFallback = o.fallbackOutboundTags()
		}
	}
	return plan
}

type errorFollowUpPlan struct {
	probePrimary  []string
	probeFallback []string
	wake          bool
}

func (o *Observer) fallbackOutboundTags() []string {
	if o.config == nil || len(o.config.FallbackSubjectSelector) == 0 {
		return nil
	}
	hs, ok := o.ohm.(outbound.HandlerSelector)
	if !ok {
		return nil
	}
	return hs.Select(o.config.FallbackSubjectSelector)
}

func (o *Observer) activeSelectors() []string {
	if o.config == nil {
		return nil
	}
	sels := append([]string{}, o.config.SubjectSelector...)
	if o.modeCtrl != nil && o.modeCtrl.IsFallbackMode() {
		sels = append(sels, o.config.FallbackSubjectSelector...)
	}
	return sels
}

func (o *Observer) primaryOutboundTags() []string {
	if o.config == nil || len(o.config.SubjectSelector) == 0 {
		return nil
	}
	hs, ok := o.ohm.(outbound.HandlerSelector)
	if !ok {
		return nil
	}
	return hs.Select(o.config.SubjectSelector)
}

func (o *Observer) primaryAndFallbackTags() []string {
	return uniqueTags(append(o.primaryOutboundTags(), o.fallbackOutboundTags()...))
}

func (o *Observer) primaryOutboundTagsExcept(except string) []string {
	return tagsExcept(o.primaryOutboundTags(), except)
}

func tagsExcept(tags []string, except string) []string {
	filtered := make([]string, 0, len(tags))
	for _, tag := range tags {
		if tag != except {
			filtered = append(filtered, tag)
		}
	}
	return filtered
}

func uniqueTags(tags []string) []string {
	seen := make(map[string]struct{}, len(tags))
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		if tag == "" {
			continue
		}
		if _, ok := seen[tag]; ok {
			continue
		}
		seen[tag] = struct{}{}
		out = append(out, tag)
	}
	return out
}

func (o *Observer) isObservedTag(tag string) bool {
	for _, candidate := range o.primaryAndFallbackTags() {
		if candidate == tag {
			return true
		}
	}
	return false
}

func (o *Observer) isFallbackSubject(tag string) bool {
	for _, candidate := range o.fallbackOutboundTags() {
		if candidate == tag {
			return true
		}
	}
	return false
}

func (o *Observer) stickyTag() string {
	if o.modeCtrl == nil {
		return ""
	}
	if provider, ok := o.modeCtrl.(interface{ CurrentStickyTag() string }); ok {
		return provider.CurrentStickyTag()
	}
	return ""
}

func (o *Observer) syncFallbackMode() {
	if o.modeCtrl == nil {
		return
	}
	if syncer, ok := o.modeCtrl.(interface{ SyncFallbackMode() }); ok {
		syncer.SyncFallbackMode()
	}
	o.noteFallbackDwell()
}

func (o *Observer) shouldSkipErrorFollowUp(tag string) bool {
	// In fallback mode no user traffic uses primaries; their stray errors must
	// not trigger sweeps against dead servers.
	if o.canaryEnabled() && o.inFallbackMode() && !o.isFallbackSubject(tag) {
		return true
	}
	if o.isFallbackSubject(tag) {
		// Ignore leftover whitelist errors after a primary has recovered.
		return o.hasConfirmedAlivePrimaryExcept("")
	}
	return o.isKnownDead(tag) && o.hasConfirmedAlivePrimaryExcept(tag)
}

func (o *Observer) isProbing() bool {
	o.probeMu.Lock()
	defer o.probeMu.Unlock()
	return o.probing
}

func (o *Observer) isKnownDead(tag string) bool {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	for _, status := range o.status {
		if status.OutboundTag == tag {
			return !status.Alive
		}
	}
	return false
}

func (o *Observer) hasConfirmedAlivePrimaryExcept(except string) bool {
	for _, tag := range o.primaryOutboundTags() {
		if tag == except {
			continue
		}
		if o.isKnownAlive(tag) {
			return true
		}
	}
	return false
}

func (o *Observer) hasConfirmedAliveFallback() bool {
	for _, tag := range o.fallbackOutboundTags() {
		if o.isKnownAlive(tag) {
			return true
		}
	}
	return false
}

// knownDeadFallbackTags returns fallback outbounds that observatory has
// already marked dead. Unprobed tags are omitted so first-enter health
// checks stay on the error/enter path, not on every recovery tick.
func (o *Observer) knownDeadFallbackTags() []string {
	var dead []string
	for _, tag := range o.fallbackOutboundTags() {
		if o.isKnownDead(tag) {
			dead = append(dead, tag)
		}
	}
	return dead
}

func (o *Observer) inFallbackMode() bool {
	return o.modeCtrl != nil && o.modeCtrl.IsFallbackMode()
}

// needsFallbackHealthCheck is true only when whitelist health is still
// unknown and routing has not entered fallback. Re-checking wl while
// already on it does not help mux recovery.
func (o *Observer) needsFallbackHealthCheck() bool {
	if o.inFallbackMode() {
		return false
	}
	return !o.hasConfirmedAliveFallback()
}

func (o *Observer) isKnownAlive(tag string) bool {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	for _, status := range o.status {
		if status.OutboundTag == tag {
			return status.Alive
		}
	}
	return false
}

func (o *Observer) tryBeginProbe() bool {
	o.probeMu.Lock()
	defer o.probeMu.Unlock()
	if o.probing {
		return false
	}
	o.probing = true
	return true
}

// waitBeginProbe blocks until tryBeginProbe succeeds or the observer is closed.
func (o *Observer) waitBeginProbe() bool {
	for {
		if o.tryBeginProbe() {
			return true
		}
		if o.finished != nil && o.finished.Done() {
			return false
		}
		if o.finished == nil {
			time.Sleep(probeSlotPollInterval)
			continue
		}
		timer := time.NewTimer(probeSlotPollInterval)
		select {
		case <-timer.C:
			timer.Stop()
		case <-o.finished.Wait():
			timer.Stop()
			return false
		}
	}
}

func (o *Observer) notePendingIfProbing(tag string) bool {
	o.probeMu.Lock()
	defer o.probeMu.Unlock()
	if !o.probing {
		return false
	}
	o.pendingErrorTag = tag
	return true
}

func (o *Observer) endProbe() {
	o.probeMu.Lock()
	o.probing = false
	pending := o.pendingErrorTag
	o.pendingErrorTag = ""
	o.probeMu.Unlock()
	if pending != "" {
		go o.probeAfterOutboundError(pending)
	}
}

func (o *Observer) tryStartErrorProbe() bool {
	o.probeMu.Lock()
	defer o.probeMu.Unlock()
	if o.probing {
		return false
	}
	now := time.Now()
	if !o.lastErrorProbe.IsZero() && now.Sub(o.lastErrorProbe) < o.errorProbeCooldown() {
		return false
	}
	o.probing = true
	o.lastErrorProbe = now
	return true
}

func (o *Observer) signalWake() {
	if o.wake == nil {
		return
	}
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

func (o *Observer) waitNext(interval time.Duration) bool {
	if o.finished != nil && o.finished.Done() {
		return false
	}
	if interval <= 0 {
		select {
		case <-o.wake:
			return true
		case <-o.finished.Wait():
			return false
		}
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-o.wake:
		return true
	case <-o.finished.Wait():
		return false
	}
}

func (o *Observer) background() {
	for o.finished == nil || !o.finished.Done() {
		hs, ok := o.ohm.(outbound.HandlerSelector)
		if !ok {
			errors.LogInfo(o.ctx, "outbound.Manager is not a HandlerSelector")
			return
		}

		known := o.primaryAndFallbackTags()
		if len(known) == 0 {
			known = hs.Select(o.activeSelectors())
		}
		o.clearRemovedOutbounds(known)

		inFallback := o.modeCtrl != nil && o.modeCtrl.IsFallbackMode()
		if inFallback && o.canaryEnabled() {
			next := o.fallbackCanaryTick()
			if !o.waitNext(next) {
				return
			}
			continue
		}
		if inFallback {
			if o.tryBeginProbe() {
				// Mux recovery plus known-dead whitelist tags only. Alive
				// fallback nodes stay off the timer so generate_204 is not
				// sent while they are already carrying traffic.
				o.probeOutbounds(append(o.primaryOutboundTags(), o.knownDeadFallbackTags()...))
				o.endProbe()
				o.syncFallbackMode()
			}
			if !o.waitNext(o.recoveryProbeInterval()) {
				return
			}
			continue
		}

		if o.canaryEnabled() && len(o.knownDeadPrimaryTags()) > 0 {
			started := time.Now()
			if !o.waitNext(deadPrimaryRecheckInterval) {
				return
			}
			// A wake-up (error probe finished) must not trigger an immediate
			// re-ping of nodes that were just marked dead.
			if time.Since(started) >= deadPrimaryRecheckInterval-time.Second {
				o.recheckDeadPrimaries()
			}
			continue
		}

		idle := o.idleProbeInterval()
		if idle > 0 {
			if tag := o.stickyTag(); tag != "" {
				if o.tryBeginProbe() {
					result := o.probe(tag)
					o.updateStatusForResult(tag, &result)
					o.endProbe()
					o.syncFallbackMode()
				}
			}
			if !o.waitNext(idle) {
				return
			}
			continue
		}

		if !o.waitNext(0) {
			return
		}
	}
}

// recheckDeadPrimaries revives primaries marked dead earlier, but only while
// the network is open, so a whitelist or offline period never produces pings.
func (o *Observer) recheckDeadPrimaries() {
	if !o.tryBeginProbe() {
		return
	}
	defer o.endProbe()
	if o.inFallbackMode() || o.CheckNetwork() != NetworkOpen {
		return
	}
	if dead := o.knownDeadPrimaryTags(); len(dead) > 0 {
		o.probeOutbounds(dead)
		o.syncFallbackMode()
	}
}

// fallbackCanaryTick is one iteration of the fallback-mode loop when the
// canary is enabled. Servers are not pinged while the network still looks like
// a whitelist: only the free direct canary runs. Once foreign targets are
// reachable again (and the minimum dwell has passed) a few random primaries are
// probed so the router can leave fallback. It returns the wait before the next
// tick.
func (o *Observer) fallbackCanaryTick() time.Duration {
	next := o.canaryPollInterval()
	state := o.CheckNetwork()
	if !o.tryBeginProbe() {
		return next
	}
	defer o.endProbe()
	if state == NetworkOpen {
		if !o.dwellElapsed() {
			return next
		}
		o.probeOutbounds(randomSample(o.primaryOutboundTags(), recoveryPrimarySample))
		o.syncFallbackMode()
		if o.inFallbackMode() {
			// Network is open but primaries still look dead: back off.
			next = o.recoveryProbeInterval()
		}
		return next
	}
	// Unknown whitelist nodes are probed at once; dead ones at most every 30s.
	unknown := o.fallbackTagsToProbe(time.Duration(1<<62-1), false)
	dead := o.fallbackTagsToProbe(deadFallbackRecheckInterval, true)
	if tags := uniqueTags(append(unknown, dead...)); len(tags) > 0 {
		o.probeOutbounds(tags)
		o.syncFallbackMode()
	}
	return next
}

func (o *Observer) probeOutbounds(outbounds []string) {
	ch := make(chan struct{}, len(outbounds))
	for _, v := range outbounds {
		go func(v string) {
			result := o.probe(v)
			o.updateStatusForResult(v, &result)
			ch <- struct{}{}
		}(v)
	}
	for range outbounds {
		if o.finished == nil {
			<-ch
			continue
		}
		select {
		case <-ch:
		case <-o.finished.Wait():
			return
		}
	}
}

func (o *Observer) clearRemovedOutbounds(outbounds []string) {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	if len(o.status) == 0 {
		return
	}
	var pruned []*observatory.OutboundStatus
	for _, status := range o.status {
		if slices.Contains(outbounds, status.OutboundTag) {
			pruned = append(pruned, status)
		}
	}
	o.status = pruned
}

func (o *Observer) probe(outboundTag string) observatory.ProbeResult {
	errorCollectorForRequest := newErrorCollector()

	httpTransport := http.Transport{
		Proxy: func(*http.Request) (*url.URL, error) {
			return nil, nil
		},
		DialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			var connection net.Conn
			taskErr := task.Run(ctx, func() error {
				dest, err := v2net.ParseDestination(network + ":" + addr)
				if err != nil {
					return errors.New("cannot understand address").Base(err)
				}
				trackedCtx := session.TrackedConnectionError(o.ctx, errorCollectorForRequest)
				trackedCtx = session.ContextWithSkipOutboundErrorReport(trackedCtx)
				conn, err := tagged.Dialer(trackedCtx, o.dispatcher, dest, outboundTag)
				if err != nil {
					return errors.New("cannot dial remote address ", dest).Base(err)
				}
				connection = conn
				return nil
			})
			if taskErr != nil {
				return nil, errors.New("cannot finish connection").Base(taskErr)
			}
			return connection, nil
		},
		TLSHandshakeTimeout: time.Second * 5,
	}
	httpClient := &http.Client{
		Transport: &httpTransport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Jar:     nil,
		Timeout: time.Second * 5,
	}
	var GETTime time.Duration
	err := task.Run(o.ctx, func() error {
		startTime := time.Now()
		probeURL := "https://www.google.com/generate_204"
		if o.config.ProbeUrl != "" {
			probeURL = o.config.ProbeUrl
		}
		req, _ := http.NewRequest(http.MethodGet, probeURL, nil)
		utils.TryDefaultHeadersWith(req.Header, "nav")
		response, err := httpClient.Do(req)
		if err != nil {
			return errors.New("outbound failed to relay connection").Base(err)
		}
		if response.Body != nil {
			response.Body.Close()
		}
		endTime := time.Now()
		GETTime = endTime.Sub(startTime)
		return nil
	})
	if err != nil {
		errorMessage := "the outbound " + outboundTag + " is dead: GET request failed:" + err.Error() + "with outbound handler report underlying connection failed"
		errors.LogInfoInner(o.ctx, errorCollectorForRequest.UnderlyingError(), errorMessage)
		return observatory.ProbeResult{Alive: false, LastErrorReason: errorMessage}
	}
	errors.LogInfo(o.ctx, "the outbound ", outboundTag, " is alive:", GETTime.Seconds())
	return observatory.ProbeResult{Alive: true, Delay: GETTime.Milliseconds()}
}

func (o *Observer) updateStatusForResult(outboundTag string, result *observatory.ProbeResult) {
	o.statusLock.Lock()
	defer o.statusLock.Unlock()
	var status *observatory.OutboundStatus
	if location := o.findStatusLocationLockHolderOnly(outboundTag); location != -1 {
		status = o.status[location]
	} else {
		status = &observatory.OutboundStatus{}
		o.status = append(o.status, status)
	}

	status.LastTryTime = time.Now().Unix()
	status.OutboundTag = outboundTag
	status.Alive = result.Alive
	if result.Alive {
		status.Delay = result.Delay
		status.LastSeenTime = status.LastTryTime
		status.LastErrorReason = ""
	} else {
		status.LastErrorReason = result.LastErrorReason
		status.Delay = 99999999
	}
}

func (o *Observer) findStatusLocationLockHolderOnly(outboundTag string) int {
	for i, v := range o.status {
		if v.OutboundTag == outboundTag {
			return i
		}
	}
	return -1
}

func New(ctx context.Context, config *Config) (*Observer, error) {
	var outboundManager outbound.Manager
	var dispatcher routing.Dispatcher
	err := core.RequireFeatures(ctx, func(om outbound.Manager, rd routing.Dispatcher) {
		outboundManager = om
		dispatcher = rd
	})
	if err != nil {
		return nil, errors.New("Cannot get depended features").Base(err)
	}
	observer := &Observer{
		config:     config,
		ctx:        ctx,
		ohm:        outboundManager,
		dispatcher: dispatcher,
		wake:       make(chan struct{}, 1),
		traffic:    make(map[string]*tagTraffic),
	}
	core.OptionalFeatures(ctx, func(router routing.Router) {
		if ctrl, ok := router.(routing.FallbackModeController); ok {
			observer.modeCtrl = ctrl
		}
	})
	return observer, nil
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		return New(ctx, config.(*Config))
	}))
}

var _ extension.OutboundErrorObserver = (*Observer)(nil)

type errorCollector struct {
	errors *errors.Error
}

func (e *errorCollector) SubmitError(err error) {
	if e.errors == nil {
		e.errors = errors.New("underlying connection error").Base(err)
		return
	}
	e.errors = e.errors.Base(errors.New("underlying connection error").Base(err))
}

func newErrorCollector() *errorCollector {
	return &errorCollector{}
}

func (e *errorCollector) UnderlyingError() error {
	if e.errors == nil {
		return errors.New("failed to produce report")
	}
	return e.errors
}
