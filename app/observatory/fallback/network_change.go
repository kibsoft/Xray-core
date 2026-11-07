package fallback

import (
	"context"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
)

// networkContext ties health checks to the physical network they started on.
func (o *Observer) networkContext() (context.Context, uint64) {
	o.networkMu.Lock()
	defer o.networkMu.Unlock()
	ctx := o.networkCtx
	if ctx == nil {
		ctx = o.ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return ctx, o.networkEpoch.Load()
}

// NotifyNetworkChanged starts a fresh check immediately, without waiting for
// traffic errors, polling, or a probe still waiting on the old network.
func (o *Observer) NotifyNetworkChanged() {
	if o.finished != nil && o.finished.Done() {
		return
	}
	o.networkMu.Lock()
	if o.networkCancel != nil {
		o.networkCancel()
	}
	parent := o.ctx
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	o.networkCtx, o.networkCancel = ctx, cancel
	epoch := o.networkEpoch.Add(1)
	o.recoverPrimaries.Store(true)
	o.ClearObservationStatus()
	o.networkMu.Unlock()
	o.stopTrafficWatchdogs()
	go o.checkNetworkChange(ctx, epoch)
}

func (o *Observer) checkNetworkChange(ctx context.Context, epoch uint64) {
	started := time.Now()
	var probes sync.WaitGroup
	startProbes := func(tags []string) {
		for _, tag := range tags {
			probes.Add(1)
			go func(tag string) {
				defer probes.Done()
				if ctx.Err() != nil || epoch != o.networkEpoch.Load() {
					return
				}
				result := o.probe(tag)
				if ctx.Err() != nil || epoch != o.networkEpoch.Load() {
					return
				}
				o.updateStatusForResult(tag, &result)
				o.syncFallbackMode()
			}(tag)
		}
	}
	// Proxy probes overlap the two canary rounds. Do not wait 3.5s + 5s.
	startProbes(o.primaryOutboundTags())
	verdict := make(chan NetworkState, 1)
	go func() { verdict <- o.CheckNetwork() }()
	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()
	var state NetworkState
	select {
	case state = <-verdict:
		if !o.canaryEnabled() || state == NetworkWhitelist {
			startProbes(o.fallbackOutboundTags())
		}
	case <-timer.C:
		startProbes(o.fallbackOutboundTags())
		select {
		case state = <-verdict:
		case <-ctx.Done():
			return
		}
	case <-ctx.Done():
		return
	}
	if ctx.Err() != nil || epoch != o.networkEpoch.Load() {
		return
	}
	o.syncFallbackMode()
	done := make(chan struct{})
	go func() { probes.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		return
	}
	if epoch != o.networkEpoch.Load() {
		return
	}
	if state == NetworkOpen {
		o.recoverPrimaryPool()
	}
	o.networkReadyEpoch.Store(epoch)
	o.syncFallbackMode()
	o.signalWake()
	errors.LogInfo(o.ctx, "network change checked: state=", state.String(),
		" fallback=", o.inFallbackMode(), " elapsed=", time.Since(started))
}

func (o *Observer) NetworkChangeReady() bool {
	if o.networkCheckInProgress() {
		return false
	}
	if !o.canaryEnabled() {
		return true
	}
	state := o.NetworkRoutingState()
	return state == "open" || state == "whitelist"
}

func (o *Observer) networkCheckInProgress() bool {
	return o.networkReadyEpoch.Load() != o.networkEpoch.Load()
}
