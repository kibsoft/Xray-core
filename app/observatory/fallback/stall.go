package fallback

import (
	"errors"
	xerrors "github.com/xtls/xray-core/common/errors"
	"time"
)

const trafficStallTimeout = 3 * time.Second
const healthyStallBackoff = 15 * time.Second
const maxHealthyStallBackoff = time.Minute

var errTrafficStalled = errors.New("outbound traffic stalled without a response")

// Watch only observed user traffic. Idle connections and internal probes do not
// create timers or pings.
func (o *Observer) WatchOutboundTraffic(tag string) bool {
	return o.probeOnErrorEnabled() && o.isObservedTag(tag)
}

func (o *Observer) ReportOutboundTraffic(tag string, downlink bool) {
	o.ReportOutboundTrafficSince(tag, downlink, time.Time{})
}

func (o *Observer) ReportOutboundTrafficSince(tag string, downlink bool, started time.Time) {
	if o.networkCheckInProgress() || (o.finished != nil && o.finished.Done()) {
		return
	}
	o.trafficMu.Lock()
	if o.closed.Load() {
		o.trafficMu.Unlock()
		return
	}
	if !started.IsZero() && started.Before(o.trafficResetAt) {
		o.trafficMu.Unlock()
		return
	}
	t := o.ensureTrafficLocked(tag)
	now := time.Now()
	if downlink {
		t.lastDownlink = now
		t.watchdogBackoff = 0
		t.pendingSince = time.Time{}
		if t.watchdog != nil {
			t.watchdog.Stop()
			t.watchdog = nil
		}
		o.trafficMu.Unlock()
		return
	}
	if !t.pendingSince.IsZero() {
		o.trafficMu.Unlock()
		return
	}
	t.pendingSince = now
	delay := trafficStallTimeout
	// A responsive node behind a silent destination needs no repeated pings.
	// Real downlink progress clears backoff for a later independent failure.
	if !t.healthyProbeAt.IsZero() && !t.lastDownlink.After(t.healthyProbeAt) {
		if remaining := time.Until(t.healthyProbeAt.Add(t.watchdogBackoff)); remaining > delay {
			delay = remaining
		}
	}
	pending := t.pendingSince
	t.watchdog = time.AfterFunc(delay, func() { o.checkStalledTraffic(tag, t, pending) })
	o.trafficMu.Unlock()
}

func (o *Observer) checkStalledTraffic(tag string, expected *tagTraffic, pending time.Time) {
	o.trafficMu.Lock()
	t := o.traffic[tag]
	stalled := t == expected && t.pendingSince.Equal(pending) && !t.lastDownlink.After(pending)
	if stalled {
		t.pendingSince = time.Time{}
		t.watchdog = nil
	}
	o.trafficMu.Unlock()
	if stalled && !o.networkCheckInProgress() && (o.finished == nil || !o.finished.Done()) {
		xerrors.LogInfo(o.ctx, "outbound traffic stalled: ", tag)
		o.ReportOutboundError(tag, errTrafficStalled)
	}
}

func (o *Observer) healthCoversError(tag string, started time.Time) bool {
	o.trafficMu.Lock()
	defer o.trafficMu.Unlock()
	t := o.traffic[tag]
	return !started.IsZero() && (started.Before(o.trafficResetAt) || (t != nil && t.healthCheckedAt.After(started)))
}

// Old transport/request failures cannot invalidate a newer completed check.
// New unanswered traffic is independently watched above.
func (o *Observer) ReportOutboundErrorSince(tag string, err error, started time.Time) {
	if !o.healthCoversError(tag, started) {
		o.ReportOutboundError(tag, err)
	}
}

func (o *Observer) noteHealthCheck(tag string, alive bool) {
	o.trafficMu.Lock()
	defer o.trafficMu.Unlock()
	t := o.ensureTrafficLocked(tag)
	t.healthCheckedAt = time.Now()
	t.pendingSince = time.Time{}
	if t.watchdog != nil {
		t.watchdog.Stop()
		t.watchdog = nil
	}
	if alive {
		t.healthyProbeAt = t.healthCheckedAt
		if t.watchdogBackoff == 0 {
			t.watchdogBackoff = healthyStallBackoff
		} else {
			t.watchdogBackoff = min(t.watchdogBackoff*2, maxHealthyStallBackoff)
		}
	}
}

func (o *Observer) stopTrafficWatchdogs() {
	o.trafficMu.Lock()
	defer o.trafficMu.Unlock()
	for _, t := range o.traffic {
		if t.watchdog != nil {
			t.watchdog.Stop()
		}
	}
	o.traffic = nil
	o.trafficResetAt = time.Now()
}
