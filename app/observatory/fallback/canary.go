package fallback

import (
	"context"
	cryptotls "crypto/tls"
	"net"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/errors"
	v2net "github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/transport/internet/tagged"
)

// NetworkState is the result of the network canary. It tells an open network
// from a whitelist (WL) network without pinging any proxy server.
type NetworkState int32

const (
	// NetworkUnknown means the canary is disabled or has not run yet.
	NetworkUnknown NetworkState = iota
	// NetworkOpen means a foreign target is reachable directly: no whitelist.
	NetworkOpen
	// NetworkWhitelist means foreign targets are unreachable but domestic ones
	// are: the typical mobile whitelist signature.
	NetworkWhitelist
	// NetworkOffline means nothing is reachable directly.
	NetworkOffline
)

func (s NetworkState) String() string {
	switch s {
	case NetworkOpen:
		return "open"
	case NetworkWhitelist:
		return "whitelist"
	case NetworkOffline:
		return "offline"
	}
	return "unknown"
}

const (
	defaultCanaryTimeout       = 2500 * time.Millisecond
	defaultCanaryConfirmRounds = 2
	defaultCanaryPollInterval  = 8 * time.Second
	defaultMinFallbackDwell    = 20 * time.Second
	// canaryReuseWindow lets concurrent callers share one fresh result.
	canaryReuseWindow = 2 * time.Second
	// whitelistEntryTTL bounds how long a whitelist verdict allows entering
	// fallback mode.
	whitelistEntryTTL = 60 * time.Second
	// canaryRoundGap separates confirmation rounds.
	canaryRoundGap = 500 * time.Millisecond
)

func (o *Observer) canaryEnabled() bool {
	return o.config != nil && len(o.config.CanaryForeign) > 0 && len(o.config.CanaryDomestic) > 0
}

func (o *Observer) canaryTimeout() time.Duration {
	if o.config != nil && o.config.CanaryTimeout != 0 {
		return time.Duration(o.config.CanaryTimeout)
	}
	return defaultCanaryTimeout
}

func (o *Observer) canaryConfirmRounds() int {
	if o.config != nil && o.config.CanaryConfirmRounds > 0 {
		return int(o.config.CanaryConfirmRounds)
	}
	return defaultCanaryConfirmRounds
}

func (o *Observer) canaryPollInterval() time.Duration {
	if o.config != nil && o.config.CanaryPollInterval != 0 {
		return time.Duration(o.config.CanaryPollInterval)
	}
	return defaultCanaryPollInterval
}

func (o *Observer) minFallbackDwell() time.Duration {
	if o.config != nil && o.config.MinFallbackDwell != 0 {
		return time.Duration(o.config.MinFallbackDwell)
	}
	return defaultMinFallbackDwell
}

func (o *Observer) canaryOutboundTag() string {
	if o.config != nil && o.config.CanaryOutboundTag != "" {
		return o.config.CanaryOutboundTag
	}
	return "direct"
}

type canaryCache struct {
	mu    sync.Mutex // serializes checks so concurrent callers reuse one result
	state NetworkState
	at    time.Time
}

// CheckNetwork runs the direct canary (or returns a very fresh result). It is
// safe to call concurrently. When the canary is disabled it returns
// NetworkUnknown.
func (o *Observer) CheckNetwork() NetworkState {
	if !o.canaryEnabled() {
		return NetworkUnknown
	}
	o.canary.mu.Lock()
	defer o.canary.mu.Unlock()
	if o.canary.state != NetworkUnknown && time.Since(o.canary.at) < canaryReuseWindow {
		return o.canary.state
	}
	state := o.runCanaryRounds()
	prev := o.canary.state
	o.canary.state = state
	o.canary.at = time.Now()
	if prev != state {
		errors.LogInfo(o.ctx, "network canary: ", prev.String(), " -> ", state.String())
	}
	return state
}

// AllowFallbackEntry reports whether the latest canary verdict permits
// entering fallback mode. With the canary disabled the old behaviour is kept.
func (o *Observer) AllowFallbackEntry() bool {
	if !o.canaryEnabled() {
		return true
	}
	o.canary.mu.Lock()
	state, at := o.canary.state, o.canary.at
	o.canary.mu.Unlock()
	return state == NetworkWhitelist && time.Since(at) < whitelistEntryTTL
}

// runCanaryRounds returns Open as soon as one foreign target answers. A
// non-open verdict must hold for every confirmation round.
func (o *Observer) runCanaryRounds() NetworkState {
	rounds := o.canaryConfirmRounds()
	domesticSeen := false
	for i := 0; i < rounds; i++ {
		foreignOK, domesticOK := o.canaryRound()
		if foreignOK {
			return NetworkOpen
		}
		domesticSeen = domesticSeen || domesticOK
		if i < rounds-1 && !o.sleepOrFinished(canaryRoundGap) {
			break
		}
	}
	if domesticSeen {
		return NetworkWhitelist
	}
	return NetworkOffline
}

func (o *Observer) sleepOrFinished(d time.Duration) bool {
	if o.finished == nil {
		time.Sleep(d)
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-o.finished.Wait():
		return false
	}
}

type canaryResult struct {
	foreign bool
	ok      bool
}

// canaryRound dials all targets in parallel; it returns early on the first
// reachable foreign target.
func (o *Observer) canaryRound() (foreignOK, domesticOK bool) {
	ctx, cancel := context.WithTimeout(o.ctx, o.canaryTimeout())
	defer cancel()

	total := len(o.config.CanaryForeign) + len(o.config.CanaryDomestic)
	results := make(chan canaryResult, total)
	for _, target := range o.config.CanaryForeign {
		go func(t string) { results <- canaryResult{foreign: true, ok: o.canaryDial(ctx, t)} }(target)
	}
	for _, target := range o.config.CanaryDomestic {
		go func(t string) { results <- canaryResult{foreign: false, ok: o.canaryDial(ctx, t)} }(target)
	}
	for i := 0; i < total; i++ {
		r := <-results
		if !r.ok {
			continue
		}
		if r.foreign {
			return true, domesticOK
		}
		domesticOK = true
	}
	return false, domesticOK
}

// canaryDial succeeds when a TLS handshake completes through the canary
// outbound. TLS is used because a plain TCP dial through the dispatcher
// reports success before the remote end has really connected.
func (o *Observer) canaryDial(parent context.Context, target string) bool {
	host, port, err := net.SplitHostPort(target)
	if err != nil {
		host, port = target, "443"
	}
	dest, err := v2net.ParseDestination("tcp:" + net.JoinHostPort(host, port))
	if err != nil {
		return false
	}
	ctx, cancel := context.WithCancel(session.ContextWithSkipOutboundErrorReport(o.ctx))
	defer cancel()
	// Stop when the round deadline passes.
	stop := context.AfterFunc(parent, cancel)
	defer stop()

	conn, err := tagged.Dialer(ctx, o.dispatcher, dest, o.canaryOutboundTag())
	if err != nil {
		return false
	}
	defer conn.Close()

	tlsConfig := &cryptotls.Config{InsecureSkipVerify: true}
	if net.ParseIP(host) == nil {
		tlsConfig.ServerName = host
	}
	return cryptotls.Client(conn, tlsConfig).HandshakeContext(ctx) == nil
}
