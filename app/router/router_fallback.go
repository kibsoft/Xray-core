package router

import (
	"time"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/routing"
)

func (r *Router) EnableFallbackMode() {
	r.fallbackMode.CompareAndSwap(false, true)
}

func (r *Router) TryEnterFallbackMode() {
	if r.hasConfirmedAliveFallback() {
		r.EnableFallbackMode()
		return
	}
	r.requestPrimaryAndFallbackProbe()
}

func (r *Router) CurrentStickyTag() string {
	if r.stickyStrategy == nil {
		return ""
	}
	return r.stickyStrategy.Current()
}

func (r *Router) SyncFallbackMode() {
	if r.hasAlivePrimary(r.primaryCandidates()) {
		if r.fallbackMode.Load() {
			// Primary recovered: drop stale Alive so a later network switch
			// can sweep primaries again. Do not clear when leaving because
			// fallback is also dead — Dead marks must keep sticky off them.
			r.DisableFallbackMode()
			r.forgetObservatoryAfterLeaveFallback()
		}
		return
	}
	if r.hasConfirmedAliveFallback() {
		r.EnableFallbackMode()
		return
	}
	if r.fallbackMode.Load() {
		r.DisableFallbackMode()
	}
}

func (r *Router) requestPrimaryAndFallbackProbe() {
	if r.observatory == nil {
		return
	}
	obs, ok := r.observatory.(extension.FallbackHealthObservatory)
	if !ok {
		return
	}
	go obs.ProbePrimaryAndFallback()
}

func (r *Router) primaryCandidates() []string {
	if r.stickyBalancerTag == "" {
		return nil
	}
	balancer, ok := r.balancers[r.stickyBalancerTag]
	if !ok {
		return nil
	}
	candidates, err := balancer.SelectOutbounds()
	if err != nil {
		return nil
	}
	return candidates
}

func (r *Router) fallbackCandidates() []string {
	if r.fallbackBalancerTag == "" {
		return nil
	}
	balancer, ok := r.balancers[r.fallbackBalancerTag]
	if !ok {
		return nil
	}
	candidates, err := balancer.SelectOutbounds()
	if err != nil {
		return nil
	}
	return candidates
}

func (r *Router) hasConfirmedAliveFallback() bool {
	return r.hasConfirmedAlive(r.fallbackCandidates())
}

func (r *Router) hasConfirmedAlive(candidates []string) bool {
	if r.observatory == nil || len(candidates) == 0 {
		return false
	}
	observeReport, err := r.observatory.GetObservation(r.ctx)
	if err != nil {
		return false
	}
	result, ok := observeReport.(*observatory.ObservationResult)
	if !ok {
		return false
	}
	statusMap := make(map[string]bool)
	for _, status := range result.Status {
		statusMap[status.OutboundTag] = status.Alive
	}
	for _, candidate := range candidates {
		if alive, found := statusMap[candidate]; found && alive {
			return true
		}
	}
	return false
}

func (r *Router) DisableFallbackMode() {
	r.fallbackMode.Store(false)
	if r.stickyStrategy != nil {
		r.stickyStrategy.Reset()
	}
}

// forgetObservatoryAfterLeaveFallback clears probe Alive marks so the next
// network failure can sweep primaries again (stale Wi-Fi Alive must not block).
// Call only after primary recovery — not when leaving because fallback died too.
func (r *Router) forgetObservatoryAfterLeaveFallback() {
	if r.observatory == nil {
		return
	}
	if clearer, ok := r.observatory.(interface{ ClearObservationStatus() }); ok {
		clearer.ClearObservationStatus()
	}
}

func (r *Router) IsFallbackMode() bool {
	return r.fallbackMode.Load()
}

func (r *Router) recoveryWatcher() {
	interval := r.recoveryInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if !r.fallbackMode.Load() || r.stickyBalancerTag == "" {
				continue
			}
			balancer, ok := r.balancers[r.stickyBalancerTag]
			if !ok {
				continue
			}
			candidates, err := balancer.SelectOutbounds()
			if err != nil || len(candidates) == 0 {
				continue
			}
			if r.hasAlivePrimary(candidates) {
				r.DisableFallbackMode()
				r.forgetObservatoryAfterLeaveFallback()
			}
		case <-r.recoveryFinished.Wait():
			return
		}
	}
}

func (r *Router) hasAlivePrimary(candidates []string) bool {
	if r.observatory == nil {
		return false
	}
	observeReport, err := r.observatory.GetObservation(r.ctx)
	if err != nil {
		return false
	}
	result, ok := observeReport.(*observatory.ObservationResult)
	if !ok {
		return false
	}
	statusMap := make(map[string]bool)
	for _, status := range result.Status {
		statusMap[status.OutboundTag] = status.Alive
	}
	for _, candidate := range candidates {
		alive, found := statusMap[candidate]
		if !found || alive {
			return true
		}
	}
	return false
}

func (r *Router) buildRuleFromRoutingRule(rule *RoutingRule) (*Rule, error) {
	cond, err := rule.BuildCondition()
	if err != nil {
		return nil, err
	}
	rr := &Rule{
		Condition: cond,
		Tag:       rule.GetTag(),
		RuleTag:   rule.GetRuleTag(),
	}
	if wh := rule.GetWebhook(); wh != nil {
		notifier, err := NewWebhookNotifier(wh)
		if err != nil {
			return nil, err
		}
		rr.Webhook = notifier
	}
	btag := rule.GetBalancingTag()
	if len(btag) > 0 {
		brule, found := r.balancers[btag]
		if !found {
			if rr.Webhook != nil {
				rr.Webhook.Close()
			}
			return nil, errors.New("balancer ", btag, " not found")
		}
		rr.Balancer = brule
	}
	return rr, nil
}

func (r *Router) loadFallbackRules(config *Config) error {
	for _, rule := range r.fallbackRules {
		if rule.Webhook != nil {
			rule.Webhook.Close()
		}
	}
	r.fallbackRules = make([]*Rule, 0, len(config.FallbackRule))
	for _, rule := range config.FallbackRule {
		rr, err := r.buildRuleFromRoutingRule(rule)
		if err != nil {
			r.closeFallbackWebhooks()
			return err
		}
		r.fallbackRules = append(r.fallbackRules, rr)
	}
	return nil
}

func (r *Router) closeFallbackWebhooks() {
	for _, rule := range r.fallbackRules {
		if rule.Webhook != nil {
			rule.Webhook.Close()
		}
	}
}

func (r *Router) wireStickyStrategies() {
	for tag, balancer := range r.balancers {
		if strategy, ok := balancer.strategy.(*StickyRandomStrategy); ok {
			strategy.SetFallbackController(r)
			if r.stickyBalancerTag == "" {
				r.stickyBalancerTag = tag
				r.stickyStrategy = strategy
			}
		}
	}
}

func (r *Router) validateFallbackConfig(config *Config) error {
	if config.FallbackBalancerTag != "" {
		if _, ok := r.balancers[config.FallbackBalancerTag]; !ok {
			return errors.New("fallback balancer ", config.FallbackBalancerTag, " not found")
		}
	}
	return nil
}

func (r *Router) pickFallbackRule(ctx routing.Context) (*Rule, routing.Context, error) {
	for _, rule := range r.fallbackRules {
		if rule.Apply(ctx) {
			return rule, ctx, nil
		}
	}
	if r.fallbackBalancerTag == "" {
		return nil, ctx, errors.New("no matching fallback rule and fallbackBalancerTag is empty")
	}
	balancer, ok := r.balancers[r.fallbackBalancerTag]
	if !ok {
		return nil, ctx, errors.New("fallback balancer ", r.fallbackBalancerTag, " not found")
	}
	return &Rule{Balancer: balancer}, ctx, nil
}
