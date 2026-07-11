package router

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/dns"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
	routing_dns "github.com/xtls/xray-core/features/routing/dns"
)

// Router is an implementation of routing.Router.
type Router struct {
	domainStrategy Config_DomainStrategy
	rules          []*Rule
	fallbackRules  []*Rule
	balancers      map[string]*Balancer
	dns            dns.Client

	fallbackBalancerTag string
	fallbackMode        atomic.Bool
	stickyBalancerTag   string
	stickyStrategy      *StickyRandomStrategy
	observatory         extension.Observatory
	recoveryInterval    time.Duration
	recoveryFinished    *done.Instance

	ctx        context.Context
	ohm        outbound.Manager
	dispatcher routing.Dispatcher
	mu         sync.Mutex
}

// Route is an implementation of routing.Route.
type Route struct {
	routing.Context
	outboundGroupTags []string
	outboundTag       string
	ruleTag           string
}

// Init initializes the Router.
func (r *Router) Init(ctx context.Context, config *Config, d dns.Client, ohm outbound.Manager, dispatcher routing.Dispatcher) error {
	r.domainStrategy = config.DomainStrategy
	r.dns = d
	r.ctx = ctx
	r.ohm = ohm
	r.dispatcher = dispatcher

	r.balancers = make(map[string]*Balancer, len(config.BalancingRule))
	for _, rule := range config.BalancingRule {
		balancer, err := rule.Build(ohm, dispatcher)
		if err != nil {
			return err
		}
		balancer.InjectContext(ctx)
		r.balancers[rule.Tag] = balancer
	}

	r.fallbackBalancerTag = config.FallbackBalancerTag
	if err := r.validateFallbackConfig(config); err != nil {
		return err
	}
	if err := r.loadFallbackRules(config); err != nil {
		return err
	}
	r.wireStickyStrategies()

	if core.FromContext(ctx) != nil {
		core.OptionalFeatures(ctx, func(obs extension.Observatory) {
			r.observatory = obs
			if fo, ok := obs.(interface{ ProbeIntervalDuration() time.Duration }); ok {
				r.recoveryInterval = fo.ProbeIntervalDuration()
			}
		})
	}

	r.rules = make([]*Rule, 0, len(config.Rule))
	for _, rule := range config.Rule {
		cond, err := rule.BuildCondition()
		if err != nil {
			r.closeWebhooks()
			return err
		}
		rr := &Rule{
			Condition: cond,
			Tag:       rule.GetTag(),
			RuleTag:   rule.GetRuleTag(),
		}
		if wh := rule.GetWebhook(); wh != nil {
			notifier, err := NewWebhookNotifier(wh)
			if err != nil {
				r.closeWebhooks()
				return err
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
				r.closeWebhooks()
				return errors.New("balancer ", btag, " not found")
			}
			rr.Balancer = brule
		}
		r.rules = append(r.rules, rr)
	}

	return nil
}

// PickRoute implements routing.Router.
func (r *Router) PickRoute(ctx routing.Context) (routing.Route, error) {
	originalCtx := ctx
	for {
		rule, ctx, err := r.pickRouteInternal(ctx)
		if err != nil {
			return nil, err
		}
		tag, err := rule.GetTag()
		if err != nil {
			return nil, err
		}
		if tag == "" {
			if r.fallbackMode.Load() {
				continue
			}
			return nil, errors.New("empty outbound tag")
		}
		if rule.Webhook != nil {
			rule.Webhook.Fire(originalCtx, tag)
		}
		return &Route{Context: ctx, outboundTag: tag, ruleTag: rule.RuleTag}, nil
	}
}

// AddRule implements routing.Router.
func (r *Router) AddRule(config *serial.TypedMessage, shouldAppend bool) error {
	inst, err := config.GetInstance()
	if err != nil {
		return err
	}
	if c, ok := inst.(*Config); ok {
		return r.ReloadRules(c, shouldAppend)
	}
	return errors.New("AddRule: config type error")
}

func (r *Router) ReloadRules(config *Config, shouldAppend bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !shouldAppend {
		for _, rule := range r.rules {
			if rule.Webhook != nil {
				rule.Webhook.Close()
			}
		}
		r.balancers = make(map[string]*Balancer, len(config.BalancingRule))
		r.rules = make([]*Rule, 0, len(config.Rule))
	}
	for _, rule := range config.BalancingRule {
		_, found := r.balancers[rule.Tag]
		if found {
			return errors.New("duplicate balancer tag")
		}
		balancer, err := rule.Build(r.ohm, r.dispatcher)
		if err != nil {
			return err
		}
		balancer.InjectContext(r.ctx)
		r.balancers[rule.Tag] = balancer
	}

	startIdx := len(r.rules)
	closeNewWebhooks := func() {
		for i := startIdx; i < len(r.rules); i++ {
			if r.rules[i].Webhook != nil {
				r.rules[i].Webhook.Close()
			}
		}
		r.rules = r.rules[:startIdx]
	}

	for _, rule := range config.Rule {
		if r.RuleExists(rule.GetRuleTag()) {
			closeNewWebhooks()
			return errors.New("duplicate ruleTag ", rule.GetRuleTag())
		}
		cond, err := rule.BuildCondition()
		if err != nil {
			closeNewWebhooks()
			return err
		}
		rr := &Rule{
			Condition: cond,
			Tag:       rule.GetTag(),
			RuleTag:   rule.GetRuleTag(),
		}
		if wh := rule.GetWebhook(); wh != nil {
			notifier, err := NewWebhookNotifier(wh)
			if err != nil {
				closeNewWebhooks()
				return err
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
				closeNewWebhooks()
				return errors.New("balancer ", btag, " not found")
			}
			rr.Balancer = brule
		}
		r.rules = append(r.rules, rr)
	}

	return nil
}

func (r *Router) RuleExists(tag string) bool {
	if tag != "" {
		for _, rule := range r.rules {
			if rule.RuleTag == tag {
				return true
			}
		}
	}
	return false
}

// RemoveRule implements routing.Router.
func (r *Router) RemoveRule(tag string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	newRules := []*Rule{}
	if tag != "" {
		for _, rule := range r.rules {
			if rule.RuleTag != tag {
				newRules = append(newRules, rule)
			} else if rule.Webhook != nil {
				rule.Webhook.Close()
			}
		}
		r.rules = newRules
		return nil
	}
	return errors.New("empty tag name!")
}

// AddFallbackRule implements routing.Router.
func (r *Router) AddFallbackRule(config *serial.TypedMessage, shouldAppend bool) error {
	inst, err := config.GetInstance()
	if err != nil {
		return err
	}
	c, ok := inst.(*Config)
	if !ok {
		return errors.New("AddFallbackRule: config type error")
	}
	return r.ReloadFallbackRules(c, shouldAppend)
}

func (r *Router) ReloadFallbackRules(config *Config, shouldAppend bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !shouldAppend {
		r.closeFallbackWebhooks()
		r.fallbackRules = make([]*Rule, 0, len(config.FallbackRule))
	}
	if config.FallbackBalancerTag != "" {
		r.fallbackBalancerTag = config.FallbackBalancerTag
		if _, ok := r.balancers[config.FallbackBalancerTag]; !ok {
			return errors.New("fallback balancer ", config.FallbackBalancerTag, " not found")
		}
	}

	startIdx := len(r.fallbackRules)
	closeNewWebhooks := func() {
		for i := startIdx; i < len(r.fallbackRules); i++ {
			if r.fallbackRules[i].Webhook != nil {
				r.fallbackRules[i].Webhook.Close()
			}
		}
		r.fallbackRules = r.fallbackRules[:startIdx]
	}

	for _, rule := range config.FallbackRule {
		if r.fallbackRuleExists(rule.GetRuleTag()) {
			closeNewWebhooks()
			return errors.New("duplicate fallback ruleTag ", rule.GetRuleTag())
		}
		rr, err := r.buildRuleFromRoutingRule(rule)
		if err != nil {
			closeNewWebhooks()
			return err
		}
		r.fallbackRules = append(r.fallbackRules, rr)
	}
	return nil
}

func (r *Router) fallbackRuleExists(tag string) bool {
	if tag == "" {
		return false
	}
	for _, rule := range r.fallbackRules {
		if rule.RuleTag == tag {
			return true
		}
	}
	return false
}

// RemoveFallbackRule implements routing.Router.
func (r *Router) RemoveFallbackRule(tag string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if tag == "" {
		return errors.New("empty tag name!")
	}
	newRules := []*Rule{}
	for _, rule := range r.fallbackRules {
		if rule.RuleTag != tag {
			newRules = append(newRules, rule)
		} else if rule.Webhook != nil {
			rule.Webhook.Close()
		}
	}
	r.fallbackRules = newRules
	return nil
}

// GetRoutingMode implements routing.Router.
func (r *Router) GetRoutingMode() (bool, error) {
	return r.fallbackMode.Load(), nil
}

// ListRule implements routing.Router
func (r *Router) ListRule() []routing.Route {
	r.mu.Lock()
	defer r.mu.Unlock()
	ruleList := make([]routing.Route, 0)
	for _, rule := range r.rules {
		ruleList = append(ruleList, &Route{
			outboundTag: rule.Tag,
			ruleTag:     rule.RuleTag,
		})
	}
	return ruleList
}

func (r *Router) pickRouteInternal(ctx routing.Context) (*Rule, routing.Context, error) {
	if r.fallbackMode.Load() {
		return r.pickRouteWithDNS(ctx, r.fallbackRules, r.pickFallbackRule)
	}
	return r.pickRouteWithDNS(ctx, r.rules, nil)
}

func (r *Router) pickRouteWithDNS(ctx routing.Context, rules []*Rule, fallback func(routing.Context) (*Rule, routing.Context, error)) (*Rule, routing.Context, error) {
	skipDNSResolve := ctx.GetSkipDNSResolve()

	if r.domainStrategy == Config_IpOnDemand && !skipDNSResolve {
		ctx = routing_dns.ContextWithDNSClient(ctx, r.dns)
	}

	rule, ctx, err := r.applyRules(ctx, rules, fallback)
	if err == nil || err != common.ErrNoClue {
		return rule, ctx, err
	}

	if r.domainStrategy != Config_IpIfNonMatch || len(ctx.GetTargetDomain()) == 0 || skipDNSResolve {
		return nil, ctx, common.ErrNoClue
	}

	ctx = routing_dns.ContextWithDNSClient(ctx, r.dns)
	return r.applyRules(ctx, rules, fallback)
}

func (r *Router) applyRules(ctx routing.Context, rules []*Rule, fallback func(routing.Context) (*Rule, routing.Context, error)) (*Rule, routing.Context, error) {
	for _, rule := range rules {
		if rule.Apply(ctx) {
			return rule, ctx, nil
		}
	}
	if fallback != nil {
		return fallback(ctx)
	}
	return nil, ctx, common.ErrNoClue
}

// Start implements common.Runnable.
func (r *Router) Start() error {
	if r.stickyBalancerTag != "" {
		r.recoveryFinished = done.New()
		go r.recoveryWatcher()
	}
	return nil
}

// closeWebhooks closes all webhook notifiers in the current rule set.
func (r *Router) closeWebhooks() {
	for _, rule := range r.rules {
		if rule.Webhook != nil {
			rule.Webhook.Close()
		}
	}
}

// Close implements common.Closable.
func (r *Router) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.recoveryFinished != nil {
		r.recoveryFinished.Close()
	}
	r.closeWebhooks()
	r.closeFallbackWebhooks()
	return nil
}

// Type implements common.HasType.
func (*Router) Type() interface{} {
	return routing.RouterType()
}

// GetOutboundGroupTags implements routing.Route.
func (r *Route) GetOutboundGroupTags() []string {
	return r.outboundGroupTags
}

// GetOutboundTag implements routing.Route.
func (r *Route) GetOutboundTag() string {
	return r.outboundTag
}

func (r *Route) GetRuleTag() string {
	return r.ruleTag
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, config interface{}) (interface{}, error) {
		r := new(Router)
		if err := core.RequireFeatures(ctx, func(d dns.Client, ohm outbound.Manager, dispatcher routing.Dispatcher) error {
			return r.Init(ctx, config.(*Config), d, ohm, dispatcher)
		}); err != nil {
			return nil, err
		}
		return r, nil
	}))
}
