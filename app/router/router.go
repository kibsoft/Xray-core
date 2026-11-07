package router

import (
	"context"
	"maps"
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
	rules          atomic.Pointer[[]*Rule]
	fallbackRules  []*Rule
	script         *scriptEngine
	balancers      atomic.Pointer[map[string]*Balancer]
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

	if config.Script != "" {
		script, err := newScriptEngine(config.Script)
		if err != nil {
			return errors.New("failed to prepare routing script").Base(err)
		}
		r.script = script
	}

	r.rules.Store(new([]*Rule))
	r.balancers.Store(&map[string]*Balancer{})
	if err := r.ReloadRules(config, false); err != nil {
		return err
	}
	r.fallbackBalancerTag = config.FallbackBalancerTag
	if err := r.validateFallbackConfig(config); err != nil {
		return err
	}
	if err := r.loadFallbackRules(config); err != nil {
		return err
	}
	r.wireStickyStrategies(config.Rule)
	if core.FromContext(ctx) != nil {
		core.OptionalFeatures(ctx, func(obs extension.Observatory) {
			r.observatory = obs
			if fo, ok := obs.(interface{ ProbeIntervalDuration() time.Duration }); ok {
				r.recoveryInterval = fo.ProbeIntervalDuration()
			}
		})
	}
	return nil
}

// PickRoute implements routing.Router.
func (r *Router) PickRoute(ctx routing.Context) (routing.Route, error) {
	if r.script != nil {
		return r.script.pickRoute(ctx)
	}

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

	oldRules := *r.rules.Load()
	oldBalancers := *r.balancers.Load()

	var newRules []*Rule
	newBalancers := make(map[string]*Balancer)
	existTags := make(map[string]bool, len(oldRules)+len(config.Rule))
	if shouldAppend {
		newRules = append(newRules, oldRules...)
		maps.Copy(newBalancers, oldBalancers)
		for _, rule := range oldRules {
			existTags[rule.RuleTag] = true
		}
	}

	for _, rule := range config.BalancingRule {
		if _, found := newBalancers[rule.Tag]; found {
			return errors.New("duplicate balancer tag")
		}
		balancer, err := rule.Build(r.ohm, r.dispatcher)
		if err != nil {
			return err
		}
		balancer.InjectContext(r.ctx)
		newBalancers[rule.Tag] = balancer
	}

	for _, rule := range config.Rule {
		cond, err := rule.BuildCondition()
		if err != nil {
			return err
		}
		rr := &Rule{
			Condition: cond,
			Tag:       rule.GetTag(),
			RuleTag:   rule.GetRuleTag(),
		}
		if rr.RuleTag != "" && existTags[rr.RuleTag] {
			return errors.New("duplicate ruleTag ", rr.RuleTag)
		}
		existTags[rr.RuleTag] = true
		if wh := rule.GetWebhook(); wh != nil {
			notifier, err := NewWebhookNotifier(wh)
			if err != nil {
				return err
			}
			rr.Webhook = notifier
		}
		if btag := rule.GetBalancingTag(); len(btag) > 0 {
			brule, found := newBalancers[btag]
			if !found {
				return errors.New("balancer ", btag, " not found")
			}
			rr.Balancer = brule
		}
		newRules = append(newRules, rr)
	}

	r.balancers.Store(&newBalancers)
	r.rules.Store(&newRules)
	if !shouldAppend {
		closeWebhooks(oldRules)
	}
	return nil
}

// RemoveRule implements routing.Router.
func (r *Router) RemoveRule(tag string) error {
	if tag == "" {
		return errors.New("empty tag name!")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	oldRules := *r.rules.Load()
	newRules := make([]*Rule, 0, len(oldRules))
	var removed []*Rule
	for _, rule := range oldRules {
		if rule.RuleTag != tag {
			newRules = append(newRules, rule)
		} else {
			removed = append(removed, rule)
		}
	}
	r.rules.Store(&newRules)
	closeWebhooks(removed)
	return nil
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
		if _, ok := (*r.balancers.Load())[config.FallbackBalancerTag]; !ok {
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
	rules := *r.rules.Load()
	ruleList := make([]routing.Route, 0, len(rules))
	for _, rule := range rules {
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
	return r.pickRouteWithDNS(ctx, *r.rules.Load(), nil)
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
	if r.script != nil {
		if err := r.script.start(r); err != nil {
			return errors.New("failed to initialize routing script").Base(err)
		}
	}
	if r.stickyBalancerTag != "" {
		r.recoveryFinished = done.New()
		go r.recoveryWatcher()
	}
	return nil
}

// closeWebhooks closes all webhook notifiers in the given rule set.
func closeWebhooks(rules []*Rule) {
	for _, rule := range rules {
		if rule.Webhook != nil {
			rule.Webhook.Close()
		}
	}
}

// Close implements common.Closable.
func (r *Router) Close() error {
	if r.script != nil {
		r.script.close()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	closeWebhooks(*r.rules.Load())
	if r.recoveryFinished != nil {
		r.recoveryFinished.Close()
	}
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
