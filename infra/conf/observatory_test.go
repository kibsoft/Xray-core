package conf_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/fallback"
	. "github.com/xtls/xray-core/infra/conf"
)

func TestFallbackObservatoryConfig_IgnoreErrors(t *testing.T) {
	raw := `{
		"subjectSelector": ["mux"],
		"fallbackSubjectSelector": ["wl"]
	}`
	cfg := new(FallbackObservatoryConfig)
	if err := json.Unmarshal([]byte(raw), cfg); err != nil {
		t.Fatal(err)
	}
	built, err := cfg.Build()
	if err != nil {
		t.Fatal(err)
	}
	fb := built.(*fallback.Config)
	if len(fb.IgnoreErrors) != 0 {
		t.Fatalf("expected empty ignoreErrors by default, got %v", fb.IgnoreErrors)
	}

	cfg.IgnoreErrors = []string{"internalError", "wsasend"}
	built, err = cfg.Build()
	if err != nil {
		t.Fatal(err)
	}
	got := built.(*fallback.Config).IgnoreErrors
	if len(got) != 2 || got[0] != fallback.IgnoreErrorInternal || got[1] != fallback.IgnoreErrorWSASend {
		t.Fatalf("unexpected ignoreErrors: %v", got)
	}

	cfg.IgnoreErrors = []string{"not-a-real-error"}
	if _, err := cfg.Build(); err == nil {
		t.Fatal("expected unknown ignoreErrors value to fail Build")
	}
}

func TestConfig_RequiresFallbackObservatory(t *testing.T) {
	raw := `{
		"routing": {
			"fallbackBalancerTag": "fallback-random",
			"rules": [],
			"balancers": []
		}
	}`
	config := new(Config)
	if err := json.Unmarshal([]byte(raw), config); err != nil {
		t.Fatal(err)
	}
	_, err := config.Build()
	if err == nil || !strings.Contains(err.Error(), "fallbackObservatory is required") {
		t.Fatalf("expected fallbackObservatory required error, got %v", err)
	}
}

func TestConfig_MutuallyExclusiveObservatory(t *testing.T) {
	raw := `{
		"observatory": {"subjectSelector": ["a"]},
		"fallbackObservatory": {"subjectSelector": ["a"], "fallbackSubjectSelector": ["b"]}
	}`
	config := new(Config)
	if err := json.Unmarshal([]byte(raw), config); err != nil {
		t.Fatal(err)
	}
	_, err := config.Build()
	if err == nil || !strings.Contains(err.Error(), "only one of observatory") {
		t.Fatalf("expected mutually exclusive observatory error, got %v", err)
	}
}

func TestRouterConfig_StickyRandomAndFallbackRules(t *testing.T) {
	raw := `{
		"domainStrategy": "AsIs",
		"fallbackBalancerTag": "fallback-random",
		"rules": [
			{
				"type": "field",
				"network": "tcp",
				"balancerTag": "main-sticky"
			}
		],
		"fallbackRules": [
			{
				"type": "field",
				"domain": ["bank.ru"],
				"outboundTag": "direct"
			}
		],
		"balancers": [
			{
				"tag": "main-sticky",
				"selector": ["primary-"],
				"strategy": {"type": "stickyRandom"}
			},
			{
				"tag": "fallback-random",
				"selector": ["fallback-"],
				"strategy": {"type": "random"}
			}
		]
	}`
	routerConfig := new(RouterConfig)
	if err := json.Unmarshal([]byte(raw), routerConfig); err != nil {
		t.Fatal(err)
	}
	built, err := routerConfig.Build()
	if err != nil {
		t.Fatal(err)
	}
	if built.FallbackBalancerTag != "fallback-random" {
		t.Fatalf("unexpected fallback balancer tag: %q", built.FallbackBalancerTag)
	}
	if len(built.FallbackRule) != 1 {
		t.Fatalf("expected 1 fallback rule, got %d", len(built.FallbackRule))
	}
	if built.BalancingRule[0].Strategy != "stickyrandom" {
		t.Fatalf("expected stickyrandom strategy, got %q", built.BalancingRule[0].Strategy)
	}
}
