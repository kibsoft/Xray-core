package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/app/dispatcher"
	. "github.com/xtls/xray-core/infra/conf"
)

func TestSpeedLimitConfigBuild(t *testing.T) {
	raw := `{
		"enabled": true,
		"defaultDownKbps": 5000,
		"defaultUpKbps": 5000,
		"unlimited": ["48d7bab1-0007-4564-a950-ce4f664b4316"],
		"overrides": {
			"2": {"downKbps": 20000, "upKbps": 20000}
		}
	}`
	c := new(SpeedLimitConfig)
	if err := json.Unmarshal([]byte(raw), c); err != nil {
		t.Fatal(err)
	}
	built, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	if !built.Enabled || built.DefaultDownKbps != 5000 || built.DefaultUpKbps != 5000 {
		t.Fatalf("unexpected defaults: %+v", built)
	}
	if len(built.Unlimited) != 1 || built.Overrides["2"].DownKbps != 20000 {
		t.Fatalf("unexpected lists: %+v", built)
	}
}

func TestSpeedLimitConfigMissingIsOff(t *testing.T) {
	raw := `{"inbounds": [], "outbounds": []}`
	c := new(Config)
	if err := json.Unmarshal([]byte(raw), c); err != nil {
		t.Fatal(err)
	}
	built, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	var disp *dispatcher.Config
	for _, app := range built.App {
		inst, err := app.GetInstance()
		if err != nil {
			t.Fatal(err)
		}
		if cfg, ok := inst.(*dispatcher.Config); ok {
			disp = cfg
		}
	}
	if disp == nil {
		t.Fatal("dispatcher config missing")
	}
	if disp.GetSpeedLimit() != nil && disp.GetSpeedLimit().GetEnabled() {
		t.Fatal("missing speedLimit must stay disabled")
	}
}

func TestSpeedLimitConfigNegativeRejected(t *testing.T) {
	raw := `{"enabled": true, "defaultDownKbps": -1}`
	c := new(SpeedLimitConfig)
	if err := json.Unmarshal([]byte(raw), c); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Build(); err == nil {
		t.Fatal("negative kbps must be rejected")
	}
}

func TestConfigOverrideSpeedLimit(t *testing.T) {
	orig := &Config{}
	over := &Config{SpeedLimit: &SpeedLimitConfig{Enabled: true}}
	orig.Override(over, "extra.json")
	if orig.SpeedLimit == nil || !orig.SpeedLimit.Enabled {
		t.Fatal("speedLimit override was not applied")
	}
}
