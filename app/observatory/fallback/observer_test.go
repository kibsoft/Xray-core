package fallback

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
)

type mockHandlerSelector struct {
	lastSelectors []string
}

func (m *mockHandlerSelector) Select(selectors []string) []string {
	m.lastSelectors = append([]string{}, selectors...)
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

var _ routing.FallbackModeController = (*mockModeController)(nil)
