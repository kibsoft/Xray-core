package router

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/app/observatory"
	"google.golang.org/protobuf/proto"
)

type mockObservatory struct {
	status []*observatory.OutboundStatus
}

func (m *mockObservatory) Type() interface{} { return nil }
func (m *mockObservatory) Start() error      { return nil }
func (m *mockObservatory) Close() error      { return nil }
func (m *mockObservatory) GetObservation(ctx context.Context) (proto.Message, error) {
	return &observatory.ObservationResult{Status: m.status}, nil
}

type mockFallbackController struct {
	enabled bool
}

func (m *mockFallbackController) EnableFallbackMode()  { m.enabled = true }
func (m *mockFallbackController) DisableFallbackMode() { m.enabled = false }
func (m *mockFallbackController) IsFallbackMode() bool { return m.enabled }

func TestStickyRandom_SticksToAlive(t *testing.T) {
	obs := &mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "a", Alive: true},
		{OutboundTag: "b", Alive: true},
	}}
	s := NewStickyRandomStrategy()
	s.observatory = obs
	s.ctx = context.Background()

	first := s.PickOutbound([]string{"a", "b"})
	for i := 0; i < 10; i++ {
		if got := s.PickOutbound([]string{"a", "b"}); got != first {
			t.Fatalf("expected sticky tag %q, got %q", first, got)
		}
	}
}

func TestStickyRandom_SwitchesOnDeath(t *testing.T) {
	obs := &mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "a", Alive: true},
		{OutboundTag: "b", Alive: true},
	}}
	s := NewStickyRandomStrategy()
	s.observatory = obs
	s.ctx = context.Background()
	s.current = "a"

	obs.status = []*observatory.OutboundStatus{
		{OutboundTag: "a", Alive: false},
		{OutboundTag: "b", Alive: true},
	}
	got := s.PickOutbound([]string{"a", "b"})
	if got != "b" {
		t.Fatalf("expected switch to b, got %q", got)
	}
}

func TestStickyRandom_EnablesFallbackWhenAllDead(t *testing.T) {
	obs := &mockObservatory{status: []*observatory.OutboundStatus{
		{OutboundTag: "a", Alive: false},
		{OutboundTag: "b", Alive: false},
	}}
	ctrl := &mockFallbackController{}
	s := NewStickyRandomStrategy()
	s.observatory = obs
	s.fallbackCtrl = ctrl
	s.ctx = context.Background()

	got := s.PickOutbound([]string{"a", "b"})
	if got != "" {
		t.Fatalf("expected empty tag, got %q", got)
	}
	if !ctrl.enabled {
		t.Fatal("expected fallback mode enabled")
	}
}

func TestStickyRandom_Reset(t *testing.T) {
	s := NewStickyRandomStrategy()
	s.current = "a"
	s.Reset()
	if s.current != "" {
		t.Fatalf("expected empty current after reset, got %q", s.current)
	}
}
