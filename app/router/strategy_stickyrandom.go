package router

import (
	"context"
	"sync"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common/dice"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/routing"
)

// StickyRandomStrategy picks a random alive outbound and sticks to it until it
// becomes unhealthy. When all candidates are dead it enables fallback routing
// mode via FallbackModeController. See app/router/FALLBACK_ROUTING.md.
type StickyRandomStrategy struct {
	ctx         context.Context
	observatory extension.Observatory
	fallbackCtrl routing.FallbackModeController

	mu      sync.Mutex
	current string
}

func NewStickyRandomStrategy() *StickyRandomStrategy {
	return &StickyRandomStrategy{}
}

func (s *StickyRandomStrategy) SetFallbackController(ctrl routing.FallbackModeController) {
	s.fallbackCtrl = ctrl
}

func (s *StickyRandomStrategy) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.current = ""
}

func (s *StickyRandomStrategy) Current() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.current
}

func (s *StickyRandomStrategy) GetPrincipleTarget(strings []string) []string {
	if s.current != "" {
		return []string{s.current}
	}
	return strings
}

func (s *StickyRandomStrategy) InjectContext(ctx context.Context) {
	s.ctx = ctx
	if core.FromContext(ctx) == nil {
		return
	}
	core.OptionalFeatures(s.ctx, func(observatory extension.Observatory) {
		s.observatory = observatory
	})
}

func (s *StickyRandomStrategy) PickOutbound(candidates []string) string {
	if len(candidates) == 0 {
		return ""
	}

	alive := s.filterAlive(candidates)
	if len(alive) == 0 {
		if s.fallbackCtrl != nil {
			if tryer, ok := s.fallbackCtrl.(interface{ TryEnterFallbackMode() }); ok {
				tryer.TryEnterFallbackMode()
			} else {
				s.fallbackCtrl.EnableFallbackMode()
			}
		}
		s.Reset()
		return ""
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.current != "" && contains(alive, s.current) {
		return s.current
	}

	s.current = alive[dice.Roll(len(alive))]
	return s.current
}

func (s *StickyRandomStrategy) filterAlive(candidates []string) []string {
	if s.observatory == nil {
		return candidates
	}
	observeReport, err := s.observatory.GetObservation(s.ctx)
	if err != nil {
		return candidates
	}
	result, ok := observeReport.(*observatory.ObservationResult)
	if !ok {
		return candidates
	}
	statusMap := make(map[string]*observatory.OutboundStatus)
	for _, outboundStatus := range result.Status {
		statusMap[outboundStatus.OutboundTag] = outboundStatus
	}
	aliveTags := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if outboundStatus, found := statusMap[candidate]; found {
			if outboundStatus.Alive {
				aliveTags = append(aliveTags, candidate)
			}
		} else {
			aliveTags = append(aliveTags, candidate)
		}
	}
	return aliveTags
}

func contains(list []string, item string) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}
