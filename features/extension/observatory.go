package extension

import (
	"context"

	"github.com/xtls/xray-core/features"
	"google.golang.org/protobuf/proto"
)

type Observatory interface {
	features.Feature

	GetObservation(ctx context.Context) (proto.Message, error)
}

type BurstObservatory interface {
	Observatory
	Check(tag []string)
}

// FallbackProbeObservatory triggers an immediate probe of fallback outbounds.
type FallbackProbeObservatory interface {
	Observatory
	ProbeFallback()
}

// OutboundErrorObserver is notified when a real outbound connection fails
// and when user sessions start and end on an outbound tag.
type OutboundErrorObserver interface {
	ReportOutboundError(tag string, err error)
	ReportOutboundSessionStart(tag string)
	ReportOutboundSessionEnd(tag string, err error)
}

// FallbackHealthObservatory probes primary and fallback outbounds on demand.
type FallbackHealthObservatory interface {
	FallbackProbeObservatory
	ProbeNow(tags []string)
	ProbePrimaryAndFallback()
}

func ObservatoryType() interface{} {
	return (*Observatory)(nil)
}
