package conf

import (
	"google.golang.org/protobuf/proto"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/app/observatory/burst"
	"github.com/xtls/xray-core/app/observatory/fallback"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/infra/conf/cfgcommon/duration"
)

type ObservatoryConfig struct {
	SubjectSelector   []string          `json:"subjectSelector"`
	ProbeURL          string            `json:"probeURL"`
	ProbeInterval     duration.Duration `json:"probeInterval"`
	EnableConcurrency bool              `json:"enableConcurrency"`
}

func (o *ObservatoryConfig) Build() (proto.Message, error) {
	return &observatory.Config{SubjectSelector: o.SubjectSelector, ProbeUrl: o.ProbeURL, ProbeInterval: int64(o.ProbeInterval), EnableConcurrency: o.EnableConcurrency}, nil
}

type FallbackObservatoryConfig struct {
	SubjectSelector         []string          `json:"subjectSelector"`
	FallbackSubjectSelector []string          `json:"fallbackSubjectSelector"`
	ProbeURL                string            `json:"probeURL"`
	ProbeInterval           duration.Duration `json:"probeInterval"`
	EnableConcurrency       bool              `json:"enableConcurrency"`
	ProbeOnError            *bool             `json:"probeOnError"`
	IgnoreErrors            []string          `json:"ignoreErrors"`
	ErrorProbeCooldown      duration.Duration `json:"errorProbeCooldown"`
	RecoveryProbeInterval   duration.Duration `json:"recoveryProbeInterval"`
	NetworkCanary           *NetworkCanaryConfig `json:"networkCanary"`
}

// NetworkCanaryConfig configures direct probes that distinguish an open
// network from a whitelist (WL) network without pinging proxy servers.
type NetworkCanaryConfig struct {
	OutboundTag      string            `json:"outboundTag"`
	Foreign          []string          `json:"foreign"`
	Domestic         []string          `json:"domestic"`
	Timeout          duration.Duration `json:"timeout"`
	ConfirmRounds    int32             `json:"confirmRounds"`
	PollInterval     duration.Duration `json:"pollInterval"`
	MinFallbackDwell duration.Duration `json:"minFallbackDwell"`
}

func (o *FallbackObservatoryConfig) Build() (proto.Message, error) {
	probeOnError := true
	if o.ProbeOnError != nil {
		probeOnError = *o.ProbeOnError
	}
	ignoreErrors, err := fallback.NormalizeIgnoreErrors(o.IgnoreErrors)
	if err != nil {
		return nil, err
	}
	cfg := &fallback.Config{
		SubjectSelector:         o.SubjectSelector,
		FallbackSubjectSelector: o.FallbackSubjectSelector,
		ProbeUrl:                o.ProbeURL,
		ProbeInterval:           int64(o.ProbeInterval),
		EnableConcurrency:       o.EnableConcurrency,
		RecoveryProbeInterval:   int64(o.RecoveryProbeInterval),
		ErrorProbeCooldown:      int64(o.ErrorProbeCooldown),
		DisableProbeOnError:     !probeOnError,
		IgnoreErrors:            ignoreErrors,
	}
	if c := o.NetworkCanary; c != nil {
		cfg.CanaryOutboundTag = c.OutboundTag
		cfg.CanaryForeign = c.Foreign
		cfg.CanaryDomestic = c.Domestic
		cfg.CanaryTimeout = int64(c.Timeout)
		cfg.CanaryConfirmRounds = c.ConfirmRounds
		cfg.CanaryPollInterval = int64(c.PollInterval)
		cfg.MinFallbackDwell = int64(c.MinFallbackDwell)
	}
	return cfg, nil
}

type BurstObservatoryConfig struct {
	SubjectSelector []string `json:"subjectSelector"`
	// health check settings
	HealthCheck *healthCheckSettings `json:"pingConfig,omitempty"`
}

func (b BurstObservatoryConfig) Build() (proto.Message, error) {
	if b.HealthCheck == nil {
		return nil, errors.New("BurstObservatory requires a valid pingConfig")
	}
	if result, err := b.HealthCheck.Build(); err == nil {
		return &burst.Config{SubjectSelector: b.SubjectSelector, PingConfig: result.(*burst.HealthPingConfig)}, nil
	} else {
		return nil, err
	}
}
