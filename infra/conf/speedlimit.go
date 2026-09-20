package conf

import (
	"math"
	"strings"

	"github.com/xtls/xray-core/app/dispatcher"
	"github.com/xtls/xray-core/common/errors"
)

// SpeedLimitUserRate is a per-user override in kbps. 0 means unlimited for that direction.
type SpeedLimitUserRate struct {
	DownKbps *int64 `json:"downKbps"`
	UpKbps   *int64 `json:"upKbps"`
}

// SpeedLimitConfig is the top-level Xray JSON object "speedLimit".
type SpeedLimitConfig struct {
	Enabled         bool                           `json:"enabled"`
	DefaultDownKbps *int64                         `json:"defaultDownKbps"`
	DefaultUpKbps   *int64                         `json:"defaultUpKbps"`
	Unlimited       []string                       `json:"unlimited"`
	Overrides       map[string]*SpeedLimitUserRate `json:"overrides"`
}

func parseKbps(v *int64, name string) (uint32, error) {
	if v == nil {
		return 0, nil
	}
	if *v < 0 {
		return 0, errors.New(name, " must not be negative")
	}
	if *v > math.MaxUint32 {
		return 0, errors.New(name, " is too large")
	}
	return uint32(*v), nil
}

// Build implements Buildable.
func (c *SpeedLimitConfig) Build() (*dispatcher.SpeedLimit, error) {
	if c == nil {
		return nil, nil
	}
	down, err := parseKbps(c.DefaultDownKbps, "speedLimit.defaultDownKbps")
	if err != nil {
		return nil, err
	}
	up, err := parseKbps(c.DefaultUpKbps, "speedLimit.defaultUpKbps")
	if err != nil {
		return nil, err
	}

	unlimited := make([]string, 0, len(c.Unlimited))
	for _, id := range c.Unlimited {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		unlimited = append(unlimited, id)
	}

	overrides := make(map[string]*dispatcher.UserRate, len(c.Overrides))
	for key, rate := range c.Overrides {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, errors.New("speedLimit.overrides contains an empty key")
		}
		if rate == nil {
			overrides[key] = &dispatcher.UserRate{}
			continue
		}
		od, err := parseKbps(rate.DownKbps, "speedLimit.overrides."+key+".downKbps")
		if err != nil {
			return nil, err
		}
		ou, err := parseKbps(rate.UpKbps, "speedLimit.overrides."+key+".upKbps")
		if err != nil {
			return nil, err
		}
		overrides[key] = &dispatcher.UserRate{DownKbps: od, UpKbps: ou}
	}

	return &dispatcher.SpeedLimit{
		Enabled:         c.Enabled,
		DefaultDownKbps: down,
		DefaultUpKbps:   up,
		Unlimited:       unlimited,
		Overrides:       overrides,
	}, nil
}
