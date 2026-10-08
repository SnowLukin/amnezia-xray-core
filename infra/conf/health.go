package conf

import (
	"net/url"
	"regexp"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/infra/conf/cfgcommon/duration"
)

type HealthBackoffConfig struct {
	Initial    duration.Duration `json:"initial"`
	Multiplier float64           `json:"multiplier"`
	Max        duration.Duration `json:"max"`
	Jitter     float64           `json:"jitter"`
}

func (b *HealthBackoffConfig) build() (*health.Backoff, error) {
	if b == nil {
		return nil, nil
	}
	if b.Jitter < 0 || b.Jitter >= 1 {
		return nil, errors.New("healthObservatory: jitter must be in [0, 1)")
	}
	return &health.Backoff{Initial: int64(b.Initial), Multiplier: b.Multiplier, Max: int64(b.Max), Jitter: b.Jitter}, nil
}

type HealthPassiveConfig struct {
	Window               duration.Duration `json:"window"`
	DialTimeout          duration.Duration `json:"dialTimeout"`
	FirstByteTimeout     duration.Duration `json:"firstByteTimeout"`
	StallTimeout         duration.Duration `json:"stallTimeout"`
	Failures             int32             `json:"failures"`
	DistinctTargets      int32             `json:"distinctTargets"`
	SuccessBytes         int64             `json:"successBytes"`
	SweepInterval        duration.Duration `json:"sweepInterval"`
	DialOnly             bool              `json:"dialOnly"`
	PartialStallAge      duration.Duration `json:"partialStallMaxAge"`
	NoPartialStalls      bool              `json:"ignorePartialStalls"`
	StallMinWriteBytes   int64             `json:"stallMinWriteBytes"`
	PartialStallMinBytes int64             `json:"partialStallMinBytes"`
}

type HealthProbeConfig struct {
	Selector        StringList        `json:"selector"`
	Destination     string            `json:"destination"`
	Method          string            `json:"method"`
	ExpectStatus    []int32           `json:"expectStatus"`
	MinBytes        int64             `json:"minBytes"`
	Timeout         duration.Duration `json:"timeout"`
	ProgressTimeout duration.Duration `json:"progressTimeout"`
}

type HealthConnectivityConfig struct {
	Outbound    string               `json:"outbound"`
	Destination string               `json:"destination"`
	Timeout     duration.Duration    `json:"timeout"`
	Recheck     *HealthBackoffConfig `json:"recheck"`
	MaxPause    duration.Duration    `json:"maxPause"`
	Witnesses   StringList           `json:"witnesses"`
}

type HealthScheduleConfig struct {
	PauseWhenIdle          duration.Duration    `json:"pauseWhenIdle"`
	Quarantine             *HealthBackoffConfig `json:"quarantine"`
	Concurrent             int32                `json:"concurrent"`
	Spacing                duration.Duration    `json:"spacing"`
	SpacingJitter          float64              `json:"spacingJitter"`
	PerServerNamePerMinute int32                `json:"perServerNamePerMinute"`
	PerMinute              int32                `json:"perMinute"`
	SuspectCooldown        duration.Duration    `json:"suspectCooldown"`
}

type HealthEventsConfig struct {
	MaxEvents int32 `json:"maxEvents"`
	MaxBytes  int64 `json:"maxBytes"`
}

type HealthNetworkConfig struct {
	Remember    *bool `json:"remember"`
	MaxNetworks int32 `json:"maxNetworks"`
}

// HealthObservatoryConfig is the "healthObservatory" section.
type HealthObservatoryConfig struct {
	SubjectSelector StringList                `json:"subjectSelector"`
	Passive         *HealthPassiveConfig      `json:"passive"`
	Probes          []*HealthProbeConfig      `json:"probes"`
	Connectivity    *HealthConnectivityConfig `json:"connectivity"`
	Schedule        *HealthScheduleConfig     `json:"schedule"`
	Events          *HealthEventsConfig       `json:"events"`
	Network         *HealthNetworkConfig      `json:"network"`
}

func checkSelectors(name string, selectors []string) error {
	for _, s := range selectors {
		if s == "" {
			return errors.New("healthObservatory: empty prefix in ", name, " matches every outbound")
		}
	}
	return nil
}

func checkProbeURL(where, dest string) error {
	if dest == "" {
		return nil
	}
	u, err := url.Parse(dest)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New("healthObservatory: ", where, " must be an http or https URL: ", dest)
	}
	return nil
}

func (c *HealthObservatoryConfig) Build() (proto.Message, error) {
	if len(c.SubjectSelector) == 0 {
		return nil, errors.New("healthObservatory: subjectSelector is empty")
	}
	if err := checkSelectors("subjectSelector", c.SubjectSelector); err != nil {
		return nil, err
	}
	out := &health.Config{SubjectSelector: c.SubjectSelector}
	if p := c.Passive; p != nil {
		out.Passive = &health.PassiveConfig{
			Window:               int64(p.Window),
			DialTimeout:          int64(p.DialTimeout),
			FirstByteTimeout:     int64(p.FirstByteTimeout),
			StallTimeout:         int64(p.StallTimeout),
			Failures:             p.Failures,
			DistinctTargets:      p.DistinctTargets,
			SuccessBytes:         p.SuccessBytes,
			SweepInterval:        int64(p.SweepInterval),
			DialOnly:             p.DialOnly,
			PartialStallMaxAge:   int64(p.PartialStallAge),
			IgnorePartialStalls:  p.NoPartialStalls,
			StallMinWriteBytes:   p.StallMinWriteBytes,
			PartialStallMinBytes: p.PartialStallMinBytes,
		}
	}
	for _, p := range c.Probes {
		if p == nil {
			continue
		}
		if err := checkProbeURL("probe destination", p.Destination); err != nil {
			return nil, err
		}
		if err := checkSelectors("probe selector", p.Selector); err != nil {
			return nil, err
		}
		switch strings.ToUpper(p.Method) {
		case "", "GET":
		case "HEAD":
			if p.MinBytes > 0 {
				return nil, errors.New("healthObservatory: a HEAD probe has no body for minBytes")
			}
		default:
			return nil, errors.New("healthObservatory: probe method must be GET or HEAD")
		}
		out.Probes = append(out.Probes, &health.ProbeProfile{
			Selector:        p.Selector,
			Destination:     p.Destination,
			Method:          p.Method,
			ExpectStatus:    p.ExpectStatus,
			MinBytes:        p.MinBytes,
			Timeout:         int64(p.Timeout),
			ProgressTimeout: int64(p.ProgressTimeout),
		})
	}
	if cc := c.Connectivity; cc != nil {
		if cc.Outbound == "" || cc.Destination == "" {
			return nil, errors.New("healthObservatory: connectivity needs outbound and destination")
		}
		if err := checkProbeURL("connectivity destination", cc.Destination); err != nil {
			return nil, err
		}
		if err := checkSelectors("connectivity witnesses", cc.Witnesses); err != nil {
			return nil, err
		}
		recheck, err := cc.Recheck.build()
		if err != nil {
			return nil, err
		}
		out.Connectivity = &health.ConnectivityConfig{
			Outbound:    cc.Outbound,
			Destination: cc.Destination,
			Timeout:     int64(cc.Timeout),
			Recheck:     recheck,
			MaxPause:    int64(cc.MaxPause),
			Witnesses:   cc.Witnesses,
		}
	}
	if s := c.Schedule; s != nil {
		quarantine, err := s.Quarantine.build()
		if err != nil {
			return nil, err
		}
		if s.SpacingJitter < 0 || s.SpacingJitter >= 1 {
			return nil, errors.New("healthObservatory: spacingJitter must be in [0, 1)")
		}
		out.Schedule = &health.ScheduleConfig{
			PauseWhenIdle:          int64(s.PauseWhenIdle),
			Quarantine:             quarantine,
			Concurrent:             s.Concurrent,
			Spacing:                int64(s.Spacing),
			SpacingJitter:          s.SpacingJitter,
			PerServerNamePerMinute: s.PerServerNamePerMinute,
			PerMinute:              s.PerMinute,
			SuspectCooldown:        int64(s.SuspectCooldown),
		}
	}
	if e := c.Events; e != nil {
		out.Events = &health.EventsConfig{MaxEvents: e.MaxEvents, MaxBytes: e.MaxBytes}
	}
	if n := c.Network; n != nil {
		out.Network = &health.NetworkConfig{NoMemory: n.Remember != nil && !*n.Remember, MaxNetworks: n.MaxNetworks}
	}
	return out, nil
}

type strategyLeastCostUpgradeConfig struct {
	Interval            duration.Duration `json:"interval"`
	RandomPhase         *bool             `json:"randomPhase"`
	Successes           int32             `json:"successes"`
	Spacing             duration.Duration `json:"spacing"`
	PenaltyWindow       duration.Duration `json:"penaltyWindow"`
	PenaltyMultiplier   float64           `json:"penaltyMultiplier"`
	MaxPenalty          duration.Duration `json:"maxPenalty"`
	OnNetworkChange     *bool             `json:"onNetworkChange"`
	UnknownNetworksOnly *bool             `json:"unknownNetworksOnly"`
}

type strategyLeastCostConfig struct {
	Costs            []*router.StrategyWeight        `json:"costs"`
	DefaultCost      float32                         `json:"defaultCost"`
	Lazy             StringList                      `json:"lazy"`
	SwitchAfterProbe *bool                           `json:"switchAfterProbe"`
	Confirm          string                          `json:"confirm"`
	MinHold          duration.Duration               `json:"minHold"`
	Upgrade          *strategyLeastCostUpgradeConfig `json:"upgrade"`
	StartPinned      string                          `json:"startPinned"`
	StartWith        string                          `json:"startWith"`
	OnNetworkChange  string                          `json:"onNetworkChange"`
}

// isFalse is true only for an explicit false; the JSON defaults of these
// options are true.
func isFalse(v *bool) bool {
	return v != nil && !*v
}

// Build implements Buildable.
func (v *strategyLeastCostConfig) Build() (proto.Message, error) {
	if err := checkSelectors("lazy", v.Lazy); err != nil {
		return nil, err
	}
	for _, c := range v.Costs {
		// A zero value means "weight from the number in the tag" to the
		// shared weight code, which here would make the outbound the most
		// expensive one.
		if c == nil || c.Match == "" || c.Value <= 0 {
			return nil, errors.New("leastCost: every cost needs a match and a value above zero")
		}
		if c.Regexp {
			if _, err := regexp.Compile(c.Match); err != nil {
				return nil, errors.New("leastCost: cost match ", c.Match).Base(err)
			}
		}
	}
	out := &health.SelectorConfig{
		Costs:              v.Costs,
		DefaultCost:        v.DefaultCost,
		Lazy:               v.Lazy,
		SwitchWithoutProbe: isFalse(v.SwitchAfterProbe),
		MinHold:            int64(v.MinHold),
		StartPinned:        v.StartPinned,
		StartWith:          v.StartWith,
	}
	switch strings.ToLower(v.Confirm) {
	case "", "auto":
		out.Confirm = health.ConfirmMode_AUTO
	case "always":
		out.Confirm = health.ConfirmMode_ALWAYS
	case "never":
		out.Confirm = health.ConfirmMode_NEVER
	default:
		return nil, errors.New("leastCost: unknown confirm mode ", v.Confirm)
	}
	switch strings.ToLower(v.OnNetworkChange) {
	case "", "recheck":
		out.OnNetworkChange = health.NetworkChangeMode_RECHECK
	case "forget":
		out.OnNetworkChange = health.NetworkChangeMode_FORGET
	case "ignore":
		out.OnNetworkChange = health.NetworkChangeMode_IGNORE
	default:
		return nil, errors.New("leastCost: unknown onNetworkChange mode ", v.OnNetworkChange)
	}
	if u := v.Upgrade; u != nil {
		out.Upgrade = &health.UpgradeConfig{
			Interval:            int64(u.Interval),
			FixedPhase:          isFalse(u.RandomPhase),
			Successes:           u.Successes,
			Spacing:             int64(u.Spacing),
			PenaltyWindow:       int64(u.PenaltyWindow),
			PenaltyMultiplier:   u.PenaltyMultiplier,
			MaxPenalty:          int64(u.MaxPenalty),
			SkipOnNetworkChange: isFalse(u.OnNetworkChange),
			AllNetworks:         isFalse(u.UnknownNetworksOnly),
		}
	}
	return out, nil
}

// checkLeastCost makes sure every outbound a leastCost balancer may choose is
// observed: the balancer only moves away from an outbound on evidence.
func checkLeastCost(routing *RouterConfig, h *HealthObservatoryConfig) error {
	if routing == nil {
		return nil
	}
	for _, b := range routing.Balancers {
		if b == nil || !strings.EqualFold(b.Strategy.Type, strategyLeastCost) {
			continue
		}
		if h == nil {
			return errors.New("balancer ", b.Tag, ": leastCost needs healthObservatory")
		}
		for _, sel := range b.Selectors {
			covered := false
			for _, subject := range h.SubjectSelector {
				if strings.HasPrefix(sel, subject) {
					covered = true
					break
				}
			}
			if !covered {
				return errors.New("balancer ", b.Tag, ": selector ", sel, " is not covered by healthObservatory.subjectSelector")
			}
		}
	}
	return nil
}
