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
	KeepPendingOnDown    bool              `json:"keepPendingOnDown"`
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

type HealthReportConfig struct {
	Interval *duration.Duration `json:"interval"`
}

type HealthCostTriggerConfig struct {
	Cost  float32           `json:"cost"`
	After duration.Duration `json:"for"`
}

// HealthProviderTriggersConfig: an explicit zero turns a trigger off, a
// missing field keeps its default.
type HealthProviderTriggersConfig struct {
	Interval        *duration.Duration       `json:"interval"`
	Jitter          *float64                 `json:"jitter"`
	MinHealthy      *int32                   `json:"minHealthy"`
	WhenExhausted   *bool                    `json:"whenExhausted"`
	WhenCostAbove   *HealthCostTriggerConfig `json:"whenCostAbove"`
	MaxAge          *duration.Duration       `json:"maxAge"`
	OnNetworkChange string                   `json:"onNetworkChange"`
	NetworkSettle   duration.Duration        `json:"networkSettle"`
}

type HealthProviderLimitsConfig struct {
	MinInterval          *duration.Duration   `json:"minInterval"`
	ExhaustedMinInterval *duration.Duration   `json:"exhaustedMinInterval"`
	MaxPerHour           *int32               `json:"maxPerHour"`
	Backoff              *HealthBackoffConfig `json:"backoff"`
}

type HealthProviderApplyConfig struct {
	Balancer     string            `json:"balancer"`
	TagPrefix    string            `json:"tagPrefix"`
	Drain        duration.Duration `json:"drain"`
	MaxOutbounds int32             `json:"maxOutbounds"`
	Protocols    StringList        `json:"protocols"`
	Routing      bool              `json:"routing"`
}

type HealthProviderConfig struct {
	Tag      string            `json:"tag"`
	URL      string            `json:"url"`
	Headers  map[string]string `json:"headers"`
	Via      StringList        `json:"via"`
	Timeout  duration.Duration `json:"timeout"`
	MaxBytes int64             `json:"maxBytes"`
	// AllowPlainHTTP permits an http url; headers then travel unencrypted.
	AllowPlainHTTP bool                          `json:"allowPlainHttp"`
	Triggers       *HealthProviderTriggersConfig `json:"triggers"`
	Limits         *HealthProviderLimitsConfig   `json:"limits"`
	Apply          *HealthProviderApplyConfig    `json:"apply"`
}

// optionalDuration maps a JSON duration where an explicit zero turns the
// feature off to the protobuf convention: zero is the default, negative off.
func optionalDuration(d *duration.Duration) int64 {
	switch {
	case d == nil:
		return 0
	case *d == 0:
		return -1
	default:
		return int64(*d)
	}
}

func optionalCount(n *int32) int32 {
	switch {
	case n == nil:
		return 0
	case *n == 0:
		return -1
	default:
		return *n
	}
}

func (c *HealthProviderConfig) build() (*health.ProviderConfig, error) {
	if c.Tag == "" {
		return nil, errors.New("healthObservatory: provider without tag")
	}
	where := "provider " + c.Tag
	if c.URL == "" {
		return nil, errors.New("healthObservatory: ", where, ": url is empty")
	}
	if err := checkProbeURL(where+" url", c.URL); err != nil {
		return nil, err
	}
	u, _ := url.Parse(c.URL)
	if u.User != nil {
		return nil, errors.New("healthObservatory: ", where, ": put credentials into headers, not into the url")
	}
	if u.Scheme != "https" && !c.AllowPlainHTTP {
		return nil, errors.New("healthObservatory: ", where, ": url must be https, or set allowPlainHttp")
	}
	a := c.Apply
	if a == nil || a.Balancer == "" || a.TagPrefix == "" {
		return nil, errors.New("healthObservatory: ", where, ": apply needs balancer and tagPrefix")
	}
	for _, v := range c.Via {
		if v == "" {
			return nil, errors.New("healthObservatory: ", where, ": empty tag in via")
		}
	}
	out := &health.ProviderConfig{
		Tag:      c.Tag,
		Url:      c.URL,
		Headers:  c.Headers,
		Via:      c.Via,
		Timeout:  int64(c.Timeout),
		MaxBytes: c.MaxBytes,
		Apply: &health.ProviderApply{
			Balancer:     a.Balancer,
			TagPrefix:    a.TagPrefix,
			Drain:        int64(a.Drain),
			MaxOutbounds: a.MaxOutbounds,
			Protocols:    a.Protocols,
			Routing:      a.Routing,
		},
	}
	if t := c.Triggers; t != nil {
		out.Triggers = &health.ProviderTriggers{
			Interval:          optionalDuration(t.Interval),
			MinHealthy:        optionalCount(t.MinHealthy),
			SkipWhenExhausted: isFalse(t.WhenExhausted),
			MaxAge:            optionalDuration(t.MaxAge),
			NetworkSettle:     int64(t.NetworkSettle),
		}
		if t.Jitter != nil {
			if *t.Jitter < 0 || *t.Jitter >= 1 {
				return nil, errors.New("healthObservatory: ", where, ": jitter must be in [0, 1)")
			}
			out.Triggers.Jitter = *t.Jitter
			if *t.Jitter == 0 {
				out.Triggers.Jitter = -1
			}
		}
		if ca := t.WhenCostAbove; ca != nil {
			if ca.Cost <= 0 || ca.After < 0 {
				return nil, errors.New("healthObservatory: ", where, ": whenCostAbove needs a cost above zero and a duration not below zero")
			}
			out.Triggers.CostAbove = &health.CostTrigger{Cost: ca.Cost, After: int64(ca.After)}
		}
		switch strings.ToLower(t.OnNetworkChange) {
		case "", "unknownnotcheapest":
			out.Triggers.OnNetworkChange = health.RefreshOnNetwork_REFRESH_UNKNOWN_NOT_CHEAPEST
		case "unknown":
			out.Triggers.OnNetworkChange = health.RefreshOnNetwork_REFRESH_UNKNOWN
		case "any":
			out.Triggers.OnNetworkChange = health.RefreshOnNetwork_REFRESH_ANY
		case "never":
			out.Triggers.OnNetworkChange = health.RefreshOnNetwork_REFRESH_NEVER
		default:
			return nil, errors.New("healthObservatory: ", where, ": unknown onNetworkChange ", t.OnNetworkChange)
		}
	}
	if l := c.Limits; l != nil {
		backoff, err := l.Backoff.build()
		if err != nil {
			return nil, err
		}
		out.Limits = &health.ProviderLimits{
			MinInterval:          optionalDuration(l.MinInterval),
			ExhaustedMinInterval: optionalDuration(l.ExhaustedMinInterval),
			MaxPerHour:           optionalCount(l.MaxPerHour),
			Backoff:              backoff,
		}
	}
	return out, nil
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
	Providers       []*HealthProviderConfig   `json:"providers"`
	Report          *HealthReportConfig       `json:"report"`
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
			KeepPendingOnDown:    p.KeepPendingOnDown,
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
	if r := c.Report; r != nil {
		out.Report = &health.ReportConfig{Interval: optionalDuration(r.Interval)}
	}
	for _, p := range c.Providers {
		if p == nil {
			continue
		}
		pc, err := p.build()
		if err != nil {
			return nil, err
		}
		for _, other := range out.Providers {
			a, b := other.Apply.TagPrefix, pc.Apply.TagPrefix
			if other.Tag == pc.Tag {
				return nil, errors.New("healthObservatory: two providers with tag ", pc.Tag)
			}
			if strings.HasPrefix(a, b) || strings.HasPrefix(b, a) {
				return nil, errors.New("healthObservatory: tag prefixes ", a, " and ", b, " of two providers overlap")
			}
		}
		out.Providers = append(out.Providers, pc)
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
	StartPinnedFor   duration.Duration               `json:"startPinnedFor"`
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
		StartPinnedFor:     int64(v.StartPinnedFor),
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
// Outbounds of providers are observed without being listed as subjects.
func checkLeastCost(routing *RouterConfig, h *HealthObservatoryConfig) error {
	var balancers []*BalancingRule
	if routing != nil {
		balancers = routing.Balancers
	}
	var observed []string
	if h != nil {
		observed = append(observed, h.SubjectSelector...)
		for _, p := range h.Providers {
			if p != nil && p.Apply != nil {
				observed = append(observed, p.Apply.TagPrefix)
			}
		}
	}
	for _, b := range balancers {
		if b == nil || !strings.EqualFold(b.Strategy.Type, strategyLeastCost) {
			continue
		}
		if h == nil {
			return errors.New("balancer ", b.Tag, ": leastCost needs healthObservatory")
		}
		for _, sel := range b.Selectors {
			covered := false
			for _, subject := range observed {
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
	if h == nil {
		return nil
	}
	for _, p := range h.Providers {
		if p == nil || p.Apply == nil {
			continue
		}
		if err := checkProviderBalancer(p, balancers); err != nil {
			return err
		}
	}
	return nil
}

// checkProviderBalancer makes sure the balancer a provider feeds exists and
// picks up the outbounds it adds.
func checkProviderBalancer(p *HealthProviderConfig, balancers []*BalancingRule) error {
	a := p.Apply
	for _, b := range balancers {
		if b == nil || b.Tag != a.Balancer {
			continue
		}
		if !strings.EqualFold(b.Strategy.Type, strategyLeastCost) {
			return errors.New("healthObservatory: provider ", p.Tag, ": balancer ", a.Balancer, " must use leastCost")
		}
		for _, sel := range b.Selectors {
			if strings.HasPrefix(a.TagPrefix, sel) {
				return nil
			}
		}
		return errors.New("healthObservatory: provider ", p.Tag, ": no selector of balancer ", a.Balancer, " matches tagPrefix ", a.TagPrefix)
	}
	return errors.New("healthObservatory: provider ", p.Tag, ": no balancer ", a.Balancer)
}
