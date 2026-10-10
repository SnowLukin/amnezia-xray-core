package conf

import (
	"bytes"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/infra/conf/cfgcommon/duration"
)

type HealthBackoffConfig struct {
	InitialDelay   *duration.Duration `json:"initialDelay"`
	Multiplier     float64            `json:"multiplier"`
	MaxDelay       *duration.Duration `json:"maxDelay"`
	JitterFraction float64            `json:"jitterFraction"`
}

func (b *HealthBackoffConfig) build() (*health.Backoff, error) {
	if b == nil {
		return nil, nil
	}
	if b.JitterFraction < 0 || b.JitterFraction >= 1 {
		return nil, errors.New("healthObservatory: jitterFraction must be in [0, 1)")
	}
	return &health.Backoff{InitialDelay: optionalDuration(b.InitialDelay), Multiplier: b.Multiplier, MaxDelay: optionalDuration(b.MaxDelay), JitterFraction: b.JitterFraction}, nil
}

type HealthPassiveConfig struct {
	Window                    *duration.Duration `json:"window"`
	DialTimeout               *duration.Duration `json:"dialTimeout"`
	FirstByteTimeout          *duration.Duration `json:"firstByteTimeout"`
	StallTimeout              *duration.Duration `json:"stallTimeout"`
	FailuresToSuspect         int32              `json:"failuresToSuspect"`
	MinDistinctTargets        int32              `json:"minDistinctTargets"`
	SuccessBytes              int64              `json:"successBytes"`
	SweepInterval             *duration.Duration `json:"sweepInterval"`
	IsDialOnly                bool               `json:"isDialOnly"`
	PartialStallAge           *duration.Duration `json:"partialStallMaxAge"`
	ShouldIgnorePartialStalls bool               `json:"shouldIgnorePartialStalls"`
	StallMinWriteBytes        int64              `json:"stallMinWriteBytes"`
	PartialStallMinBytes      int64              `json:"partialStallMinBytes"`
	ShouldKeepPendingOnDown   bool               `json:"shouldKeepPendingOnDown"`
	ShouldKeepSilentOnDown    bool               `json:"shouldKeepSilentOnDown"`
}

type HealthProbeConfig struct {
	Selector         StringList         `json:"selector"`
	Destination      string             `json:"destination"`
	Method           string             `json:"method"`
	ExpectedStatuses []int32            `json:"expectedStatuses"`
	MinBytes         int64              `json:"minBytes"`
	Timeout          *duration.Duration `json:"timeout"`
	ProgressTimeout  *duration.Duration `json:"progressTimeout"`
}

type HealthConnectivityConfig struct {
	Outbound       string               `json:"outbound"`
	Destination    string               `json:"destination"`
	Timeout        *duration.Duration   `json:"timeout"`
	RecheckBackoff *HealthBackoffConfig `json:"recheckBackoff"`
	MaxPause       *duration.Duration   `json:"maxPause"`
	Witnesses      StringList           `json:"witnesses"`
}

type HealthScheduleConfig struct {
	IdleAfter                    *duration.Duration      `json:"idleAfter"`
	Quarantine                   *HealthBackoffConfig    `json:"quarantine"`
	MaxConcurrentProbes          int32                   `json:"maxConcurrentProbes"`
	MinProbeInterval             *duration.Duration      `json:"minProbeInterval"`
	ProbeJitterFraction          float64                 `json:"probeJitterFraction"`
	ProbesPerServerNamePerMinute int32                   `json:"probesPerServerNamePerMinute"`
	ProbesPerMinute              int32                   `json:"probesPerMinute"`
	SuspectCooldown              *duration.Duration      `json:"suspectCooldown"`
	HostProbes                   *HealthHostProbesConfig `json:"hostProbes"`
}

type HealthHostProbesConfig struct {
	ProbesPerMinute int32              `json:"probesPerMinute"`
	ReuseWindow     *duration.Duration `json:"reuseWindow"`
	MaxOutbounds    int32              `json:"maxOutbounds"`
	Timeout         *duration.Duration `json:"timeout"`
}

type HealthEventsConfig struct {
	MaxEvents int32 `json:"maxEvents"`
	MaxBytes  int64 `json:"maxBytes"`
}

type HealthNetworkConfig struct {
	ShouldRememberNetworks *bool `json:"shouldRememberNetworks"`
	MaxNetworks            int32 `json:"maxNetworks"`
}

type HealthReportConfig struct {
	Interval *duration.Duration `json:"interval"`
}

type HealthSleepConfig struct {
	AfterLastUse *duration.Duration `json:"afterLastUse"`
	// CloseIdleAfter: "0s" or missing keeps the connections of an outbound
	// that can sleep whatever their silence, the default.
	CloseIdleAfter *duration.Duration `json:"closeIdleAfter"`
}

type HealthCostTriggerConfig struct {
	Cost  float32            `json:"cost"`
	After *duration.Duration `json:"after"`
}

// HealthProviderTriggersConfig: an explicit zero turns a trigger off, a
// missing field keeps its default.
type HealthProviderTriggersConfig struct {
	Interval                   *duration.Duration       `json:"interval"`
	JitterFraction             *float64                 `json:"jitterFraction"`
	MinHealthyOutbounds        *int32                   `json:"minHealthyOutbounds"`
	ShouldRefreshWhenExhausted *bool                    `json:"shouldRefreshWhenExhausted"`
	WhenCostAbove              *HealthCostTriggerConfig `json:"whenCostAbove"`
	MaxAge                     *duration.Duration       `json:"maxAge"`
	NetworkChangeMode          string                   `json:"networkChangeMode"`
	NetworkSettleDelay         *duration.Duration       `json:"networkSettleDelay"`
	Expiry                     *HealthExpiryConfig      `json:"expiry"`
}

// HealthExpiryConfig renews provider leases: shares of a lease, where an
// explicit renewAt of 0 turns renewal off and an explicit jitter of 0 is
// none.
type HealthExpiryConfig struct {
	RenewAtFraction  *float64 `json:"renewAtFraction"`
	UrgentAtFraction *float64 `json:"urgentAtFraction"`
	JitterFraction   *float64 `json:"jitterFraction"`
}

func (c *HealthExpiryConfig) build(where string) (*health.ExpiryTrigger, error) {
	if c == nil {
		return nil, nil
	}
	out := &health.ExpiryTrigger{}
	renewAt := 0.5
	if c.RenewAtFraction != nil {
		renewAt = *c.RenewAtFraction
		if renewAt < 0 || renewAt > 0.95 {
			return nil, errors.New("healthObservatory: ", where, ": expiry.renewAtFraction must be in [0, 0.95]")
		}
		out.RenewAtFraction = renewAt
		if renewAt == 0 {
			out.RenewAtFraction = -1
		}
	}
	if c.UrgentAtFraction != nil {
		if *c.UrgentAtFraction < renewAt || *c.UrgentAtFraction > 0.95 {
			return nil, errors.New("healthObservatory: ", where, ": expiry.urgentAtFraction must be in [renewAt, 0.95]")
		}
		out.UrgentAtFraction = *c.UrgentAtFraction
	}
	if c.JitterFraction != nil {
		if *c.JitterFraction < 0 || *c.JitterFraction > 0.5 {
			return nil, errors.New("healthObservatory: ", where, ": expiry.jitterFraction must be in [0, 0.5]")
		}
		out.JitterFraction = *c.JitterFraction
		if *c.JitterFraction == 0 {
			out.JitterFraction = -1
		}
	}
	return out, nil
}

type HealthProviderLimitsConfig struct {
	MinInterval          *duration.Duration   `json:"minInterval"`
	ExhaustedMinInterval *duration.Duration   `json:"exhaustedMinInterval"`
	MaxRefreshesPerHour  *int32               `json:"maxRefreshesPerHour"`
	Backoff              *HealthBackoffConfig `json:"backoff"`
	// AuthRetryDelay: "0s" waits for new headers after the backend refused them.
	AuthRetryDelay *duration.Duration `json:"authRetryDelay"`
}

type HealthProviderApplyConfig struct {
	Balancer          string             `json:"balancer"`
	TagPrefix         string             `json:"tagPrefix"`
	DrainTimeout      *duration.Duration `json:"drainTimeout"`
	MaxOutbounds      int32              `json:"maxOutbounds"`
	AllowedProtocols  StringList         `json:"allowedProtocols"`
	IsRoutingAccepted bool               `json:"isRoutingAccepted"`
	// Headers: an explicit [] lets no answer set a header.
	AcceptedHeaders *StringList `json:"acceptedHeaders"`
}

type HealthProviderConfig struct {
	Tag      string             `json:"tag"`
	URL      string             `json:"url"`
	Method   string             `json:"method"`
	Headers  map[string]string  `json:"headers"`
	Via      StringList         `json:"via"`
	Timeout  *duration.Duration `json:"timeout"`
	MaxBytes int64              `json:"maxBytes"`
	// IsPlainHTTPAllowed permits an http url; headers then travel unencrypted.
	IsPlainHTTPAllowed bool                          `json:"isPlainHttpAllowed"`
	Triggers           *HealthProviderTriggersConfig `json:"triggers"`
	Limits             *HealthProviderLimitsConfig   `json:"limits"`
	Apply              *HealthProviderApplyConfig    `json:"apply"`
	// Initial is an answer applied at the start. It is passed on as it is:
	// a bad saved set must not keep the core from starting, so the
	// observatory checks all of it, its form and size too, reports a set it
	// refuses and starts without it.
	InitialAnswer json.RawMessage `json:"initialAnswer"`
	// BaseVersion names this config as the backend that generated it did.
	BaseVersion string `json:"baseVersion"`
}

// maxBaseVersion bounds baseVersion, as the observatory bounds the one of
// an answer.
const maxBaseVersion = 256

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

func requirePositiveDuration(key string, value *duration.Duration) error {
	if value != nil && *value <= 0 {
		return errors.New(key, " must be greater than 0s")
	}
	return nil
}

func (c *HealthProviderConfig) build() (*health.ProviderConfig, error) {
	if c.Tag == "" {
		return nil, errors.New("healthObservatory: provider without tag")
	}
	where := "provider " + c.Tag
	if err := requirePositiveDuration("healthObservatory: "+where+" timeout", c.Timeout); err != nil {
		return nil, err
	}
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
	if u.Scheme != "https" && !c.IsPlainHTTPAllowed {
		return nil, errors.New("healthObservatory: ", where, ": url must be https, or set isPlainHttpAllowed")
	}
	// Empty stays empty: the observatory takes it for POST.
	method := strings.ToUpper(c.Method)
	if method != "" && method != "GET" && method != "POST" {
		return nil, errors.New("healthObservatory: ", where, ": method must be GET or POST")
	}
	if len(c.BaseVersion) > maxBaseVersion {
		return nil, errors.New("healthObservatory: ", where, ": baseVersion is longer than 256 bytes")
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
	if a.AcceptedHeaders != nil {
		for _, name := range *a.AcceptedHeaders {
			if err := health.CheckHeaderName(name); err != nil {
				return nil, errors.New("healthObservatory: ", where, ": apply.acceptedHeaders: ", err.Error())
			}
		}
	}
	out := &health.ProviderConfig{
		Tag:         c.Tag,
		Url:         c.URL,
		Method:      method,
		Headers:     c.Headers,
		Via:         c.Via,
		Timeout:     optionalDuration(c.Timeout),
		MaxBytes:    c.MaxBytes,
		BaseVersion: c.BaseVersion,
		Apply: &health.ProviderApply{
			Balancer:          a.Balancer,
			TagPrefix:         a.TagPrefix,
			DrainTimeout:      optionalDuration(a.DrainTimeout),
			MaxOutbounds:      a.MaxOutbounds,
			AllowedProtocols:  a.AllowedProtocols,
			IsRoutingAccepted: a.IsRoutingAccepted,
		},
	}
	if a.AcceptedHeaders != nil {
		out.Apply.AcceptedHeaders = *a.AcceptedHeaders
		out.Apply.FixedHeaders = len(*a.AcceptedHeaders) == 0
	}
	if raw := bytes.TrimSpace(c.InitialAnswer); len(raw) > 0 && !bytes.Equal(raw, []byte("null")) {
		out.InitialAnswer = raw
	}
	if t := c.Triggers; t != nil {
		out.Triggers = &health.ProviderTriggers{
			Interval:                 optionalDuration(t.Interval),
			MinHealthyOutbounds:      optionalCount(t.MinHealthyOutbounds),
			SkipRefreshWhenExhausted: isFalse(t.ShouldRefreshWhenExhausted),
			MaxAge:                   optionalDuration(t.MaxAge),
			NetworkSettleDelay:       optionalDuration(t.NetworkSettleDelay),
		}
		if t.JitterFraction != nil {
			if *t.JitterFraction < 0 || *t.JitterFraction >= 1 {
				return nil, errors.New("healthObservatory: ", where, ": jitter must be in [0, 1)")
			}
			out.Triggers.JitterFraction = *t.JitterFraction
			if *t.JitterFraction == 0 {
				out.Triggers.JitterFraction = -1
			}
		}
		expiry, err := t.Expiry.build(where)
		if err != nil {
			return nil, err
		}
		out.Triggers.Expiry = expiry
		if ca := t.WhenCostAbove; ca != nil {
			if ca.Cost <= 0 || (ca.After != nil && *ca.After < 0) {
				return nil, errors.New("healthObservatory: ", where, ": whenCostAbove needs a cost above zero and a duration not below zero")
			}
			out.Triggers.WhenCostAbove = &health.CostTrigger{Cost: ca.Cost, After: optionalDuration(ca.After)}
		}
		switch strings.ToLower(t.NetworkChangeMode) {
		case "", "unknownnotcheapest":
			out.Triggers.NetworkChangeMode = health.RefreshOnNetwork_REFRESH_UNKNOWN_NOT_CHEAPEST
		case "unknown":
			out.Triggers.NetworkChangeMode = health.RefreshOnNetwork_REFRESH_UNKNOWN
		case "any":
			out.Triggers.NetworkChangeMode = health.RefreshOnNetwork_REFRESH_ANY
		case "never":
			out.Triggers.NetworkChangeMode = health.RefreshOnNetwork_REFRESH_NEVER
		default:
			return nil, errors.New("healthObservatory: ", where, ": unknown networkChangeMode ", t.NetworkChangeMode)
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
			MaxRefreshesPerHour:  optionalCount(l.MaxRefreshesPerHour),
			Backoff:              backoff,
			AuthRetryDelay:       optionalDuration(l.AuthRetryDelay),
		}
		// Shorter, a refused token loads the backend for little; longer, a
		// mistake of the backend keeps the fleet without fresh sets.
		if r := l.AuthRetryDelay; r != nil && *r != 0 && (*r < duration.Duration(5*time.Minute) || *r > duration.Duration(24*time.Hour)) {
			return nil, errors.New("healthObservatory: ", where, ": limits.authRetryDelay must be 0s or in [5m, 24h]")
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
	Sleep           *HealthSleepConfig        `json:"sleep"`
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
		for _, field := range []struct {
			name  string
			value *duration.Duration
		}{{"passive.dialTimeout", p.DialTimeout}, {"passive.firstByteTimeout", p.FirstByteTimeout}, {"passive.stallTimeout", p.StallTimeout}} {
			if err := requirePositiveDuration("healthObservatory: "+field.name, field.value); err != nil {
				return nil, err
			}
		}
		out.Passive = &health.PassiveConfig{
			Window:                    optionalDuration(p.Window),
			DialTimeout:               optionalDuration(p.DialTimeout),
			FirstByteTimeout:          optionalDuration(p.FirstByteTimeout),
			StallTimeout:              optionalDuration(p.StallTimeout),
			FailuresToSuspect:         p.FailuresToSuspect,
			MinDistinctTargets:        p.MinDistinctTargets,
			SuccessBytes:              p.SuccessBytes,
			SweepInterval:             optionalDuration(p.SweepInterval),
			IsDialOnly:                p.IsDialOnly,
			PartialStallMaxAge:        optionalDuration(p.PartialStallAge),
			ShouldIgnorePartialStalls: p.ShouldIgnorePartialStalls,
			StallMinWriteBytes:        p.StallMinWriteBytes,
			PartialStallMinBytes:      p.PartialStallMinBytes,
			ShouldKeepPendingOnDown:   p.ShouldKeepPendingOnDown,
			ShouldKeepSilentOnDown:    p.ShouldKeepSilentOnDown,
		}
	}
	for _, p := range c.Probes {
		if p == nil {
			continue
		}
		for _, field := range []struct {
			name  string
			value *duration.Duration
		}{{"probes[].timeout", p.Timeout}, {"probes[].progressTimeout", p.ProgressTimeout}} {
			if err := requirePositiveDuration("healthObservatory: "+field.name, field.value); err != nil {
				return nil, err
			}
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
			Selector:         p.Selector,
			Destination:      p.Destination,
			Method:           p.Method,
			ExpectedStatuses: p.ExpectedStatuses,
			MinBytes:         p.MinBytes,
			Timeout:          optionalDuration(p.Timeout),
			ProgressTimeout:  optionalDuration(p.ProgressTimeout),
		})
	}
	if cc := c.Connectivity; cc != nil {
		if err := requirePositiveDuration("healthObservatory: connectivity.timeout", cc.Timeout); err != nil {
			return nil, err
		}
		if cc.Outbound == "" || cc.Destination == "" {
			return nil, errors.New("healthObservatory: connectivity needs outbound and destination")
		}
		if err := checkProbeURL("connectivity destination", cc.Destination); err != nil {
			return nil, err
		}
		if err := checkSelectors("connectivity witnesses", cc.Witnesses); err != nil {
			return nil, err
		}
		recheck, err := cc.RecheckBackoff.build()
		if err != nil {
			return nil, err
		}
		out.Connectivity = &health.ConnectivityConfig{
			Outbound:       cc.Outbound,
			Destination:    cc.Destination,
			Timeout:        optionalDuration(cc.Timeout),
			RecheckBackoff: recheck,
			MaxPause:       optionalDuration(cc.MaxPause),
			Witnesses:      cc.Witnesses,
		}
	}
	if s := c.Schedule; s != nil {
		if s.HostProbes != nil {
			if err := requirePositiveDuration("healthObservatory: schedule.hostProbes.timeout", s.HostProbes.Timeout); err != nil {
				return nil, err
			}
		}
		quarantine, err := s.Quarantine.build()
		if err != nil {
			return nil, err
		}
		if s.ProbeJitterFraction < 0 || s.ProbeJitterFraction >= 1 {
			return nil, errors.New("healthObservatory: probeJitterFraction must be in [0, 1)")
		}
		out.Schedule = &health.ScheduleConfig{
			IdleAfter:                    optionalDuration(s.IdleAfter),
			Quarantine:                   quarantine,
			MaxConcurrentProbes:          s.MaxConcurrentProbes,
			MinProbeInterval:             optionalDuration(s.MinProbeInterval),
			ProbeJitterFraction:          s.ProbeJitterFraction,
			ProbesPerServerNamePerMinute: s.ProbesPerServerNamePerMinute,
			ProbesPerMinute:              s.ProbesPerMinute,
			SuspectCooldown:              optionalDuration(s.SuspectCooldown),
		}
		if hp := s.HostProbes; hp != nil {
			out.Schedule.HostProbes = &health.HostProbesConfig{
				ProbesPerMinute: hp.ProbesPerMinute,
				ReuseWindow:     optionalDuration(hp.ReuseWindow),
				MaxOutbounds:    hp.MaxOutbounds,
				Timeout:         optionalDuration(hp.Timeout),
			}
		}
	}
	if e := c.Events; e != nil {
		out.Events = &health.EventsConfig{MaxEvents: e.MaxEvents, MaxBytes: e.MaxBytes}
	}
	if n := c.Network; n != nil {
		out.Network = &health.NetworkConfig{NoMemory: n.ShouldRememberNetworks != nil && !*n.ShouldRememberNetworks, MaxNetworks: n.MaxNetworks}
	}
	if r := c.Report; r != nil {
		out.Report = &health.ReportConfig{Interval: optionalDuration(r.Interval)}
	}
	if s := c.Sleep; s != nil {
		// Sooner, a connection that just ended may still be closing inside
		// the tunnel that sleep releases.
		if a := s.AfterLastUse; a != nil && *a != 0 && *a < duration.Duration(10*time.Second) {
			return nil, errors.New("healthObservatory: sleep.afterLastUse must be 0s or at least 10s")
		}
		out.Sleep = &health.SleepConfig{AfterLastUse: optionalDuration(s.AfterLastUse)}
		// Shorter, a connection between two requests would count as idle;
		// longer, the observer may have stopped following it.
		if a := s.CloseIdleAfter; a != nil && *a != 0 {
			if *a < duration.Duration(10*time.Second) || *a > duration.Duration(10*time.Minute) {
				return nil, errors.New("healthObservatory: sleep.closeIdleAfter must be 0s or in [10s, 10m]")
			}
			out.Sleep.CloseIdleAfter = int64(*a)
		}
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

type strategyLeastCostReturnToCheaperConfig struct {
	Interval                 *duration.Duration `json:"interval"`
	HasRandomPhase           *bool              `json:"hasRandomPhase"`
	ProbesToReturn           int32              `json:"probesToReturn"`
	ProbeInterval            *duration.Duration `json:"probeInterval"`
	PenaltyWindow            *duration.Duration `json:"penaltyWindow"`
	PenaltyMultiplier        float64            `json:"penaltyMultiplier"`
	MaxPenaltyDelay          *duration.Duration `json:"maxPenaltyDelay"`
	ShouldTryOnNetworkChange *bool              `json:"shouldTryOnNetworkChange"`
	IsUnknownNetworksOnly    *bool              `json:"isUnknownNetworksOnly"`
}

type strategyCostGroupConfig struct {
	FailedInRowToLeave int32 `json:"failedInRowToLeave"`
}

type strategyCostEntry struct {
	Match          string                   `json:"match"`
	IsRegexp       bool                     `json:"isRegexp"`
	Cost           float32                  `json:"cost"`
	ReturnInterval *duration.Duration       `json:"returnInterval"`
	Group          *strategyCostGroupConfig `json:"group"`
}

type strategyLeastCostConfig struct {
	Costs                   []*strategyCostEntry                    `json:"costs"`
	DefaultCost             float32                                 `json:"defaultCost"`
	LazyOutbounds           StringList                              `json:"lazyOutbounds"`
	ShouldProbeBeforeSwitch *bool                                   `json:"shouldProbeBeforeSwitch"`
	ConfirmWithProbe        string                                  `json:"confirmWithProbe"`
	MinHoldTime             *duration.Duration                      `json:"minHoldTime"`
	ReturnToCheaper         *strategyLeastCostReturnToCheaperConfig `json:"returnToCheaper"`
	StartPinnedOutbound     string                                  `json:"startPinnedOutbound"`
	StartPinTimeout         *duration.Duration                      `json:"startPinTimeout"`
	StartOutbound           string                                  `json:"startOutbound"`
	NetworkChangeAction     string                                  `json:"networkChangeAction"`
}

// isFalse is true only for an explicit false; the JSON defaults of these
// options are true.
func isFalse(v *bool) bool {
	return v != nil && !*v
}

// Build implements Buildable.
func (v *strategyLeastCostConfig) Build() (proto.Message, error) {
	if v.ReturnToCheaper != nil {
		if err := requirePositiveDuration("leastCost: returnToCheaper.interval", v.ReturnToCheaper.Interval); err != nil {
			return nil, err
		}
	}
	if err := checkSelectors("lazyOutbounds", v.LazyOutbounds); err != nil {
		return nil, err
	}
	for _, c := range v.Costs {
		// A zero value means "weight from the number in the tag" to the
		// shared weight code, which here would make the outbound the most
		// expensive one.
		if c == nil || c.Match == "" || c.Cost <= 0 {
			return nil, errors.New("leastCost: every cost needs a match and a cost above zero")
		}
		if c.IsRegexp {
			if _, err := regexp.Compile(c.Match); err != nil {
				return nil, errors.New("leastCost: cost match ", c.Match).Base(err)
			}
		}
	}
	costs := make([]*health.CostEntry, 0, len(v.Costs))
	for _, c := range v.Costs {
		entry := &health.CostEntry{Match: c.Match, IsRegexp: c.IsRegexp, Cost: c.Cost}
		if c.ReturnInterval != nil {
			entry.ReturnInterval = int64(*c.ReturnInterval)
		}
		if c.Group != nil {
			entry.Group = &health.CostGroupConfig{FailedInRowToLeave: c.Group.FailedInRowToLeave}
		}
		costs = append(costs, entry)
	}
	out := &health.SelectorConfig{
		Costs:                 costs,
		DefaultCost:           v.DefaultCost,
		LazyOutbounds:         v.LazyOutbounds,
		SkipProbeBeforeSwitch: isFalse(v.ShouldProbeBeforeSwitch),
		MinHoldTime:           optionalDuration(v.MinHoldTime),
		StartPinnedOutbound:   v.StartPinnedOutbound,
		StartPinTimeout:       optionalDuration(v.StartPinTimeout),
		StartOutbound:         v.StartOutbound,
	}
	switch strings.ToLower(v.ConfirmWithProbe) {
	case "", "auto":
		out.ConfirmWithProbe = health.ConfirmMode_AUTO
	case "always":
		out.ConfirmWithProbe = health.ConfirmMode_ALWAYS
	case "never":
		out.ConfirmWithProbe = health.ConfirmMode_NEVER
	default:
		return nil, errors.New("leastCost: unknown confirm mode ", v.ConfirmWithProbe)
	}
	switch strings.ToLower(v.NetworkChangeAction) {
	case "", "recheck":
		out.NetworkChangeAction = health.NetworkChangeMode_RECHECK
	case "forget":
		out.NetworkChangeAction = health.NetworkChangeMode_FORGET
	case "ignore":
		out.NetworkChangeAction = health.NetworkChangeMode_IGNORE
	default:
		return nil, errors.New("leastCost: unknown networkChangeAction ", v.NetworkChangeAction)
	}
	if u := v.ReturnToCheaper; u != nil {
		out.ReturnToCheaper = &health.ReturnToCheaperConfig{
			Interval:               optionalDuration(u.Interval),
			SkipRandomPhase:        isFalse(u.HasRandomPhase),
			ProbesToReturn:         u.ProbesToReturn,
			ProbeInterval:          optionalDuration(u.ProbeInterval),
			PenaltyWindow:          optionalDuration(u.PenaltyWindow),
			PenaltyMultiplier:      u.PenaltyMultiplier,
			MaxPenaltyDelay:        optionalDuration(u.MaxPenaltyDelay),
			SkipTryOnNetworkChange: isFalse(u.ShouldTryOnNetworkChange),
			TryOnAllNetworks:       isFalse(u.IsUnknownNetworksOnly),
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
