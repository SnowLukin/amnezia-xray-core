package conf

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/infra/conf/cfgcommon/duration"
	"google.golang.org/protobuf/proto"
)

// The v2 JSON schema is independent of the internal protobuf representation.
// Optional feature objects enable by presence. Only default-on objects expose
// isEnabled:false; the internal protobuf still carries explicit feature flags.
type HealthFeatureConfig struct {
	IsEnabled *bool `json:"isEnabled"`
}
type HealthTimeoutConfig struct {
	HealthFeatureConfig
	Timeout *duration.Duration `json:"timeout"`
}
type HealthAfterConfig struct {
	HealthFeatureConfig
	After *duration.Duration `json:"after"`
}
type HealthEvidenceConfig struct {
	DialFailures     HealthFeatureConfig       `json:"dialFailures"`
	DialTimeout      HealthTimeoutConfig       `json:"dialTimeout"`
	ServerUnverified HealthFeatureConfig       `json:"serverUnverified"`
	FirstReply       HealthTimeoutConfig       `json:"firstReply"`
	EarlyClose       HealthFeatureConfig       `json:"earlyClose"`
	StalledRequests  HealthTimeoutConfig       `json:"stalledRequests"`
	PartialStalls    HealthPartialStallsConfig `json:"partialStalls"`
	UDPReplies       HealthTimeoutConfig       `json:"udpReplies"`
	LoneFailureProbe HealthAfterConfig         `json:"loneFailureProbe"`
}
type HealthPartialStallsConfig struct {
	HealthFeatureConfig
	MaxAge *duration.Duration `json:"maxAge"`
}
type HealthProbeStepConfig struct {
	Name             string             `json:"name"`
	Destinations     []string           `json:"destinations"`
	IsOptional       bool               `json:"isOptional"`
	Method           string             `json:"method"`
	ExpectedStatuses []int32            `json:"expectedStatuses"`
	MinBytes         int64              `json:"minBytes"`
	Timeout          *duration.Duration `json:"timeout"`
	ProgressTimeout  *duration.Duration `json:"progressTimeout"`
}
type HealthProbeConfig struct {
	Name  string                  `json:"name"`
	Steps []HealthProbeStepConfig `json:"steps"`
}
type HealthProbeLimitsConfig struct {
	MaxConcurrent          *int32 `json:"maxConcurrent"`
	PerMinute              *int32 `json:"perMinute"`
	PerServerNamePerMinute *int32 `json:"perServerNamePerMinute"`
	HostPerMinute          *int32 `json:"hostPerMinute"`
}
type HealthProbeOnlyConfig struct {
	HealthFeatureConfig
	KeepVerified    *int32             `json:"keepVerified"`
	RecheckInterval *duration.Duration `json:"recheckInterval"`
}
type HealthHostProbesConfig struct {
	HealthFeatureConfig
	MaxOutbounds *int32             `json:"maxOutbounds"`
	Timeout      *duration.Duration `json:"timeout"`
}
type HealthUDPProbeConfig struct {
	HealthFeatureConfig
	Destinations []string           `json:"destinations"`
	Requests     *int32             `json:"requests"`
	Interval     *duration.Duration `json:"interval"`
	Timeout      *duration.Duration `json:"timeout"`
	PerHour      *int32             `json:"perHour"`
}
type HealthProbesConfig struct {
	Profiles     []*HealthProbeConfig    `json:"profiles"`
	StepProfiles HealthFeatureConfig     `json:"stepProfiles"`
	Limits       HealthProbeLimitsConfig `json:"limits"`
	Candidates   HealthProbeOnlyConfig   `json:"candidates"`
	Requests     HealthHostProbesConfig  `json:"requests"`
	UDP          HealthUDPProbeConfig    `json:"udp"`
}
type HealthConnectivityConfig struct {
	HealthFeatureConfig
	Outbound     string             `json:"outbound"`
	Destinations []string           `json:"destinations"`
	Timeout      *duration.Duration `json:"timeout"`
}
type HealthWitnessesConfig struct {
	HealthFeatureConfig
	Outbounds []string `json:"outbounds"`
}
type HealthNetworkMemoryConfig struct {
	HealthFeatureConfig
	MaxNetworks *int32 `json:"maxNetworks"`
}
type HealthLabelRequestConfig struct {
	HealthFeatureConfig
	Outbound        string             `json:"outbound"`
	Destinations    []string           `json:"destinations"`
	Labels          map[string]string  `json:"labels"`
	Timeout         *duration.Duration `json:"timeout"`
	RequestsPerHour *int32             `json:"requestsPerHour"`
}
type HealthNetworkConfig struct {
	Connectivity HealthConnectivityConfig  `json:"connectivity"`
	Witnesses    HealthWitnessesConfig     `json:"witnesses"`
	Memory       HealthNetworkMemoryConfig `json:"memory"`
	HostLabels   HealthFeatureConfig       `json:"hostLabels"`
	LabelRequest HealthLabelRequestConfig  `json:"labelRequest"`
}
type HealthWakeConfig struct {
	HealthFeatureConfig
	Freshness   *duration.Duration `json:"freshness"`
	MinInterval *duration.Duration `json:"minInterval"`
}
type HealthViewConfig struct {
	HealthFeatureConfig
	Balancer string `json:"balancer"`
}
type HealthHostConfig struct {
	Pinning        HealthFeatureConfig `json:"pinning"`
	UserPresent    HealthWakeConfig    `json:"userPresent"`
	MemoryPressure HealthFeatureConfig `json:"memoryPressure"`
	View           HealthViewConfig    `json:"view"`
}
type HealthSleepConfig struct {
	HealthFeatureConfig
	AfterLastUse *duration.Duration `json:"afterLastUse"`
}
type HealthResourcesConfig struct {
	TunnelSleep       HealthSleepConfig `json:"tunnelSleep"`
	CloseUnchosenIdle HealthAfterConfig `json:"closeUnchosenIdle"`
}
type HealthEventsConfig struct {
	HealthFeatureConfig
	MaxEvents *int32 `json:"maxEvents"`
	MaxBytes  *int64 `json:"maxBytes"`
}
type HealthSessionLabelsConfig struct {
	HealthFeatureConfig
	Labels map[string]string `json:"labels"`
}
type HealthReportConfig struct {
	HealthFeatureConfig
	Interval            *duration.Duration        `json:"interval"`
	QuietSampleFraction float64                   `json:"quietSampleFraction"`
	SessionLabels       HealthSessionLabelsConfig `json:"sessionLabels"`
}
type HealthCensusConfig struct {
	SampleFraction *float64           `json:"sampleFraction"`
	StartAfter     *duration.Duration `json:"startAfter"`
	OnDemand       []string           `json:"onDemand"`
}

// HealthTraceConfig keeps raw evidence for diagnostics: every event of the
// core and, unless turned off, every user connection without its destination.
type HealthTraceConfig struct {
	MaxBytes    *int64              `json:"maxBytes"`
	Connections HealthFeatureConfig `json:"connections"`
}

type HealthTelemetryConfig struct {
	Census            *HealthCensusConfig `json:"census"`
	Trace             *HealthTraceConfig  `json:"trace"`
	Events            HealthEventsConfig  `json:"events"`
	Report            HealthReportConfig  `json:"report"`
	Quality           HealthFeatureConfig `json:"quality"`
	PathLabels        HealthFeatureConfig `json:"pathLabels"`
	ProbeMeasurements HealthFeatureConfig `json:"probeMeasurements"`
	Metrics           HealthFeatureConfig `json:"metrics"`
}
type HealthPeriodicConfig struct {
	HealthFeatureConfig
	Interval *HealthDelayRange `json:"interval"`
}
type HealthMinHealthyConfig struct {
	HealthFeatureConfig
	Outbounds *int32            `json:"outbounds"`
	Delay     *HealthDelayRange `json:"delay"`
}
type HealthCostTriggerConfig struct {
	HealthFeatureConfig
	Cost  *float32          `json:"cost"`
	After *HealthDelayRange `json:"after"`
}
type HealthMaxAgeConfig struct {
	HealthFeatureConfig
	Age *duration.Duration `json:"age"`
}
type HealthProviderNetworkChangeConfig struct {
	HealthFeatureConfig
	Mode        string            `json:"mode"`
	SettleDelay *HealthDelayRange `json:"settleDelay"`
}
type HealthExpiryConfig struct {
	HealthFeatureConfig
	RenewAtFraction  *float64 `json:"renewAtFraction"`
	UrgentAtFraction *float64 `json:"urgentAtFraction"`
}
type HealthExhaustedConfig struct {
	HealthFeatureConfig
	Delay *HealthDelayRange `json:"delay"`
}
type HealthProviderTriggersConfig struct {
	Exhausted       HealthExhaustedConfig             `json:"exhausted"`
	Periodic        HealthPeriodicConfig              `json:"periodic"`
	MinHealthy      HealthMinHealthyConfig            `json:"minHealthy"`
	WhenCostAbove   HealthCostTriggerConfig           `json:"whenCostAbove"`
	MaxAge          HealthMaxAgeConfig                `json:"maxAge"`
	NetworkChange   HealthProviderNetworkChangeConfig `json:"networkChange"`
	LeaseRenewal    HealthExpiryConfig                `json:"leaseRenewal"`
	BackendSchedule HealthFeatureConfig               `json:"backendSchedule"`
}
type HealthProviderLimitsConfig struct {
	MinInterval         *duration.Duration `json:"minInterval"`
	UrgentMinInterval   *duration.Duration `json:"urgentMinInterval"`
	MaxRefreshesPerHour *int32             `json:"maxRefreshesPerHour"`
}
type HealthProviderApplyConfig struct {
	Balancers            []string            `json:"balancers"`
	TagPrefix            string              `json:"tagPrefix"`
	DrainTimeout         *duration.Duration  `json:"drainTimeout"`
	MaxOutbounds         *int32              `json:"maxOutbounds"`
	AllowedProtocols     []string            `json:"allowedProtocols"`
	AcceptedHeaders      *[]string           `json:"acceptedHeaders"`
	ExpiredLeaseFallback HealthFeatureConfig `json:"expiredLeaseFallback"`
}
type HealthProbeOnlyRoadsConfig struct{}

type HealthProviderConfig struct {
	HealthFeatureConfig
	Tag                string                       `json:"tag"`
	URLs               []string                     `json:"urls"`
	Method             string                       `json:"method"`
	Headers            map[string]string            `json:"headers"`
	Via                []string                     `json:"via"`
	ProbeOnlyRoads     *HealthProbeOnlyRoadsConfig  `json:"probeOnlyRoads"`
	Timeout            *duration.Duration           `json:"timeout"`
	IsPlainHTTPAllowed bool                         `json:"isPlainHttpAllowed"`
	Triggers           HealthProviderTriggersConfig `json:"triggers"`
	Limits             HealthProviderLimitsConfig   `json:"limits"`
	Apply              HealthProviderApplyConfig    `json:"apply"`
	InitialAnswer      json.RawMessage              `json:"initialAnswer"`
	BaseVersion        string                       `json:"baseVersion"`
}

const maxBaseVersion = 256

type HealthObservatoryConfig struct {
	Evidence  HealthEvidenceConfig     `json:"evidence"`
	Probes    HealthProbesConfig       `json:"probes"`
	Network   HealthNetworkConfig      `json:"network"`
	Host      HealthHostConfig         `json:"host"`
	Resources HealthResourcesConfig    `json:"resources"`
	Telemetry HealthTelemetryConfig    `json:"telemetry"`
	Providers []*HealthProviderConfig  `json:"providers"`
	Paths     []*health.PathDefinition `json:"-"`
}

// validateJSONShape enforces only the adaptive API. The enclosing stock Xray
// config continues to use its original permissive decoder.
func validateJSONShape(raw []byte, typ reflect.Type, where string) error {
	if typ == reflect.TypeOf(json.RawMessage{}) {
		return nil
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return errors.New(where, " must not be null")
	}
	switch typ.Kind() {
	case reflect.Struct:
		fields := map[string]reflect.Type{}
		var add func(reflect.Type)
		add = func(t reflect.Type) {
			for i := 0; i < t.NumField(); i++ {
				f := t.Field(i)
				if f.Anonymous {
					add(f.Type)
					continue
				}
				name := strings.Split(f.Tag.Get("json"), ",")[0]
				if name != "" && name != "-" {
					fields[name] = f.Type
				}
			}
		}
		add(typ)
		var values map[string]json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil || values == nil {
			return errors.New(where, " must be an object")
		}
		for name, value := range values {
			if name == "isEnabled" {
				if !defaultOnFeature(where) {
					return errors.New(where, ".isEnabled is not supported; include the block to enable it or omit it to disable it")
				}
				if !bytes.Equal(bytes.TrimSpace(value), []byte("false")) {
					return errors.New(where, ".isEnabled only accepts false for a feature enabled by default")
				}
			}
			t, ok := fields[name]
			if !ok {
				return errors.New(where, ".", name, ": unknown key")
			}
			if err := validateJSONShape(value, t, where+"."+name); err != nil {
				return err
			}
		}
	case reflect.Slice:
		var values []json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return errors.New(where, " must be an array")
		}
		for i, value := range values {
			if err := validateJSONShape(value, typ.Elem(), fmt.Sprintf("%s[%d]", where, i)); err != nil {
				return err
			}
		}
	case reflect.Map:
		var values map[string]json.RawMessage
		if err := json.Unmarshal(raw, &values); err != nil {
			return errors.New(where, " must be an object")
		}
		for name, value := range values {
			if err := validateJSONShape(value, typ.Elem(), where+"."+name); err != nil {
				return err
			}
		}
	default:
		if err := json.Unmarshal(raw, reflect.New(typ).Interface()); err != nil {
			return errors.New(where, ": ", err)
		}
	}
	return nil
}

// Only the adaptive top-level field is strict; stock Xray field handling is
// preserved by decoding the rest through the original struct.
func (c *Config) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	for name, value := range fields {
		if strings.EqualFold(name, "healthObservatory") {
			if name != "healthObservatory" {
				return errors.New(name, ": unknown key; use healthObservatory")
			}
			if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return errors.New("healthObservatory must not be null")
			}
		}
	}
	type plain Config
	return json.Unmarshal(raw, (*plain)(c))
}
func (c *HealthObservatoryConfig) UnmarshalJSON(raw []byte) error {
	if err := validateJSONShape(raw, reflect.TypeOf(*c), "healthObservatory"); err != nil {
		return err
	}
	type plain HealthObservatoryConfig
	if err := json.Unmarshal(raw, (*plain)(c)); err != nil {
		return err
	}
	applyFeaturePresence(raw, reflect.ValueOf(c).Elem(), "healthObservatory")
	return nil
}

func defaultOnFeature(where string) bool {
	if inheritedReturnFeature(where) {
		return true
	}
	switch where {
	case "healthObservatory.evidence.dialFailures", "healthObservatory.evidence.dialTimeout", "healthObservatory.evidence.serverUnverified", "healthObservatory.evidence.firstReply", "healthObservatory.evidence.earlyClose", "healthObservatory.evidence.stalledRequests", "leastCost.settings.failover.closePending", "leastCost.settings.failover.closeSilent", "healthObservatory.telemetry.trace.connections":
		return true
	}
	if strings.HasPrefix(where, "healthObservatory.providers[") {
		for _, tail := range []string{".triggers.exhausted", ".triggers.leaseRenewal", ".triggers.backendSchedule", ".apply.expiredLeaseFallback"} {
			if strings.HasSuffix(where, tail) {
				return true
			}
		}
	}
	return false
}

func inheritedReturnFeature(where string) bool {
	return strings.HasSuffix(where, ".returnToCheaper") && (strings.HasPrefix(where, "selection.") || strings.HasPrefix(where, "outbounds[].selection.") || strings.Contains(where, ".groups["))
}

// The strict public JSON shape is decoded before presence is translated into
// internal protobuf flags. Absent optional blocks retain a nil flag (off).
func applyFeaturePresence(raw []byte, value reflect.Value, where string) {
	for value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return
		}
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.Struct:
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil || fields == nil {
			return
		}
		if flag := value.FieldByName("IsEnabled"); flag.IsValid() && (!defaultOnFeature(where) || (inheritedReturnFeature(where) && flag.IsNil())) {
			flag.Set(reflect.ValueOf(featurePresence(true)))
		}
		typ := value.Type()
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.Anonymous {
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if next, exists := fields[name]; exists && name != "" && name != "-" {
				applyFeaturePresence(next, value.Field(i), where+"."+name)
			}
		}
	case reflect.Slice:
		var elements []json.RawMessage
		if json.Unmarshal(raw, &elements) != nil {
			return
		}
		for i, element := range elements {
			applyFeaturePresence(element, value.Index(i), fmt.Sprintf("%s[%d]", where, i))
		}
	}
}

func featurePresence(on bool) *bool { return &on }
func enabled(v *bool, def bool) bool {
	if v == nil {
		return def
	}
	return *v
}
func durationPresence(v *duration.Duration) *int64 {
	if v == nil {
		return nil
	}
	n := int64(*v)
	return &n
}
func optionalDuration(v *duration.Duration) int64 {
	if v == nil {
		return 0
	}
	return int64(*v)
}
func count(v *int32) int32 {
	if v == nil {
		return 0
	}
	return *v
}
func sizeValue(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}
func floatValue(v *float32) float32 {
	if v == nil {
		return 0
	}
	return *v
}
func fraction(v *float64, def float64) float64 {
	if v == nil {
		return def
	}
	return *v
}

// inRange refuses a set value outside [lo, hi] and names the range; hi 0
// means no upper bound. Durations pass as *time.Duration to print as such.
func inRange[T ~int32 | ~int64](where string, v *T, lo, hi T) error {
	if v == nil || *v >= lo && (hi == 0 || *v <= hi) {
		return nil
	}
	if hi == 0 {
		return errors.New(where, " must be at least ", lo)
	}
	return errors.New(where, " must be from ", lo, " to ", hi)
}
func durationIn(where string, v *duration.Duration, lo, hi time.Duration) error {
	return inRange(where, (*time.Duration)(v), lo, hi)
}
func checkProbeURL(where, dest string) error {
	if dest == "" {
		return nil
	}
	u, err := url.Parse(dest)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return errors.New(where, " must be an http or https URL")
	}
	return nil
}
func checkURLs(where string, destinations []string) error {
	if len(destinations) < 1 || len(destinations) > 4 {
		return errors.New(where, " must contain 1 to 4 URLs")
	}
	for i, dest := range destinations {
		path := fmt.Sprintf("%s[%d]", where, i)
		if dest == "" {
			return errors.New(path, " must not be empty")
		}
		if err := checkProbeURL(path, dest); err != nil {
			return err
		}
	}
	return nil
}
func validLabelName(name string) bool {
	if len(name) < 1 || len(name) > 24 {
		return false
	}
	for _, c := range name {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_') {
			return false
		}
	}
	return true
}
func validateLabels(where string, labels map[string]string, limit int) error {
	if len(labels) > limit {
		return errors.New(where, " must contain at most ", limit, " labels")
	}
	if err := health.ValidateLabels(labels, limit); err != nil {
		return errors.New(where, ".", err)
	}
	return nil
}
func validateLabelHeaders(where string, labels map[string]string) error {
	if len(labels) > 6 {
		return errors.New(where, " must contain at most 6 labels")
	}
	for name, header := range labels {
		if !validLabelName(name) {
			return errors.New(where, ".", name, ": invalid label name")
		}
		if err := health.CheckHeaderName(header); err != nil {
			return errors.New(where, ".", name, ": ", err)
		}
	}
	return nil
}

func (c *HealthObservatoryConfig) Build() (proto.Message, error) {
	e, p, n, h, r, t := c.Evidence, c.Probes, c.Network, c.Host, c.Resources, c.Telemetry
	out := &health.Config{Paths: c.Paths, Features: &health.FeatureConfig{
		DialFailures: e.DialFailures.IsEnabled, DialTimeout: e.DialTimeout.IsEnabled, ServerUnverified: e.ServerUnverified.IsEnabled, FirstReply: e.FirstReply.IsEnabled, EarlyClose: e.EarlyClose.IsEnabled, StalledRequests: e.StalledRequests.IsEnabled,
		PartialStalls: e.PartialStalls.IsEnabled, UdpReplies: e.UDPReplies.IsEnabled, LoneFailureProbe: e.LoneFailureProbe.IsEnabled,
		StepProfiles: p.StepProfiles.IsEnabled, Candidates: p.Candidates.IsEnabled, ProbeRequests: p.Requests.IsEnabled, UdpProbes: p.UDP.IsEnabled,
		Connectivity: n.Connectivity.IsEnabled, Witnesses: n.Witnesses.IsEnabled, NetworkMemory: n.Memory.IsEnabled, HostLabels: n.HostLabels.IsEnabled, LabelRequest: n.LabelRequest.IsEnabled,
		Pinning: h.Pinning.IsEnabled, UserPresent: h.UserPresent.IsEnabled, MemoryPressure: h.MemoryPressure.IsEnabled,
		TunnelSleep: r.TunnelSleep.IsEnabled, CloseUnchosenIdle: r.CloseUnchosenIdle.IsEnabled,
		Events: t.Events.IsEnabled, Report: t.Report.IsEnabled, Quality: t.Quality.IsEnabled, PathLabels: t.PathLabels.IsEnabled, ProbeMeasurements: t.ProbeMeasurements.IsEnabled, Metrics: t.Metrics.IsEnabled, SessionLabels: t.Report.SessionLabels.IsEnabled,
	}}
	// Bounds of the values that set how often the core reaches the network,
	// how much memory it takes and how long it waits. A config goes to every
	// client at once, so a value out of them is refused rather than cut.
	for _, f := range []struct {
		name   string
		value  *duration.Duration
		lo, hi time.Duration
	}{
		{"evidence.dialTimeout.timeout", e.DialTimeout.Timeout, 1, 0}, {"evidence.firstReply.timeout", e.FirstReply.Timeout, 1, 0}, {"evidence.stalledRequests.timeout", e.StalledRequests.Timeout, 1, 0}, {"evidence.partialStalls.maxAge", e.PartialStalls.MaxAge, 1, 0}, {"evidence.udpReplies.timeout", e.UDPReplies.Timeout, 1, 0}, {"evidence.loneFailureProbe.after", e.LoneFailureProbe.After, 1, 0},
		{"probes.candidates.recheckInterval", p.Candidates.RecheckInterval, 1, 0}, {"probes.requests.timeout", p.Requests.Timeout, 1, 10 * time.Minute}, {"probes.udp.interval", p.UDP.Interval, 1, 2 * time.Second}, {"probes.udp.timeout", p.UDP.Timeout, 1, 10 * time.Second},
		{"network.connectivity.timeout", n.Connectivity.Timeout, 1, 30 * time.Second}, {"network.labelRequest.timeout", n.LabelRequest.Timeout, 1, 30 * time.Second}, {"host.userPresent.freshness", h.UserPresent.Freshness, 1, 0}, {"host.userPresent.minInterval", h.UserPresent.MinInterval, 1, 0},
		{"resources.tunnelSleep.afterLastUse", r.TunnelSleep.AfterLastUse, 10 * time.Second, 0}, {"resources.closeUnchosenIdle.after", r.CloseUnchosenIdle.After, 10 * time.Second, 10 * time.Minute}, {"telemetry.report.interval", t.Report.Interval, 1, 0},
	} {
		if err := durationIn("healthObservatory."+f.name, f.value, f.lo, f.hi); err != nil {
			return nil, err
		}
	}
	for _, f := range []struct {
		name  string
		value *int32
		max   int32
	}{
		{"probes.limits.maxConcurrent", p.Limits.MaxConcurrent, 8}, {"probes.limits.perMinute", p.Limits.PerMinute, 60}, {"probes.limits.perServerNamePerMinute", p.Limits.PerServerNamePerMinute, 10}, {"probes.limits.hostPerMinute", p.Limits.HostPerMinute, 0},
		{"probes.candidates.keepVerified", p.Candidates.KeepVerified, 4}, {"probes.requests.maxOutbounds", p.Requests.MaxOutbounds, 16}, {"probes.udp.requests", p.UDP.Requests, 32}, {"probes.udp.perHour", p.UDP.PerHour, 60},
		{"network.memory.maxNetworks", n.Memory.MaxNetworks, 64}, {"network.labelRequest.requestsPerHour", n.LabelRequest.RequestsPerHour, 60}, {"telemetry.events.maxEvents", t.Events.MaxEvents, 4096},
	} {
		if err := inRange("healthObservatory."+f.name, f.value, 1, f.max); err != nil {
			return nil, err
		}
	}
	if err := inRange("healthObservatory.telemetry.events.maxBytes", t.Events.MaxBytes, 1, 256<<10); err != nil {
		return nil, err
	}
	if p.Limits.HostPerMinute != nil && *p.Limits.HostPerMinute > max(countOr(p.Limits.PerMinute, 12)/2, 1) {
		return nil, errors.New("healthObservatory.probes.limits.hostPerMinute must not exceed half of perMinute")
	}
	out.Passive = &health.PassiveConfig{DialTimeout: optionalDuration(e.DialTimeout.Timeout), FirstByteTimeout: optionalDuration(e.FirstReply.Timeout), StallTimeout: optionalDuration(e.StalledRequests.Timeout), PartialStallMaxAge: optionalDuration(e.PartialStalls.MaxAge), UdpReplyTimeout: optionalDuration(e.UDPReplies.Timeout), LoneFailureAfter: optionalDuration(e.LoneFailureProbe.After)}
	for i, profile := range p.Profiles {
		where := fmt.Sprintf("healthObservatory.probes.profiles[%d]", i)
		if profile == nil {
			return nil, errors.New(where, " must not be null")
		}
		if len(profile.Steps) == 0 {
			return nil, errors.New(where, ".steps must contain 1 to 4 steps")
		}
		pp := &health.ProbeProfile{Name: profile.Name}
		for j, step := range profile.Steps {
			stepWhere := fmt.Sprintf("%s.steps[%d]", where, j)
			for _, f := range []struct {
				name  string
				value *duration.Duration
			}{{"timeout", step.Timeout}, {"progressTimeout", step.ProgressTimeout}} {
				if err := durationIn(stepWhere+"."+f.name, f.value, 1, 30*time.Second); err != nil {
					return nil, err
				}
			}
			if err := inRange(stepWhere+".minBytes", &step.MinBytes, 0, 1<<20); err != nil {
				return nil, err
			}
			for _, status := range step.ExpectedStatuses {
				if status < 100 || status > 599 {
					return nil, errors.New(stepWhere, ".expectedStatuses must contain HTTP status codes")
				}
			}
			pp.Steps = append(pp.Steps, &health.ProbeStep{Name: step.Name, Destinations: step.Destinations, IsOptional: step.IsOptional, Method: step.Method, ExpectedStatuses: step.ExpectedStatuses, MinBytes: step.MinBytes, Timeout: optionalDuration(step.Timeout), ProgressTimeout: optionalDuration(step.ProgressTimeout)})
		}
		out.Probes = append(out.Probes, pp)
	}
	if enabled(p.UDP.IsEnabled, false) || p.UDP.Destinations != nil {
		out.UdpProbe = &health.UDPProbeConfig{Destinations: p.UDP.Destinations, Requests: count(p.UDP.Requests), Interval: optionalDuration(p.UDP.Interval), Timeout: optionalDuration(p.UDP.Timeout), PerHour: count(p.UDP.PerHour)}
	}
	if err := out.ValidateProbeSteps(); err != nil {
		return nil, errors.New(strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(err.Error(), "healthObservatory: ", "healthObservatory."), "probes[", "probes.profiles["), "udpProbe", "probes.udp"))
	}
	cc := n.Connectivity
	for _, v := range n.Witnesses.Outbounds {
		if v == "" {
			return nil, errors.New("healthObservatory.network.witnesses.outbounds must not contain an empty prefix")
		}
	}
	out.Connectivity = &health.ConnectivityConfig{Outbound: cc.Outbound, Destinations: cc.Destinations, Timeout: optionalDuration(cc.Timeout), Witnesses: n.Witnesses.Outbounds}
	lr := n.LabelRequest
	if enabled(lr.IsEnabled, false) && (lr.Outbound == "" || len(lr.Labels) == 0) {
		return nil, errors.New("healthObservatory.network.labelRequest requires outbound, destinations and labels")
	}
	if enabled(lr.IsEnabled, false) {
		if err := checkURLs("healthObservatory.network.labelRequest.destinations", lr.Destinations); err != nil {
			return nil, err
		}
	}
	if err := validateLabelHeaders("healthObservatory.network.labelRequest.labels", lr.Labels); err != nil {
		return nil, err
	}
	out.LabelRequest = &health.LabelRequestConfig{Outbound: lr.Outbound, Destinations: lr.Destinations, Labels: lr.Labels, Timeout: optionalDuration(lr.Timeout), RequestsPerHour: count(lr.RequestsPerHour)}
	out.Schedule = &health.ScheduleConfig{MaxConcurrentProbes: count(p.Limits.MaxConcurrent), ProbesPerMinute: count(p.Limits.PerMinute), ProbesPerServerNamePerMinute: count(p.Limits.PerServerNamePerMinute), ProbeOnly: &health.ProbeOnlyConfig{KeepVerified: count(p.Candidates.KeepVerified), RecheckInterval: optionalDuration(p.Candidates.RecheckInterval)}, HostProbes: &health.HostProbesConfig{ProbesPerMinute: count(p.Limits.HostPerMinute), MaxOutbounds: count(p.Requests.MaxOutbounds), Timeout: optionalDuration(p.Requests.Timeout)}}
	out.Network = &health.NetworkConfig{MaxNetworks: count(n.Memory.MaxNetworks)}
	out.Wake = &health.WakeConfig{Freshness: optionalDuration(h.UserPresent.Freshness), MinInterval: optionalDuration(h.UserPresent.MinInterval)}
	if enabled(h.View.IsEnabled, false) {
		out.View = &health.ViewConfig{Balancer: h.View.Balancer}
	}
	out.Sleep = &health.SleepConfig{AfterLastUse: optionalDuration(r.TunnelSleep.AfterLastUse), CloseIdleAfter: optionalDuration(r.CloseUnchosenIdle.After)}
	out.Events = &health.EventsConfig{MaxEvents: count(t.Events.MaxEvents), MaxBytes: sizeValue(t.Events.MaxBytes)}
	if math.IsNaN(t.Report.QuietSampleFraction) || math.IsInf(t.Report.QuietSampleFraction, 0) || t.Report.QuietSampleFraction < 0 || t.Report.QuietSampleFraction > 1 {
		return nil, errors.New("healthObservatory.telemetry.report.quietSampleFraction must be in [0, 1]")
	}
	if err := validateLabels("healthObservatory.telemetry.report.sessionLabels.labels", t.Report.SessionLabels.Labels, 8); err != nil {
		return nil, err
	}
	if trace := t.Trace; trace != nil {
		if err := inRange("healthObservatory.telemetry.trace.maxBytes", trace.MaxBytes, 64<<10, 2<<20); err != nil {
			return nil, err
		}
		out.Trace = &health.TraceConfig{ConnectionsEnabled: trace.Connections.IsEnabled}
		if trace.MaxBytes != nil {
			out.Trace.MaxBytes = *trace.MaxBytes
		}
	}
	if census := t.Census; census != nil {
		if census.SampleFraction == nil || math.IsNaN(*census.SampleFraction) || math.IsInf(*census.SampleFraction, 0) || *census.SampleFraction <= 0 || *census.SampleFraction > 1 {
			return nil, errors.New("healthObservatory.telemetry.census.sampleFraction is required and must be in (0, 1]")
		}
		if err := durationIn("healthObservatory.telemetry.census.startAfter", census.StartAfter, 1, 0); err != nil {
			return nil, err
		}
		out.Census = &health.CensusConfig{SampleFraction: *census.SampleFraction, StartAfter: optionalDuration(census.StartAfter), OnDemand: census.OnDemand}
	}
	out.Report = &health.ReportConfig{Interval: optionalDuration(t.Report.Interval), QuietShare: t.Report.QuietSampleFraction}
	out.SessionLabels = t.Report.SessionLabels.Labels
	for i, pc := range c.Providers {
		if pc == nil {
			return nil, errors.New(fmt.Sprintf("healthObservatory.providers[%d] must not be null", i))
		}
		built, err := pc.build()
		if err != nil {
			return nil, err
		}
		if nc := pc.Triggers.NetworkChange; enabled(nc.IsEnabled, false) && nc.Mode != "any" && !enabled(n.Memory.IsEnabled, false) {
			return nil, errors.New("healthObservatory.providers[", pc.Tag, "].triggers.networkChange.mode:", cmp.Or(nc.Mode, "unknownNotCheapest"), " requires healthObservatory.network.memory")
		}
		for _, other := range out.Providers {
			if other.Tag == built.Tag {
				return nil, errors.New("healthObservatory.providers: duplicate tag ", built.Tag)
			}
			a, b := other.Apply.TagPrefix, built.Apply.TagPrefix
			if a != "" && b != "" && (strings.HasPrefix(a, b) || strings.HasPrefix(b, a)) {
				return nil, errors.New("healthObservatory.providers.apply.tagPrefix overlaps")
			}
		}
		out.Providers = append(out.Providers, built)
	}
	if err := out.ValidateFeatures(); err != nil {
		return nil, err
	}
	return out, nil
}
func countOr(v *int32, def int32) int32 {
	if v == nil {
		return def
	}
	return *v
}

func (c *HealthProviderConfig) build() (*health.ProviderConfig, error) {
	where := "healthObservatory.providers[" + c.Tag + "]"
	if len(c.Tag) > 128 {
		return nil, errors.New(where, ".tag must not exceed 128 bytes")
	}
	if c.Tag == "" || len(c.Apply.Balancers) == 0 || c.Apply.TagPrefix == "" {
		return nil, errors.New(where, " requires tag, urls, apply.balancers and apply.tagPrefix")
	}
	if len(c.Apply.Balancers) > 4 {
		return nil, errors.New(where, ".apply.balancers must contain 1 to 4 tags")
	}
	seen := map[string]bool{}
	for _, tag := range c.Apply.Balancers {
		if tag == "" || seen[tag] {
			return nil, errors.New(where, ".apply.balancers must contain distinct nonempty tags")
		}
		seen[tag] = true
	}
	if err := checkURLs(where+".urls", c.URLs); err != nil {
		return nil, err
	}
	for i, dest := range c.URLs {
		u, _ := url.Parse(dest)
		if u.User != nil {
			return nil, errors.New(where, ".urls[", i, "]: put credentials into headers")
		}
		if u.Scheme != "https" && !c.IsPlainHTTPAllowed {
			return nil, errors.New(where, ".urls[", i, "] must be https or set isPlainHttpAllowed")
		}
	}
	method := strings.ToUpper(c.Method)
	if method != "" && method != "GET" && method != "POST" {
		return nil, errors.New(where, ".method must be GET or POST")
	}
	if len(c.BaseVersion) > maxBaseVersion {
		return nil, errors.New(where, ".baseVersion must not exceed 256 bytes")
	}
	for _, v := range c.Via {
		if v == "" {
			return nil, errors.New(where, ".via must not contain empty tags")
		}
	}
	for name, value := range c.Headers {
		if err := health.CheckHeaderName(name); err != nil || strings.ContainsAny(value, "\r\n\x00") {
			return nil, errors.New(where, ".headers: invalid header")
		}
	}
	t, l, a := c.Triggers, c.Limits, c.Apply
	for _, f := range []struct {
		name   string
		value  *duration.Duration
		lo, hi time.Duration
	}{{"timeout", c.Timeout, 1, 0}, {"triggers.maxAge.age", t.MaxAge.Age, 1, 0}, {"limits.minInterval", l.MinInterval, 1, 0}, {"limits.urgentMinInterval", l.UrgentMinInterval, 1, 0}, {"apply.drainTimeout", a.DrainTimeout, 1, 30 * time.Minute}} {
		if err := durationIn(where+"."+f.name, f.value, f.lo, f.hi); err != nil {
			return nil, err
		}
	}
	for _, f := range []struct {
		name  string
		value *int32
		max   int32
	}{{"triggers.minHealthy.outbounds", t.MinHealthy.Outbounds, 0}, {"limits.maxRefreshesPerHour", l.MaxRefreshesPerHour, 60}, {"apply.maxOutbounds", a.MaxOutbounds, 128}} {
		if err := inRange(where+"."+f.name, f.value, 1, f.max); err != nil {
			return nil, err
		}
	}
	if t.Periodic.Interval != nil && rangeProto(t.Periodic.Interval).Min == 0 {
		return nil, errors.New(where, ".triggers.periodic.interval must be greater than 0s")
	}
	if t.NetworkChange.SettleDelay != nil && !t.NetworkChange.SettleDelay.valid() {
		return nil, errors.New(where, ".triggers.networkChange.settleDelay must be nonnegative")
	}
	if t.WhenCostAbove.Cost != nil && (math.IsNaN(float64(*t.WhenCostAbove.Cost)) || math.IsInf(float64(*t.WhenCostAbove.Cost), 0) || *t.WhenCostAbove.Cost <= 0) {
		return nil, errors.New(where, ".triggers.whenCostAbove.cost must be above zero")
	}
	if enabled(t.WhenCostAbove.IsEnabled, false) && t.WhenCostAbove.Cost == nil {
		return nil, errors.New(where, ".triggers.whenCostAbove.cost is required")
	}
	if t.WhenCostAbove.After != nil && !t.WhenCostAbove.After.valid() {
		return nil, errors.New(where, ".triggers.whenCostAbove.after must be nonnegative")
	}
	for _, d := range []*HealthDelayRange{t.Periodic.Interval, t.Exhausted.Delay, t.MinHealthy.Delay} {
		if d != nil && !d.valid() {
			return nil, errors.New(where, ": invalid delay range")
		}
	}
	renew := fraction(t.LeaseRenewal.RenewAtFraction, .5)
	urgent := fraction(t.LeaseRenewal.UrgentAtFraction, max(.8, renew))
	if math.IsNaN(renew) || math.IsNaN(urgent) || renew <= 0 || renew > .95 || urgent < renew || urgent > .95 {
		return nil, errors.New(where, ".triggers.leaseRenewal: renewAtFraction must be in (0, 0.95], urgentAtFraction in [renewAtFraction, 0.95]")
	}
	if a.AllowedProtocols != nil && len(a.AllowedProtocols) == 0 {
		return nil, errors.New(where, ".apply.allowedProtocols must not be empty; omit it for the default")
	}
	protocols := make([]string, 0, len(a.AllowedProtocols))
	for _, name := range a.AllowedProtocols {
		name = strings.ToLower(name)
		if _, err := outboundConfigLoader.cache.CreateConfig(name); err != nil {
			return nil, errors.New(where, ".apply.allowedProtocols: unknown protocol ", name)
		}
		protocols = append(protocols, name)
	}
	mode := health.RefreshOnNetwork_REFRESH_UNKNOWN_NOT_CHEAPEST
	switch t.NetworkChange.Mode {
	case "", "unknownNotCheapest":
	case "unknown":
		mode = health.RefreshOnNetwork_REFRESH_UNKNOWN
	case "any":
		mode = health.RefreshOnNetwork_REFRESH_ANY
	default:
		return nil, errors.New(where, ".triggers.networkChange.mode must be unknownNotCheapest, unknown or any")
	}
	out := &health.ProviderConfig{ProbeOnlyRoads: c.ProbeOnlyRoads != nil, IsPlainHttpAllowed: c.IsPlainHTTPAllowed, Tag: c.Tag, Urls: c.URLs, Method: method, Headers: c.Headers, Via: c.Via, Timeout: optionalDuration(c.Timeout), BaseVersion: c.BaseVersion,
		Features: &health.ProviderFeatureConfig{Periodic: t.Periodic.IsEnabled, MinHealthy: t.MinHealthy.IsEnabled, CostAbove: t.WhenCostAbove.IsEnabled, MaxAge: t.MaxAge.IsEnabled, NetworkChange: t.NetworkChange.IsEnabled, Exhausted: t.Exhausted.IsEnabled, LeaseRenewal: t.LeaseRenewal.IsEnabled, BackendSchedule: t.BackendSchedule.IsEnabled, ExpiredLeaseFallback: a.ExpiredLeaseFallback.IsEnabled},
		Triggers: &health.ProviderTriggers{Interval: rangeProto(t.Periodic.Interval), ExhaustedDelay: rangeProto(t.Exhausted.Delay), MinHealthyDelay: rangeProto(t.MinHealthy.Delay), MinHealthyOutbounds: count(t.MinHealthy.Outbounds), MaxAge: optionalDuration(t.MaxAge.Age), NetworkChangeMode: mode, NetworkSettle: rangeProto(t.NetworkChange.SettleDelay), Expiry: &health.ExpiryTrigger{RenewAtFraction: renew, UrgentAtFraction: urgent}},
		Limits:   &health.ProviderLimits{MinInterval: optionalDuration(l.MinInterval), ExhaustedMinInterval: optionalDuration(l.UrgentMinInterval), MaxRefreshesPerHour: count(l.MaxRefreshesPerHour)},
		Apply:    &health.ProviderApply{Balancers: a.Balancers, TagPrefix: a.TagPrefix, DrainTimeout: optionalDuration(a.DrainTimeout), MaxOutbounds: count(a.MaxOutbounds), AllowedProtocols: protocols}}
	if t.WhenCostAbove.Cost != nil || enabled(t.WhenCostAbove.IsEnabled, false) {
		out.Triggers.WhenCostAbove = &health.CostTrigger{Cost: floatValue(t.WhenCostAbove.Cost), After: rangeProto(t.WhenCostAbove.After)}
	}
	if a.AcceptedHeaders != nil {
		out.Apply.AcceptedHeaders = *a.AcceptedHeaders
		out.Apply.FixedHeaders = len(*a.AcceptedHeaders) == 0
		for _, name := range *a.AcceptedHeaders {
			if err := health.CheckHeaderName(name); err != nil {
				return nil, errors.New(where, ".apply.acceptedHeaders: ", err)
			}
		}
	}
	if raw := bytes.TrimSpace(c.InitialAnswer); len(raw) > 0 && !bytes.Equal(raw, []byte("null")) {
		out.InitialAnswer = raw
	}
	return out, nil
}

type SelectionReturnConfig struct {
	HealthFeatureConfig
	Interval *duration.Duration `json:"interval"`
}
type OutboundSelectionConfig struct {
	Balancer        string                 `json:"balancer"`
	Group           *string                `json:"group"`
	Cost            *float32               `json:"cost"`
	ProbePolicy     *string                `json:"probePolicy"`
	ReturnToCheaper *SelectionReturnConfig `json:"returnToCheaper"`
	Probe           *string                `json:"probe"`
	IsProbeOnly     bool                   `json:"isProbeOnly"`
	Labels          map[string]string      `json:"labels"`
}

func (p *OutboundSelectionConfig) UnmarshalJSON(raw []byte) error {
	if err := validateJSONShape(raw, reflect.TypeOf(*p), "selection"); err != nil {
		return err
	}
	type plain OutboundSelectionConfig
	if err := json.Unmarshal(raw, (*plain)(p)); err != nil {
		return err
	}
	applyFeaturePresence(raw, reflect.ValueOf(p).Elem(), "selection")
	return nil
}
func (o *OutboundDetourConfig) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	// Xray never refused unknown keys of an outbound; only near misses of
	// the adaptive blocks are, in any case.
	for name := range fields {
		if strings.EqualFold(name, "path") {
			return errors.New("outbounds[].path is not supported; use selection")
		}
		for _, key := range []string{"selection", "speedLimit"} {
			if name != key && editDistance(strings.ToLower(name), strings.ToLower(key)) <= 2 {
				return errors.New("outbounds[].", name, ": unknown key; use ", key)
			}
		}
	}
	if raw, ok := fields["selection"]; ok {
		if err := validateJSONShape(raw, reflect.TypeOf(OutboundSelectionConfig{}), "outbounds[].selection"); err != nil {
			return err
		}
	}
	if raw, ok := fields["speedLimit"]; ok {
		if err := validateJSONShape(raw, reflect.TypeOf(SpeedLimitConfig{}), "outbounds[].speedLimit"); err != nil {
			return err
		}
	}
	type plain OutboundDetourConfig
	return json.Unmarshal(raw, (*plain)(o))
}

// editDistance is the Levenshtein distance between a and b.
func editDistance(a, b string) int {
	prev := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		cur := []int{i}
		for j := 1; j <= len(b); j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			cur = append(cur, min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost))
		}
		prev = cur
	}
	return prev[len(b)]
}

type strategyPathGroupConfig struct {
	Name               string                 `json:"name"`
	Cost               *float32               `json:"cost"`
	ProbePolicy        *string                `json:"probePolicy"`
	ReturnToCheaper    *SelectionReturnConfig `json:"returnToCheaper"`
	Probe              *string                `json:"probe"`
	FailedInRowToLeave *int32                 `json:"failedInRowToLeave"`
	Labels             map[string]string      `json:"labels"`
}
type strategyFailoverConfig struct {
	ShouldProbeBeforeSwitch *bool               `json:"shouldProbeBeforeSwitch"`
	ConfirmWithProbe        string              `json:"confirmWithProbe"`
	DownAudit               HealthFeatureConfig `json:"downAudit"`
	ClosePending            HealthFeatureConfig `json:"closePending"`
	CloseSilent             HealthAfterConfig   `json:"closeSilent"`
}
type strategyReturnNetworkConfig struct {
	HealthFeatureConfig
	Scope string `json:"scope"`
}
type strategyLeastCostReturnToCheaperConfig struct {
	HealthFeatureConfig
	Interval        *duration.Duration          `json:"interval"`
	MinHoldTime     *duration.Duration          `json:"minHoldTime"`
	OnNetworkChange strategyReturnNetworkConfig `json:"onNetworkChange"`
}
type strategyStartupPinConfig struct {
	HealthFeatureConfig
	Outbound string             `json:"outbound"`
	Timeout  *duration.Duration `json:"timeout"`
}
type strategyStartupConfig struct {
	Outbound string                   `json:"outbound"`
	Pin      strategyStartupPinConfig `json:"pin"`
}
type strategyNetworkChangesConfig struct {
	HealthFeatureConfig
	Action string `json:"action"`
}
type strategyLeastCostConfig struct {
	Paths            []*health.PathDefinition               `json:"-"`
	FallbackBalancer *string                                `json:"fallbackBalancer"`
	Failover         strategyFailoverConfig                 `json:"failover"`
	Groups           []*strategyPathGroupConfig             `json:"groups"`
	ReturnToCheaper  strategyLeastCostReturnToCheaperConfig `json:"returnToCheaper"`
	Startup          strategyStartupConfig                  `json:"startup"`
	NetworkChanges   strategyNetworkChangesConfig           `json:"networkChanges"`
}

func (v *strategyLeastCostConfig) UnmarshalJSON(raw []byte) error {
	if err := validateJSONShape(raw, reflect.TypeOf(*v), "leastCost.settings"); err != nil {
		return err
	}
	type plain strategyLeastCostConfig
	if err := json.Unmarshal(raw, (*plain)(v)); err != nil {
		return err
	}
	applyFeaturePresence(raw, reflect.ValueOf(v).Elem(), "leastCost.settings")
	return nil
}
func derefString(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
func probePolicy(where string, v *string) (bool, error) {
	switch derefString(v) {
	case "", "regular":
		return false, nil
	case "onDemand":
		return true, nil
	default:
		return false, errors.New(where, ".probePolicy must be regular or onDemand")
	}
}
func selectionReturn(where string, v *SelectionReturnConfig, selectorEnabled bool) (*bool, int64, error) {
	if v == nil {
		return nil, 0, nil
	}
	if err := durationIn(where+".returnToCheaper.interval", v.Interval, 1, 0); err != nil {
		return nil, 0, err
	}
	if enabled(v.IsEnabled, false) && !selectorEnabled {
		return nil, 0, errors.New(where, ".returnToCheaper requires balancer.returnToCheaper")
	}
	return v.IsEnabled, optionalDuration(v.Interval), nil
}
func positiveCost(where string, v *float32) error {
	if v == nil || *v <= 0 || math.IsNaN(float64(*v)) || math.IsInf(float64(*v), 0) {
		return errors.New(where, " needs cost above zero")
	}
	return nil
}
func (v *strategyLeastCostConfig) Build() (proto.Message, error) {
	if v.FallbackBalancer != nil && *v.FallbackBalancer == "" {
		return nil, errors.New("leastCost.settings.fallbackBalancer must be a nonempty balancer tag")
	}
	f, u, s, n := v.Failover, v.ReturnToCheaper, v.Startup, v.NetworkChanges
	for _, x := range []struct {
		name  string
		value *duration.Duration
	}{{"returnToCheaper.interval", u.Interval}, {"failover.closeSilent.after", f.CloseSilent.After}} {
		if err := durationIn("leastCost.settings."+x.name, x.value, 1, 0); err != nil {
			return nil, err
		}
	}
	if u.MinHoldTime != nil && *u.MinHoldTime < 0 {
		return nil, errors.New("leastCost.settings.returnToCheaper.minHoldTime must be nonnegative")
	}
	if s.Pin.Timeout != nil && *s.Pin.Timeout < 0 {
		return nil, errors.New("leastCost.settings.startup.pin.timeout must be nonnegative")
	}
	if enabled(s.Pin.IsEnabled, false) && s.Pin.Outbound == "" {
		return nil, errors.New("leastCost.settings.startup.pin.outbound is required")
	}
	if enabled(u.OnNetworkChange.IsEnabled, false) && (!enabled(u.IsEnabled, false) || !enabled(n.IsEnabled, false)) {
		return nil, errors.New("leastCost.settings.returnToCheaper.onNetworkChange requires returnToCheaper and networkChanges")
	}
	if enabled(u.OnNetworkChange.IsEnabled, false) && n.Action == "forget" {
		return nil, errors.New("leastCost.settings.returnToCheaper.onNetworkChange requires networkChanges.action:recheck")
	}
	switch u.OnNetworkChange.Scope {
	case "", "unknown", "all":
	default:
		return nil, errors.New("leastCost.settings.returnToCheaper.onNetworkChange.scope must be unknown or all")
	}
	out := &health.SelectorConfig{Paths: v.Paths, FallbackBalancer: derefString(v.FallbackBalancer), Features: &health.SelectorFeatureConfig{ProbeBeforeSwitch: f.ShouldProbeBeforeSwitch, ClosePending: f.ClosePending.IsEnabled, CloseSilent: f.CloseSilent.IsEnabled, DownAudit: f.DownAudit.IsEnabled, Groups: featurePresence(v.Groups != nil), ReturnToCheaper: u.IsEnabled, ReturnOnNetworkChange: u.OnNetworkChange.IsEnabled, NetworkChanges: n.IsEnabled, StartupPin: s.Pin.IsEnabled}, CloseSilentAfter: optionalDuration(f.CloseSilent.After), MinHoldTime: durationPresence(u.MinHoldTime), StartOutbound: s.Outbound, StartPinTimeout: optionalDuration(s.Pin.Timeout), ReturnToCheaper: &health.ReturnToCheaperConfig{Interval: optionalDuration(u.Interval), TryOnAllNetworks: u.OnNetworkChange.Scope == "all"}}
	// Startup preference and pin presence are independent.
	out.StartPinnedOutbound = s.Pin.Outbound
	switch f.ConfirmWithProbe {
	case "", "auto":
		out.ConfirmWithProbe = health.ConfirmMode_AUTO
	case "always":
		out.ConfirmWithProbe = health.ConfirmMode_ALWAYS
	case "never":
		out.ConfirmWithProbe = health.ConfirmMode_NEVER
	default:
		return nil, errors.New("leastCost.settings.failover.confirmWithProbe must be auto, always or never")
	}
	if enabled(f.DownAudit.IsEnabled, false) && out.ConfirmWithProbe != health.ConfirmMode_AUTO {
		return nil, errors.New("leastCost.settings.failover.downAudit requires failover.confirmWithProbe:auto")
	}
	switch n.Action {
	case "", "recheck":
		out.NetworkChangeAction = health.NetworkChangeMode_RECHECK
	case "forget":
		out.NetworkChangeAction = health.NetworkChangeMode_FORGET
	default:
		return nil, errors.New("leastCost.settings.networkChanges.action must be recheck or forget")
	}
	seen := map[string]bool{}
	for _, g := range v.Groups {
		if g == nil || g.Name == "" {
			return nil, errors.New("leastCost.settings.groups[].name must be nonempty")
		}
		where := "leastCost.settings.groups[" + g.Name + "]"
		if len(g.Name) > 128 {
			return nil, errors.New(where, ".name must not exceed 128 bytes")
		}
		if seen[g.Name] {
			return nil, errors.New(where, ": duplicate group ", g.Name)
		}
		seen[g.Name] = true
		if err := positiveCost(where, g.Cost); err != nil {
			return nil, err
		}
		lazy, err := probePolicy(where, g.ProbePolicy)
		if err != nil {
			return nil, err
		}
		ret, interval, err := selectionReturn(where, g.ReturnToCheaper, enabled(u.IsEnabled, false))
		if err != nil {
			return nil, err
		}
		limit := countOr(g.FailedInRowToLeave, 3)
		if limit < 1 || limit > 16 {
			return nil, errors.New(where, ".failedInRowToLeave must be from 1 to 16")
		}
		if err := validateLabels(where+".labels", g.Labels, 6); err != nil {
			return nil, err
		}
		out.Groups = append(out.Groups, &health.PathGroup{Name: g.Name, Cost: *g.Cost, IsLazy: lazy, ReturnIsEnabled: ret, ReturnInterval: interval, Probe: derefString(g.Probe), FailedInRowToLeave: limit, Labels: g.Labels})
	}
	for _, p := range v.Paths {
		if p.IsProbeOnly && (p.Tag == s.Outbound || (enabled(s.Pin.IsEnabled, false) && p.Tag == s.Pin.Outbound)) {
			return nil, errors.New("leastCost.settings.startup: outbound ", p.Tag, " is probe-only")
		}
	}
	return out, nil
}
func (b *BalancingRule) UnmarshalJSON(raw []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	var strategy struct {
		Type string `json:"type"`
	}
	if raw := fields["strategy"]; len(raw) > 0 {
		if err := json.Unmarshal(raw, &strategy); err != nil {
			return err
		}
	}
	if strings.EqualFold(strategy.Type, strategyLeastCost) {
		if _, ok := fields["selector"]; ok {
			return errors.New("leastCost selector is not supported; use outbounds[].selection")
		}
	}
	type plain BalancingRule
	return json.Unmarshal(raw, (*plain)(b))
}
func checkLeastCost(c *Config) error {
	if c.HealthObservatory != nil {
		c.HealthObservatory.Paths = nil
		if enabled(c.HealthObservatory.Telemetry.Metrics.IsEnabled, false) && c.Metrics == nil {
			return errors.New("healthObservatory.telemetry.metrics requires metrics exporter")
		}
	}
	balancers := map[string]*BalancingRule{}
	parsed := map[string]*strategyLeastCostConfig{}
	groups := map[string]map[string]*strategyPathGroupConfig{}
	providers := map[string][]*HealthProviderConfig{}
	if c.HealthObservatory != nil {
		for _, p := range c.HealthObservatory.Providers {
			if p != nil && enabled(p.IsEnabled, false) {
				for _, tag := range p.Apply.Balancers {
					providers[tag] = append(providers[tag], p)
				}
			}
		}
	}
	if c.RouterConfig != nil {
		for _, b := range c.RouterConfig.Balancers {
			if b == nil {
				continue
			}
			balancers[b.Tag] = b
			if !strings.EqualFold(b.Strategy.Type, strategyLeastCost) {
				continue
			}
			if c.HealthObservatory == nil {
				return errors.New("balancer ", b.Tag, ": leastCost needs healthObservatory")
			}
			if b.FallbackTag == "" {
				return errors.New("balancer ", b.Tag, ": leastCost requires explicit fallbackTag")
			}
			if b.Selectors != nil && b.leastCost == nil {
				return errors.New("balancer ", b.Tag, ": leastCost selector is not supported; use outbounds[].selection")
			}
			v := &strategyLeastCostConfig{}
			if b.Strategy.Settings != nil {
				if err := json.Unmarshal(*b.Strategy.Settings, v); err != nil {
					return errors.New("balancer ", b.Tag, ": ", err)
				}
			}
			if _, err := v.Build(); err != nil {
				return errors.New("balancer ", b.Tag, ": ", err)
			}
			e := c.HealthObservatory.Evidence
			if enabled(v.Failover.DownAudit.IsEnabled, false) && !enabled(e.DialFailures.IsEnabled, true) && !enabled(e.DialTimeout.IsEnabled, true) && !enabled(e.ServerUnverified.IsEnabled, true) {
				return errors.New("balancer ", b.Tag, ".failover.downAudit requires an enabled path-level detector")
			}
			if onChange := v.ReturnToCheaper.OnNetworkChange; enabled(onChange.IsEnabled, false) && onChange.Scope != "all" && !enabled(c.HealthObservatory.Network.Memory.IsEnabled, false) {
				return errors.New("balancer ", b.Tag, ".returnToCheaper.onNetworkChange.scope:unknown requires healthObservatory.network.memory")
			}
			if enabled(v.Startup.Pin.IsEnabled, false) && !enabled(c.HealthObservatory.Host.Pinning.IsEnabled, false) {
				return errors.New("balancer ", b.Tag, ".startup.pin requires healthObservatory.host.pinning")
			}
			gs := map[string]*strategyPathGroupConfig{}
			for _, g := range v.Groups {
				if g.Probe == nil || *g.Probe == "" {
					return errors.New("balancer ", b.Tag, ".groups[", g.Name, "].probe is required")
				}
				if !probeExists(c.HealthObservatory, *g.Probe) {
					return errors.New("balancer ", b.Tag, ".groups[", g.Name, "].probe: unknown probe ", *g.Probe)
				}
				gs[g.Name] = g
			}
			groups[b.Tag] = gs
			parsed[b.Tag] = v
			b.leastCost = v
			b.Selectors = nil
		}
	}
	if err := validateFallbackBalancers(balancers, parsed); err != nil {
		return err
	}
	seenTags := map[string]bool{}
	for i := range c.OutboundConfigs {
		o := &c.OutboundConfigs[i]
		p := o.Selection
		if p == nil {
			continue
		}
		where := "outbound " + o.Tag + ".selection"
		v := parsed[p.Balancer]
		if v == nil {
			return errors.New(where, ".balancer ", p.Balancer, " must name a leastCost balancer")
		}
		if len(o.Tag) > 128 {
			return errors.New(where, ".tag must not exceed 128 bytes")
		}
		if o.Tag == "" || seenTags[o.Tag] {
			return errors.New(where, ": tag must be nonempty and unique")
		}
		seenTags[o.Tag] = true
		if p.IsProbeOnly && !enabled(c.HealthObservatory.Probes.Candidates.IsEnabled, false) && c.HealthObservatory.Telemetry.Census == nil {
			return errors.New(where, ".isProbeOnly requires healthObservatory.probes.candidates or healthObservatory.telemetry.census")
		}
		if err := validateLabels(where+".labels", p.Labels, 6); err != nil {
			return err
		}
		lazy, err := probePolicy(where, p.ProbePolicy)
		if err != nil {
			return err
		}
		ret, interval, err := selectionReturn(where, p.ReturnToCheaper, enabled(v.ReturnToCheaper.IsEnabled, false))
		if err != nil {
			return err
		}
		group := derefString(p.Group)
		if p.Group != nil {
			if group == "" {
				return errors.New(where, ".group needs nonempty name")
			}
			if v.Groups == nil {
				return errors.New(where, ".group requires balancer.groups")
			}
			g := groups[p.Balancer][group]
			if g == nil {
				return errors.New(where, ": unknown group ", group)
			}
			for _, field := range []struct {
				name    string
				present bool
			}{{"cost", p.Cost != nil}, {"probePolicy", p.ProbePolicy != nil}, {"returnToCheaper", p.ReturnToCheaper != nil}, {"probe", p.Probe != nil}} {
				if field.present {
					return errors.New(where, ": ", field.name, " is set by group ", group)
				}
			}
		} else {
			if err := positiveCost(where, p.Cost); err != nil {
				return err
			}
			if p.Probe == nil || *p.Probe == "" {
				return errors.New(where, ".probe is required")
			}
			if !probeExists(c.HealthObservatory, *p.Probe) {
				return errors.New(where, ".probe: unknown probe ", *p.Probe)
			}
		}
		def := &health.PathDefinition{Tag: o.Tag, Balancer: p.Balancer, Protocol: strings.ToLower(o.Protocol), Group: group, Cost: floatValue(p.Cost), IsLazy: lazy, ReturnIsEnabled: ret, ReturnInterval: interval, Probe: derefString(p.Probe), IsProbeOnly: p.IsProbeOnly, Labels: p.Labels}
		if group != "" {
			g := groups[p.Balancer][group]
			def.Probe = derefString(g.Probe)
			def.Labels = make(map[string]string, len(g.Labels)+len(p.Labels))
			for name, value := range g.Labels {
				def.Labels[name] = value
			}
			for name, value := range p.Labels {
				def.Labels[name] = value
			}
			if err := validateLabels(where+".labels", def.Labels, 6); err != nil {
				return err
			}
		}
		v.Paths = append(v.Paths, def)
		c.HealthObservatory.Paths = append(c.HealthObservatory.Paths, def)
	}
	for tag, v := range parsed {
		b := balancers[tag]
		if len(v.Paths) == 0 && len(providers[tag]) == 0 {
			return errors.New("balancer ", tag, ": leastCost has no selections and no enabled provider")
		}
		if v.Groups != nil {
			for name := range groups[tag] {
				used := false
				for _, p := range v.Paths {
					if p.Group == name {
						used = true
						break
					}
				}
				if !used {
					return errors.New("balancer ", tag, ": group ", name, " has no outbounds")
				}
			}
		}
		for _, p := range v.Paths {
			b.Selectors = append(b.Selectors, p.Tag)
		}
		for _, p := range providers[tag] {
			b.Selectors = append(b.Selectors, p.Apply.TagPrefix)
		}
		if _, err := v.Build(); err != nil {
			return errors.New("balancer ", tag, ": ", err)
		}
		if v.Startup.Outbound != "" || enabled(v.Startup.Pin.IsEnabled, false) {
			for _, start := range []string{v.Startup.Outbound, func() string {
				if enabled(v.Startup.Pin.IsEnabled, false) {
					return v.Startup.Pin.Outbound
				}
				return ""
			}()} {
				if start == "" {
					continue
				}
				found := false
				for _, p := range v.Paths {
					if p.Tag == start {
						found = true
						break
					}
				}
				// An outbound of a provider that feeds the balancer: the host
				// starts with the one that worked last, from the set it kept and
				// gives as initialAnswer. The core takes it if the set has it
				// and starts as without it otherwise.
				for _, p := range providers[tag] {
					if p.Apply.TagPrefix != "" && strings.HasPrefix(start, p.Apply.TagPrefix) && len(start) > len(p.Apply.TagPrefix) {
						found = true
					}
				}
				if !found {
					return errors.New("balancer ", tag, ".startup: unknown outbound ", start)
				}
			}
		}
	}
	for tag, ps := range providers {
		if parsed[tag] == nil {
			return errors.New("healthObservatory.providers[", ps[0].Tag, "].apply.balancers ", tag, " must use leastCost")
		}
	}
	if h := c.HealthObservatory; h != nil && enabled(h.Host.View.IsEnabled, false) {
		view := &h.Host.View
		if view.Balancer == "" && len(parsed) == 1 {
			for tag := range parsed {
				view.Balancer = tag
			}
		}
		if view.Balancer == "" {
			return errors.New("healthObservatory.host.view.balancer is required unless the config has exactly one leastCost balancer")
		}
		if parsed[view.Balancer] == nil {
			return errors.New("healthObservatory.host.view.balancer ", view.Balancer, " must name a leastCost balancer")
		}
	}
	if c.HealthObservatory != nil && c.HealthObservatory.Telemetry.Census != nil {
		if err := validateCensusOnDemand(c.HealthObservatory.Telemetry.Census.OnDemand, parsed, groups); err != nil {
			return err
		}
	}
	return checkTagReferences(c, balancers, parsed)
}

// checkTagReferences makes sure the outbounds and balancers the config names
// exist, and that no provider owns an outbound it was not given: a provider
// removes every outbound under its tagPrefix that its answer lacks.
func checkTagReferences(c *Config, balancers map[string]*BalancingRule, leastCost map[string]*strategyLeastCostConfig) error {
	h := c.HealthObservatory
	if h == nil {
		return nil
	}
	var providers []*HealthProviderConfig
	for _, p := range h.Providers {
		if p != nil && p.Apply.TagPrefix != "" {
			providers = append(providers, p)
		}
	}
	outbounds := map[string]bool{}
	for _, o := range c.OutboundConfigs {
		outbounds[o.Tag] = true
		for _, p := range providers {
			if !strings.HasPrefix(o.Tag, p.Apply.TagPrefix) {
				continue
			}
			if o.Selection == nil || !slices.Contains(p.Apply.Balancers, o.Selection.Balancer) {
				return errors.New("healthObservatory.providers[", p.Tag, "]: outbound ", o.Tag, " is under apply.tagPrefix ", p.Apply.TagPrefix, " and the provider would remove it")
			}
		}
	}
	refs := [][2]string{}
	for tag := range leastCost {
		refs = append(refs, [2]string{"balancer " + tag + ".fallbackTag", balancers[tag].FallbackTag})
	}
	if enabled(h.Network.Connectivity.IsEnabled, false) {
		refs = append(refs, [2]string{"healthObservatory.network.connectivity.outbound", h.Network.Connectivity.Outbound})
	}
	if enabled(h.Network.LabelRequest.IsEnabled, false) {
		refs = append(refs, [2]string{"healthObservatory.network.labelRequest.outbound", h.Network.LabelRequest.Outbound})
	}
	for _, ref := range refs {
		if !outbounds[ref[1]] {
			return errors.New(ref[0], ": unknown outbound ", ref[1])
		}
	}
	for _, p := range providers {
		for _, via := range p.Via {
			if !outbounds[via] && balancers[via] == nil {
				return errors.New("healthObservatory.providers[", p.Tag, "].via: unknown outbound or balancer ", via)
			}
		}
	}
	for _, w := range h.Network.Witnesses.Outbounds {
		matched := false
		for tag := range outbounds {
			matched = matched || strings.HasPrefix(tag, w)
		}
		for _, p := range providers {
			matched = matched || strings.HasPrefix(w, p.Apply.TagPrefix) || strings.HasPrefix(p.Apply.TagPrefix, w)
		}
		if !matched {
			return errors.New("healthObservatory.network.witnesses.outbounds: ", w, " matches no outbound")
		}
	}
	return nil
}

// An unqualified name must identify an on-demand path or group everywhere it
// occurs. Allowing a regular homonym would unintentionally admit a path that
// the backend did not intend to include.
func validateCensusOnDemand(names []string, selectors map[string]*strategyLeastCostConfig, groups map[string]map[string]*strategyPathGroupConfig) error {
	// Every report row needs an unambiguous key, including regular paths
	// that are admitted automatically and do not occur in onDemand.
	keys := make(map[string]string)
	for balancer, selector := range selectors {
		for name := range groups[balancer] {
			owner := "group " + name + " in balancer " + balancer
			if previous := keys[name]; previous != "" {
				return errors.New("healthObservatory.telemetry.census: ambiguous report key ", name, ": ", previous, " and ", owner)
			}
			keys[name] = owner
		}
		for _, path := range selector.Paths {
			if path.Group != "" {
				continue
			}
			owner := "path " + path.Tag + " in balancer " + balancer
			if previous := keys[path.Tag]; previous != "" {
				return errors.New("healthObservatory.telemetry.census: ambiguous report key ", path.Tag, ": ", previous, " and ", owner)
			}
			keys[path.Tag] = owner
		}
	}
	policies := make(map[string][]bool)
	for balancer, selector := range selectors {
		for _, path := range selector.Paths {
			lazy := path.IsLazy
			if path.Group != "" {
				group := groups[balancer][path.Group]
				lazy = group.ProbePolicy != nil && *group.ProbePolicy == "onDemand"
			}
			policies[path.Tag] = append(policies[path.Tag], lazy)
		}
		for name, group := range groups[balancer] {
			policies[name] = append(policies[name], group.ProbePolicy != nil && *group.ProbePolicy == "onDemand")
		}
	}
	for i, name := range names {
		where := fmt.Sprintf("healthObservatory.telemetry.census.onDemand[%d]", i)
		matches := policies[name]
		if len(matches) == 0 {
			return errors.New(where, ": unknown path or group ", name)
		}
		for _, lazy := range matches {
			if !lazy {
				return errors.New(where, ": ", name, " must use probePolicy:onDemand")
			}
		}
	}
	return nil
}

func validateFallbackBalancers(balancers map[string]*BalancingRule, selectors map[string]*strategyLeastCostConfig) error {
	for tag, selector := range selectors {
		next := derefString(selector.FallbackBalancer)
		if next == "" {
			continue
		}
		where := "routing.balancers[" + tag + "].strategy.settings.fallbackBalancer"
		if balancers[next] == nil {
			return errors.New(where, ": unknown balancer ", next, "; chain ", tag, " -> ", next)
		}
		if next == tag {
			return errors.New(where, ": cannot fall back to itself; chain ", tag, " -> ", tag)
		}
		chain := []string{tag}
		seen := map[string]bool{tag: true}
		for next != "" {
			chain = append(chain, next)
			if seen[next] {
				return errors.New(where, ": cycle ", strings.Join(chain, " -> "))
			}
			seen[next] = true
			if target := selectors[next]; target != nil {
				next = derefString(target.FallbackBalancer)
			} else {
				next = ""
			}
		}
		if len(chain) > 4 {
			return errors.New(where, ": chain must contain at most 4 balancers: ", strings.Join(chain, " -> "))
		}
	}
	return nil
}
func probeExists(c *HealthObservatoryConfig, name string) bool {
	if c == nil || name == "" {
		return false
	}
	for _, p := range c.Probes.Profiles {
		if p != nil && p.Name == name {
			return true
		}
	}
	return false
}
