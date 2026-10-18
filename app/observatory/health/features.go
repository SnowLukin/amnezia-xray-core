package health

import (
	"fmt"
	"math"
	"net/url"
)

// featureValue preserves absence independently of an explicit false.
func featureValue(value *bool, fallback bool) bool {
	if value == nil {
		return fallback
	}
	return *value
}

type featureSettings struct {
	Census            bool
	Trace             bool
	DialFailures      bool
	DialTimeout       bool
	ServerUnverified  bool
	FirstReply        bool
	EarlyClose        bool
	StalledRequests   bool
	PartialStalls     bool
	UdpReplies        bool
	LoneFailureProbe  bool
	StepProfiles      bool
	Candidates        bool
	ProbeRequests     bool
	UdpProbes         bool
	Connectivity      bool
	Witnesses         bool
	NetworkMemory     bool
	HostLabels        bool
	LabelRequest      bool
	Pinning           bool
	UserPresent       bool
	MemoryPressure    bool
	TunnelSleep       bool
	CloseUnchosenIdle bool
	Events            bool
	Report            bool
	Quality           bool
	PathLabels        bool
	ProbeMeasurements bool
	Metrics           bool
	SessionLabels     bool
}

func newFeatureSettings(c *FeatureConfig) featureSettings {
	if c == nil {
		c = &FeatureConfig{}
	}
	return featureSettings{
		DialFailures:      featureValue(c.DialFailures, true),
		DialTimeout:       featureValue(c.DialTimeout, true),
		ServerUnverified:  featureValue(c.ServerUnverified, true),
		FirstReply:        featureValue(c.FirstReply, true),
		EarlyClose:        featureValue(c.EarlyClose, true),
		StalledRequests:   featureValue(c.StalledRequests, true),
		PartialStalls:     featureValue(c.PartialStalls, false),
		UdpReplies:        featureValue(c.UdpReplies, false),
		LoneFailureProbe:  featureValue(c.LoneFailureProbe, false),
		StepProfiles:      featureValue(c.StepProfiles, false),
		Candidates:        featureValue(c.Candidates, false),
		ProbeRequests:     featureValue(c.ProbeRequests, false),
		UdpProbes:         featureValue(c.UdpProbes, false),
		Connectivity:      featureValue(c.Connectivity, false),
		Witnesses:         featureValue(c.Witnesses, false),
		NetworkMemory:     featureValue(c.NetworkMemory, false),
		HostLabels:        featureValue(c.HostLabels, false),
		LabelRequest:      featureValue(c.LabelRequest, false),
		Pinning:           featureValue(c.Pinning, false),
		UserPresent:       featureValue(c.UserPresent, false),
		MemoryPressure:    featureValue(c.MemoryPressure, false),
		TunnelSleep:       featureValue(c.TunnelSleep, false),
		CloseUnchosenIdle: featureValue(c.CloseUnchosenIdle, false),
		Events:            featureValue(c.Events, false),
		Report:            featureValue(c.Report, false),
		Quality:           featureValue(c.Quality, false),
		PathLabels:        featureValue(c.PathLabels, false),
		ProbeMeasurements: featureValue(c.ProbeMeasurements, false),
		Metrics:           featureValue(c.Metrics, false),
		SessionLabels:     featureValue(c.SessionLabels, false),
	}
}

func (f featureSettings) passiveTCP() bool {
	return f.DialFailures || f.DialTimeout || f.ServerUnverified || f.FirstReply || f.EarlyClose || f.StalledRequests || f.PartialStalls || f.LoneFailureProbe
}
func (f featureSettings) failure(kind failureKind) bool {
	switch kind {
	case failDial:
		return f.DialFailures
	case failDialTimeout:
		return f.DialTimeout
	case failUnverified:
		return f.ServerUnverified
	case failNoResponse:
		return f.FirstReply
	case failClosed:
		return f.EarlyClose
	case failStall:
		return f.StalledRequests
	case failPartialStall:
		return f.PartialStalls
	}
	return false
}

func (f featureSettings) status() map[string]bool {
	return map[string]bool{
		"census":              f.Census,
		"trace":               f.Trace,
		"dial_failures":       f.DialFailures,
		"dial_timeout":        f.DialTimeout,
		"server_unverified":   f.ServerUnverified,
		"first_reply":         f.FirstReply,
		"early_close":         f.EarlyClose,
		"stalled_requests":    f.StalledRequests,
		"partial_stalls":      f.PartialStalls,
		"udp_replies":         f.UdpReplies,
		"lone_failure_probe":  f.LoneFailureProbe,
		"step_profiles":       f.StepProfiles,
		"candidates":          f.Candidates,
		"probe_requests":      f.ProbeRequests,
		"udp_probe":           f.UdpProbes,
		"connectivity":        f.Connectivity,
		"witnesses":           f.Witnesses,
		"network_memory":      f.NetworkMemory,
		"host_labels":         f.HostLabels,
		"label_request":       f.LabelRequest,
		"pinning":             f.Pinning,
		"user_present":        f.UserPresent,
		"memory_pressure":     f.MemoryPressure,
		"tunnel_sleep":        f.TunnelSleep,
		"close_unchosen_idle": f.CloseUnchosenIdle,
		"events":              f.Events,
		"report":              f.Report,
		"quality":             f.Quality,
		"path_labels":         f.PathLabels,
		"probe_measurements":  f.ProbeMeasurements,
		"metrics":             f.Metrics,
		"session_labels":      f.SessionLabels,
	}
}

// ValidateFeatures applies dependencies to direct protobuf configurations too.
func (c *Config) ValidateFeatures() error {
	if err := validateProviderConfigurations(c); err != nil {
		return err
	}
	f := newFeatureSettings(c.GetFeatures())
	if cc := c.GetCensus(); cc != nil {
		if !f.Report {
			return fmt.Errorf("healthObservatory.telemetry.census requires telemetry.report")
		}
		if math.IsNaN(cc.SampleFraction) || math.IsInf(cc.SampleFraction, 0) || cc.SampleFraction <= 0 || cc.SampleFraction > 1 {
			return fmt.Errorf("healthObservatory.telemetry.census.sampleFraction must be in (0, 1]")
		}
		if cc.StartAfter < 0 {
			return fmt.Errorf("healthObservatory.telemetry.census.startAfter must be positive")
		}
	}
	if f.LoneFailureProbe && !f.DialFailures && !f.DialTimeout && !f.ServerUnverified {
		return fmt.Errorf("healthObservatory.evidence.loneFailureProbe requires evidence.dialFailures, evidence.dialTimeout or evidence.serverUnverified")
	}
	if c.GetReport().GetQuietShare() > 0 && !f.Report {
		return fmt.Errorf("healthObservatory.telemetry.report.quietSampleFraction requires telemetry.report")
	}
	if f.SessionLabels && !f.Report {
		return fmt.Errorf("healthObservatory.telemetry.report.sessionLabels requires telemetry.report")
	}
	if f.Witnesses && len(c.GetConnectivity().GetWitnesses()) == 0 {
		return fmt.Errorf("healthObservatory.network.witnesses.outbounds must be nonempty when witnesses")
	}
	if f.Connectivity && (c.GetConnectivity().GetOutbound() == "" || len(c.GetConnectivity().GetDestinations()) == 0) {
		return fmt.Errorf("healthObservatory.network.connectivity requires outbound and destinations")
	}
	if cc := c.GetConnectivity(); cc != nil {
		if len(cc.Destinations) > 4 {
			return fmt.Errorf("healthObservatory.network.connectivity.destinations must contain 1 to 4 addresses")
		}
		for i, destination := range cc.Destinations {
			u, err := url.Parse(destination)
			if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") || u.User != nil || u.Fragment != "" {
				return fmt.Errorf("healthObservatory.network.connectivity.destinations[%d] must be an HTTP(S) URL without credentials or fragment", i)
			}
		}
	}
	if f.UdpProbes && len(c.GetUdpProbe().GetDestinations()) == 0 {
		return fmt.Errorf("healthObservatory.probes.udp.destinations must be nonempty when udp")
	}
	for i, p := range c.GetPaths() {
		if p.GetIsProbeOnly() && !f.Candidates && c.GetCensus() == nil {
			return fmt.Errorf("healthObservatory.paths[%d].isProbeOnly requires probes.candidates or telemetry.census", i)
		}
	}
	for i, p := range c.GetProbes() {
		needsSteps := len(p.GetSteps()) > 1
		for _, step := range p.GetSteps() {
			needsSteps = needsSteps || step.GetIsOptional()
		}
		if needsSteps && !f.StepProfiles {
			return fmt.Errorf("healthObservatory.probes.profiles[%d].steps requires probes.stepProfiles", i)
		}
	}
	return nil
}

func validateSelectorFeatures(c *SelectorConfig, f featureSettings) error {
	groups := make(map[string]map[string]string)
	for i, g := range c.GetGroups() {
		if err := ValidateLabels(g.GetLabels(), 6); err != nil {
			return fmt.Errorf("leastCost.settings.groups[%d].labels: %w", i, err)
		}
		groups[g.Name] = g.GetLabels()
	}
	for i, p := range c.GetPaths() {
		if err := ValidateLabels(p.GetLabels(), 6); err != nil {
			return fmt.Errorf("leastCost.settings.paths[%d].labels: %w", i, err)
		}
		merged := make(map[string]string)
		for k, v := range groups[p.GetGroup()] {
			merged[k] = v
		}
		for k, v := range p.GetLabels() {
			merged[k] = v
		}
		if err := ValidateLabels(merged, 6); err != nil {
			return fmt.Errorf("leastCost.settings.paths[%d].effectiveLabels: %w", i, err)
		}
	}

	sf := c.GetFeatures()
	if sf == nil {
		sf = &SelectorFeatureConfig{}
	}
	returns := featureValue(sf.ReturnToCheaper, false)
	if featureValue(sf.DownAudit, false) && (c.GetConfirmWithProbe() != ConfirmMode_AUTO || (!f.DialFailures && !f.DialTimeout && !f.ServerUnverified)) {
		return fmt.Errorf("leastCost.settings.failover.downAudit requires confirmWithProbe:auto and an enabled path-level evidence detector")
	}
	if featureValue(sf.ReturnOnNetworkChange, false) && (!returns || !featureValue(sf.NetworkChanges, false)) {
		return fmt.Errorf("leastCost.settings.returnToCheaper.onNetworkChange requires returnToCheaper and networkChanges")
	}
	if featureValue(sf.StartupPin, false) && !f.Pinning {
		return fmt.Errorf("leastCost.settings.startup.pin requires healthObservatory.host.pinning")
	}
	for i, p := range c.GetPaths() {
		if p.GetGroup() != "" && !featureValue(sf.Groups, false) {
			return fmt.Errorf("leastCost.settings.paths[%d].group requires groups", i)
		}
		if p.ReturnIsEnabled != nil && *p.ReturnIsEnabled && !returns {
			return fmt.Errorf("leastCost.settings.paths[%d].returnToCheaper requires balancer returnToCheaper", i)
		}
	}
	for i, g := range c.GetGroups() {
		if g.ReturnIsEnabled != nil && *g.ReturnIsEnabled && !returns {
			return fmt.Errorf("leastCost.settings.groups[%d].returnToCheaper requires balancer returnToCheaper", i)
		}
	}
	return nil
}
