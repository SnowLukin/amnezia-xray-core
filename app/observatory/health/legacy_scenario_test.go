package health

import (
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
)

// The core runs the timings and thresholds below at their defaults: no
// config sets them. Unit tests shorten time and fix random choices through
// them, attached with tune to the part of the test config they belong to.
// Zero keeps the default and a negative duration turns the timer off, as
// for config values.
type passiveKnobs struct {
	window          time.Duration
	failures        int
	minTargets      int
	successBytes    int64
	sweep           time.Duration
	stallMinWrite   int64
	partialMinBytes int64
}

type scheduleKnobs struct {
	idleAfter time.Duration
	// Zero fields keep those of the default.
	quarantine      backoff
	spacing         time.Duration
	spacingJitter   float64
	suspectCooldown time.Duration
	hostReuse       time.Duration
}

type returnKnobs struct {
	fixedPhase        bool
	successes         int
	spacing           time.Duration
	penaltyWindow     time.Duration
	penaltyMultiplier float64
	maxPenalty        time.Duration
}

type providerKnobs struct {
	// Spread of the refresh interval; negative is none.
	jitter    float64
	backoff   backoff
	authRetry time.Duration
}

// renewJitter is the spread of lease renewal of an ExpiryTrigger; negative
// is none.
type renewJitter float64

var knobs sync.Map

// tune attaches k to part, a new one for nil, and returns part.
func tune[M, K any](part *M, k K) *M {
	if part == nil {
		part = new(M)
	}
	knobs.Store(part, &k)
	return part
}

// knobsOf returns the knobs of part to change; part is not nil.
func knobsOf[K any](part any) *K {
	v, _ := knobs.LoadOrStore(part, new(K))
	return v.(*K)
}

func knobsFor[K any](part any) K {
	if v, ok := knobs.Load(part); ok {
		return *v.(*K)
	}
	var k K
	return k
}

func fill(b, def backoff) backoff {
	return backoff{
		initial:    dur(int64(b.initial), def.initial),
		multiplier: fnum(b.multiplier, def.multiplier),
		max:        dur(int64(b.max), def.max),
		jitter:     jitter(b.jitter, def.jitter),
	}
}

// legacyScenarioConfig turns on the features the unit timelines were
// written with, unless the config sets them.
func legacyScenarioConfig(input *Config) *Config {
	if input == nil {
		input = &Config{}
	}
	c := proto.Clone(input).(*Config)
	if c.Features == nil {
		c.Features = &FeatureConfig{}
	}
	f := c.Features
	p := c.GetPassive()
	cc := c.GetConnectivity()
	legacyFlag(&f.Events, true)
	legacyFlag(&f.Report, true)
	legacyFlag(&f.Quality, true)
	legacyFlag(&f.PartialStalls, true)
	legacyFlag(&f.UdpReplies, p.GetUdpReplyTimeout() >= 0)
	legacyFlag(&f.LoneFailureProbe, p.GetLoneFailureAfter() >= 0)
	legacyFlag(&f.UserPresent, true)
	legacyFlag(&f.Pinning, true)
	legacyFlag(&f.MemoryPressure, true)
	legacyFlag(&f.TunnelSleep, c.GetSleep().GetAfterLastUse() >= 0)
	legacyFlag(&f.CloseUnchosenIdle, c.GetSleep().GetCloseIdleAfter() > 0)
	legacyFlag(&f.NetworkMemory, true)
	legacyFlag(&f.Connectivity, cc != nil && cc.GetOutbound() != "" && len(cc.GetDestinations()) > 0)
	legacyFlag(&f.Witnesses, len(cc.GetWitnesses()) > 0)
	legacyFlag(&f.ProbeRequests, true)
	legacyFlag(&f.Candidates, true)
	legacyFlag(&f.StepProfiles, true)
	legacyFlag(&f.UdpProbes, c.GetUdpProbe() != nil)
	return c
}

func legacyScenarioSettings(input *Config) *settings {
	c := legacyScenarioConfig(input)
	s := newSettings(c)
	p := knobsFor[passiveKnobs](input.GetPassive())
	s.passive.window = dur(int64(p.window), s.passive.window)
	s.passive.failures = num(int32(p.failures), s.passive.failures)
	s.passive.minDistinctTargets = num(int32(p.minTargets), s.passive.minDistinctTargets)
	s.passive.successBytes = size(p.successBytes, s.passive.successBytes)
	s.passive.sweepInterval = dur(int64(p.sweep), s.passive.sweepInterval)
	s.passive.stallMinWrite = size(p.stallMinWrite, s.passive.stallMinWrite)
	s.passive.partialMinBytes = size(p.partialMinBytes, s.passive.partialMinBytes)
	sc := knobsFor[scheduleKnobs](input.GetSchedule())
	s.schedule.idleAfter = dur(int64(sc.idleAfter), s.schedule.idleAfter)
	s.schedule.quarantine = fill(sc.quarantine, s.schedule.quarantine)
	s.schedule.spacing = dur(int64(sc.spacing), s.schedule.spacing)
	s.schedule.spacingJitter = jitter(sc.spacingJitter, s.schedule.spacingJitter)
	s.schedule.suspectCooldown = dur(int64(sc.suspectCooldown), s.schedule.suspectCooldown)
	s.schedule.hostProbes.reuse = dur(int64(sc.hostReuse), s.schedule.hostProbes.reuse)
	if s.features.Report {
		s.reportInterval = optional(c.GetReport().GetInterval(), defaultReportInterval)
	}
	return s
}

func legacyFlag(dst **bool, value bool) {
	if *dst == nil {
		*dst = proto.Bool(value)
	}
}

// legacyScenarioSelector turns on the selector features the unit timelines
// were written with, unless the config sets them.
func legacyScenarioSelector(input *SelectorConfig, observer *Config) *selectorSettings {
	if input == nil {
		input = &SelectorConfig{}
	}
	c := proto.Clone(input).(*SelectorConfig)
	if c.Features == nil {
		c.Features = &SelectorFeatureConfig{}
	}
	f := c.Features
	u := c.GetReturnToCheaper()
	legacyFlag(&f.ProbeBeforeSwitch, true)
	legacyFlag(&f.DownAudit, true)
	legacyFlag(&f.Groups, len(c.GetGroups()) > 0)
	legacyFlag(&f.ReturnToCheaper, u.GetInterval() >= 0)
	legacyFlag(&f.NetworkChanges, c.GetNetworkChangeAction() != NetworkChangeMode_IGNORE)
	legacyFlag(&f.ReturnOnNetworkChange, true)
	legacyFlag(&f.StartupPin, c.GetStartPinnedOutbound() != "")
	legacyFlag(&f.ClosePending, true)
	legacyFlag(&f.CloseSilent, true)
	if c.CloseSilentAfter == 0 {
		c.CloseSilentAfter = int64(dur(observer.GetPassive().GetStallTimeout(), defaultStallTimeout))
	}
	for _, p := range c.Paths {
		if p.ReturnIsEnabled == nil && p.ReturnInterval < 0 {
			p.ReturnIsEnabled = proto.Bool(false)
		}
	}
	for _, g := range c.Groups {
		if g.ReturnIsEnabled == nil && g.ReturnInterval < 0 {
			g.ReturnIsEnabled = proto.Bool(false)
		}
	}
	s := newSelectorSettings(c)
	k := knobsFor[returnKnobs](input.GetReturnToCheaper())
	r := &s.returnToCheaper
	r.randomPhase = !k.fixedPhase
	r.successes = num(int32(k.successes), r.successes)
	r.spacing = dur(int64(k.spacing), r.spacing)
	r.penaltyWindow = dur(int64(k.penaltyWindow), r.penaltyWindow)
	r.penaltyMultiplier = fnum(k.penaltyMultiplier, r.penaltyMultiplier)
	r.maxPenalty = dur(int64(k.maxPenalty), r.maxPenalty)
	return s
}

// probeFirst sets whether the selector probes a candidate before it switches.
func probeFirst(v bool) *SelectorFeatureConfig {
	return &SelectorFeatureConfig{ProbeBeforeSwitch: proto.Bool(v)}
}

func noProbeNoAudit() *SelectorFeatureConfig {
	return &SelectorFeatureConfig{ProbeBeforeSwitch: proto.Bool(false), DownAudit: proto.Bool(false)}
}

// fixed is a delay range of one value.
func fixed(d time.Duration) *DurationRange { return &DurationRange{Min: int64(d), Max: int64(d)} }
