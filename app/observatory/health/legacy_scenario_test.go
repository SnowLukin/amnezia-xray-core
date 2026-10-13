package health

import (
	"google.golang.org/protobuf/proto"
)

// legacyScenarioSettings is the explicit product profile used by pre-v2 unit
// timelines. Default/off/parity tests use the real constructor directly. The
// retired fast timing knobs below live only in this test recipe, after normal
// positive feature construction; they are not accepted configuration aliases.
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
	legacyFlag(&f.Quality, !p.GetShouldSkipQuality())
	legacyFlag(&f.PartialStalls, !p.GetShouldIgnorePartialStalls())
	legacyFlag(&f.UdpReplies, p.GetUdpReplyTimeout() >= 0)
	legacyFlag(&f.LoneFailureProbe, p.GetLoneFailureAfter() >= 0)
	legacyFlag(&f.UserPresent, true)
	legacyFlag(&f.Pinning, true)
	legacyFlag(&f.MemoryPressure, true)
	legacyFlag(&f.TunnelSleep, c.GetSleep().GetAfterLastUse() >= 0)
	legacyFlag(&f.CloseUnchosenIdle, c.GetSleep().GetCloseIdleAfter() > 0)
	legacyFlag(&f.NetworkMemory, !c.GetNetwork().GetNoMemory())
	legacyFlag(&f.Connectivity, cc != nil && cc.GetOutbound() != "" && cc.GetDestination() != "")
	legacyFlag(&f.Witnesses, len(cc.GetWitnesses()) > 0)
	legacyFlag(&f.ProbeRequests, true)
	legacyFlag(&f.Candidates, true)
	legacyFlag(&f.StepProfiles, true)
	legacyFlag(&f.UdpProbes, c.GetUdpProbe() != nil)
	for _, pc := range c.GetProviders() {
		if pc.IsEnabled == nil {
			pc.IsEnabled = proto.Bool(true)
		}
	}
	return c
}

func legacyScenarioSettings(input *Config) *settings {
	c := legacyScenarioConfig(input)
	p := c.GetPassive()
	cc := c.GetConnectivity()
	s := newSettings(c)
	s.passive.window = dur(p.GetWindow(), defaultWindow)
	s.passive.failures = num(p.GetFailuresToSuspect(), defaultFailures)
	s.passive.minDistinctTargets = num(p.GetMinDistinctTargets(), defaultMinDistinctTargets)
	s.passive.successBytes = size(p.GetSuccessBytes(), defaultSuccessBytes)
	s.passive.sweepInterval = dur(p.GetSweepInterval(), defaultSweepInterval)
	s.passive.dialOnly = p.GetIsDialOnly()
	s.passive.stallMinWrite = size(p.GetStallMinWriteBytes(), defaultStallMinWrite)
	s.passive.partialMinBytes = size(p.GetPartialStallMinBytes(), defaultPartialMinBytes)
	if s.connectivity != nil {
		s.connectivity.recheck = toBackoff(cc.GetRecheckBackoff(), defaultConnRecheck)
		s.connectivity.maxPause = dur(cc.GetMaxPause(), defaultMaxPause)
	}
	sc := c.GetSchedule()
	s.schedule.idleAfter = dur(sc.GetIdleAfter(), defaultIdleAfter)
	s.schedule.quarantine = toBackoff(sc.GetQuarantine(), defaultQuarantine)
	s.schedule.spacing = dur(sc.GetMinProbeInterval(), defaultSpacing)
	s.schedule.spacingJitter = jitter(sc.GetProbeJitterFraction(), defaultSpacingJitter)
	s.schedule.suspectCooldown = dur(sc.GetSuspectCooldown(), defaultSuspectCooldown)
	s.schedule.hostProbes.reuse = min(dur(sc.GetHostProbes().GetReuseWindow(), defaultHostReuse), maxHostReuse)
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
	legacyFlag(&f.ProbeBeforeSwitch, !c.GetSkipProbeBeforeSwitch())
	legacyFlag(&f.DownAudit, !c.GetShouldSkipDownAudit())
	legacyFlag(&f.Groups, len(c.GetGroups()) > 0)
	legacyFlag(&f.ReturnToCheaper, u.GetInterval() >= 0)
	legacyFlag(&f.NetworkChanges, c.GetNetworkChangeAction() != NetworkChangeMode_IGNORE)
	legacyFlag(&f.ReturnOnNetworkChange, !u.GetSkipTryOnNetworkChange())
	legacyFlag(&f.StartupPin, c.GetStartPinnedOutbound() != "")
	legacyFlag(&f.ClosePending, !observer.GetPassive().GetShouldKeepPendingOnDown())
	legacyFlag(&f.CloseSilent, !observer.GetPassive().GetShouldKeepSilentOnDown())
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
	s.returnToCheaper.randomPhase = !u.GetSkipRandomPhase()
	s.returnToCheaper.successes = num(u.GetProbesToReturn(), defaultReturnToCheaperSuccesses)
	s.returnToCheaper.spacing = dur(u.GetProbeInterval(), defaultReturnToCheaperSpacing)
	s.returnToCheaper.penaltyWindow = dur(u.GetPenaltyWindow(), defaultPenaltyWindow)
	s.returnToCheaper.penaltyMultiplier = fnum(u.GetPenaltyMultiplier(), defaultPenaltyMultiplier)
	s.returnToCheaper.maxPenalty = dur(u.GetMaxPenaltyDelay(), defaultMaxPenalty)
	return s
}
