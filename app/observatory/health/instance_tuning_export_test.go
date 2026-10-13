package health

import (
	"fmt"
	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/core"
)

// TuneInstanceScenario applies retired test clocks and evidence thresholds to
// the already constructed observer. Feature flags and collector allocations
// come exclusively from the real v2 constructor. It must run before Start.
func TuneInstanceScenario(instance *core.Instance, cfg *core.Config) error {
	o := FromInstance(instance)
	if o == nil {
		return fmt.Errorf("test scenario has no health observer")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return fmt.Errorf("test clocks must be set before Start")
	}
	for _, app := range cfg.App {
		raw, err := app.GetInstance()
		if err != nil {
			return err
		}
		switch c := raw.(type) {
		case *Config:
			p, sc := c.GetPassive(), c.GetSchedule()
			o.s.passive.window = dur(p.GetWindow(), defaultWindow)
			o.s.passive.failures = num(p.GetFailuresToSuspect(), defaultFailures)
			o.s.passive.minDistinctTargets = num(p.GetMinDistinctTargets(), defaultMinDistinctTargets)
			o.s.passive.successBytes = size(p.GetSuccessBytes(), defaultSuccessBytes)
			o.s.passive.sweepInterval = dur(p.GetSweepInterval(), defaultSweepInterval)
			o.s.passive.stallMinWrite = size(p.GetStallMinWriteBytes(), defaultStallMinWrite)
			o.s.passive.partialMinBytes = size(p.GetPartialStallMinBytes(), defaultPartialMinBytes)
			o.s.schedule.spacing = dur(sc.GetMinProbeInterval(), defaultSpacing)
			o.s.schedule.suspectCooldown = dur(sc.GetSuspectCooldown(), defaultSuspectCooldown)
			o.s.schedule.hostProbes.reuse = dur(sc.GetHostProbes().GetReuseWindow(), defaultHostReuse)
		case *router.Config:
			for _, b := range c.BalancingRule {
				raw, err := b.GetStrategySettings().GetInstance()
				if err != nil {
					return err
				}
				c, ok := raw.(*SelectorConfig)
				if !ok {
					continue
				}
				selector := o.e.selectors[b.Tag]
				if selector == nil {
					return fmt.Errorf("test scenario selector %s missing", b.Tag)
				}
				u := c.GetReturnToCheaper()
				selector.cfg.returnToCheaper.randomPhase = !u.GetSkipRandomPhase()
				selector.cfg.returnToCheaper.successes = num(u.GetProbesToReturn(), defaultReturnToCheaperSuccesses)
				selector.cfg.returnToCheaper.spacing = dur(u.GetProbeInterval(), defaultReturnToCheaperSpacing)
				selector.cfg.returnToCheaper.penaltyWindow = dur(u.GetPenaltyWindow(), defaultPenaltyWindow)
				selector.cfg.returnToCheaper.penaltyMultiplier = fnum(u.GetPenaltyMultiplier(), defaultPenaltyMultiplier)
				selector.cfg.returnToCheaper.maxPenalty = dur(u.GetMaxPenaltyDelay(), defaultMaxPenalty)
			}
		}
	}
	return nil
}
