package health

import (
	"fmt"
	"time"

	"github.com/xtls/xray-core/core"
)

// InstanceKnobs are timings and thresholds that no config sets; integration
// tests shorten time with them. Zero keeps the default.
type InstanceKnobs struct {
	Sweep              time.Duration
	Failures           int
	MinDistinctTargets int
	SuccessBytes       int64
	PartialMinBytes    int64
	ProbeSpacing       time.Duration
	SuspectCooldown    time.Duration
	HostReuse          time.Duration
	ReturnSuccesses    int
	FixedReturnPhase   bool
}

// TuneInstance applies k to the observer of instance. It must run before
// Start.
func TuneInstance(instance *core.Instance, k InstanceKnobs) error {
	o := FromInstance(instance)
	if o == nil {
		return fmt.Errorf("test scenario has no health observer")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.started {
		return fmt.Errorf("test knobs must be set before Start")
	}
	p := &o.s.passive
	p.sweepInterval = dur(int64(k.Sweep), p.sweepInterval)
	p.failures = num(int32(k.Failures), p.failures)
	p.minDistinctTargets = num(int32(k.MinDistinctTargets), p.minDistinctTargets)
	p.successBytes = size(k.SuccessBytes, p.successBytes)
	p.partialMinBytes = size(k.PartialMinBytes, p.partialMinBytes)
	sc := &o.s.schedule
	sc.spacing = dur(int64(k.ProbeSpacing), sc.spacing)
	sc.suspectCooldown = dur(int64(k.SuspectCooldown), sc.suspectCooldown)
	sc.hostProbes.reuse = dur(int64(k.HostReuse), sc.hostProbes.reuse)
	for _, s := range o.e.selectors {
		r := &s.cfg.returnToCheaper
		r.randomPhase = !k.FixedReturnPhase
		r.successes = num(int32(k.ReturnSuccesses), r.successes)
	}
	return nil
}
