package health

import "time"

type probeResult struct {
	ok       bool
	rtt      time.Duration
	bytes    int64
	err      string
	class    string
	canceled bool
	profile  string
	state    pathStatus
	steps    []probeStepResult
}

type probeStepResult struct {
	name        string
	ok          bool
	connect     time.Duration
	elapsed     time.Duration
	bytes       int64
	destination int
	class       string
}

func (r probeResult) pathState() pathStatus {
	if r.state != statusUnknown {
		return r.state
	}
	if r.ok {
		return statusUp
	}
	return statusDown
}

func (r probeResult) snapshot() probeResult {
	r.steps = append([]probeStepResult(nil), r.steps...)
	return r
}

func (r probeResult) degradedReason() string {
	for _, step := range r.steps {
		if !step.ok {
			return step.name
		}
	}
	return "probe"
}

func (r probeResult) eventSteps() []map[string]any {
	steps := make([]map[string]any, 0, len(r.steps))
	for _, step := range r.steps {
		v := map[string]any{
			"name": step.name, "is_passed": step.ok,
			"connect_ms": step.connect.Milliseconds(), "ms": step.elapsed.Milliseconds(),
			"bytes": step.bytes,
		}
		if step.ok {
			v["destination"] = step.destination
		} else {
			v["error"] = step.class
		}
		steps = append(steps, v)
	}
	return steps
}
