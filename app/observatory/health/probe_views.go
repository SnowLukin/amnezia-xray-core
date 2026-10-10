package health

import "time"

type statusProbeStepView struct {
	Name        string `json:"name"`
	IsPassed    bool   `json:"is_passed"`
	ConnectMs   int64  `json:"connect_ms"`
	Ms          int64  `json:"ms"`
	Bytes       int64  `json:"bytes"`
	Destination *int   `json:"destination,omitempty"`
	Error       string `json:"error,omitempty"`
}

type statusProbeView struct {
	Profile string                `json:"profile"`
	State   string                `json:"state"`
	AgoMs   int64                 `json:"ago_ms"`
	Steps   []statusProbeStepView `json:"steps"`
}

type requestProbeStep struct {
	Name        string `json:"name"`
	IsPassed    bool   `json:"isPassed"`
	ConnectMs   int64  `json:"connectMs"`
	Ms          int64  `json:"ms"`
	Bytes       int64  `json:"bytes"`
	Destination *int   `json:"destination,omitempty"`
	Error       string `json:"error,omitempty"`
}

func (p *path) statusProbe(now time.Time) *statusProbeView {
	if p.lastProbeEnd.IsZero() {
		return nil
	}
	r := p.lastProbeResult
	v := &statusProbeView{
		Profile: r.profile,
		State:   r.pathState().String(),
		AgoMs:   max(now.Sub(p.lastProbeEnd).Milliseconds(), 0),
		Steps:   make([]statusProbeStepView, 0, len(r.steps)),
	}
	for _, step := range r.steps {
		s := statusProbeStepView{
			Name: step.name, IsPassed: step.ok,
			ConnectMs: step.connect.Milliseconds(), Ms: step.elapsed.Milliseconds(), Bytes: step.bytes,
		}
		if step.ok && step.destination >= 0 {
			s.Destination = &step.destination
		} else if !step.ok {
			s.Error = step.class
		}
		v.Steps = append(v.Steps, s)
	}
	return v
}

func (r probeResult) requestSteps() []requestProbeStep {
	steps := make([]requestProbeStep, 0, len(r.steps))
	for _, step := range r.steps {
		s := requestProbeStep{
			Name: step.name, IsPassed: step.ok,
			ConnectMs: step.connect.Milliseconds(), Ms: step.elapsed.Milliseconds(), Bytes: step.bytes,
		}
		if step.ok && step.destination >= 0 {
			s.Destination = &step.destination
		} else if !step.ok {
			s.Error = step.class
		}
		steps = append(steps, s)
	}
	return steps
}
