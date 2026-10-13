package health

// A probe contributes once per logical probe, including multi-step profiles.
// The first step's connection phase is comparable to a user dial; do not
// sum repeated handshakes from the other steps or use HTTP response latency.
func (q *qualitySummary) addProbe(epoch uint64, tag, reason string, result probeResult) {
	if q == nil || epoch != q.epoch || result.canceled {
		return
	}
	for _, c := range []*qualityCollection{&q.current, &q.open} {
		a := c.accumulator(tag, false)
		if a == nil {
			continue
		}
		a.probes++
		if !result.ok {
			a.probesFailed++
		}
		if a.probeReasons == nil {
			a.probeReasons = make(map[string]uint64)
		}
		a.probeReasons[reason]++
		if len(result.steps) > 0 && (result.steps[0].connect > 0 || result.steps[0].ok) {
			a.probeConnect.add(result.steps[0].connect)
		}
	}
	q.trimSegments()
}
