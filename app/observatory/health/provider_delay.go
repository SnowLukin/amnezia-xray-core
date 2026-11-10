package health

import "time"

type durationRange struct{ min, max time.Duration }

func protoRange(r *DurationRange, fallback durationRange) durationRange {
	if r == nil {
		return fallback
	}
	return durationRange{time.Duration(r.Min), time.Duration(r.Max)}
}

func (r durationRange) bounds() (time.Duration, time.Duration) { return r.min, r.max }

// Int63n keeps both bounds reachable, including a full [0, MaxInt64] range.
func (e *engine) randomDelay(low, high time.Duration) time.Duration {
	if high <= low {
		return low
	}
	span := int64(high - low)
	if span == int64(^uint64(0)>>1) {
		return low + time.Duration(e.rnd.Int63())
	}
	return low + time.Duration(e.rnd.Int63n(span+1))
}

func (p *providerSettings) feeds(tag string) bool {
	for _, b := range p.balancers {
		if b == tag {
			return true
		}
	}
	return false
}
func pathsForBalancer(paths []*PathDefinition, tag string) []*PathDefinition {
	var out []*PathDefinition
	for _, p := range paths {
		if p.Balancer == tag {
			out = append(out, p)
		}
	}
	return out
}
