package proxy

import "github.com/xtls/xray-core/features/stats"

// splicedCounter is a counter that wants to know when bytes start to move
// without per-read counting: during a splice they are added in one go at
// its end, so silence of the counter means nothing.
type splicedCounter interface {
	Spliced()
}

func notifySpliced(counters ...stats.Counter) {
	for _, c := range counters {
		if s, ok := c.(splicedCounter); ok {
			s.Spliced()
		}
	}
}
