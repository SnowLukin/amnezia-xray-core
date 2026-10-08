package health

import (
	"encoding/json"
	"sort"
	"time"
)

// Event is one decision or observation, kept until the host drains it.
// Fields holds event specific values; keys are stable and documented in
// README.md next to this file.
type Event struct {
	Seq    uint64         `json:"seq"`
	Time   int64          `json:"ts"`
	Type   string         `json:"type"`
	Fields map[string]any `json:"fields,omitempty"`
}

type eventLog struct {
	maxEvents int
	maxBytes  int
	seq       uint64
	events    []json.RawMessage
	bytes     int
	dropped   uint64
}

func newEventLog(maxEvents, maxBytes int) *eventLog {
	return &eventLog{maxEvents: maxEvents, maxBytes: maxBytes}
}

func (l *eventLog) add(now time.Time, typ string, fields map[string]any) {
	l.seq++
	raw, err := json.Marshal(Event{Seq: l.seq, Time: now.UnixMilli(), Type: typ, Fields: fields})
	if err != nil {
		l.dropped++
		return
	}
	l.events = append(l.events, raw)
	l.bytes += len(raw)
	for len(l.events) > 0 && (len(l.events) > l.maxEvents || l.bytes > l.maxBytes) {
		l.bytes -= len(l.events[0])
		l.events[0] = nil
		l.events = l.events[1:]
		l.dropped++
	}
}

// drain returns kept events as a JSON array and forgets them.
func (l *eventLog) drain() ([]byte, uint64) {
	out := make([]byte, 0, l.bytes+len(l.events)+2)
	out = append(out, '[')
	for i, e := range l.events {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, e...)
	}
	out = append(out, ']')
	dropped := l.dropped
	l.events = nil
	l.bytes = 0
	l.dropped = 0
	return out, dropped
}

// histogram keeps durations in fixed buckets, enough for p50 and p90 without
// storing samples.
type histogram struct {
	counts [len(histogramBounds) + 1]int64
	total  int64
}

var histogramBounds = [...]time.Duration{
	50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond,
	500 * time.Millisecond, 750 * time.Millisecond, time.Second, 1500 * time.Millisecond,
	2 * time.Second, 3 * time.Second, 5 * time.Second, 8 * time.Second,
}

func (h *histogram) add(d time.Duration) {
	i := sort.Search(len(histogramBounds), func(i int) bool { return d <= histogramBounds[i] })
	h.counts[i]++
	h.total++
}

// quantile returns the upper bound of the bucket holding quantile q, or -1
// without samples. The last bucket has no bound and reports the largest one.
func (h *histogram) quantile(q float64) time.Duration {
	if h.total == 0 {
		return -1
	}
	want := int64(q*float64(h.total) + 0.5)
	if want < 1 {
		want = 1
	}
	var seen int64
	for i, c := range h.counts {
		seen += c
		if seen >= want {
			if i < len(histogramBounds) {
				return histogramBounds[i]
			}
			return histogramBounds[len(histogramBounds)-1]
		}
	}
	return histogramBounds[len(histogramBounds)-1]
}
