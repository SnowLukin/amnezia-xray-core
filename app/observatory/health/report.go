package health

import (
	"maps"
	"slices"
	"sort"
	"time"
)

// Reasons of a report.
const (
	reportStop     = "stop"
	reportInterval = "interval"
	reportSnapshot = "snapshot"
)

const (
	// A report lists at most this many paths, the first ones by tag.
	reportPaths = 16
	// switch_path keeps this many of the latest switches.
	reportSwitches = 10
)

// reportCounts is what the events of a report window add up to.
type reportCounts struct {
	Events                int64            `json:"events"`
	EventsDropped         uint64           `json:"events_dropped"`
	Switches              int64            `json:"switches"`
	SwitchReasons         map[string]int64 `json:"switch_reasons"`
	SwitchPath            []string         `json:"switch_path"`
	Exhausted             int64            `json:"exhausted"`
	PathDown              int64            `json:"path_down"`
	PathSuspect           int64            `json:"path_suspect"`
	PendingClosed         int64            `json:"pending_closed"`
	Probes                int64            `json:"probes"`
	ProbesFailed          int64            `json:"probes_failed"`
	ProbeBytes            int64            `json:"probe_bytes"`
	UpgradeAttempts       int64            `json:"upgrade_attempts"`
	UpgradeFailures       int64            `json:"upgrade_failures"`
	PinnedDown            int64            `json:"pinned_down"`
	NetworkChanges        int64            `json:"network_changes"`
	NetworkKnown          int64            `json:"network_known"`
	NetworkDown           int64            `json:"network_down"`
	ProviderFetches       int64            `json:"provider_fetches"`
	ProviderFetchFailures int64            `json:"provider_fetch_failures"`
	ProviderTriggers      map[string]int64 `json:"provider_triggers"`
	ProviderApplied       int64            `json:"provider_applied"`
	ProviderRejected      int64            `json:"provider_rejected"`
	ProviderRestartNeeded int64            `json:"provider_restart_needed"`
	Pins                  int64            `json:"pins"`
	Releases              int64            `json:"releases"`
	MemoryLow             int64            `json:"memory_low"`
}

type reportPath struct {
	State       string `json:"state"`
	TTFBP50Ms   int64  `json:"ttfb_p50_ms"`
	ActiveMs    int64  `json:"active_ms"`
	BytesUp     int64  `json:"bytes_up"`
	BytesDown   int64  `json:"bytes_down"`
	ConnsOK     int64  `json:"conns_ok"`
	ConnsFailed int64  `json:"conns_failed"`
	Probes      int64  `json:"probes"`
	ProbeBytes  int64  `json:"probe_bytes"`
}

type reportView struct {
	Run        string `json:"health_run"`
	Reason     string `json:"report_reason"`
	Index      int    `json:"report_index"`
	WindowSec  int64  `json:"window_sec"`
	SessionSec int64  `json:"session_sec"`
	reportCounts
	Active            map[string]string     `json:"active"`
	BalancerExhausted bool                  `json:"balancer_exhausted"`
	NetworkUp         bool                  `json:"network_up"`
	Connectivity      string                `json:"connectivity,omitempty"`
	Paths             map[string]reportPath `json:"paths"`
}

// pathTotals are the counters of a path that a report shows as growth since
// the previous report.
type pathTotals struct {
	active      time.Duration
	bytesUp     int64
	bytesDown   int64
	connsOK     int64
	connsFailed int64
	probes      int64
	probeBytes  int64
}

// report counts every event as it is emitted, so the report depends neither
// on the event log being drained nor on it overflowing.
type report struct {
	counts reportCounts
	// Decisions in the window; whether there was any in the run.
	decisions int
	decided   bool
	// Wall clock: a device asleep stops the monotonic clock, and a window
	// has to count that time.
	started     time.Time
	windowStart time.Time
	taken       int
	final       bool
	// Events the log had dropped when the window started.
	droppedBefore uint64
}

func (r *report) begin(now time.Time) {
	r.started = now.Round(0)
	r.windowStart = r.started
}

func (r *report) count(typ string, f map[string]any) {
	c := &r.counts
	c.Events++
	switch typ {
	case "switch":
		reason, _ := f["reason"].(string)
		from, _ := f["from"].(string)
		to, _ := f["to"].(string)
		c.Switches++
		if c.SwitchReasons == nil {
			c.SwitchReasons = map[string]int64{}
		}
		c.SwitchReasons[reason]++
		if len(c.SwitchPath) == reportSwitches {
			c.SwitchPath = append(c.SwitchPath[:0], c.SwitchPath[1:]...)
		}
		c.SwitchPath = append(c.SwitchPath, from+">"+to+":"+reason)
		if reason != "start" && reason != "start_pinned" && reason != "start_remembered" {
			r.decide()
		}
	case "exhausted":
		c.Exhausted++
		r.decide()
	case "path_state":
		switch f["state"] {
		case "down":
			c.PathDown++
		case "suspect":
			c.PathSuspect++
		}
	case "pending_closed":
		c.PendingClosed += number(f["connections"])
	case "probe_result":
		c.Probes++
		if f["ok"] != true {
			c.ProbesFailed++
		}
		c.ProbeBytes += number(f["bytes"])
	case "upgrade_start":
		c.UpgradeAttempts++
	case "upgrade_failed":
		c.UpgradeFailures++
		r.decide()
	case "pinned_down":
		c.PinnedDown++
		r.decide()
	case "network_change":
		c.NetworkChanges++
		if f["known"] == true {
			c.NetworkKnown++
		}
	case "network_down":
		c.NetworkDown++
	case "provider_fetch":
		c.ProviderFetches++
		if f["ok"] != true {
			c.ProviderFetchFailures++
		}
		if trigger, ok := f["trigger"].(string); ok {
			if c.ProviderTriggers == nil {
				c.ProviderTriggers = map[string]int64{}
			}
			c.ProviderTriggers[trigger]++
		}
	case "provider_apply":
		switch {
		case f["ok"] == false:
			c.ProviderRejected++
			r.decide()
		case f["changed"] == true:
			c.ProviderApplied++
			r.decide()
		}
	case "provider_restart_needed":
		c.ProviderRestartNeeded++
		r.decide()
	case "pin":
		c.Pins++
	case "release":
		c.Releases++
	case "memory_low":
		if f["on"] == true {
			c.MemoryLow++
		}
	}
}

func (r *report) decide() {
	r.decisions++
	r.decided = true
}

func number(v any) int64 {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case int32:
		return int64(n)
	}
	return 0
}

func wholeSeconds(d time.Duration) int64 {
	return max(int64(d/time.Second), 0)
}

// takeReport returns the report when it is due and starts a new window, or
// nil. A final report is due when the run had a decision; nothing is due
// after it.
func (e *engine) takeReport(final bool, now time.Time) *reportView {
	r := &e.report
	if r.final {
		return nil
	}
	reason := ""
	switch {
	case final:
		r.final = true
		if r.decided {
			reason = reportStop
		}
	case r.decisions > 0 && e.s.reportInterval > 0 && now.Round(0).Sub(r.windowStart) >= e.s.reportInterval:
		reason = reportInterval
	}
	if reason == "" {
		return nil
	}
	v := e.reportView(reason, now, true)
	r.taken++
	r.counts = reportCounts{}
	r.decisions = 0
	r.windowStart = now.Round(0)
	r.droppedBefore = e.events.droppedTotal
	return v
}

// snapshotReport returns what a final report would be now without closing
// the window, or nil when it would be nil.
func (e *engine) snapshotReport(now time.Time) *reportView {
	if e.report.final || !e.report.decided {
		return nil
	}
	return e.reportView(reportSnapshot, now, false)
}

// reportView builds the report of the window. take makes the paths count
// their growth from now on and hands the counters of the window to the view.
func (e *engine) reportView(reason string, now time.Time, take bool) *reportView {
	r := &e.report
	wall := now.Round(0)
	v := &reportView{
		Reason:       reason,
		Index:        r.taken + 1,
		WindowSec:    wholeSeconds(wall.Sub(r.windowStart)),
		SessionSec:   wholeSeconds(wall.Sub(r.started)),
		reportCounts: r.counts,
		Active:       map[string]string{},
		NetworkUp:    !e.networkDown,
		Connectivity: e.connectivityState(),
		Paths:        map[string]reportPath{},
	}
	v.EventsDropped = e.events.droppedTotal - r.droppedBefore
	if !take {
		v.SwitchReasons = maps.Clone(v.SwitchReasons)
		v.SwitchPath = slices.Clone(v.SwitchPath)
		v.ProviderTriggers = maps.Clone(v.ProviderTriggers)
	}
	if v.SwitchReasons == nil {
		v.SwitchReasons = map[string]int64{}
	}
	if v.SwitchPath == nil {
		v.SwitchPath = []string{}
	}
	if v.ProviderTriggers == nil {
		v.ProviderTriggers = map[string]int64{}
	}
	active := map[string]time.Duration{}
	for _, s := range e.selectors {
		v.Active[s.tag] = s.current()
		v.BalancerExhausted = v.BalancerExhausted || s.exhausted
		if !s.activeSince.IsZero() {
			active[s.current()] += now.Sub(s.activeSince)
		}
	}
	tags := make([]string, 0, len(e.paths))
	for tag := range e.paths {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	for i, tag := range tags {
		p := e.paths[tag]
		total := pathTotals{
			active:      p.stats.activeTime + active[tag],
			bytesUp:     p.stats.bytesUp,
			bytesDown:   p.stats.bytesDown,
			connsOK:     p.stats.connsOK,
			connsFailed: p.stats.connsFailed,
			probes:      p.stats.probes,
			probeBytes:  p.stats.probeBytes,
		}
		if i < reportPaths {
			base := p.reported
			v.Paths[tag] = reportPath{
				State:       p.status.String(),
				TTFBP50Ms:   p.stats.ttfb.quantile(0.5).Milliseconds(),
				ActiveMs:    (total.active - base.active).Milliseconds(),
				BytesUp:     total.bytesUp - base.bytesUp,
				BytesDown:   total.bytesDown - base.bytesDown,
				ConnsOK:     total.connsOK - base.connsOK,
				ConnsFailed: total.connsFailed - base.connsFailed,
				Probes:      total.probes - base.probes,
				ProbeBytes:  total.probeBytes - base.probeBytes,
			}
		}
		if take {
			p.reported = total
		}
	}
	return v
}
