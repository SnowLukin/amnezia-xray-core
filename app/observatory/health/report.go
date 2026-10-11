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

// Reasons of a switch the core takes at the start, which are no decision.
var startReasons = []string{"start", "start_pinned", "start_remembered", "start_with"}

const (
	// A report lists at most this many paths, the first ones by tag.
	reportPaths = 64
	// switch_path keeps this many of the latest switches.
	reportSwitches = 10
)

// reportCounts is what the events of a report window add up to.
type reportCounts struct {
	Events                  int64            `json:"events"`
	EventsDropped           uint64           `json:"events_dropped"`
	Switches                int64            `json:"switches"`
	SwitchReasons           map[string]int64 `json:"switch_reasons"`
	SwitchPath              []string         `json:"switch_path"`
	Exhausted               int64            `json:"exhausted_count"`
	GroupDown               int64            `json:"group_down_count"`
	PathDown                int64            `json:"path_down_count"`
	PathSuspect             int64            `json:"path_suspect_count"`
	PathDegraded            int64            `json:"path_degraded_count"`
	PathRescued             int64            `json:"path_rescued_count"`
	AuditCount              int64            `json:"audit_count"`
	AuditPassed             int64            `json:"audit_passed_count"`
	AuditSkipped            int64            `json:"audit_skipped_count"`
	WakeProbeCount          int64            `json:"wake_probe_count"`
	WakeProbeFailed         int64            `json:"wake_probe_failed_count"`
	WakeSkipped             int64            `json:"wake_skipped_count"`
	LoneProbeCount          int64            `json:"lone_probe_count"`
	LoneProbeFailed         int64            `json:"lone_probe_failed_count"`
	CandidateProbes         int64            `json:"candidate_probes"`
	CandidateProbesFailed   int64            `json:"candidate_probes_failed"`
	ProbeOnlyPromoted       int64            `json:"probe_only_promoted_count"`
	FreshPathDown           int64            `json:"fresh_path_down_count"`
	PendingClosed           int64            `json:"pending_closed_count"`
	IdleClosed              int64            `json:"idle_closed_count"`
	Probes                  int64            `json:"probes"`
	ProbesFailed            int64            `json:"probes_failed"`
	ProbeBytes              int64            `json:"probe_bytes"`
	HostProbeRequests       int64            `json:"host_probe_requests"`
	HostProbes              int64            `json:"host_probes"`
	ReturnToCheaperAttempts int64            `json:"return_attempts"`
	ReturnToCheaperFailures int64            `json:"return_failures"`
	PinnedDown              int64            `json:"pinned_down_count"`
	NetworkChanges          int64            `json:"network_changes"`
	NetworkKnown            int64            `json:"network_known"`
	NetworkDown             int64            `json:"network_down_count"`
	ProviderFetches         int64            `json:"provider_fetches"`
	ProviderFetchFailures   int64            `json:"provider_fetch_failures"`
	ProviderTriggers        map[string]int64 `json:"provider_triggers"`
	ProviderRetryAfter      int64            `json:"provider_retry_after"`
	ProviderApplied         int64            `json:"provider_applied"`
	ProviderRejected        int64            `json:"provider_rejected"`
	ProviderRestartNeeded   int64            `json:"provider_restart_needed"`
	ProviderExpired         int64            `json:"provider_expired"`
	ProviderRenewals        int64            `json:"provider_renewals"`
	ProviderAuthFailed      int64            `json:"provider_auth_failed"`
	ProviderBaseChanged     int64            `json:"provider_base_changed"`
	Pins                    int64            `json:"pins"`
	Releases                int64            `json:"releases"`
	MemoryLow               int64            `json:"memory_low_count"`
	OutboundSleeps          int64            `json:"outbound_sleeps"`
}

type reportPath struct {
	State              string                   `json:"state"`
	TTFBP50Ms          int64                    `json:"ttfb_p50_ms"`
	ActiveMs           int64                    `json:"active_ms"`
	BytesUp            int64                    `json:"bytes_up"`
	BytesDown          int64                    `json:"bytes_down"`
	ConnsOK            int64                    `json:"conns_ok"`
	ConnsFailed        int64                    `json:"conns_failed"`
	Probes             int64                    `json:"probes"`
	ProbeBytes         int64                    `json:"probe_bytes"`
	ProbeSteps         []reportProbeStep        `json:"probe_steps"`
	DegradedMs         int64                    `json:"degraded_ms"`
	DownsStrong        int64                    `json:"downs_strong"`
	DownsWeak          int64                    `json:"downs_weak"`
	Audits             int64                    `json:"audits"`
	AuditsPassed       int64                    `json:"audits_passed"`
	LoneProbes         int64                    `json:"lone_probes"`
	LoneProbesFailed   int64                    `json:"lone_probes_failed"`
	UDPFlows           int64                    `json:"udp_flows"`
	UDPFlowsUnanswered int64                    `json:"udp_flows_unanswered"`
	UDPBytesUp         int64                    `json:"udp_bytes_up"`
	UDPBytesDown       int64                    `json:"udp_bytes_down"`
	UDPWrites          int64                    `json:"udp_writes"`
	UDPReads           int64                    `json:"udp_reads"`
	UDPFirstReplyP50Ms int64                    `json:"udp_first_reply_p50_ms"`
	UDPPorts           map[string]reportUDPPort `json:"udp_ports,omitempty"`
}

type reportUDPPort struct {
	Flows      int64 `json:"flows"`
	Unanswered int64 `json:"unanswered"`
}

type reportProbeStep struct {
	Name   string `json:"name"`
	Passed int64  `json:"passed"`
	Failed int64  `json:"failed"`
}

type reportStepTotals struct {
	order  []string
	byName map[string]reportProbeStep
}

type reportView struct {
	Run       string `json:"health_run"`
	Reason    string `json:"report_reason"`
	Index     int    `json:"report_index"`
	WindowMs  int64  `json:"window_ms"`
	SessionMs int64  `json:"session_ms"`
	reportCounts
	Active              map[string]string     `json:"active"`
	IsBalancerExhausted bool                  `json:"is_balancer_exhausted"`
	IsNetworkUp         bool                  `json:"is_network_up"`
	IsQuietSample       bool                  `json:"is_quiet_sample"`
	Connectivity        string                `json:"connectivity,omitempty"`
	Groups              []groupStatusView     `json:"groups,omitempty"`
	Paths               map[string]reportPath `json:"paths"`
}

// pathTotals are the counters of a path that a report shows as growth since
// the previous report.
type pathTotals struct {
	active           time.Duration
	bytesUp          int64
	bytesDown        int64
	connsOK          int64
	connsFailed      int64
	probes           int64
	probeBytes       int64
	degraded         time.Duration
	downsStrong      int64
	downsWeak        int64
	audits           int64
	auditsPassed     int64
	loneProbes       int64
	loneProbesFailed int64
	udp              udpPathTotals
}

// report counts every event as it is emitted, so the report depends neither
// on the event log being drained nor on it overflowing.
type report struct {
	counts     reportCounts
	stepCounts map[string]*reportStepTotals
	// Decisions in the window; whether there was any in the run.
	decisions int
	decided   bool
	// Wall clock: a device asleep stops the monotonic clock, and a window
	// has to count that time.
	started     time.Time
	windowStart time.Time
	taken       int
	final       bool
	quietSample bool
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
		if !slices.Contains(startReasons, reason) {
			r.decide()
		}
	case "exhausted":
		c.Exhausted++
		r.decide()
	case "group_state":
		if f["state"] == "down" {
			c.GroupDown++
		}
	case "path_state":
		switch f["state"] {
		case "down":
			c.PathDown++
		case "suspect":
			c.PathSuspect++
		case "degraded":
			c.PathDegraded++
		case "up":
			// A down undone changes no choice by itself: a balancer it
			// brings back tells so with its own switch.
			if f["reason"] == "rescued" {
				c.PathRescued++
				if f["by"] == "audit" {
					c.AuditPassed++
				}
			}
		}
	case "probe_only_promoted":
		c.ProbeOnlyPromoted++
	case "fresh_path_down":
		c.FreshPathDown++
	case "pending_closed":
		c.PendingClosed += number(f["connections"])
	case "idle_closed":
		// Ending what idles on a path that works changes no choice.
		c.IdleClosed += number(f["connections"])
	case "probe_result":
		c.Probes++
		if f["is_ok"] != true {
			c.ProbesFailed++
			if f["reason"] == "candidate" {
				c.CandidateProbesFailed++
			}
			if f["reason"] == "wake" {
				c.WakeProbeFailed++
			}
			if f["reason"] == "lone" {
				c.LoneProbeFailed++
			}
		}
		c.ProbeBytes += number(f["bytes"])
		if f["reason"] == "host" {
			c.HostProbes++
		}
		r.countProbeSteps(f)
	case "probe_start":
		if f["reason"] == "candidate" {
			c.CandidateProbes++
		}
		if f["reason"] == "wake" {
			c.WakeProbeCount++
		}
		if f["reason"] == "lone" {
			c.LoneProbeCount++
		}
		if f["reason"] == "audit" {
			c.AuditCount++
		}
	case "probe_request":
		c.HostProbeRequests++
	case "return_start":
		c.ReturnToCheaperAttempts++
	case "return_failed":
		c.ReturnToCheaperFailures++
		r.decide()
	case "pinned_down":
		c.PinnedDown++
		// A start pin holds while the host verifies the connection, and the
		// host reports a failed verification itself.
		if f["is_start_pinned"] != true {
			r.decide()
		}
	case "network_change":
		c.NetworkChanges++
		if f["is_known"] == true {
			c.NetworkKnown++
		}
	case "network_down":
		c.NetworkDown++
	case "provider_fetch":
		c.ProviderFetches++
		if f["is_ok"] != true {
			c.ProviderFetchFailures++
		}
		if trigger, ok := f["trigger"].(string); ok {
			if c.ProviderTriggers == nil {
				c.ProviderTriggers = map[string]int64{}
			}
			c.ProviderTriggers[trigger]++
		}
		if _, ok := f["retry_after_ms"]; ok {
			c.ProviderRetryAfter++
		}
	case "provider_apply":
		// The initial set the core starts with is no decision, like a
		// start switch, or every session would count as one. Refused, it
		// is: the core runs without the set the host gave it.
		switch {
		case f["is_ok"] == false:
			c.ProviderRejected++
			r.decide()
		case f["is_changed"] == true && f["trigger"] != "initial":
			c.ProviderApplied++
			r.decide()
		}
		// A renewal keeps what the balancer has: it is no decision.
		if f["is_ok"] == true && number(f["renewed"]) > 0 && f["trigger"] != "initial" {
			c.ProviderRenewals++
		}
	case "provider_restart_needed":
		c.ProviderRestartNeeded++
		r.decide()
	case "provider_expired":
		// The balancer leaves the outbound or keeps it as the last resort.
		// A lease that ended in a saved set is the usual morning after a
		// device was off: the core asks for a new one at the start.
		c.ProviderExpired++
		if f["renewal"] != "saved" {
			r.decide()
		}
	case "provider_auth_failed":
		// The provider stops refreshing until the host brings new headers.
		c.ProviderAuthFailed++
		r.decide()
	case "provider_base_changed":
		// The host is to fetch the config anew and restart the core.
		c.ProviderBaseChanged++
		r.decide()
	case "pin":
		c.Pins++
	case "release":
		c.Releases++
	case "memory_low":
		if f["is_low"] == true {
			c.MemoryLow++
		}
	case "outbound_sleep":
		// Releasing what an idle outbound holds changes no choice.
		c.OutboundSleeps++
	}
}

func (r *report) forgetProbeSteps(tag string) {
	delete(r.stepCounts, tag)
}

func (r *report) countProbeSteps(f map[string]any) {
	tag, ok := f["outbound"].(string)
	if !ok || tag == "" {
		return
	}
	steps, ok := f["steps"].([]map[string]any)
	if !ok || len(steps) == 0 {
		return
	}
	if r.stepCounts == nil {
		r.stepCounts = make(map[string]*reportStepTotals)
	}
	totals := r.stepCounts[tag]
	if totals == nil {
		if len(r.stepCounts) == reportPaths {
			largest := tag
			for existing := range r.stepCounts {
				if existing > largest {
					largest = existing
				}
			}
			if largest == tag {
				return
			}
			delete(r.stepCounts, largest)
		}
		totals = &reportStepTotals{byName: make(map[string]reportProbeStep)}
		r.stepCounts[tag] = totals
	}
	for _, raw := range steps {
		name, ok := raw["name"].(string)
		if !ok || name == "" {
			continue
		}
		v, exists := totals.byName[name]
		if !exists {
			totals.order = append(totals.order, name)
			v.Name = name
		}
		if raw["is_passed"] == true {
			v.Passed++
		} else {
			v.Failed++
		}
		totals.byName[name] = v
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

func wholeMilliseconds(d time.Duration) int64 {
	return max(int64(d/time.Millisecond), 0)
}

// takeReport returns the report when it is due and starts a new window, or
// nil. A final report is due when the run had a decision or was sampled;
// nothing is due after it.
func (e *engine) takeReport(final bool, now time.Time) *reportView {
	r := &e.report
	if r.final {
		return nil
	}
	reason := ""
	switch {
	case final:
		r.final = true
		if r.decided || r.quietSample {
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
	r.stepCounts = nil
	r.decisions = 0
	r.windowStart = now.Round(0)
	r.droppedBefore = e.events.droppedTotal
	return v
}

// snapshotReport returns what a final report would be now without closing
// the window, or nil when it would be nil.
func (e *engine) snapshotReport(now time.Time) *reportView {
	if e.report.final || (!e.report.decided && !e.report.quietSample) {
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
		Groups:        e.groupViews(now),
		Reason:        reason,
		Index:         r.taken + 1,
		WindowMs:      wholeMilliseconds(wall.Sub(r.windowStart)),
		SessionMs:     wholeMilliseconds(wall.Sub(r.started)),
		reportCounts:  r.counts,
		Active:        map[string]string{},
		IsNetworkUp:   !e.networkDown,
		IsQuietSample: r.quietSample,
		Connectivity:  e.connectivityState(),
		Paths:         map[string]reportPath{},
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
		v.IsBalancerExhausted = v.IsBalancerExhausted || s.exhausted
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
		degraded := p.stats.degradedTime
		if p.status == statusDegraded && !p.statusSince.IsZero() && wall.After(p.statusSince.Round(0)) {
			degraded += wall.Sub(p.statusSince.Round(0))
		}
		total := pathTotals{
			active:           p.stats.activeTime + active[tag],
			bytesUp:          p.stats.bytesUp,
			bytesDown:        p.stats.bytesDown,
			udp:              p.stats.udp,
			connsOK:          p.stats.connsOK,
			connsFailed:      p.stats.connsFailed,
			probes:           p.stats.probes,
			probeBytes:       p.stats.probeBytes,
			degraded:         degraded,
			downsStrong:      p.stats.downsStrong,
			downsWeak:        p.stats.downsWeak,
			audits:           p.stats.audits,
			auditsPassed:     p.stats.auditsPassed,
			loneProbes:       p.stats.loneProbes,
			loneProbesFailed: p.stats.loneProbesFailed,
		}
		if i < reportPaths {
			base := p.reported
			udp := total.udp.delta(base.udp)
			v.Paths[tag] = reportPath{
				State:              p.status.String(),
				TTFBP50Ms:          p.stats.ttfb.quantile(0.5).Milliseconds(),
				ActiveMs:           (total.active - base.active).Milliseconds(),
				BytesUp:            total.bytesUp - base.bytesUp,
				BytesDown:          total.bytesDown - base.bytesDown,
				ConnsOK:            total.connsOK - base.connsOK,
				ConnsFailed:        total.connsFailed - base.connsFailed,
				Probes:             total.probes - base.probes,
				ProbeBytes:         total.probeBytes - base.probeBytes,
				ProbeSteps:         e.reportProbeSteps(tag),
				DegradedMs:         max((total.degraded - base.degraded).Milliseconds(), 0),
				DownsStrong:        total.downsStrong - base.downsStrong,
				DownsWeak:          total.downsWeak - base.downsWeak,
				Audits:             total.audits - base.audits,
				AuditsPassed:       total.auditsPassed - base.auditsPassed,
				LoneProbes:         total.loneProbes - base.loneProbes,
				LoneProbesFailed:   total.loneProbesFailed - base.loneProbesFailed,
				UDPFlows:           udp.flows,
				UDPFlowsUnanswered: udp.unanswered,
				UDPBytesUp:         udp.bytesUp,
				UDPBytesDown:       udp.bytesDown,
				UDPWrites:          udp.writes,
				UDPReads:           udp.reads,
				UDPFirstReplyP50Ms: p.stats.udpFirstReply.quantile(0.5).Milliseconds(),
				UDPPorts:           udp.reportPorts(),
			}
		}
		if take {
			p.reported = total
			p.stats.udpFirstReply = histogram{}
		}
	}
	return v
}

func (e *engine) reportProbeSteps(tag string) []reportProbeStep {
	steps := []reportProbeStep{}
	totals := e.report.stepCounts[tag]
	if totals == nil {
		return steps
	}
	seen := make(map[string]bool, len(totals.order))
	for _, step := range e.s.probeFor(tag).steps {
		if v, ok := totals.byName[step.name]; ok {
			steps = append(steps, v)
			seen[step.name] = true
		}
	}
	for _, name := range totals.order {
		if !seen[name] {
			steps = append(steps, totals.byName[name])
		}
	}
	return steps
}
