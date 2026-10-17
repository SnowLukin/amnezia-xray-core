package health

import (
	"maps"
	"slices"
	"time"
)

// A report lists at most this many incidents and provider refreshes of its
// window; the rest only count.
const (
	reportIncidents = 10
	reportRefreshes = 10
)

// reportIncident is one outage of a balancer: its chosen outbound failed and
// the users waited until a connection went through another one. Analytics
// takes percentiles of the durations, one event per incident. The start is
// the first evidence the core had, not the moment the outbound broke, which
// nobody sees.
type reportIncident struct {
	Balancer string `json:"balancer"`
	// failover when the balancer had another outbound to go to, exhausted
	// when it had none and waited for a probe or the backend.
	Kind string `json:"kind"`
	From string `json:"from"`
	To   string `json:"to,omitempty"`
	// First evidence to the outbound found dead, found dead to another one
	// chosen, chosen to the first user connection through it that
	// succeeded, and first evidence to that connection.
	DetectMs  int64  `json:"detect_ms"`
	SwitchMs  *int64 `json:"switch_ms,omitempty"`
	RecoverMs *int64 `json:"recover_ms,omitempty"`
	TotalMs   int64  `json:"total_ms"`
	// The provider refresh that brought the outbound the balancer went to,
	// from its first attempt to the set applied; none without one.
	BackendMs *int64 `json:"backend_ms,omitempty"`
	// recovered, or open when the run ended before it did.
	Outcome string `json:"outcome"`
	// The labels of the network the host reported when the outage began:
	// the network may change before it ends.
	Network map[string]string `json:"network,omitempty"`
}

// reportRefresh is one provider refresh, as lastRefresh tells the backend,
// without addresses.
type reportRefresh struct {
	Provider string                 `json:"provider"`
	Trigger  string                 `json:"trigger"`
	IsOK     bool                   `json:"is_ok"`
	Ms       int64                  `json:"ms"`
	Attempts []reportRefreshAttempt `json:"attempts"`
}

type reportRefreshAttempt struct {
	Via      string `json:"via"`
	Outbound string `json:"outbound,omitempty"`
	Mirror   int    `json:"mirror"`
	IsOK     bool   `json:"is_ok"`
	Status   int    `json:"status"`
	Code     string `json:"code,omitempty"`
	Ms       int64  `json:"ms"`
}

// openIncident is an outage under way.
type openIncident struct {
	kind, from, to                string
	first, down, switched, backed time.Time
	labels                        map[string]string
}

// incidentTracker follows the outages of every balancer through the events
// as they are emitted. It outlives report windows: an outage counts in the
// window where it ends.
type incidentTracker struct {
	active map[string]string
	open   map[string]*openIncident
	// When the latest provider refresh made its first attempt.
	refreshStart time.Time
	// The start of the core waits for its first user connection.
	startPending bool
	// The labels of the current network, as the host reported them.
	labels map[string]string
}

func (r *report) track(now time.Time, typ string, f map[string]any) {
	t := &r.incidents
	if t.active == nil {
		t.active, t.open = map[string]string{}, map[string]*openIncident{}
	}
	balancer, _ := f["balancer"].(string)
	switch typ {
	case "switch":
		to, _ := f["to"].(string)
		reason, _ := f["reason"].(string)
		t.active[balancer] = to
		if slices.Contains(startReasons, reason) {
			return
		}
		if in := t.open[balancer]; in != nil && in.switched.IsZero() {
			in.switched, in.to = now, to
		}
	case "path_state":
		tag, _ := f["outbound"].(string)
		state, _ := f["state"].(string)
		for b, active := range t.active {
			if active != tag || tag == "" {
				continue
			}
			in := t.open[b]
			switch state {
			case "suspect", "down":
				first := now
				if evidence := number(f["evidence_ms"]); evidence > 0 {
					first = now.Add(-time.Duration(evidence) * time.Millisecond)
				}
				if in == nil {
					in = &openIncident{kind: "failover", from: tag, first: first, labels: maps.Clone(t.labels)}
					t.open[b] = in
				}
				if first.Before(in.first) {
					in.first = first
				}
				if state == "down" && in.down.IsZero() {
					in.down = now
				}
			case "up", "degraded":
				// A suspicion the traffic or a probe proved false is no
				// outage; one that ended with a switch is told by it.
				if in != nil && in.switched.IsZero() && in.down.IsZero() && in.from == tag {
					delete(t.open, b)
				}
			}
		}
	case "exhausted":
		from, _ := f["from"].(string)
		in := t.open[balancer]
		if in == nil {
			in = &openIncident{from: from, first: now, labels: maps.Clone(t.labels)}
			t.open[balancer] = in
		}
		in.kind = "exhausted"
		if in.down.IsZero() {
			in.down = now
		}
		t.active[balancer] = ""
	case "provider_fetch":
		if ms := number(f["ms"]); f["attempt"] != nil && number(f["attempt"]) <= 1 {
			t.refreshStart = now.Add(-time.Duration(ms) * time.Millisecond)
		}
	case "provider_apply":
		if f["is_ok"] != true || t.refreshStart.IsZero() {
			return
		}
		for _, in := range t.open {
			if in.kind == "exhausted" && in.switched.IsZero() && !t.refreshStart.Before(in.first) {
				in.backed = now
			}
		}
	case "first_success":
		tag, _ := f["outbound"].(string)
		if in := t.open[balancer]; in != nil && !in.switched.IsZero() && in.to == tag {
			r.closeIncident(in, balancer, now, "recovered")
			delete(t.open, balancer)
			return
		}
		if t.startPending {
			t.startPending = false
			ms := wholeMilliseconds(now.Round(0).Sub(r.started))
			r.counts.StartMs = &ms
		}
	}
}

func (r *report) closeIncident(in *openIncident, balancer string, now time.Time, outcome string) {
	v := reportIncident{Balancer: balancer, Kind: in.kind, From: in.from, To: in.to, Outcome: outcome,
		TotalMs: wholeMilliseconds(now.Sub(in.first)), Network: in.labels}
	if !in.down.IsZero() {
		v.DetectMs = wholeMilliseconds(in.down.Sub(in.first))
	}
	if !in.switched.IsZero() {
		start := in.down
		if start.IsZero() {
			start = in.first
		}
		sw := wholeMilliseconds(in.switched.Sub(start))
		v.SwitchMs = &sw
		if outcome == "recovered" {
			rec := wholeMilliseconds(now.Sub(in.switched))
			v.RecoverMs = &rec
		}
	}
	if !in.backed.IsZero() {
		backend := wholeMilliseconds(in.backed.Sub(r.incidents.refreshStart))
		if backend > 0 {
			v.BackendMs = &backend
		}
	}
	if len(r.counts.Incidents) == reportIncidents {
		r.counts.IncidentsDropped++
		return
	}
	r.counts.Incidents = append(r.counts.Incidents, v)
}

// closeOpenIncidents ends the outages still under way when the run stops.
func (r *report) closeOpenIncidents(now time.Time) {
	for b, in := range r.incidents.open {
		r.closeIncident(in, b, now, "open")
	}
	clear(r.incidents.open)
}

// countRefresh lists a finished provider refresh in the window.
func (r *report) countRefresh(provider string, last *requestRefresh) {
	v := reportRefresh{Provider: provider, Trigger: last.Trigger, IsOK: last.IsOK, Attempts: []reportRefreshAttempt{}}
	for _, a := range last.Attempts {
		v.Ms += a.Ms
		v.Attempts = append(v.Attempts, reportRefreshAttempt{Via: a.Via, Outbound: a.Outbound, Mirror: a.Mirror,
			IsOK: a.IsOK, Status: a.Status, Code: a.Code, Ms: a.Ms})
	}
	if len(r.counts.ProviderRefreshes) == reportRefreshes {
		r.counts.ProviderRefreshesDropped++
		return
	}
	r.counts.ProviderRefreshes = append(r.counts.ProviderRefreshes, v)
}
