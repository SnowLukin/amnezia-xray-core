package health

import (
	"sort"
	"time"
)

type pathStatusView struct {
	Outbound        string           `json:"outbound"`
	State           string           `json:"state"`
	Reason          string           `json:"reason,omitempty"`
	ForMs           int64            `json:"for_ms"`
	Failures        int              `json:"failures"`
	QuarantineMs    int64            `json:"quarantine_ms,omitempty"`
	QuarantineLevel int              `json:"quarantine_level,omitempty"`
	Provisional     bool             `json:"provisional,omitempty"`
	RTTMs           int64            `json:"rtt_ms,omitempty"`
	LastError       string           `json:"last_error,omitempty"`
	ServerName      string           `json:"server_name,omitempty"`
	ConnsOK         int64            `json:"conns_ok"`
	ConnsFailed     int64            `json:"conns_failed"`
	BytesUp         int64            `json:"bytes_up"`
	BytesDown       int64            `json:"bytes_down"`
	FailuresByKind  map[string]int64 `json:"failures_by_kind,omitempty"`
	TTFBP50Ms       int64            `json:"ttfb_p50_ms"`
	TTFBP90Ms       int64            `json:"ttfb_p90_ms"`
	Probes          int64            `json:"probes"`
	ProbesFailed    int64            `json:"probes_failed"`
	ProbeBytes      int64            `json:"probe_bytes"`
	ActiveMs        int64            `json:"active_ms"`
}

type balancerStatusView struct {
	Balancer      string   `json:"balancer"`
	Active        string   `json:"active"`
	Pinned        string   `json:"pinned,omitempty"`
	Exhausted     bool     `json:"exhausted"`
	ActiveForMs   int64    `json:"active_for_ms,omitempty"`
	UpgradeInMs   *int64   `json:"upgrade_in_ms,omitempty"`
	UpgradeTarget string   `json:"upgrade_target,omitempty"`
	Candidates    []string `json:"candidates"`
}

type providerStatusView struct {
	Provider       string `json:"provider"`
	Version        string `json:"version,omitempty"`
	Members        int    `json:"members"`
	Draining       int    `json:"draining"`
	Fetching       bool   `json:"fetching"`
	Refreshes      int64  `json:"refreshes"`
	Failures       int    `json:"failures"`
	LastError      string `json:"last_error,omitempty"`
	SetAgeMs       int64  `json:"set_age_ms"`
	NextTrigger    string `json:"next_trigger,omitempty"`
	NextInMs       *int64 `json:"next_in_ms,omitempty"`
	BackoffMs      int64  `json:"backoff_ms,omitempty"`
	AttemptsInHour int    `json:"attempts_last_hour"`
}

type statusView struct {
	// Names this run of the core; pin and release requests may carry it.
	Run       string `json:"run,omitempty"`
	Network   string `json:"network,omitempty"`
	NetworkUp bool   `json:"network_up"`
	// "ok", "paused", "doubt" or "ignored"; empty without a connectivity check.
	Connectivity string               `json:"connectivity,omitempty"`
	Generation   uint64               `json:"generation"`
	Idle         bool                 `json:"idle"`
	Queued       int                  `json:"probes_queued"`
	Running      int                  `json:"probes_running"`
	Paths        []pathStatusView     `json:"paths"`
	Balancers    []balancerStatusView `json:"balancers"`
	Providers    []providerStatusView `json:"providers,omitempty"`
}

func (e *engine) status(now time.Time) statusView {
	v := statusView{
		Network:      e.networkID,
		NetworkUp:    !e.networkDown,
		Connectivity: e.connectivityState(),
		Generation:   e.generation,
		Idle:         e.idle(now),
		Queued:       len(e.queue),
		Running:      len(e.running),
	}
	active := map[string]time.Duration{}
	for _, s := range e.sortedSelectors() {
		b := balancerStatusView{
			Balancer:      s.tag,
			Active:        s.current(),
			Pinned:        s.pinned,
			Exhausted:     s.exhausted,
			UpgradeTarget: s.upgradeTarget,
			Candidates:    s.candidates,
		}
		if !s.activeSince.IsZero() {
			b.ActiveForMs = now.Sub(s.activeSince).Milliseconds()
			active[s.current()] += now.Sub(s.activeSince)
		}
		if !s.upgradeAt.IsZero() {
			in := s.upgradeAt.Sub(now).Milliseconds()
			b.UpgradeInMs = &in
		}
		v.Balancers = append(v.Balancers, b)
	}
	tags := make([]string, 0, len(e.paths))
	for tag := range e.paths {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	for _, tag := range tags {
		p := e.paths[tag]
		pv := pathStatusView{
			Outbound:     tag,
			State:        p.status.String(),
			Reason:       p.reason,
			Failures:     len(p.failures),
			RTTMs:        p.lastRTT.Milliseconds(),
			LastError:    p.lastError,
			ServerName:   p.serverName,
			ConnsOK:      p.stats.connsOK,
			ConnsFailed:  p.stats.connsFailed,
			BytesUp:      p.stats.bytesUp,
			BytesDown:    p.stats.bytesDown,
			TTFBP50Ms:    p.stats.ttfb.quantile(0.5).Milliseconds(),
			TTFBP90Ms:    p.stats.ttfb.quantile(0.9).Milliseconds(),
			Probes:       p.stats.probes,
			ProbesFailed: p.stats.probesFailed,
			ProbeBytes:   p.stats.probeBytes,
			ActiveMs:     (p.stats.activeTime + active[tag]).Milliseconds(),
		}
		if !p.statusSince.IsZero() {
			pv.ForMs = now.Sub(p.statusSince).Milliseconds()
		}
		if p.status == statusDown && now.Before(p.quarantineUntil) {
			pv.QuarantineMs = p.quarantineUntil.Sub(now).Milliseconds()
			pv.QuarantineLevel = p.quarantineLevel
			pv.Provisional = p.provisional
		}
		for k, n := range p.stats.failuresByKind {
			if n > 0 {
				if pv.FailuresByKind == nil {
					pv.FailuresByKind = map[string]int64{}
				}
				pv.FailuresByKind[failureKind(k).String()] = n
			}
		}
		v.Paths = append(v.Paths, pv)
	}
	for _, p := range e.providers {
		pv := providerStatusView{
			Provider:  p.cfg.tag,
			Version:   p.version,
			Members:   p.members,
			Draining:  p.draining,
			Fetching:  p.fetching,
			Refreshes: p.refreshes,
			Failures:  p.failures,
			LastError: p.lastError,
		}
		if !p.setAt.IsZero() {
			pv.SetAgeMs = now.Sub(p.setAt).Milliseconds()
		}
		if !p.nextAt.IsZero() {
			in := p.nextAt.Sub(now).Milliseconds()
			pv.NextInMs = &in
			pv.NextTrigger = p.nextTrigger.String()
		}
		if p.failures > 0 && now.Before(p.backoffUntil) {
			pv.BackoffMs = p.backoffUntil.Sub(now).Milliseconds()
		}
		for _, t := range p.attempts {
			if now.Sub(t) < time.Hour {
				pv.AttemptsInHour++
			}
		}
		v.Providers = append(v.Providers, pv)
	}
	return v
}

func (e *engine) connectivityState() string {
	switch {
	case e.s.connectivity == nil:
		return ""
	case e.connectivityIgnored:
		return "ignored"
	case e.doubt:
		return "doubt"
	case e.networkDown:
		return "paused"
	default:
		return "ok"
	}
}
