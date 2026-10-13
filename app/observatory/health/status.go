package health

import (
	"maps"
	"slices"
	"sort"
	"time"
)

// UDPStatus describes the latest UDP probe, independently of path health.
type UDPStatus struct {
	State    string `json:"state"`
	Answered int    `json:"answered"`
	Sent     int    `json:"sent"`
	RTTMs    int64  `json:"rtt_ms"`
	JitterMs int64  `json:"jitter_ms"`
	AgeMs    int64  `json:"age_ms"`
	Reason   string `json:"reason,omitempty"`
}

func (p *path) statusUDP(now time.Time) UDPStatus {
	if p.udp.at.IsZero() {
		return UDPStatus{State: "unknown"}
	}
	return UDPStatus{State: p.udp.state, Answered: p.udp.answered, Sent: p.udp.sent,
		RTTMs: p.udp.rtt.Milliseconds(), JitterMs: p.udp.jitter.Milliseconds(),
		AgeMs: max(now.Sub(p.udp.at).Milliseconds(), 0), Reason: p.udp.reason.String()}
}

type pathStatusView struct {
	Labels               map[string]string `json:"labels,omitempty"`
	CanSleep             *bool             `json:"can_sleep,omitempty"`
	CanCloseUnchosenIdle *bool             `json:"can_close_unchosen_idle,omitempty"`
	Quality              *qualityPathView  `json:"quality,omitempty"`
	Outbound             string            `json:"outbound"`
	IsProbeOnly          bool              `json:"is_probe_only"`
	State                string            `json:"state"`
	Reason               string            `json:"reason,omitempty"`
	ForMs                int64             `json:"for_ms"`
	Failures             int               `json:"failures"`
	QuarantineMs         int64             `json:"quarantine_ms,omitempty"`
	QuarantineLevel      int               `json:"quarantine_level,omitempty"`
	IsProvisional        bool              `json:"is_provisional,omitempty"`
	RTTMs                int64             `json:"rtt_ms,omitempty"`
	LastError            string            `json:"last_error,omitempty"`
	ServerName           string            `json:"server_name,omitempty"`
	ConnsOK              int64             `json:"conns_ok"`
	ConnsFailed          int64             `json:"conns_failed"`
	BytesUp              int64             `json:"bytes_up"`
	BytesDown            int64             `json:"bytes_down"`
	FailuresByKind       map[string]int64  `json:"failures_by_kind,omitempty"`
	TTFBP50Ms            int64             `json:"ttfb_p50_ms"`
	TTFBP90Ms            int64             `json:"ttfb_p90_ms"`
	Probes               int64             `json:"probes"`
	ProbesFailed         int64             `json:"probes_failed"`
	ProbeBytes           int64             `json:"probe_bytes"`
	Probe                *statusProbeView  `json:"probe,omitempty"`
	UDP                  UDPStatus         `json:"udp"`
	ActiveMs             int64             `json:"active_ms"`
	// What is left of the provider lease, below zero once it ended; none
	// without a lease.
	ExpiresInMs *int64 `json:"expires_in_ms,omitempty"`
	IsExpired   bool   `json:"is_expired,omitempty"`
	// The outbound released its tunnel; its next connection creates it.
	IsAsleep bool `json:"is_asleep,omitempty"`
}

type balancerStatusView struct {
	FallbackBalancer      string   `json:"fallback_balancer,omitempty"`
	IsFallingBack         bool     `json:"is_falling_back"`
	Balancer              string   `json:"balancer"`
	Active                string   `json:"active"`
	Pinned                string   `json:"pinned,omitempty"`
	IsExhausted           bool     `json:"is_exhausted"`
	ActiveForMs           int64    `json:"active_for_ms,omitempty"`
	ReturnToCheaperInMs   *int64   `json:"return_in_ms,omitempty"`
	ReturnToCheaperTarget string   `json:"return_target,omitempty"`
	Candidates            []string `json:"candidates"`
}

type providerStatusView struct {
	Provider          string `json:"provider"`
	Method            string `json:"method"`
	Version           string `json:"version,omitempty"`
	Members           int    `json:"members"`
	ProbeOnlyVerified int    `json:"probe_only_verified"`
	Draining          int    `json:"draining"`
	IsFetching        bool   `json:"is_fetching"`
	Refreshes         int64  `json:"refreshes"`
	Failures          int    `json:"failures"`
	LastError         string `json:"last_error,omitempty"`
	SetAgeMs          int64  `json:"set_age_ms"`
	NextTrigger       string `json:"next_trigger,omitempty"`
	NextInMs          *int64 `json:"next_in_ms,omitempty"`
	BackoffMs         int64  `json:"backoff_ms,omitempty"`
	RetryAfterMs      int64  `json:"retry_after_ms,omitempty"`
	AttemptsInHour    int    `json:"attempts_last_hour"`
	// The next renewal of a lease, below zero while it waits; leases that
	// ended; and how far the clock the last answer counts from is ahead of
	// the device, by which clock.
	RenewInMs *int64 `json:"renew_in_ms,omitempty"`
	Expired   int    `json:"expired,omitempty"`
	SkewMs    int64  `json:"skew_ms,omitempty"`
	Clock     string `json:"clock,omitempty"`
	// Names of the headers an answer or the host set, never their values;
	// whether the backend refused the headers, and how long the pause after
	// that lasts, none when it waits for new headers.
	Headers       []string `json:"headers,omitempty"`
	IsAuthRefused bool     `json:"is_auth_refused,omitempty"`
	AuthRetryInMs *int64   `json:"auth_retry_in_ms,omitempty"`
	// The number ProviderSet returns with the set.
	AnswerSeq uint64 `json:"answer_seq,omitempty"`
	// The base config the core started with, the one the last answer named,
	// and whether the host was told of another base that no answer took back.
	BaseVersion       string `json:"base_version,omitempty"`
	AnswerBaseVersion string `json:"answer_base_version,omitempty"`
	IsBaseChanged     bool   `json:"is_base_changed,omitempty"`
}

type statusView struct {
	Census                *censusStatusView `json:"census,omitempty"`
	IsLocalOnly           bool              `json:"is_local_only"`
	Features              map[string]bool   `json:"features"`
	WitnessQuality        *qualityPathView  `json:"witness_quality,omitempty"`
	QualitySamplesDropped uint64            `json:"quality_samples_dropped,omitempty"`
	BrokenSubsystems      []string          `json:"broken_subsystems"`
	// Names this run of the core; pin and release requests may carry it.
	Run         string             `json:"run,omitempty"`
	NetworkID   string             `json:"network_id,omitempty"`
	Network     *networkLabelsView `json:"network,omitempty"`
	IsNetworkUp bool               `json:"is_network_up"`
	// "ok", "paused", "doubt" or "ignored"; empty without a connectivity check.
	Connectivity string               `json:"connectivity,omitempty"`
	Generation   uint64               `json:"generation"`
	IsIdle       bool                 `json:"is_idle"`
	IsMemoryLow  bool                 `json:"is_memory_low"`
	Queued       int                  `json:"probes_queued"`
	Running      int                  `json:"probes_running"`
	Groups       []groupStatusView    `json:"groups,omitempty"`
	Paths        []pathStatusView     `json:"paths"`
	Balancers    []balancerStatusView `json:"balancers"`
	Providers    []providerStatusView `json:"providers,omitempty"`
	// Probe requests of the host, from the oldest.
	ProbeRequests []probeRequestView `json:"probe_requests,omitempty"`
}

func (e *engine) status(now time.Time) statusView {
	quality := e.currentQuality(now)
	v := statusView{
		Census:         e.censusStatus(),
		IsLocalOnly:    true,
		Features:       e.effectiveFeatures(),
		Groups:         e.groupViews(now),
		WitnessQuality: quality.Witness, QualitySamplesDropped: quality.SamplesDropped,
		NetworkID: e.networkID, Network: e.networkLabels(),
		IsNetworkUp:  !e.networkDown,
		Connectivity: e.connectivityState(),
		Generation:   e.generation,
		IsIdle:       e.idle(now),
		IsMemoryLow:  e.memoryLow,
		Queued:       len(e.queue),
		Running:      len(e.running),
	}
	active := map[string]time.Duration{}
	for _, s := range e.sortedSelectors() {
		b := balancerStatusView{
			Balancer:              s.tag,
			Active:                s.current(),
			Pinned:                s.pinned,
			IsExhausted:           s.exhausted,
			FallbackBalancer:      s.cfg.fallbackBalancer,
			IsFallingBack:         s.fallingBack,
			ReturnToCheaperTarget: s.returnToCheaperTarget,
			Candidates:            s.candidates,
		}
		if !s.activeSince.IsZero() {
			b.ActiveForMs = now.Sub(s.activeSince).Milliseconds()
			active[s.current()] += now.Sub(s.activeSince)
		}
		if !s.returnToCheaperAt.IsZero() {
			in := s.returnToCheaperAt.Sub(now).Milliseconds()
			b.ReturnToCheaperInMs = &in
		}
		v.Balancers = append(v.Balancers, b)
	}
	leases := map[string]*lease{}
	for _, p := range e.providers {
		for tag, l := range p.leases {
			leases[tag] = l
		}
	}
	tags := make([]string, 0, len(e.paths))
	for tag := range e.paths {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	for _, tag := range tags {
		p := e.paths[tag]
		pv := pathStatusView{
			Outbound: tag, Labels: e.s.pathLabels(tag),
			IsProbeOnly:  e.isProbeOnly(tag),
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
			Probe:        p.statusProbe(now),
			UDP:          p.statusUDP(now),
			ActiveMs:     (p.stats.activeTime + active[tag]).Milliseconds(),
			IsAsleep:     p.asleep,
		}
		if p.sleeper != sleeperUnknown {
			capable := p.sleeper == sleeperYes
			pv.CanSleep = &capable
			pv.CanCloseUnchosenIdle = &capable
		}
		if q, ok := quality.Paths[tag]; ok {
			pv.Quality = &q
		}
		if !p.statusSince.IsZero() {
			pv.ForMs = now.Sub(p.statusSince).Milliseconds()
		}
		if p.status == statusDown && now.Before(p.quarantineUntil) {
			pv.QuarantineMs = p.quarantineUntil.Sub(now).Milliseconds()
			pv.QuarantineLevel = p.quarantineLevel
			pv.IsProvisional = p.provisional
		}
		for k, n := range p.stats.failuresByKind {
			if n > 0 {
				if pv.FailuresByKind == nil {
					pv.FailuresByKind = map[string]int64{}
				}
				pv.FailuresByKind[failureKind(k).String()] = n
			}
		}
		if l := leases[tag]; l != nil {
			left := l.left(now).Milliseconds()
			pv.ExpiresInMs = &left
			pv.IsExpired = l.expired
		}
		v.Paths = append(v.Paths, pv)
	}
	for _, p := range e.providers {
		pv := providerStatusView{
			Provider:   p.cfg.tag,
			Method:     p.cfg.method,
			Version:    p.version,
			Members:    len(p.tags),
			Draining:   p.draining,
			IsFetching: p.fetching,
			Refreshes:  p.refreshes,
			Failures:   p.failures,
			LastError:  p.lastError,
		}
		for _, tag := range p.tags {
			if e.isProbeOnly(tag) && e.candidateVerified(e.paths[tag], now) {
				pv.ProbeOnlyVerified++
			}
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
		if now.Before(p.retryAfterUntil) {
			pv.RetryAfterMs = p.retryAfterUntil.Sub(now).Milliseconds()
		}
		for _, t := range p.attempts {
			if now.Sub(t) < time.Hour {
				pv.AttemptsInHour++
			}
		}
		for _, l := range p.leases {
			if l.expired {
				pv.Expired++
				continue
			}
			if l.asked || p.cfg.renewAt == 0 {
				continue
			}
			if in := (l.renewIn - l.elapsed(now)).Milliseconds(); pv.RenewInMs == nil || in < *pv.RenewInMs {
				pv.RenewInMs = &in
			}
		}
		pv.SkewMs, pv.Clock = p.skew.Milliseconds(), p.clock
		if len(p.rotated) > 0 {
			pv.Headers = slices.Sorted(maps.Keys(p.rotated))
		}
		pv.IsAuthRefused = p.authFailed
		pv.AnswerSeq = p.seq
		pv.BaseVersion, pv.AnswerBaseVersion, pv.IsBaseChanged = p.base, p.answerBase, p.baseReported != ""
		if p.authFailed && !p.authRetryAt.IsZero() {
			in := p.authRetryAt.Sub(now).Milliseconds()
			pv.AuthRetryInMs = &in
		}
		v.Providers = append(v.Providers, pv)
	}
	for _, r := range e.requests {
		view := r.view(now)
		view.Run = e.run
		v.ProbeRequests = append(v.ProbeRequests, view)
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

// Construct this map only for a requested diagnostic snapshot.
func (e *engine) effectiveFeatures() map[string]bool {
	flags := e.s.features.status()
	for tag, s := range e.selectors {
		prefix := "balancers." + tag + "."
		raw := s.cfg.raw.GetFeatures()
		if raw == nil {
			raw = &SelectorFeatureConfig{}
		}
		flags[prefix+"fallback_balancer"] = s.cfg.fallbackBalancer != ""
		flags[prefix+"probe_before_switch"] = s.cfg.switchAfterProbe
		flags[prefix+"close_pending"] = s.cfg.closePending
		flags[prefix+"close_silent"] = s.cfg.closeSilent
		flags[prefix+"down_audit"] = !s.cfg.skipDownAudit
		flags[prefix+"groups"] = featureValue(raw.Groups, false)
		flags[prefix+"return_to_cheaper"] = s.cfg.returnToCheaper.interval > 0
		flags[prefix+"return_on_network_change"] = s.cfg.returnToCheaper.onNetworkChange
		flags[prefix+"network_changes"] = s.cfg.onNetworkChange != NetworkChangeMode_IGNORE
		flags[prefix+"startup_pin"] = s.cfg.startPinned != ""
	}
	for _, p := range e.providers {
		prefix := "providers." + p.cfg.tag + "."
		flags[prefix+"is_enabled"] = true
		flags[prefix+"periodic"] = p.cfg.interval > 0
		flags[prefix+"min_healthy"] = p.cfg.minHealthy > 0
		flags[prefix+"cost_above"] = p.cfg.costAbove
		flags[prefix+"max_age"] = p.cfg.maxAge > 0
		flags[prefix+"network_change"] = p.cfg.onNetwork != RefreshOnNetwork_REFRESH_NEVER
		flags[prefix+"exhausted"] = p.cfg.whenExhausted
		flags[prefix+"lease_renewal"] = p.cfg.leaseRenewal
		flags[prefix+"backend_schedule"] = p.cfg.backendSchedule
		flags[prefix+"expired_lease_fallback"] = p.cfg.expiredFallback
	}
	for tag := range e.s.disabledProviders {
		flags["providers."+tag+".is_enabled"] = false
	}
	return flags
}
