package health

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

// providerRequest is the body of a POST to the provider in protocol 1: what
// the core knows about the set of the provider, so that the backend sees why
// it is asked and what broke. The engine gathers it under the observer lock
// when a refresh starts, from what it keeps anyway; it is marshaled once
// outside the lock, and every via sends the same bytes. Keys are camelCase
// like the config and the answer. Destinations of connections, server
// addresses and header values never go in.
type providerRequest struct {
	Protocol    int               `json:"protocol"`
	Provider    string            `json:"provider"`
	Run         string            `json:"run"`
	Refresh     uint64            `json:"refresh"`
	Trigger     string            `json:"trigger"`
	Retry       int               `json:"retry"`
	LastFailure *requestFailure   `json:"lastFailure,omitempty"`
	Version     string            `json:"version,omitempty"`
	ClientTime  time.Time         `json:"clientTime"`
	Network     requestNetwork    `json:"network"`
	MemoryLow   bool              `json:"memoryLow"`
	Balancer    requestBalancer   `json:"balancer"`
	Outbounds   []requestOutbound `json:"outbounds"`

	// The network as the host named it; it goes out hashed, see providerHash.
	networkID string
}

type requestFailure struct {
	Status int    `json:"status"`
	Error  string `json:"error,omitempty"`
	AgoMs  int64  `json:"agoMs"`
}

type requestNetwork struct {
	ID    string `json:"id,omitempty"`
	Known bool   `json:"known"`
	Up    bool   `json:"up"`
}

type requestBalancer struct {
	Tag string `json:"tag"`
	// The chosen outbound, only when it is one of the provider.
	Current   string  `json:"current,omitempty"`
	Own       bool    `json:"own"`
	Cost      float64 `json:"cost,omitempty"`
	Exhausted bool    `json:"exhausted"`
	Pinned    bool    `json:"pinned"`
	HeldMs    int64   `json:"heldMs,omitempty"`
}

type requestOutbound struct {
	Tag             string           `json:"tag"`
	State           string           `json:"state"`
	Reason          string           `json:"reason,omitempty"`
	StateMs         int64            `json:"stateMs,omitempty"`
	SeenMs          int64            `json:"seenMs,omitempty"`
	QuarantineLevel int              `json:"quarantineLevel,omitempty"`
	QuarantineMs    int64            `json:"quarantineMs,omitempty"`
	Failures        map[string]int64 `json:"failures,omitempty"`
	Probe           *requestProbe    `json:"probe,omitempty"`
	ConnsOK         int64            `json:"connsOk,omitempty"`
	ConnsFailed     int64            `json:"connsFailed,omitempty"`
	BytesUp         int64            `json:"bytesUp,omitempty"`
	BytesDown       int64            `json:"bytesDown,omitempty"`
	ActiveMs        int64            `json:"activeMs,omitempty"`
	TTFBP50Ms       int64            `json:"ttfbP50Ms,omitempty"`
}

type requestProbe struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
	RTTMs int64  `json:"rttMs,omitempty"`
	AgoMs int64  `json:"agoMs"`
}

// providerRequest gathers the body of the refresh j of p that starts now.
func (e *engine) providerRequest(p *provider, j *refreshJob, now time.Time) *providerRequest {
	r := &providerRequest{
		Protocol:   providerProtocol,
		Provider:   p.cfg.tag,
		Refresh:    j.refresh,
		Trigger:    j.trigger.String(),
		Retry:      j.retry,
		Version:    p.version,
		ClientTime: now.Round(0).UTC(),
		Network:    requestNetwork{Known: e.networks[e.networkID] != nil, Up: !e.networkDown},
		MemoryLow:  e.memoryLow,
		Balancer:   requestBalancer{Tag: p.cfg.balancer},
		Outbounds:  make([]requestOutbound, 0, len(p.tags)),
		networkID:  e.networkID,
	}
	if !p.lastFailedAt.IsZero() {
		r.LastFailure = &requestFailure{Status: p.lastStatus, Error: cutText(p.lastClass, maxFailureText),
			AgoMs: now.Sub(p.lastFailedAt).Milliseconds()}
	}
	active := map[string]time.Duration{}
	for _, s := range e.selectors {
		if !s.activeSince.IsZero() {
			active[s.current()] += now.Sub(s.activeSince)
		}
	}
	if s := e.selectors[p.cfg.balancer]; s != nil {
		b := &r.Balancer
		cur := s.current()
		// Another path of the balancer is no business of this backend: it
		// sees only that its own outbounds are not in use.
		if cur != "" && strings.HasPrefix(cur, p.cfg.prefix) {
			b.Own = true
			b.Current = strings.TrimPrefix(cur, p.cfg.prefix)
		}
		if cur != "" {
			if c := e.cost(s, cur); !math.IsInf(c, 0) && !math.IsNaN(c) {
				b.Cost = c
			}
		}
		b.Exhausted = s.exhausted
		b.Pinned = s.pinned != ""
		if !s.activeSince.IsZero() {
			b.HeldMs = now.Sub(s.activeSince).Milliseconds()
		}
	}
	for _, tag := range p.tags {
		// An outbound that left the set is no longer the backend's concern.
		if e.retiring[tag] {
			continue
		}
		r.Outbounds = append(r.Outbounds, describeOutbound(e.paths[tag], strings.TrimPrefix(tag, p.cfg.prefix),
			active[tag], now))
	}
	return r
}

// describeOutbound tells the backend about one outbound of the set; nil p
// is one the observatory knows nothing about yet. active is the time it is
// chosen now, beyond what its counters hold.
func describeOutbound(p *path, tag string, active time.Duration, now time.Time) requestOutbound {
	o := requestOutbound{Tag: tag, State: statusUnknown.String()}
	if p == nil {
		return o
	}
	o.State = p.status.String()
	o.Reason = p.reason
	if !p.statusSince.IsZero() {
		o.StateMs = now.Sub(p.statusSince).Milliseconds()
	}
	if !p.created.IsZero() {
		o.SeenMs = now.Sub(p.created).Milliseconds()
	}
	if p.quarantined(now) {
		o.QuarantineLevel = p.quarantineLevel
		o.QuarantineMs = p.quarantineUntil.Sub(now).Milliseconds()
	}
	// The counts over the life of the outbound: a failover clears the window
	// of failures at the moment the backend needs it.
	for k, n := range p.stats.failuresByKind {
		if n > 0 {
			if o.Failures == nil {
				o.Failures = map[string]int64{}
			}
			o.Failures[failureKind(k).String()] = n
		}
	}
	// Dated by its result: a probe that started in the tick that gathers
	// the body must not make the result of an older one look fresh.
	if !p.lastProbeEnd.IsZero() {
		o.Probe = &requestProbe{OK: p.lastError == "", AgoMs: now.Sub(p.lastProbeEnd).Milliseconds()}
		if o.Probe.OK {
			o.Probe.RTTMs = p.lastRTT.Milliseconds()
		} else {
			o.Probe.Error = p.lastClass
		}
	}
	o.ConnsOK = p.stats.connsOK
	o.ConnsFailed = p.stats.connsFailed
	o.BytesUp = p.stats.bytesUp
	o.BytesDown = p.stats.bytesDown
	o.ActiveMs = (p.stats.activeTime + active).Milliseconds()
	if p50 := p.stats.ttfb.quantile(0.5); p50 > 0 {
		o.TTFBP50Ms = p50.Milliseconds()
	}
	return o
}

// requestBody marshals the request of a refresh, nil for a GET. The run of
// the core and the network go out hashed per provider.
func (o *Observer) requestBody(rt *providerRuntime, j *refreshJob) []byte {
	r := j.request
	if r == nil {
		return nil
	}
	r.Run = providerHash(rt.cfg.url, o.run)
	if r.networkID != "" {
		r.Network.ID = providerHash(rt.cfg.url, r.networkID)
	}
	body, _ := json.Marshal(r)
	return body
}

// providerHash names a value for one provider: stable across runs for that
// backend, while two backends cannot match theirs. The raw run is the
// health_run of the report, by which a backend would tie an account to the
// analytics of the host.
func providerHash(url, value string) string {
	sum := sha256.Sum256([]byte(url + "\x00" + value))
	return hex.EncodeToString(sum[:])[:16]
}

// cutText shortens s to at most n bytes without splitting a character.
func cutText(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}
