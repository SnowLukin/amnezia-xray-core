package health

import (
	"maps"
	"reflect"
	"slices"
	"time"
)

// ViewData is what happens to the traffic of the user now, for the screen of
// an application. Unlike Status it may be shown: it has no server addresses
// or names, no error texts and no network id.
type ViewData struct {
	// Grows when the snapshot differs from the one returned before in
	// anything but its durations.
	Seq   uint64 `json:"seq"`
	Run   string `json:"run"`
	State string `json:"state"`
	// What the core does now to get to connected; none once there and
	// without a network.
	Phase string `json:"phase,omitempty"`
	// Since View first saw the state.
	SinceMs    int64          `json:"since_ms"`
	Needs      []string       `json:"needs"`
	Doing      []string       `json:"doing"`
	Balancer   string         `json:"balancer"`
	Outbound   viewOutbound   `json:"outbound"`
	Network    viewNetwork    `json:"network"`
	IsIdle     bool           `json:"is_idle"`
	Paths      []viewPath     `json:"paths"`
	Providers  []viewProvider `json:"providers"`
	LastSwitch *viewSwitch    `json:"last_switch,omitempty"`
}

type viewOutbound struct {
	Tag      string            `json:"tag"`
	Protocol string            `json:"protocol,omitempty"`
	Labels   map[string]string `json:"labels,omitempty"`
	IsHeld   bool              `json:"is_held"`
}

type viewNetwork struct {
	IsUp         bool   `json:"is_up"`
	Connectivity string `json:"connectivity,omitempty"`
	Generation   uint64 `json:"generation"`
}

type viewPath struct {
	Outbound     string            `json:"outbound"`
	Balancer     string            `json:"balancer"`
	Group        string            `json:"group,omitempty"`
	Protocol     string            `json:"protocol,omitempty"`
	Cost         float32           `json:"cost"`
	IsProbeOnly  bool              `json:"is_probe_only"`
	State        string            `json:"state"`
	ForMs        int64             `json:"for_ms"`
	IsCurrent    bool              `json:"is_current"`
	IsActive     bool              `json:"is_active"`
	RTTMs        int64             `json:"rtt_ms,omitempty"`
	Probing      *viewProbing      `json:"probing,omitempty"`
	QuarantineMs int64             `json:"quarantine_ms,omitempty"`
	IsAsleep     bool              `json:"is_asleep,omitempty"`
	ExpiresInMs  *int64            `json:"expires_in_ms,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
}

type viewProbing struct {
	Reason string `json:"reason"`
	ForMs  int64  `json:"for_ms"`
}

type viewProvider struct {
	Provider string `json:"provider"`
	State    string `json:"state"`
	NextInMs *int64 `json:"next_in_ms,omitempty"`
	SetAgeMs int64  `json:"set_age_ms"`
}

type viewSwitch struct {
	From   string `json:"from"`
	To     string `json:"to"`
	Reason string `json:"reason"`
	AgoMs  int64  `json:"ago_ms"`
}

// The states of the view, see viewState.
const (
	viewNoNetwork    = "no_network"
	viewConnecting   = "connecting"
	viewConnected    = "connected"
	viewReconnecting = "reconnecting"
	viewBlocked      = "blocked"
)

// The phases of the view, see viewState.
const (
	phaseVerifying  = "verifying"
	phaseSearching  = "searching"
	phaseRefreshing = "refreshing"
	phaseWaiting    = "waiting"
)

// timeless is the view without its durations.
func (v ViewData) timeless() ViewData {
	v.Seq, v.SinceMs = 0, 0
	v.Paths = slices.Clone(v.Paths)
	for i := range v.Paths {
		p := &v.Paths[i]
		p.ForMs, p.RTTMs, p.QuarantineMs, p.ExpiresInMs = 0, 0, 0, nil
		if p.Probing != nil {
			p.Probing = &viewProbing{Reason: p.Probing.Reason}
		}
	}
	v.Providers = slices.Clone(v.Providers)
	for i := range v.Providers {
		v.Providers[i].NextInMs, v.Providers[i].SetAgeMs = nil, 0
	}
	if v.LastSwitch != nil {
		s := *v.LastSwitch
		s.AgoMs = 0
		v.LastSwitch = &s
	}
	return v
}

// View returns the snapshot for the screen. It reads what the engine keeps
// and changes nothing in it: it is no demand of the user and wakes nothing.
func (o *Observer) View() (ViewData, error) {
	if err := o.FeatureError("view"); err != nil {
		return ViewData{}, err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.now()
	v := o.e.view(now)
	v.Run = o.run
	if len(o.brokenSubsystems()) > 0 {
		v.Needs = append([]string{"restart"}, v.Needs...)
	}
	if same := v.timeless(); !reflect.DeepEqual(same, o.viewed) {
		if same.State != o.viewed.State {
			o.viewedSince = now
		}
		o.viewed = same
		o.viewSeq++
	}
	v.Seq, v.SinceMs = o.viewSeq, now.Sub(o.viewedSince).Milliseconds()
	return v, nil
}

func (e *engine) view(now time.Time) ViewData {
	v := ViewData{
		Needs: []string{}, Doing: []string{}, Balancer: e.s.viewBalancer,
		Network:   viewNetwork{IsUp: !e.networkDown, Connectivity: e.connectivityState(), Generation: e.generation},
		IsIdle:    e.idle(now),
		Paths:     []viewPath{},
		Providers: []viewProvider{},
	}
	main := e.selectors[e.s.viewBalancer]
	// The balancer whose choice or held outbound the current outbound is;
	// nil when the connections go to a fallbackTag or nobody knows where,
	// and before the first connection asked the balancer to choose.
	var by *selector
	if main != nil && main.started {
		v.Outbound.Tag, by = e.outboundNowBy(main, now)
		if !main.switchedAt.IsZero() {
			last := main.switched
			last.AgoMs = now.Sub(main.switchedAt).Milliseconds()
			v.LastSwitch = &last
		}
	}
	if by != nil {
		v.Outbound.IsHeld = by.current() == ""
		v.Outbound.Labels = e.s.pathLabels(v.Outbound.Tag)
		if c := by.cfg.paths[v.Outbound.Tag]; c != nil {
			v.Outbound.Protocol = c.protocol
		}
	}
	v.State, v.Phase = e.viewState(main, by, v.Outbound.Tag, now)

	probing := map[string]*probeJob{}
	checking := false
	for _, j := range e.running {
		if j.reason == reasonConnectivity {
			checking = true
		} else if was := probing[j.tag]; was == nil || j.id < was.id {
			probing[j.tag] = j
		}
	}
	returning := false
	for _, s := range e.sortedSelectors() {
		returning = returning || s.returnToCheaperTarget != ""
		for _, tag := range slices.Sorted(maps.Keys(s.cfg.paths)) {
			c := s.cfg.paths[tag]
			pv := viewPath{
				Outbound: tag, Balancer: s.tag, Group: c.groupName, Protocol: c.protocol, Cost: float32(c.cost),
				IsProbeOnly: c.isProbeOnly || e.isProbeOnly(tag),
				State:       statusUnknown.String(),
				IsCurrent:   by != nil && tag == v.Outbound.Tag,
				IsActive:    tag == s.current(),
				Labels:      e.s.pathLabels(tag),
			}
			if p := e.paths[tag]; p != nil {
				pv.State, pv.RTTMs, pv.IsAsleep = p.status.String(), p.lastRTT.Milliseconds(), p.asleep
				if !p.statusSince.IsZero() {
					pv.ForMs = now.Sub(p.statusSince).Milliseconds()
				}
				if p.status == statusDown && now.Before(p.quarantineUntil) {
					pv.QuarantineMs = p.quarantineUntil.Sub(now).Milliseconds()
				}
			}
			if j := probing[tag]; j != nil {
				pv.Probing = &viewProbing{Reason: j.reason.String(), ForMs: now.Sub(j.began).Milliseconds()}
			}
			for _, p := range e.providers {
				if l := p.leases[tag]; l != nil {
					left := l.left(now).Milliseconds()
					pv.ExpiresInMs = &left
				}
			}
			v.Paths = append(v.Paths, pv)
		}
	}

	fetching, refused, rebased := false, false, false
	for _, p := range e.providers {
		pv := viewProvider{Provider: p.cfg.tag, State: "ok"}
		switch {
		case p.fetching:
			pv.State = "fetching"
		case p.authFailed:
			pv.State = "refused"
		case p.failures > 0 && now.Before(p.backoffUntil) || now.Before(p.retryAfterUntil):
			pv.State = "waiting"
		}
		if !p.setAt.IsZero() {
			pv.SetAgeMs = now.Sub(p.setAt).Milliseconds()
		}
		if !p.nextAt.IsZero() {
			in := p.nextAt.Sub(now).Milliseconds()
			pv.NextInMs = &in
		}
		fetching, refused, rebased = fetching || p.fetching, refused || p.authFailed, rebased || p.baseReported != ""
		v.Providers = append(v.Providers, pv)
	}
	add := func(list *[]string, is bool, name string) {
		if is {
			*list = append(*list, name)
		}
	}
	add(&v.Needs, refused, "session")
	add(&v.Needs, rebased, "config")
	add(&v.Doing, len(probing) > 0, "probing")
	add(&v.Doing, fetching, "refreshing")
	add(&v.Doing, returning, "returning")
	add(&v.Doing, checking, "checking_network")
	return v
}

// viewState is the state of the view and its phase. main is the balancer of
// the user, by the one that hands out tag, nil for a fallbackTag. Until an
// outbound was proven in this run the state is connecting while the core
// has something left to try, after that reconnecting; the phase names what
// it tries.
func (e *engine) viewState(main, by *selector, tag string, now time.Time) (string, string) {
	if e.networkDown {
		return viewNoNetwork, ""
	}
	chain := e.fallbackChain(main)
	trying := viewConnecting
	for _, s := range chain {
		if s.confirmed {
			trying = viewReconnecting
		}
	}
	switch {
	case main == nil:
		return viewBlocked, phaseWaiting
	case !main.started:
		return trying, phaseWaiting
	case by == nil:
		coming, first := e.setComing(chain, now)
		phase := phaseWaiting
		switch {
		case e.probingFor(chain):
			phase = phaseSearching
		case coming:
			phase = phaseRefreshing
		}
		if first && trying == viewConnecting {
			// A config without a working path starts on the set of its
			// provider: that is still the way to the first connection.
			return viewConnecting, phase
		}
		return viewBlocked, phase
	case by.current() == "":
		// The balancer hands out its last outbound while it looks for
		// another.
		return trying, phaseSearching
	}
	status, proven := statusUnknown, false
	if p := e.paths[tag]; p != nil {
		status, proven = p.status, p.suspectProven
	}
	switch {
	case status == statusUp || status == statusDegraded || status == statusSuspect && proven:
		// A doubt about an outbound that worked does not blink on the screen.
		return viewConnected, ""
	case status != statusDown:
		return trying, phaseVerifying
	case by.pinned != "":
		// A pinned balancer does not leave a dead outbound.
		return viewBlocked, phaseWaiting
	}
	return trying, phaseSearching
}

// fallbackChain is the balancer and the fallback balancers its connections
// may reach, as outboundNowBy follows them.
func (e *engine) fallbackChain(s *selector) []*selector {
	var chain []*selector
	for ; s != nil && len(chain) <= 3; s = e.selectors[s.cfg.fallbackBalancer] {
		chain = append(chain, s)
	}
	return chain
}

// probingFor tells whether a probe one of the balancers asked for runs now.
func (e *engine) probingFor(chain []*selector) bool {
	for _, j := range e.running {
		for _, s := range chain {
			if j.selector == s.tag && !j.invalidated {
				return true
			}
		}
	}
	return false
}

// setComing tells whether a set for one of the balancers is on its way: a
// request of a provider that feeds it runs, or the request for a balancer
// that ran out is due and nothing holds it back. first: it is, and no
// request failed or brought the same set since the last one that helped.
// Unlike wantedRefresh it changes nothing.
func (e *engine) setComing(chain []*selector, now time.Time) (coming, first bool) {
	idle := e.idle(now)
	for _, p := range e.providers {
		for _, s := range chain {
			if !p.cfg.feeds(s.tag) {
				continue
			}
			due := p.cfg.whenExhausted && p.cfg.balancer == s.tag && s.exhausted && p.expect == "" && p.failures == 0 &&
				!now.Before(p.retryAfterUntil) && !p.hourSpent(now) && e.heldBack(p, triggerExhausted, expiryWant{}, idle, now) == ""
			// The set is here and demandAtStart brings its outbounds to the
			// balancer with the next turn of the loop.
			arrived := p.expect != "" && s.cfg.startProbe && !s.confirmed
			if p.fetching || due || arrived {
				coming, first = true, first || p.failures == 0
			}
		}
	}
	return coming, first
}

// hourSpent tells whether maxPerHour holds the next refresh back.
func (p *provider) hourSpent(now time.Time) bool {
	recent := 0
	for _, t := range p.attempts {
		if now.Sub(t) < time.Hour {
			recent++
		}
	}
	return p.cfg.maxPerHour > 0 && recent >= p.cfg.maxPerHour
}
