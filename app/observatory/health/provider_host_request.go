package health

import "time"

// The states of a refresh the host asked for.
const (
	requestPending = "pending"
	requestApplied = "applied"
	requestFailed  = "failed"
)

// hostRequest is the last refresh the host asked a provider for. Only a
// refresh that starts after the request answers it: one already under way
// went with the headers it had then, which may be older than the request. A
// request that comes while an earlier one waits takes its place, and one
// refresh answers both.
type hostRequest struct {
	id    uint64
	state string
	// The code of the failure: the code of a protocol error body, rejected
	// or unreachable.
	code string
	// When the request came or was answered.
	at time.Time
	// The refresh under way that answers it; 0 until one starts.
	refresh uint64
}

// viewRequest is the last request of the host in View and Status.
type viewRequest struct {
	ID    uint64 `json:"id"`
	State string `json:"state"`
	Code  string `json:"code,omitempty"`
	AgoMs int64  `json:"ago_ms"`
	// How long the request still waits for a pause, when that is known.
	WaitMs int64 `json:"wait_ms,omitempty"`
}

// hostRefresh asks the provider with the tag, or every provider when the
// tag is empty, to refresh as soon as the limits allow. It returns the
// number of the request, growing within the run, or 0 when there is no such
// provider.
func (e *engine) hostRefresh(tag string, now time.Time) uint64 {
	if len(e.providers) == 0 || (tag != "" && e.provider(tag) == nil) {
		return 0
	}
	e.hostRequests++
	id := e.hostRequests
	for _, p := range e.providers {
		if tag == "" || p.cfg.tag == tag {
			p.host = true
			p.req = hostRequest{id: id, state: requestPending, at: now}
		}
	}
	return id
}

// answerRequest closes the request of the host the refresh answers.
func (e *engine) answerRequest(j *refreshJob, r refreshOutcome, now time.Time) {
	p := j.p
	if p.req.state != requestPending || p.req.refresh != j.refresh {
		return
	}
	p.req.state, p.req.code, p.req.at, p.req.refresh = requestApplied, "", now, 0
	if !r.ok {
		p.req.state, p.req.code = requestFailed, r.requestCode()
	}
	f := map[string]any{"provider": p.cfg.tag, "id": p.req.id, "state": p.req.state}
	if p.req.code != "" {
		f["code"] = p.req.code
	}
	e.emit(now, "provider_request", f)
}

// requestCode names a failed refresh for the host: the code the backend
// gave, rejected for an answer or a set the core refused, unreachable when
// no road brought an answer of the backend.
func (r refreshOutcome) requestCode() string {
	switch {
	case r.code != "":
		return r.code
	case r.class == "invalid_answer" || r.class == "invalid_set":
		return "rejected"
	}
	return "unreachable"
}

// requestView is the last request of the host, nil before the first. A
// request that waits tells how long when a pause holds it.
func (e *engine) requestView(p *provider, now time.Time) *viewRequest {
	if p.req.id == 0 {
		return nil
	}
	v := &viewRequest{ID: p.req.id, State: p.req.state, Code: p.req.code, AgoMs: now.Sub(p.req.at).Milliseconds()}
	if p.req.state != requestPending || p.fetching {
		return v
	}
	var until time.Time
	switch why := e.heldBack(p, triggerHost, expiryWant{}, e.idle(now), now); {
	case why == "auth" && !p.authRetryAt.IsZero():
		until = p.authRetryAt
	case why != "":
		// It waits for traffic, a release, the network or memory.
		return v
	}
	if at, _ := p.allowedAt(true, now); at.After(until) {
		until = at
	}
	if now.Before(until) {
		v.WaitMs = until.Sub(now).Milliseconds()
	}
	return v
}
