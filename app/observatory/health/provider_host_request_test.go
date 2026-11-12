package health

import (
	"context"
	"testing"
	"time"
)

func (te *testEngine) hostReq(now time.Time) viewRequest {
	if r := te.requestView(te.prov(), now); r != nil {
		return *r
	}
	return viewRequest{}
}

// The number grows within the run, for one provider or all of them, and an
// unknown provider takes none.
func TestHostRequestNumberGrows(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), nil, "a1")
	if r := te.requestView(te.prov(), t0); r != nil {
		t.Fatalf("a request before the first: %+v", r)
	}
	for i, tag := range []string{"fresh", "fresh", "other", ""} {
		want := map[int]uint64{0: 1, 1: 2, 2: 0, 3: 3}[i]
		if id := te.hostRefresh(tag, t0); id != want {
			t.Fatalf("request %d for %q: number %d, want %d", i, tag, id, want)
		}
	}
	if r := te.hostReq(t0); r.ID != 3 || r.State != requestPending {
		t.Fatalf("request %+v", r)
	}
}

// A refresh that starts after the request answers it: applied for an
// applied set, failed with the code of the backend, rejected for a refused
// answer or set, unreachable when no road brought an answer.
func TestHostRequestOutcome(t *testing.T) {
	for _, tc := range []struct {
		outcome refreshOutcome
		state   string
		code    string
	}{
		{refreshOutcome{ok: true}, requestApplied, ""},
		{refreshOutcome{ok: true, changed: true}, requestApplied, ""},
		{refreshOutcome{err: "status", status: 409, class: "status", code: "location_unavailable", via: "direct"}, requestFailed, "location_unavailable"},
		{refreshOutcome{err: "status", status: 400, class: "status", code: "location_unknown", via: "direct"}, requestFailed, "location_unknown"},
		{refreshOutcome{err: "status", status: 503, class: "status", code: "unavailable", retryAfter: time.Minute}, requestFailed, "unavailable"},
		{refused, requestFailed, "unauthorized"},
		{refreshOutcome{err: "invalid answer", class: "invalid_answer"}, requestFailed, "rejected"},
		{refreshOutcome{err: "outbound x1", class: "invalid_set"}, requestFailed, "rejected"},
		{refreshOutcome{err: "timeout", class: "timeout"}, requestFailed, "unreachable"},
		{refreshOutcome{err: "status", status: 502, class: "status"}, requestFailed, "unreachable"},
	} {
		te := newProviderEngine(t, quietProvider(nil), nil, "a1")
		id := te.hostRefresh("fresh", t0)
		j := expectRefresh(t, te, at(time.Second), "host")
		if r := te.hostReq(at(2 * time.Second)); r != (viewRequest{ID: id, State: requestPending, AgoMs: 2000}) {
			t.Fatalf("while it runs: %+v", r)
		}
		te.eventsOf(t, "provider_request")
		te.onRefreshDone(j, tc.outcome, at(3*time.Second))
		if r := te.hostReq(at(4 * time.Second)); r != (viewRequest{ID: id, State: tc.state, Code: tc.code, AgoMs: 1000}) {
			t.Fatalf("%+v: request %+v, want %s %s", tc.outcome, r, tc.state, tc.code)
		}
		evs := te.eventsOf(t, "provider_request")
		if len(evs) != 1 || evs[0].Fields["id"] != float64(id) || evs[0].Fields["state"] != tc.state ||
			evs[0].Fields["provider"] != "fresh" || (evs[0].Fields["code"] != nil) != (tc.code != "") {
			t.Fatalf("events %+v", evs)
		}
	}
}

// A refusal of the backend keeps the set: the outbounds and their version
// stay those of the last applied answer.
func TestHostRequestFailureKeepsTheSet(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), nil, "a1")
	p := te.prov()
	p.tags, p.version = []string{"p-x1", "p-x2"}, "v1"
	te.hostRefresh("fresh", t0)
	j := expectRefresh(t, te, t0, "host")
	te.onRefreshDone(j, refreshOutcome{err: "status", status: 409, class: "status", code: "location_unavailable"}, at(time.Second))
	if r := te.hostReq(at(time.Second)); r.State != requestFailed || len(p.tags) != 2 || p.tags[1] != "p-x2" || p.version != "v1" {
		t.Fatalf("request %+v, set %v %s", r, p.tags, p.version)
	}
}

// A request that comes while an earlier one waits takes its place: one
// refresh answers both, under the number of the later.
func TestHostRequestsMerge(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), nil, "a1")
	p := te.prov()
	te.hostRefresh("fresh", t0)
	j := expectRefresh(t, te, t0, "host")
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(time.Second))
	// The next waits for the interval of urgent refreshes.
	first := te.hostRefresh("fresh", at(2*time.Second))
	expectNoRefresh(t, te, at(3*time.Second))
	due := t0.Add(p.cfg.exhaustedMinInterval)
	if r := te.hostReq(at(3 * time.Second)); r.ID != first || r.State != requestPending || r.WaitMs != due.Sub(at(3*time.Second)).Milliseconds() {
		t.Fatalf("waiting: %+v, due in %v", r, due.Sub(at(3*time.Second)))
	}
	second := te.hostRefresh("fresh", at(4*time.Second))
	j = expectRefresh(t, te, due, "host")
	expectNoRefresh(t, te, due.Add(time.Hour))
	te.onRefreshDone(j, refreshOutcome{ok: true}, due.Add(time.Second))
	if r := te.hostReq(due.Add(time.Second)); second != first+1 || r.ID != second || r.State != requestApplied {
		t.Fatalf("request %+v, numbers %d %d", r, first, second)
	}
}

// A refresh another trigger started before the request went with the
// headers before it: it does not answer the request, which gets a refresh
// of its own. So does a request whose headers the host changed while its
// refresh ran.
func TestHostRequestDuringAnotherRefresh(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), nil, "a1")
	p := te.prov()
	p.nextAt, p.nextTrigger = at(time.Second), triggerInterval
	j := expectRefresh(t, te, at(time.Second), "interval")
	id := te.hostRefresh("fresh", at(2*time.Second))
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(3*time.Second))
	if r := te.hostReq(at(3 * time.Second)); r.ID != id || r.State != requestPending {
		t.Fatalf("after the refresh of another trigger: %+v", r)
	}
	due := at(time.Second).Add(p.cfg.exhaustedMinInterval)
	j = expectRefresh(t, te, due, "host")
	if err := te.setProviderHeaders("fresh", map[string]string{"X-Location": "country:NL"}, due.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	te.onRefreshDone(j, refreshOutcome{err: "status", status: 409, class: "status", code: "location_unavailable"}, due.Add(2*time.Second))
	if r := te.hostReq(due.Add(2 * time.Second)); r.State != requestPending {
		t.Fatalf("after a refresh with the old headers: %+v", r)
	}
	again := due.Add(time.Second).Add(p.cfg.backoff.max)
	j = expectRefresh(t, te, again, "host")
	if j.headers["X-Location"] != "country:NL" {
		t.Fatalf("the refresh went with %v", j.headers)
	}
	te.onRefreshDone(j, refreshOutcome{ok: true}, again.Add(time.Second))
	if r := te.hostReq(again.Add(time.Second)); r.ID != id || r.State != requestApplied {
		t.Fatalf("request %+v", r)
	}
}

// After a refusal of the headers a new request waits for the end of the
// pause and tells how long; new headers end it.
func TestHostRequestWaitsForThePauseAfter401(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), nil, "a1")
	p := te.prov()
	te.hostRefresh("fresh", t0)
	j := expectRefresh(t, te, t0, "host")
	te.onRefreshDone(j, refused, at(time.Second))
	if r := te.hostReq(at(time.Second)); r.State != requestFailed || r.Code != "unauthorized" {
		t.Fatalf("refused: %+v", r)
	}
	id := te.hostRefresh("fresh", at(time.Minute))
	expectNoRefresh(t, te, at(2*time.Minute))
	if r := te.hostReq(at(2 * time.Minute)); r.ID != id || r.State != requestPending || r.WaitMs != p.authRetryAt.Sub(at(2*time.Minute)).Milliseconds() {
		t.Fatalf("in the pause: %+v, pause ends in %v", r, p.authRetryAt.Sub(at(2*time.Minute)))
	}
	if err := te.setProviderHeaders("fresh", map[string]string{"Authorization": "Bearer new"}, at(3*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if r := te.hostReq(at(3 * time.Minute)); r.State != requestPending || r.WaitMs != 0 {
		t.Fatalf("after new headers: %+v", r)
	}
	j = expectRefresh(t, te, at(3*time.Minute), "host")
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(4*time.Minute))
	if r := te.hostReq(at(4 * time.Minute)); r.ID != id || r.State != requestApplied {
		t.Fatalf("request %+v", r)
	}
}

// The state of the request is part of the snapshot: seq grows when it
// changes, and not while only its age does.
func TestViewSeqFollowsTheHostRequest(t *testing.T) {
	now := t0
	o := newObserver(context.Background(), &Config{
		Paths:     []*PathDefinition{{Tag: "a1", Balancer: "auto", Cost: 1, Probe: "check", Protocol: "vless"}},
		Probes:    []*ProbeProfile{stepProfile("check", "http://127.0.0.1/probe", 0)},
		View:      &ViewConfig{Balancer: "auto"},
		Providers: []*ProviderConfig{quietProvider(nil)},
	})
	o.clock = func() time.Time { return now }
	t.Cleanup(func() { o.Close() })
	view := func() ViewData {
		t.Helper()
		v, err := o.View()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	before := view()
	if before.Providers[0].Request != nil {
		t.Fatalf("a request before the first: %+v", before.Providers[0].Request)
	}
	id := o.RequestProviderRefresh("fresh")
	pending := view()
	now = now.Add(time.Second)
	older := view()
	if id != 1 || pending.Seq != before.Seq+1 || older.Seq != pending.Seq || older.Providers[0].Request.AgoMs != 1000 {
		t.Fatalf("seq %d, %d, %d; request %+v", before.Seq, pending.Seq, older.Seq, older.Providers[0].Request)
	}
	o.mu.Lock()
	o.e.noteDemand(now)
	jobs, _ := o.e.tickProviders(now)
	o.e.onRefreshDone(jobs[0], refreshOutcome{err: "status", status: 409, class: "status", code: "location_unavailable"}, now)
	o.mu.Unlock()
	failed := view()
	if r := failed.Providers[0].Request; failed.Seq != older.Seq+1 || r.State != requestFailed || r.Code != "location_unavailable" {
		t.Fatalf("seq %d after %d, request %+v", failed.Seq, older.Seq, r)
	}
}
