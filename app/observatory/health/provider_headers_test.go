package health

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// refused is the outcome of a refresh the backend answered with a 401 and
// an error body that refuses the headers.
var refused = refreshOutcome{err: "status 401", status: 401, class: "status", code: "unauthorized", via: "direct"}

// An answer sets only the names apply.headers allows, with values HTTP
// takes, and replaces the header of the config whatever the case of its name.
func TestAnswerRotatesOnlyAllowedHeaders(t *testing.T) {
	allowed := newProviderSettings(quietProvider(nil))
	none := newProviderSettings(quietProvider(func(p *ProviderConfig) { p.Apply.FixedHeaders = true }))
	two := newProviderSettings(quietProvider(func(p *ProviderConfig) { p.Apply.Headers = []string{"x-api-key", "Authorization"} }))
	if fmt.Sprint(allowed.rotate, none.rotate, two.rotate) != "[Authorization] [] [X-Api-Key Authorization]" {
		t.Fatalf("names an answer may set: %v, %v, %v", allowed.rotate, none.rotate, two.rotate)
	}
	many := make([]string, 0, maxRotatedHeaders+1)
	for i := 0; i <= maxRotatedHeaders; i++ {
		many = append(many, fmt.Sprintf(`"X-H%d": "v"`, i))
	}
	for _, tc := range []struct {
		headers string
		cfg     *providerSettings
		want    string
	}{
		{`{"X-Api-Key": "k"}`, allowed, "header X-Api-Key is not in apply.headers"},
		{`{"Authorization": "Bearer a"}`, none, "headers are not accepted: apply.headers is empty"},
		{`{}`, none, "headers are not accepted"},
		{`{"Authorization": "Bearer a\r\nX-Evil: 1"}`, allowed, "header Authorization has a value that is not valid"},
		{`{"Authorization": "Bearer \u0000"}`, allowed, "has a value that is not valid"},
		{`{"Authorization": ""}`, allowed, "header Authorization has no value"},
		{`{"Authorization": "` + strings.Repeat("a", maxHeaderValue+1) + `"}`, allowed, "longer than 4096 bytes"},
		{`{"Authorization": 5}`, allowed, "headers must be an object of strings"},
		{`["Authorization"]`, allowed, "headers must be an object of strings"},
		{`{"Host": "example.com"}`, allowed, "header Host cannot be set"},
		{`{"Proxy-Authorization": "x"}`, allowed, "header Proxy-Authorization cannot be set"},
		{`{"Bad Name": "x"}`, allowed, `header name "Bad Name" is not a token`},
		{`{"authorization": "a", "Authorization": "b"}`, allowed, "headers: Authorization given twice"},
		{`{` + strings.Join(many, ", ") + `}`, two, "headers: more than 8"},
	} {
		a, err := parseAnswer([]byte(`{"outbounds": [{"tag": "x1", "protocol": "vless"}], "headers": ` + tc.headers + `}`))
		if err == nil {
			err = checkRotation(a, tc.cfg)
		}
		if err == nil || !isAnswerError(err) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want a refusal about %q", cut(tc.headers), err, tc.want)
		}
		if err != nil && (strings.Contains(err.Error(), "Bearer") || strings.Contains(err.Error(), "aaaa")) {
			t.Errorf("%s: the refusal quotes a value: %v", cut(tc.headers), err)
		}
	}

	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Headers = map[string]string{"Authorization": "Bearer t0", "X-Client": "core"}
	}), nil, "a1")
	a, err := parseAnswer([]byte(`{"outbounds": [{"tag": "x1", "protocol": "vless"}], "headers": {"authorization": "Bearer t1"}}`))
	if err != nil || checkRotation(a, te.prov().cfg) != nil {
		t.Fatalf("a header of another case: %v", err)
	}
	if names, stale := te.takeAnswerHeaders(te.prov(), a.headers, 0, "answer", t0); fmt.Sprint(names) != "[Authorization]" || stale {
		t.Fatalf("names %v, stale %v", names, stale)
	}
	if got := fmt.Sprint(te.prov().requestHeaders()); got != "map[Authorization:Bearer t1 X-Client:core]" {
		t.Fatalf("request headers %s; the answer replaces the header of the config", got)
	}
	ev := te.lastOf(t, "provider_headers")
	if fmt.Sprint(ev.Fields) != "map[by:answer names:[Authorization] provider:fresh]" {
		t.Fatalf("provider_headers %v", ev.Fields)
	}
	// The same value again changes nothing and tells nothing.
	te.takeAnswerHeaders(te.prov(), a.headers, te.prov().headersGen, "answer", t0)
	if evs := te.eventsOf(t, "provider_headers"); len(evs) != 0 || te.prov().headersGen != 1 {
		t.Fatalf("the same value again: %v, generation %d", evs, te.prov().headersGen)
	}
}

// The headers an answer or the host set are at most eight together, as the
// set the host keeps carries them: an answer that would rotate more is
// refused whole and changes nothing, so ProviderSet stays a valid initial
// set and the host can still replace a header.
func TestRotatedHeadersBoundedTogether(t *testing.T) {
	f := newApplyFixture(t, time.Minute, func(p *ProviderConfig) { p.Apply.Headers = []string{"Authorization", "X-Extra"} })
	host := map[string]string{}
	for i := range maxRotatedHeaders - 1 {
		host[fmt.Sprintf("X-H%d", i)] = "v"
	}
	if err := f.o.SetProviderHeaders("fresh", host); err != nil {
		t.Fatal(err)
	}
	job := func() *refreshJob {
		f.o.mu.Lock()
		defer f.o.mu.Unlock()
		return &refreshJob{p: f.rt.p, trigger: triggerHost, headersGen: f.rt.p.headersGen}
	}
	two := answer(t, fakeOutbound{Tag: "x1", V: 1})
	two.headers = map[string]string{"Authorization": "Bearer t1", "X-Extra": "e"}
	if _, err := f.o.applySet(f.rt, two, job()); err == nil ||
		err.Error() != "invalid answer: headers: 9 with those set before, at most 8" {
		t.Fatalf("a ninth header: %v", err)
	}
	if f.m.tags() != "" {
		t.Fatalf("the refused answer added %s", f.m.tags())
	}
	one := answer(t, fakeOutbound{Tag: "x1", V: 1})
	one.headers = map[string]string{"Authorization": "Bearer t1"}
	if _, err := f.o.applySet(f.rt, one, job()); err != nil {
		t.Fatal(err)
	}
	raw, _, err := f.o.ProviderSet("fresh", 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parseInitial(raw); err != nil {
		t.Fatalf("the kept set is no valid initial: %v", err)
	}
	// The same set again, with a header that would make nine.
	if _, err := f.o.applySet(f.rt, two, job()); err == nil {
		t.Fatal("the same set with a ninth header applied")
	}
	if err := f.o.SetProviderHeaders("fresh", map[string]string{"Authorization": "Bearer host"}); err != nil {
		t.Fatalf("replacing the eighth: %v", err)
	}
	if err := f.o.SetProviderHeaders("fresh", map[string]string{"X-Device": "d1"}); err == nil {
		t.Fatal("the host added a ninth")
	}
}

// A host that set headers before the start leaves the initial set room
// for eight in all: a set that would bring more is refused, and the core
// starts on the outbounds of the config.
func TestInitialHeadersBoundedWithThoseOfTheHost(t *testing.T) {
	f, _ := initialFixture(t, `{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}],
		"headers": {"Authorization": "Bearer saved", "X-Session": "s1", "X-H0": "saved"}}`)
	host := map[string]string{}
	for i := range maxRotatedHeaders - 1 {
		host[fmt.Sprintf("X-H%d", i)] = "v"
	}
	if err := f.o.SetProviderHeaders("fresh", host); err != nil {
		t.Fatal(err)
	}
	f.o.applyInitial()
	if ev := f.lastApply(t); ev["ok"] != false || !strings.Contains(fmt.Sprint(ev["error"]), "headers: 9 with those set before") {
		t.Fatalf("apply %v", ev)
	}
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	if n := len(f.rt.p.rotated); n != maxRotatedHeaders-1 || f.m.tags() != "" {
		t.Fatalf("%d headers, outbounds %s", n, f.m.tags())
	}
}

// Only a call that adds past eight fails: one that replaces a header goes
// through, also when more than eight came from before.
func TestHostReplacesAHeaderPastTheBound(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), nil, "a1")
	p := te.prov()
	p.rotated = map[string]string{}
	for i := range maxRotatedHeaders + 1 {
		p.rotated[fmt.Sprintf("X-H%d", i)] = "v"
	}
	if err := te.setProviderHeaders("fresh", map[string]string{"X-H0": "w"}, t0); err != nil {
		t.Fatalf("replacing one of nine: %v", err)
	}
	if err := te.setProviderHeaders("fresh", map[string]string{"X-H0": "", "X-New": "n"}, t0); err != nil {
		t.Fatalf("one for another: %v", err)
	}
	if err := te.setProviderHeaders("fresh", map[string]string{"X-More": "m"}, t0); err == nil {
		t.Fatal("a tenth header")
	}
}

// Only an applied answer rotates the token: a refused one keeps the token
// in use, which the backend still honors.
func TestRejectedAnswerKeepsTheToken(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	broken := answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", Bad: true})
	broken.headers = map[string]string{"Authorization": "Bearer t1"}
	if _, err := f.apply(t, broken); err == nil {
		t.Fatal("a set that does not build applied")
	}
	f.o.mu.Lock()
	rotated, gen := fmt.Sprint(f.rt.p.rotated), f.rt.p.headersGen
	f.o.mu.Unlock()
	if rotated != "map[]" || gen != 0 {
		t.Fatalf("the refused answer rotated the token: %s, generation %d", rotated, gen)
	}
	good := answer(t, fakeOutbound{Tag: "x1", V: 1})
	good.headers = map[string]string{"Authorization": "Bearer t1"}
	if _, err := f.apply(t, good); err != nil {
		t.Fatal(err)
	}
	if ev := f.lastApply(t); fmt.Sprint(ev["headers"]) != "[Authorization]" || ev["headers_stale"] != nil {
		t.Fatalf("apply %v", ev)
	}
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	if got := f.rt.p.rotated["Authorization"]; got != "Bearer t1" {
		t.Fatalf("rotated %q", got)
	}
}

// The host changed the headers while a request ran: the headers of its
// answer were picked before that, so the host wins and the set still
// applies, also when the answer is the applied set.
func TestHostHeadersWinOverAnAnswerInFlight(t *testing.T) {
	for _, same := range []bool{false, true} {
		f := newApplyFixture(t, time.Minute)
		if same {
			if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
				t.Fatal(err)
			}
		}
		j := &refreshJob{p: f.rt.p, trigger: triggerHost, headersGen: f.rt.p.headersGen}
		if err := f.o.SetProviderHeaders("fresh", map[string]string{"Authorization": "Bearer host"}); err != nil {
			t.Fatal(err)
		}
		a := answer(t, fakeOutbound{Tag: "x1", V: 1})
		a.headers = map[string]string{"Authorization": "Bearer answer"}
		if _, err := f.o.applySet(f.rt, a, j); err != nil {
			t.Fatal(err)
		}
		ev := f.lastApply(t)
		if ev["ok"] != true || ev["changed"] != !same || ev["headers_stale"] != true {
			t.Fatalf("same set %v: apply %v", same, ev)
		}
		f.o.mu.Lock()
		got := f.rt.p.requestHeaders()["Authorization"]
		f.o.mu.Unlock()
		if got != "Bearer host" {
			t.Fatalf("same set %v: the request sends %q", same, got)
		}
	}
}

// A backend that refuses the headers is asked again only by the host until
// the pause ends: the same headers would be refused again. The refusal
// grows no backoff.
func TestAuthFailureHoldsAllButTheHost(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.SkipWhenExhausted = false
		p.Limits.ExhaustedMinInterval = int64(time.Second)
	}), nil, "a1")
	te.exhaust(te.sel, t0)
	j := expectRefresh(t, te, at(time.Second), "exhausted")
	te.onRefreshDone(j, refused, at(2*time.Second))
	p := te.prov()
	// authRetry, an hour, spread by backoff.jitter.
	if pause := p.authRetryAt.Sub(at(2 * time.Second)); !p.authFailed || p.failures != 0 || p.retrying ||
		pause < 42*time.Minute || pause > 78*time.Minute {
		t.Fatalf("auth failed %v, failures %d, retrying %v, pause %v", p.authFailed, p.failures, p.retrying, pause)
	}
	ev := te.lastOf(t, "provider_auth_failed")
	if fmt.Sprint(ev.Fields) != "map[code:unauthorized provider:fresh status:401 trigger:exhausted via:direct]" {
		t.Fatalf("provider_auth_failed %v", ev.Fields)
	}
	for now := at(3 * time.Second); now.Before(p.authRetryAt); now = now.Add(time.Minute) {
		if jobs, wake := te.refreshAt(now); len(jobs) != 0 || !wake.Equal(p.authRetryAt) {
			t.Fatalf("at %v: refreshes %v, wake %v", now.Sub(t0), triggers(jobs), wake.Sub(t0))
		}
	}
	skips := te.eventsOf(t, "provider_skip")
	if len(skips) != 1 || skips[0].Fields["reason"] != "auth" ||
		skips[0].Fields["wait_ms"] != float64(p.authRetryAt.Sub(at(3*time.Second)).Milliseconds()) {
		t.Fatalf("skips %+v, want one auth skip", skips)
	}
	if st := te.status(at(time.Minute)).Providers[0]; !st.AuthFailed || st.AuthRetryInMs == nil ||
		*st.AuthRetryInMs != p.authRetryAt.Sub(at(time.Minute)).Milliseconds() {
		t.Fatalf("status %+v", st)
	}
	te.requestRefresh("fresh")
	expectRefresh(t, te, at(10*time.Minute), "host")
}

// After the pause one refresh tries the headers again, for any trigger: a
// fleet refused by a mistake of the backend heals by itself. A lease asked
// about by the refused refresh is asked about again then.
func TestAuthRetryOnceAfterTheHold(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1")
	j := expectRefresh(t, te, at(time.Minute), "expiry")
	if len(j.asked) != 1 {
		t.Fatalf("the refresh asks about %v", j.asked)
	}
	te.onRefreshDone(j, refused, at(time.Minute+time.Second))
	if l := te.prov().leases["p-x1"]; l.asked || !l.failed {
		t.Fatalf("lease after the refusal %+v", l)
	}
	back := te.prov().authRetryAt
	expectNoRefresh(t, te, back.Add(-time.Second))
	j = expectRefresh(t, te, back, "expiry")
	if len(j.asked) != 1 {
		t.Fatalf("the retry asks about %v", j.asked)
	}
	// It fails on the way: the pause starts again from its start, so the
	// triggers do not try the refused headers in a loop.
	te.onRefreshDone(j, refreshOutcome{err: "timeout", class: "timeout"}, back.Add(time.Second))
	again := te.prov().authRetryAt
	if d := again.Sub(back); d < 42*time.Minute || d > 78*time.Minute {
		t.Fatalf("the next try %v after the retry, want an hour spread by backoff.jitter", d)
	}
	expectNoRefresh(t, te, back.Add(10*time.Minute))
	j = expectRefresh(t, te, again, "expiry")
	te.onRefreshDone(j, refreshOutcome{ok: true}, again)
	if p := te.prov(); p.authFailed || !p.authRetryAt.IsZero() {
		t.Fatalf("a refresh that got through keeps the pause: %v until %v", p.authFailed, p.authRetryAt)
	}
}

// With authRetry off only new headers end the pause.
func TestAuthRetryOffWaitsForNewHeaders(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.Interval = int64(time.Minute)
		p.Triggers.Jitter = -1
		p.Limits.MinInterval = -1
		p.Limits.AuthRetry = -1
	}), nil, "a1")
	j := expectRefresh(t, te, at(time.Minute), "interval")
	te.onRefreshDone(j, refused, at(time.Minute))
	for now := at(2 * time.Minute); now.Before(at(48 * time.Hour)); now = now.Add(time.Hour) {
		if jobs, wake := te.refreshAt(now); len(jobs) != 0 || !wake.IsZero() {
			t.Fatalf("at %v: refreshes %v, wake %v", now.Sub(t0), triggers(jobs), wake)
		}
	}
	if err := te.setProviderHeaders("fresh", map[string]string{"Authorization": "Bearer new"}, at(48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	expectRefresh(t, te, at(48*time.Hour), "interval")
}

// A 401 to a request sent before the host changed the headers says nothing
// about the new ones: the refresh is tried again with them, without a pause.
func TestStale401RetriesWithNewHeaders(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Headers = map[string]string{"Authorization": "Bearer old"}
		p.Limits.ExhaustedMinInterval = int64(time.Second)
	}), nil, "a1")
	te.requestRefresh("fresh")
	j := expectRefresh(t, te, at(time.Second), "host")
	if j.headers["Authorization"] != "Bearer old" {
		t.Fatalf("the request sends %v", j.headers)
	}
	if err := te.setProviderHeaders("fresh", map[string]string{"Authorization": "Bearer new"}, at(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	te.onRefreshDone(j, refused, at(3*time.Second))
	if p := te.prov(); p.authFailed || p.failures != 0 || len(te.eventsOf(t, "provider_auth_failed")) != 0 {
		t.Fatalf("a stale refusal: auth failed %v, failures %d", p.authFailed, p.failures)
	}
	j = expectRefresh(t, te, at(4*time.Second), "host")
	if j.headers["Authorization"] != "Bearer new" {
		t.Fatalf("the retry sends %v", j.headers)
	}
}

// New headers end the pause after a refusal, but not the pause a busy
// backend asked for, and start no refresh by themselves.
func TestSetProviderHeadersClearsAuthButNotRetryAfter(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.Interval = int64(time.Minute)
		p.Triggers.Jitter = -1
		p.Limits.MinInterval = -1
	}), nil, "a1")
	j := expectRefresh(t, te, at(time.Minute), "interval")
	te.onRefreshDone(j, refused, at(time.Minute))
	te.prov().retryAfterUntil = at(10 * time.Minute)
	if err := te.setProviderHeaders("fresh", map[string]string{"Authorization": "Bearer new"}, at(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if p := te.prov(); p.authFailed || p.host {
		t.Fatalf("auth failed %v, host request %v", p.authFailed, p.host)
	}
	if jobs, wake := te.refreshAt(at(3 * time.Minute)); len(jobs) != 0 || !wake.Equal(at(10*time.Minute)) {
		t.Fatalf("refreshes %v, wake %v; want the pause of the backend", triggers(jobs), wake.Sub(t0))
	}
	expectRefresh(t, te, at(10*time.Minute), "interval")
	ev := te.lastOf(t, "provider_headers")
	if fmt.Sprint(ev.Fields) != "map[by:host names:[Authorization] provider:fresh]" {
		t.Fatalf("provider_headers %v", ev.Fields)
	}
}

func TestSetProviderHeadersRejectsBadNames(t *testing.T) {
	o := newObserver(context.Background(), &Config{Providers: []*ProviderConfig{quietProvider(func(p *ProviderConfig) {
		p.Headers = map[string]string{"Authorization": "Bearer config"}
	})}})
	defer o.Close()
	many := map[string]string{}
	for i := 0; i <= maxRotatedHeaders; i++ {
		many[fmt.Sprintf("X-H%d", i)] = "v"
	}
	for _, tc := range []struct {
		provider string
		headers  map[string]string
		want     string
	}{
		{"other", map[string]string{"Authorization": "Bearer x"}, "no provider other"},
		{"fresh", map[string]string{"Bad Name": "x"}, "is not a token"},
		{"fresh", map[string]string{"host": "example.com"}, "header Host cannot be set"},
		{"fresh", map[string]string{"Proxy-Authorization": "x"}, "header Proxy-Authorization cannot be set"},
		{"fresh", map[string]string{"Content-Type": "text/plain"}, "header Content-Type cannot be set"},
		{"fresh", map[string]string{"Authorization": "Bearer secret\r\nX: 1"}, "header Authorization has a value that is not valid"},
		{"fresh", map[string]string{"Authorization": strings.Repeat("s", maxHeaderValue+1)}, "longer than"},
		{"fresh", map[string]string{"authorization": "a", "Authorization": "b"}, "given twice"},
		{"fresh", many, "more than 8 headers"},
	} {
		err := o.SetProviderHeaders(tc.provider, tc.headers)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%v: %v, want %q", tc.headers, err, tc.want)
		}
		if err != nil && (strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "sss")) {
			t.Errorf("the error quotes a value: %v", err)
		}
	}
	// Eight in all: the host cannot add a ninth to those an answer set.
	o.mu.Lock()
	o.e.providers[0].rotated = map[string]string{"X-A": "1", "X-B": "1", "X-C": "1", "X-D": "1", "X-E": "1", "X-F": "1", "X-G": "1"}
	o.mu.Unlock()
	if err := o.SetProviderHeaders("fresh", map[string]string{"X-H": "1", "X-I": "1"}); err == nil {
		t.Fatal("a ninth header")
	}
	if err := o.SetProviderHeaders("fresh", map[string]string{"X-H": "1", "X-A": ""}); err != nil {
		t.Fatalf("one replaced by another: %v", err)
	}
	// An empty value removes what the host set, and the config applies again.
	if err := o.SetProviderHeaders("fresh", map[string]string{"Authorization": "Bearer host"}); err != nil {
		t.Fatal(err)
	}
	if err := o.SetProviderHeaders("fresh", map[string]string{"authorization": ""}); err != nil {
		t.Fatal(err)
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if got := o.e.providers[0].requestHeaders()["Authorization"]; got != "Bearer config" {
		t.Fatalf("after the removal the request sends %q", got)
	}
}
