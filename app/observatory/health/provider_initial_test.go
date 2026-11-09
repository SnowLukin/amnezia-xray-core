package health

import (
	"encoding/json"
	"fmt"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"google.golang.org/protobuf/proto"
	"strings"
	"testing"
	"time"
)

// initialFixture is an observer about to start with an initial set, at a
// clock that stands still unless a test moves it.
func initialFixture(t *testing.T, initial string) (*applyFixture, time.Time) {
	t.Helper()
	f := newApplyFixture(t, time.Minute)
	now := time.Now()
	f.o.clock = func() time.Time { return now }
	s := f.o.e.selectors["auto"]
	s.started = false
	s.exhausted = false
	s.setActive("")
	f.o.e.report = report{}
	f.o.e.report.begin(now)
	f.rt.cfg.initial = fixtureAnswerBody([]byte(initial))
	return f, now
}

func (f *applyFixture) events(t *testing.T) []Event {
	t.Helper()
	raw, _ := f.o.DrainEvents()
	var evs []Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		t.Fatal(err)
	}
	return evs
}

func stamp(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func TestInitialZeroNextRefreshStaysImmediateInProviderSet(t *testing.T) {
	f, now := initialFixture(t, `{"outbounds":[{"tag":"x1","protocol":"vless","v":1}],"nextRefreshIn":"0s"}`)
	f.rt.cfg.jitter = 0
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	if p := f.rt.p; !p.nextAt.Equal(now) || p.nextTrigger != triggerNextRefresh {
		t.Fatalf("next refresh at %v with trigger %s, want immediate next_refresh", p.nextAt, p.nextTrigger)
	}
	raw, _, err := f.o.ExportProviderSet("fresh", 0)
	if err != nil {
		t.Fatal(err)
	}
	a, err := parseInitial(raw)
	if err != nil || !a.hasNextRefreshIn || a.nextRefreshIn != 0 || !strings.Contains(string(raw), `"nextRefreshIn":"0s"`) {
		t.Fatalf("saved set nextRefreshIn: %+v, %v", a, err)
	}
}

// A broken initial set is reported, and the core starts on the outbounds
// of the config with the usual schedule of the provider.
func TestBrokenInitialKeepsTheCoreRunning(t *testing.T) {
	for _, tc := range []struct{ name, initial, want string }{
		{"garbage", `{"outbounds": [`, "invalid answer: unexpected end of JSON input"},
		{"not an object", `[1]`, "invalid answer: "},
		{"too large", `{"outbounds": [{"tag": "x1", "v": "` + strings.Repeat("x", 262144+keptSetRoom) + `"}]}`,
			fmt.Sprintf("initial larger than %d bytes", 262144+keptSetRoom)},
		{"protocol 2", `{"protocol": 2, "outbounds": [{"tag": "x1", "protocol": "vless"}]}`, "protocol 2 is not supported"},
		{"bad receivedAt", `{"outbounds": [{"tag": "x1", "protocol": "vless"}], "receivedAt": "yesterday"}`, "receivedAt"},
		{"does not build", `{"outbounds": [{"tag": "x1", "protocol": "vless", "bad": true}]}`, "cannot build"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := initialFixture(t, tc.initial)
			f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
			f.o.applyInitial()
			ev := f.lastApply(t)
			if ev["trigger"] != "initial" || ev["is_ok"] != false || !strings.Contains(fmt.Sprint(ev["error"]), tc.want) {
				t.Fatalf("apply %v, want a refusal about %q", ev, tc.want)
			}
			f.o.mu.Lock()
			defer f.o.mu.Unlock()
			if p := f.rt.p; p.started || p.version != "" || f.m.tags() != "" || f.rt.cfg.initial != nil {
				t.Fatalf("started %v, version %q, outbounds %s, initial kept %v", p.started, p.version, f.m.tags(), f.rt.cfg.initial != nil)
			}
		})
	}
}

// The host passes the kept set at every start: an outbound of it whose
// constructor panics refuses the set, not the start of the core.
func TestInitialBuildPanicRefusesTheSet(t *testing.T) {
	f, _ := initialFixture(t, `{"outbounds":[{"tag":"x1","protocol":"vless","v":1}]}`)
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.newHandler = func(*core.OutboundHandlerConfig) (outbound.Handler, error) { panic("constructor") }
	if panics(func() {
		if err := f.o.Start(); err != nil {
			t.Error(err)
		}
	}) {
		t.Fatal("a panic building the initial set ended the start")
	}
	ev := f.lastApply(t)
	if ev["trigger"] != "initial" || ev["is_ok"] != false || ev["error"] == "" {
		t.Fatalf("apply %v, want a refusal of the initial set", ev)
	}
	if f.o.failed.Load() || f.m.tags() != "" {
		t.Fatalf("failed %v, outbounds %s after a refused initial set", f.o.failed.Load(), f.m.tags())
	}
}

// The leases of the initial set count by the clock it came with: a set the
// host saved went on aging while the core was down, also with maxAge off,
// and its nextRefreshIn counts from when it arrived.
func TestInitialLeaseClock(t *testing.T) {
	now := time.Now()
	wall := now.Round(0)
	for _, tc := range []struct {
		name    string
		initial string
		left    time.Duration
		clock   string
		skew    time.Duration
		age     time.Duration
		next    time.Duration
	}{
		// Kept two hours ago by a device ten minutes behind the backend;
		// the lease had five hours left then, and the next refresh three.
		{"saved", fmt.Sprintf(`"serverTime": %q, "receivedAt": %q, "expiresAt": {"x1": %q}, "nextRefreshIn": "3h"`,
			stamp(wall.Add(-110*time.Minute)), stamp(wall.Add(-2*time.Hour)), stamp(wall.Add(190*time.Minute))),
			3 * time.Hour, "saved", 10 * time.Minute, 2 * time.Hour, time.Hour},
		// The next refresh it asked for has passed: the interval, off here.
		{"saved without serverTime", fmt.Sprintf(`"receivedAt": %q, "expiresAt": {"x1": %q}, "nextRefreshIn": "1h"`,
			stamp(wall.Add(-2*time.Hour)), stamp(wall.Add(time.Hour))), time.Hour, "saved", 0, 2 * time.Hour, 0},
		// The answer of a connect: serverTime is when it arrived.
		{"fresh", fmt.Sprintf(`"serverTime": %q, "expiresAt": {"x1": %q}, "nextRefreshIn": "3h"`,
			stamp(wall.Add(time.Hour)), stamp(wall.Add(6*time.Hour))), 5 * time.Hour, "server", time.Hour, 0, 3 * time.Hour},
		{"no clock", fmt.Sprintf(`"expiresAt": {"x1": %q}`, stamp(wall.Add(5*time.Hour))), 5 * time.Hour, "device", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := initialFixture(t, `{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}], `+tc.initial+`}`)
			f.o.clock = func() time.Time { return now }
			f.rt.cfg.jitter = 0
			f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
			f.o.applyInitial()
			if ev := f.lastApply(t); ev["is_ok"] != true || ev["trigger"] != "initial" || ev["clock"] != tc.clock {
				t.Fatalf("apply %v", ev)
			}
			f.o.mu.Lock()
			defer f.o.mu.Unlock()
			p := f.rt.p
			l := p.leases["p-x1"]
			if l == nil || l.left(now) != tc.left || p.skew != tc.skew || p.clock != tc.clock {
				t.Fatalf("lease %+v left %v, skew %v, clock %s", l, l.left(now), p.skew, p.clock)
			}
			next := time.Duration(0)
			if !p.nextAt.IsZero() {
				next = p.nextAt.Sub(now)
			}
			if !p.started || !p.setAt.Equal(now.Add(-tc.age)) || next != tc.next || p.nextTrigger != triggerNextRefresh && next != 0 {
				t.Fatalf("started %v, set %v ago, next %s in %v", p.started, now.Sub(p.setAt), p.nextTrigger, next)
			}
			// The first tick keeps that schedule.
			f.o.e.tickProviders(now)
			if !p.setAt.Equal(now.Add(-tc.age)) {
				t.Fatalf("the first tick redated the set: %v ago", now.Sub(p.setAt))
			}
		})
	}
}

// A saved set older than its leases is the usual morning after the device
// was off: the leases end at the start, as saved, and one urgent refresh
// asks for new ones; a backend that keeps them ended is not asked again.
func TestExpiredInitialRefreshesOnce(t *testing.T) {
	f, now := initialFixture(t, "")
	wall := now.Round(0)
	f.rt.cfg.initial = []byte(fmt.Sprintf(`{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}],
		"serverTime": %q, "receivedAt": %q, "expiresAt": {"x1": %q}}`,
		stamp(wall.Add(-10*time.Hour)), stamp(wall.Add(-10*time.Hour)), stamp(wall.Add(-2*time.Hour))))
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	var ended []string
	for _, e := range f.events(t) {
		if e.Type == "provider_expired" {
			ended = append(ended, fmt.Sprint(e.Fields["outbound"], " ", e.Fields["renewal"]))
		}
	}
	if fmt.Sprint(ended) != "[p-x1 saved]" {
		t.Fatalf("provider_expired %v", ended)
	}
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	e := f.o.e
	jobs, _ := e.tickProviders(now)
	if len(jobs) != 1 || jobs[0].trigger != triggerExpiry || len(jobs[0].asked) != 1 {
		t.Fatalf("refreshes at the start %v", triggers(jobs))
	}
	// The backend answers with the same ended lease.
	end := wall.Add(-2 * time.Hour)
	e.acceptLeases(f.rt.p, f.rt.p.tags, leaseAnswer{ends: map[string]time.Time{"p-x1": end}, serverTime: wall}, now)
	e.onRefreshDone(jobs[0], refreshOutcome{ok: true}, now)
	for later := now; later.Before(now.Add(time.Hour)); later = later.Add(time.Minute) {
		e.noteDemand(later)
		if jobs, _ := e.tickProviders(later); len(jobs) != 0 {
			t.Fatalf("at %v: refreshes %v", later.Sub(now), triggers(jobs))
		}
	}
}

// A host that verifies the outbound of a start pin starts the core with the
// kept set, whose leases ended overnight: the one request about them goes
// under the pin, or the core would run on ended leases on every start. A
// retry waits for the release.
func TestExpiredInitialRefreshesUnderTheStartPin(t *testing.T) {
	f, now := initialFixture(t, "")
	wall := now.Round(0)
	f.rt.cfg.initial = []byte(fmt.Sprintf(`{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}],
		"serverTime": %q, "receivedAt": %q, "expiresAt": {"x1": %q}}`,
		stamp(wall.Add(-10*time.Hour)), stamp(wall.Add(-10*time.Hour)), stamp(wall.Add(-2*time.Hour))))
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	e := f.o.e
	// The first connection through the balancer starts it on the pin.
	delete(e.selectors, "auto")
	s := e.addSelector("auto", newSelectorSettings(&SelectorConfig{Features: &SelectorFeatureConfig{StartupPin: proto.Bool(true)}, Paths: pathSpecs("p-", 1), StartPinnedOutbound: "p-x1"}))
	e.updateCandidates(s, []string{"p-x1"}, now)
	if s.pinned != "p-x1" {
		t.Fatalf("setup: pinned %q", s.pinned)
	}
	jobs, _ := e.tickProviders(now)
	if len(jobs) != 1 || jobs[0].trigger != triggerExpiry || len(jobs[0].asked) != 1 {
		t.Fatalf("refreshes at the start %v", triggers(jobs))
	}
	e.onRefreshDone(jobs[0], refreshOutcome{err: "timeout", class: "timeout"}, now)
	for later := now.Add(time.Minute); later.Before(now.Add(time.Hour)); later = later.Add(time.Minute) {
		e.noteDemand(later)
		if jobs, _ := e.tickProviders(later); len(jobs) != 0 {
			t.Fatalf("at %v under the pin: refreshes %v", later.Sub(now), triggers(jobs))
		}
	}
}

// A lease that came ended in the answer of a connect did not end while the
// device was off: it is no saved one. It is still asked about once, as no
// refresh of this run has asked about it, and it is a decision.
func TestEndedLeaseOfAFreshInitialIsNotSaved(t *testing.T) {
	f, now := initialFixture(t, "")
	wall := now.Round(0)
	f.rt.cfg.initial = []byte(fmt.Sprintf(`{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}],
		"serverTime": %q, "expiresAt": {"x1": %q}}`, stamp(wall), stamp(wall.Add(-time.Minute))))
	f.o.e.report.begin(now)
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	var ended []string
	for _, e := range f.events(t) {
		if e.Type == "provider_expired" {
			ended = append(ended, fmt.Sprint(e.Fields["outbound"], " ", e.Fields["renewal"]))
		}
	}
	if fmt.Sprint(ended) != "[p-x1 pending]" {
		t.Fatalf("provider_expired %v", ended)
	}
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	if jobs, _ := f.o.e.tickProviders(now); len(jobs) != 1 || jobs[0].trigger != triggerExpiry {
		t.Fatalf("refreshes at the start %v", triggers(jobs))
	}
	if v := f.o.e.takeReport(true, now); v == nil || v.ProviderExpired != 1 {
		t.Fatal("an ended lease of a fresh initial set is no decision")
	}
}

// A refresh that brings the set of the initial one changes no outbound.
func TestInitialThenSameVersionChangesNoHandler(t *testing.T) {
	f, _ := initialFixture(t, `{"version": "v1", "outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}]}`)
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	if ev := f.lastApply(t); ev["is_ok"] != true || ev["is_changed"] != true || ev["added"] != 1.0 {
		t.Fatalf("initial %v", ev)
	}
	x1, created := f.m.GetHandler("p-x1"), len(f.created)
	a := answer(t, fakeOutbound{Tag: "x1", V: 1})
	a.version = "v1"
	if changed, err := f.apply(t, a); changed || err != nil {
		t.Fatalf("the same set: %v, %v", changed, err)
	}
	if ev := f.lastApply(t); ev["is_changed"] != false || ev["trigger"] != "host" {
		t.Fatalf("apply %v", ev)
	}
	if len(f.created) != created || f.m.GetHandler("p-x1") != x1 {
		t.Fatal("the same set built a handler")
	}
}

// The headers of the initial set lie over those of the config, also names
// that apply.acceptedHeaders does not list: the host saved them and may have set
// them itself. Headers the host set before the start lie over them, name by
// name.
func TestInitialHeadersOverrideConfig(t *testing.T) {
	initial := `{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}],
		"headers": {"authorization": "Bearer saved", "X-Session": "s1"}}`
	f, _ := initialFixture(t, initial)
	f.rt.cfg.headers = map[string]string{"Authorization": "Bearer config", "X-Client": "core"}
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	var by []string
	for _, e := range f.events(t) {
		if e.Type == "provider_headers" {
			by = append(by, fmt.Sprint(e.Fields["by"], e.Fields["names"]))
		}
	}
	f.o.mu.Lock()
	got := fmt.Sprint(f.rt.p.requestHeaders())
	f.o.mu.Unlock()
	if got != "map[Authorization:Bearer saved X-Client:core X-Session:s1]" || fmt.Sprint(by) != "[initial[Authorization X-Session]]" {
		t.Fatalf("request headers %s, provider_headers %v", got, by)
	}

	f, _ = initialFixture(t, initial)
	if err := f.o.SetProviderHeaders("fresh", map[string]string{"Authorization": "Bearer host"}); err != nil {
		t.Fatal(err)
	}
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	if ev := f.lastApply(t); ev["is_ok"] != true || ev["is_headers_stale"] != true {
		t.Fatalf("apply %v", ev)
	}
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	if got := fmt.Sprint(f.rt.p.requestHeaders()); got != "map[Authorization:Bearer host X-Session:s1]" {
		t.Fatalf("the request sends %s, want the Authorization of the host and the session of the set", got)
	}
}

// A header of another name that the host set before the start, such as one
// that names the device, leaves the token of the initial set in place.
func TestHostHeaderBeforeTheStartKeepsTheInitialToken(t *testing.T) {
	f, _ := initialFixture(t, `{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}],
		"headers": {"Authorization": "Bearer saved"}}`)
	f.rt.cfg.headers = map[string]string{"Authorization": "Bearer config"}
	if err := f.o.SetProviderHeaders("fresh", map[string]string{"X-Device": "d1"}); err != nil {
		t.Fatal(err)
	}
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	if ev := f.lastApply(t); ev["is_ok"] != true || ev["is_headers_stale"] != nil || fmt.Sprint(ev["headers"]) != "[Authorization]" {
		t.Fatalf("apply %v", ev)
	}
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	if got := fmt.Sprint(f.rt.p.requestHeaders()); got != "map[Authorization:Bearer saved X-Device:d1]" {
		t.Fatalf("the request sends %s", got)
	}
}

// The set a session starts with is no decision, nor are the leases it
// brings ended; a refused one is.
func TestInitialIsNoDecision(t *testing.T) {
	f, now := initialFixture(t, `{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}]}`)
	f.o.e.report.begin(now)
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	if raw := f.o.TakeReport(true); raw != nil {
		t.Fatalf("a report after the initial set alone: %s", raw)
	}
}

func TestSavedExpiredLeaseIsNoDecision(t *testing.T) {
	f, now := initialFixture(t, "")
	wall := now.Round(0)
	f.rt.cfg.initial = []byte(fmt.Sprintf(`{"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}],
		"serverTime": %q, "receivedAt": %q, "expiresAt": {"x1": %q}}`,
		stamp(wall.Add(-time.Hour)), stamp(wall.Add(-time.Hour)), stamp(wall.Add(-time.Minute))))
	f.o.e.report.begin(now)
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	f.o.mu.Lock()
	v := f.o.e.takeReport(true, now)
	f.o.mu.Unlock()
	if v != nil {
		t.Fatalf("a report after a saved lease ended: %+v", v.reportCounts)
	}
}

func TestRejectedInitialIsADecision(t *testing.T) {
	f, now := initialFixture(t, `{"protocol": 2, "outbounds": [{"tag": "x1", "protocol": "vless"}]}`)
	f.o.e.report.begin(now)
	f.rt.cfg.initial = fixtureAnswerBody(f.rt.cfg.initial)
	f.o.applyInitial()
	var r map[string]any
	if err := json.Unmarshal(f.o.TakeReport(true), &r); err != nil || r["provider_rejected"] != 1.0 || r["provider_applied"] != 0.0 {
		t.Fatalf("report %v, %v", r, err)
	}
}

// The outbound the host passes to start with is no decision either.
func TestStartWithIsNoDecision(t *testing.T) {
	te := newReportEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), StartOutbound: "b1"}, "a1", "b1")
	if te.sel.current() != "b1" {
		t.Fatalf("started on %s", te.sel.current())
	}
	if v := te.takeReport(true, at(time.Minute)); v != nil {
		t.Fatalf("a report after the start: %+v", v.reportCounts)
	}
}
