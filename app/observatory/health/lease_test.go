package health

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

// lease applies a set of tags whose leases end ttl after now by the clock of
// the backend, which agrees with the device; a tag without a ttl has none.
func (te *testEngine) lease(now time.Time, ttl map[string]time.Duration, tags ...string) leaseCounts {
	ends := map[string]time.Time{}
	for tag, d := range ttl {
		ends[tag] = now.Add(d)
	}
	c, _ := te.acceptLeases(te.prov(), tags, leaseAnswer{ends: ends, serverTime: now}, now)
	te.applyProviderSet(te.prov(), providerChange{tags: tags}, now)
	return c
}

// sameSet answers with the applied set again and these ends, as an answer
// of the same version does.
func (te *testEngine) sameSet(now time.Time, ends map[string]time.Time) leaseCounts {
	c, moved := te.acceptLeases(te.prov(), te.prov().tags, leaseAnswer{ends: ends, serverTime: now}, now)
	if moved {
		te.refreshCandidates(now)
	}
	return c
}

// leaseEngine has renewal off unless mod turns it on: most tests here are
// about the end of a lease.
func leaseEngine(t *testing.T, mod func(*ProviderConfig), sc *SelectorConfig, candidates ...string) *testEngine {
	t.Helper()
	return newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Limits.ExhaustedMinInterval = int64(time.Second)
		p.Triggers.Expiry = &ExpiryTrigger{RenewAt: -1}
		if mod != nil {
			mod(p)
		}
	}), sc, candidates...)
}

func (te *testEngine) lastOf(t *testing.T, typ string) Event {
	t.Helper()
	evs := te.eventsOf(t, typ)
	if len(evs) == 0 {
		t.Fatalf("no %s event", typ)
	}
	return evs[len(evs)-1]
}

func TestLeaseCountsFromTheLaterOfServerTimeAndDate(t *testing.T) {
	wall := t0
	for _, tc := range []struct {
		name               string
		serverTime, date   time.Time
		wantRef            time.Time
		wantClock          string
		wantLeft, wantSkew time.Duration
	}{
		// The device is an hour behind the backend.
		{"server time", wall.Add(time.Hour), wall.Add(time.Hour - time.Minute), wall.Add(time.Hour), "server", 2 * time.Hour, time.Hour},
		// A cache sent an old serverTime; the Date plus Age it keeps is later.
		{"date and age of a cache", wall.Add(-time.Hour), wall, wall, "date", 3 * time.Hour, 0},
		{"date only", time.Time{}, wall.Add(30 * time.Minute), wall.Add(30 * time.Minute), "date", 150 * time.Minute, 30 * time.Minute},
		{"server time only", wall.Add(-time.Minute), time.Time{}, wall.Add(-time.Minute), "server", 181 * time.Minute, -time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, clock := leaseRef(tc.serverTime, tc.date, wall)
			if !ref.Equal(tc.wantRef) || clock != tc.wantClock {
				t.Fatalf("ref %v by %s, want %v by %s", ref.Sub(wall), clock, tc.wantRef.Sub(wall), tc.wantClock)
			}
			te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
			end := wall.Add(3 * time.Hour)
			te.acceptLeases(te.prov(), []string{"p-x1"},
				leaseAnswer{ends: map[string]time.Time{"p-x1": end}, serverTime: tc.serverTime, date: tc.date, at: wall}, wall)
			if l := te.prov().leases["p-x1"]; l == nil || l.left(wall) != tc.wantLeft || te.prov().skew != tc.wantSkew ||
				te.prov().clock != tc.wantClock {
				t.Fatalf("lease %+v, skew %v, clock %s", l, te.prov().skew, te.prov().clock)
			}
		})
	}
}

func TestLeaseFallsBackToTheDeviceClock(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	c, _ := te.acceptLeases(te.prov(), []string{"p-x1"},
		leaseAnswer{ends: map[string]time.Time{"p-x1": at(time.Hour)}, at: at(time.Minute)}, at(time.Minute))
	if l := te.prov().leases["p-x1"]; c.leases != 1 || l.left(at(time.Minute)) != 59*time.Minute ||
		te.prov().clock != "device" || te.prov().skew != 0 {
		t.Fatalf("counts %+v, lease %+v, clock %q", c, l, te.prov().clock)
	}
}

// Status tells the clock of the last answer as provider_apply does: one
// that names no lease tells none, however it was dated.
func TestClockOnlyOfAnAnswerWithLeases(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	tags := []string{"p-x1"}
	clockOf := func() string {
		pv := te.status(at(time.Minute)).Providers[0]
		return fmt.Sprint(pv.Clock, " ", pv.SkewMs)
	}
	te.acceptLeases(te.prov(), tags, leaseAnswer{date: t0.Add(-300 * time.Millisecond), at: t0}, t0)
	if got := clockOf(); got != " 0" {
		t.Fatalf("an answer without leases: clock %q", got)
	}
	te.acceptLeases(te.prov(), tags, leaseAnswer{ends: map[string]time.Time{"p-x1": at(2 * time.Hour)}, serverTime: at(time.Hour), at: t0}, t0)
	if got := clockOf(); got != "server 3600000" {
		t.Fatalf("an answer with a lease: clock %q", got)
	}
	te.acceptLeases(te.prov(), tags, leaseAnswer{serverTime: at(time.Hour), at: t0}, t0)
	if got := clockOf(); got != " 0" {
		t.Fatalf("an answer that names no lease any more: clock %q", got)
	}
}

func TestLeaseCappedAt30Days(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.acceptLeases(te.prov(), []string{"p-x1"},
		leaseAnswer{ends: map[string]time.Time{"p-x1": t0.AddDate(5, 0, 0)}, serverTime: t0}, t0)
	if l := te.prov().leases["p-x1"]; l.left(t0) != maxLease {
		t.Fatalf("a lease of five years counts %v", l.left(t0))
	}
}

// An answer with the end it sent before does not stretch the lease, however
// long after the first one it comes: the end names the lease, and the fresh
// clock of the server only tells how much is left of it.
func TestSameEndDoesNotExtendTheLease(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": 2 * time.Hour}, "p-x1")
	if c := te.sameSet(at(time.Hour), map[string]time.Time{"p-x1": at(2 * time.Hour)}); c.renewed != 0 {
		t.Fatalf("the same end counted as renewed: %+v", c)
	}
	l := te.prov().leases["p-x1"]
	if left := l.left(at(time.Hour)); left != time.Hour {
		t.Fatalf("an hour after a lease of two hours %v is left", left)
	}
	jobs, wake := te.refreshAt(at(time.Hour))
	if len(jobs) != 0 || wake != at(2*time.Hour) {
		t.Fatalf("refreshes %v, wake %v; want the end at 2h", triggers(jobs), wake.Sub(t0))
	}
	te.refreshAt(at(2 * time.Hour))
	if !l.expired || !te.expired["p-x1"] {
		t.Fatal("the lease did not end at its end")
	}
}

// The clocks a lease counts by: the monotonic one stands while the device
// sleeps, the wall clock jumps. Go keeps no monotonic reading for times made
// up in a test, so the reading of a real time stands in for the one of a
// device that did not sleep.
func TestLeaseElapsedTakesTheLongerClock(t *testing.T) {
	l := &lease{at: t0, ttl: time.Hour}
	for _, tc := range []struct {
		name string
		now  time.Time
		want time.Duration
	}{
		{"wall forward", at(10 * time.Minute), 10 * time.Minute},
		{"wall back", at(-time.Hour), 0},
	} {
		if got := l.elapsed(tc.now); got != tc.want {
			t.Errorf("%s: elapsed %v, want %v", tc.name, got, tc.want)
		}
	}
	start := time.Now()
	l = &lease{at: start, ttl: time.Hour}
	if got := l.elapsed(start.Add(20 * time.Minute)); got != 20*time.Minute {
		t.Errorf("both clocks: elapsed %v", got)
	}
	// After a sleep the monotonic clock is behind the wall clock: a reading
	// of the wall clock alone counts the sleep.
	if got := l.elapsed(start.Round(0).Add(8 * time.Hour)); got != 8*time.Hour {
		t.Errorf("sleep: elapsed %v, want the 8 h of the wall clock", got)
	}
	if left := l.left(start.Round(0).Add(8 * time.Hour)); left != -7*time.Hour {
		t.Errorf("left after the sleep %v", left)
	}
}

// A device whose clock is a day behind the backend still ends a lease of
// 40 seconds 40 seconds after the answer.
func TestServerClockADayAheadStillEndsOnTime(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	server := at(24 * time.Hour)
	c, _ := te.acceptLeases(te.prov(), []string{"p-x1"},
		leaseAnswer{ends: map[string]time.Time{"p-x1": server.Add(40 * time.Second)}, serverTime: server}, t0)
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x1"}}, t0)
	if c.expired != 0 || te.prov().skew != 24*time.Hour {
		t.Fatalf("counts %+v, skew %v", c, te.prov().skew)
	}
	if _, wake := te.refreshAt(at(39 * time.Second)); wake != at(40*time.Second) || te.expired["p-x1"] {
		t.Fatalf("wake %v, expired %v", wake.Sub(t0), te.expired["p-x1"])
	}
	te.refreshAt(at(40 * time.Second))
	if !te.expired["p-x1"] {
		t.Fatal("not expired after 40 s")
	}
}

// The balancer leaves an outbound whose lease ended for a live one, after a
// probe of the live one. The outbound is not probed, not quarantined and
// its state does not change: it may well work.
func TestExpiredOutboundGetsNoNewConnections(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute, "p-x2": time.Hour}, "p-x1", "p-x2")
	te.onSuccess("p-x1", at(time.Second))
	te.eventsOf(t, "")
	te.refreshAt(at(time.Minute))
	jobs := te.run(t, at(time.Minute), nil)
	if len(jobs) != 1 || jobs[0].tag != "p-x2" {
		t.Fatalf("probes %+v, want one of p-x2", jobs)
	}
	sw := te.lastOf(t, "switch")
	if te.sel.current() != "p-x2" || sw.Fields["from"] != "p-x1" || sw.Fields["reason"] != "expired" {
		t.Fatalf("active %q after %v", te.sel.current(), sw.Fields)
	}
	if p := te.path("p-x1"); p.status != statusUp || p.quarantined(at(time.Minute)) {
		t.Fatalf("p-x1 is %v, quarantined %v", p.status, p.quarantined(at(time.Minute)))
	}
	if r := te.ranked(te.sel, at(time.Minute), nil); fmt.Sprint(r) != "[p-x2 p-x1]" {
		t.Fatalf("ranked %v, want the expired one last", r)
	}
	te.traffic(at(time.Hour - time.Minute))
	for _, j := range te.run(t, at(time.Hour-time.Minute), nil) {
		t.Fatalf("probe %s of %s: no return to an expired outbound", j.reason, j.tag)
	}
}

// The turn of the loop in which a lease ends starts the probe of the
// outbound to move to, not a later turn whatever wakes it.
func TestLeaseEndStartsTheProbeInTheSameTurn(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute, "p-x2": time.Hour}, "p-x1", "p-x2")
	te.traffic(at(50 * time.Second))
	if jobs, _, wake := te.step(at(50 * time.Second)); len(jobs) != 0 || wake != at(time.Minute) {
		t.Fatalf("probes %+v, wake %v; want the end of the lease", jobs, wake.Sub(t0))
	}
	jobs, _, _ := te.step(at(time.Minute))
	if len(jobs) != 1 || jobs[0].tag != "p-x2" || jobs[0].reason != reasonFailover {
		t.Fatalf("probes in the turn the lease ended %+v, want the one of p-x2", jobs)
	}
}

// A lease that ends while nothing live is usable leaves the balancer where
// it is, waiting for a return. The loop follows only the wakes of step:
// the outbound still works, so its traffic sends no kick, and the backend
// answered the renewal without one, so no refresh comes either. The turn
// in which the lease ends must give the wake of that return.
func TestLeaseEndGivesTheWakeOfTheReturn(t *testing.T) {
	te := leaseEngine(t, renewing(nil), &SelectorConfig{Costs: costs("p-x1", 1, "a", 2)}, "p-x1", "a1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1")
	te.failN(3, "a1", failDial, at(time.Second))
	rested := te.path("a1").quarantineUntil
	now := at(time.Second)
	for now.Before(at(time.Hour)) && te.sel.current() != "a1" {
		te.traffic(now)
		jobs, refreshes, wake := te.step(now)
		for _, j := range refreshes {
			te.sameSet(now, map[string]time.Time{"p-x1": at(time.Minute)})
			te.onRefreshDone(j, refreshOutcome{ok: true}, now)
		}
		for _, j := range jobs {
			te.onProbeResult(j, probeResult{ok: true, rtt: 50 * time.Millisecond, bytes: 40_000}, now)
		}
		if len(jobs) > 0 || len(refreshes) > 0 {
			// What ended kicks the loop, which steps at once.
			continue
		}
		if wake.IsZero() {
			t.Fatalf("no wake at %v on %q (expired %v), the return planned at %v",
				now.Sub(t0), te.sel.current(), te.expired[te.sel.current()], te.sel.upgradeAt.Sub(t0))
		}
		now = wake
	}
	if sw := te.lastOf(t, "switch"); te.sel.current() != "a1" || sw.Fields["reason"] != "upgrade" ||
		now.Sub(rested) > time.Minute {
		t.Fatalf("on %q at %v after %v; a1 rested at %v", te.sel.current(), now.Sub(t0), sw.Fields, rested.Sub(t0))
	}
}

func TestExpiryEventTellsWhyThereWasNoRenewal(t *testing.T) {
	for _, tc := range []struct {
		renewAt float64
		want    string
	}{
		{0, "[p-x1 true pending p-x2 false pending]"},
		{-1, "[p-x1 true off p-x2 false off]"},
	} {
		te := leaseEngine(t, func(p *ProviderConfig) { p.Triggers.Expiry.RenewAt = tc.renewAt },
			&SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2")
		te.lease(t0, map[string]time.Duration{"p-x1": time.Minute, "p-x2": time.Minute}, "p-x1", "p-x2")
		te.eventsOf(t, "")
		// The first tick after the lease ended: no refresh asked meanwhile.
		te.refreshAt(at(time.Minute))
		var got []string
		for _, e := range te.eventsOf(t, "provider_expired") {
			got = append(got, fmt.Sprint(e.Fields["outbound"], " ", e.Fields["active"], " ", e.Fields["renewal"]))
		}
		if fmt.Sprint(got) != tc.want {
			t.Errorf("renewAt %v: expired events %v, want %s", tc.renewAt, got, tc.want)
		}
	}
}

// Leaving an expired outbound never ends on another one: the balancer stays
// where it is.
func TestLeavingNeverMovesToAnotherExpired(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2, "a", 3)}, "p-x1", "p-x2", "a1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute, "p-x2": time.Minute}, "p-x1", "p-x2")
	te.failN(3, "a1", failDial, at(time.Second))
	te.eventsOf(t, "")
	te.refreshAt(at(time.Minute))
	if jobs := te.run(t, at(time.Minute), nil); len(jobs) != 0 {
		t.Fatalf("probes %+v while nothing live is usable", jobs)
	}
	if te.sel.current() != "p-x1" || te.sel.exhausted || len(te.eventsOf(t, "switch")) != 0 {
		t.Fatalf("active %q exhausted %v, want to stay on p-x1", te.sel.current(), te.sel.exhausted)
	}
	// Once the live one rested its quarantine, the balancer returns to it.
	for now := at(2 * time.Minute); now.Before(at(time.Hour)) && te.sel.current() != "a1"; now = now.Add(time.Second) {
		te.traffic(now)
		te.run(t, now, nil)
	}
	if sw := te.lastOf(t, "switch"); te.sel.current() != "a1" || sw.Fields["reason"] != "upgrade" {
		t.Fatalf("active %q after %v", te.sel.current(), sw.Fields)
	}
}

// A failover takes an expired outbound only after every live one failed.
func TestFailoverTakesExpiredAsLastResort(t *testing.T) {
	for _, tc := range []struct {
		name  string
		b1    bool
		want  string
		order string
	}{
		{"a live one works", true, "b1", "[b1]"},
		{"nothing live works", false, "p-x1", "[b1 p-x1]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "a", 2, "b", 3)}, "p-x1", "a1", "b1")
			te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1")
			te.refreshAt(at(time.Minute))
			te.run(t, at(time.Minute), nil)
			if te.sel.current() != "a1" {
				t.Fatalf("setup: active %q", te.sel.current())
			}
			te.failN(3, "a1", failDial, at(2*time.Minute))
			var order []string
			for _, j := range te.run(t, at(2*time.Minute), map[string]bool{"b1": tc.b1}) {
				order = append(order, j.tag)
			}
			if te.sel.current() != tc.want || te.sel.exhausted || fmt.Sprint(order) != tc.order {
				t.Fatalf("active %q exhausted %v after probes %v", te.sel.current(), te.sel.exhausted, order)
			}
			if sw := te.lastOf(t, "switch"); sw.Fields["reason"] != "failover" {
				t.Fatalf("switch %v", sw.Fields)
			}
		})
	}
}

// When every lease ended and the backend does not answer, traffic goes on
// through the expired outbound, not to the fallback, and the refresh backs
// off.
func TestAllExpiredStaysOffBlock(t *testing.T) {
	te := leaseEngine(t, func(p *ProviderConfig) {
		p.Limits.MaxPerHour = -1
		p.Limits.Backoff = &Backoff{Initial: int64(30 * time.Second), Multiplier: 2, Max: int64(time.Hour), Jitter: 0.1}
	}, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1")
	var starts []time.Time
	for now := at(time.Minute); now.Before(at(20 * time.Minute)); now = now.Add(time.Second) {
		jobs, _ := te.refreshAt(now)
		te.run(t, now, nil)
		for _, j := range jobs {
			if j.trigger != triggerExpiry {
				t.Fatalf("refresh %s", j.trigger)
			}
			starts = append(starts, now)
			te.onRefreshDone(j, refreshOutcome{err: "timeout"}, now)
		}
	}
	if te.sel.current() != "p-x1" || te.sel.exhausted || len(te.eventsOf(t, "exhausted")) != 0 {
		t.Fatalf("active %q exhausted %v", te.sel.current(), te.sel.exhausted)
	}
	if len(starts) != 6 || starts[0] != at(time.Minute) {
		t.Fatalf("%d refreshes from %v", len(starts), starts[0].Sub(t0))
	}
	for i := 1; i < len(starts); i++ {
		base := (30 * time.Second) << (i - 1)
		if gap := starts[i].Sub(starts[i-1]); gap < base*9/10 || gap > base*11/10+time.Second {
			t.Fatalf("gap %d is %v, want %v +-10%%", i, gap, base)
		}
	}
}

// An expired outbound that a balancer still uses is judged as any other:
// it fails, and the balancer moves on, to another expired one if that is
// all there is.
func TestChosenExpiredStillFailsOver(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute, "p-x2": time.Minute}, "p-x1", "p-x2")
	te.refreshAt(at(time.Minute))
	te.eventsOf(t, "")
	te.failN(3, "p-x1", failDial, at(2*time.Minute))
	jobs := te.run(t, at(2*time.Minute), nil)
	if te.path("p-x1").status != statusDown || len(jobs) != 1 || jobs[0].tag != "p-x2" || te.sel.current() != "p-x2" {
		t.Fatalf("p-x1 %v, probes %+v, active %q", te.path("p-x1").status, jobs, te.sel.current())
	}
	if sw := te.lastOf(t, "switch"); sw.Fields["reason"] != "failover" {
		t.Fatalf("switch %v", sw.Fields)
	}
}

// Old connections of an outbound whose lease ended fail as the backend
// revokes it: no balancer chooses it, so that is no evidence.
func TestEvidenceOfAnIdleExpiredIgnored(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1", "p-x2")
	te.refreshAt(at(time.Minute))
	te.run(t, at(time.Minute), nil)
	if te.sel.current() != "p-x2" {
		t.Fatalf("setup: active %q", te.sel.current())
	}
	te.eventsOf(t, "")
	te.failN(5, "p-x1", failDial, at(2*time.Minute))
	if p := te.path("p-x1"); p.status == statusDown || len(p.failures) != 0 || len(te.eventsOf(t, "path_state")) != 0 {
		t.Fatalf("p-x1 %v with %d failures", p.status, len(p.failures))
	}
}

// A pinned balancer stays on its pin when the lease ends; a release lets it
// go.
func TestPinnedExpiredStays(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2), StartPinned: "p-x1"}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1", "p-x2")
	te.refreshAt(at(time.Minute))
	if jobs := te.run(t, at(time.Minute), nil); len(jobs) != 0 || te.sel.current() != "p-x1" {
		t.Fatalf("pinned balancer: probes %+v, active %q", jobs, te.sel.current())
	}
	te.release("auto", at(2*time.Minute))
	te.run(t, at(2*time.Minute), nil)
	if sw := te.lastOf(t, "switch"); te.sel.current() != "p-x2" || sw.Fields["reason"] != "expired" {
		t.Fatalf("after release active %q, %v", te.sel.current(), sw.Fields)
	}
}

func TestExpiredNeitherRememberedNorUpgradeTargetNorInHostPlan(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2, "a", 3)}, "p-x1", "p-x2", "a1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1", "p-x2")
	te.refreshAt(at(time.Minute))
	te.run(t, at(time.Minute), nil)
	if te.sel.current() != "p-x2" {
		t.Fatalf("setup: active %q", te.sel.current())
	}
	if te.hasCheaper(te.sel, "p-x2") || te.upgradeTargetFor(te.sel, at(time.Minute)) != "" || !te.sel.upgradeAt.IsZero() {
		t.Fatalf("a return to the expired p-x1 is planned at %v", te.sel.upgradeAt)
	}
	if plan := te.probePlan(te.sel, at(time.Minute)); fmt.Sprint(plan) != "[p-x2 a1]" {
		t.Fatalf("host plan %v", plan)
	}
	if _, tags, err := te.requestProbe("auto", []string{"p-x1"}, at(time.Minute)); err != nil || fmt.Sprint(tags) != "[p-x1]" {
		t.Fatalf("a listed expired outbound: %v, %v", tags, err)
	}
	te.networks["home"] = map[string]string{"auto": "p-x1"}
	te.onNetworkChange("home", at(2*time.Minute))
	for _, j := range te.queue {
		if j.tag == "p-x1" && j.reason == reasonRemembered {
			t.Fatal("the expired outbound remembered for the network is probed")
		}
	}
	// A balancer that starts now takes neither the remembered nor the
	// passed outbound.
	s := te.addSelector("other", newSelectorSettings(&SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2), StartWith: "p-x1"}))
	te.networks["home"]["other"] = "p-x1"
	te.updateCandidates(s, []string{"p-x1", "p-x2"}, at(3*time.Minute))
	if s.current() != "p-x2" {
		t.Fatalf("a new balancer started on %q", s.current())
	}
}

func TestMinHealthyCountsExpiredAsGone(t *testing.T) {
	te := leaseEngine(t, func(p *ProviderConfig) { p.Triggers.MinHealthy = 3 }, &SelectorConfig{Costs: costs("p-", 1)},
		"p-x1", "p-x2", "p-x3")
	te.lease(t0, map[string]time.Duration{"p-x3": time.Hour}, "p-x1", "p-x2", "p-x3")
	expectNoRefresh(t, te, at(30*time.Second))
	// Ended on arrival: no expiry refresh for it, only minHealthy may want one.
	te.lease(at(31*time.Second), map[string]time.Duration{"p-x3": 0}, "p-x1", "p-x2", "p-x3")
	if !te.expired["p-x3"] || te.healthy(te.sel) != 2 {
		t.Fatalf("setup: p-x3 expired %v, %d healthy", te.expired["p-x3"], te.healthy(te.sel))
	}
	expectRefresh(t, te, at(32*time.Second), "min_healthy")
}

// A backend that answers without renewing an expired lease is not asked
// about it again: one request per value of a lease.
func TestExpiryRefreshOncePerLeaseValue(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1", "p-x2")
	expectNoRefresh(t, te, at(59*time.Second))
	j := expectRefresh(t, te, at(time.Minute), "expiry")
	if len(j.asked) != 1 || j.asked[0].tag != "p-x1" {
		t.Fatalf("the refresh asks about %+v", j.asked)
	}
	te.sameSet(at(61*time.Second), map[string]time.Time{"p-x1": at(time.Minute)})
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(61*time.Second))
	for now := at(2 * time.Minute); now.Before(at(3 * time.Hour)); now = now.Add(time.Minute) {
		expectNoRefresh(t, te, now)
	}
	// A later value is a new lease and is asked about once it ends.
	te.sameSet(at(3*time.Hour), map[string]time.Time{"p-x1": at(3*time.Hour + time.Minute)})
	if te.expired["p-x1"] {
		t.Fatal("the renewed lease is still expired")
	}
	expectRefresh(t, te, at(3*time.Hour+time.Minute), "expiry")
}

func TestExpiryRearmedAfterAFailedRefresh(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1")
	j := expectRefresh(t, te, at(time.Minute), "expiry")
	te.onRefreshDone(j, refreshOutcome{err: "status 503", retryAfter: 20 * time.Second}, at(time.Minute))
	if l := te.prov().leases["p-x1"]; l.asked || !l.failed {
		t.Fatalf("after a busy backend the lease is asked %v, failed %v", l.asked, l.failed)
	}
	until := te.prov().retryAfterUntil
	expectNoRefresh(t, te, until.Add(-time.Second))
	j = expectRefresh(t, te, until, "expiry")
	te.onRefreshDone(j, refreshOutcome{err: "timeout"}, until)
	until = te.prov().backoffUntil
	expectNoRefresh(t, te, until.Add(-time.Second))
	j = expectRefresh(t, te, until, "expiry")
	te.onRefreshDone(j, refreshOutcome{ok: true}, until)
	if l := te.prov().leases["p-x1"]; !l.asked || l.failed {
		t.Fatalf("after an answer the lease is asked %v, failed %v", l.asked, l.failed)
	}
	expectNoRefresh(t, te, at(time.Hour))
}

// Without user traffic the first request for a lease goes out, a retry
// after a failure waits for traffic: a dead backend does not wake a
// sleeping device every backoff.
func TestFailedExpiryRetryWaitsForTrafficWhenIdle(t *testing.T) {
	te := leaseEngine(t, func(p *ProviderConfig) {
		p.Limits.Backoff = &Backoff{Initial: int64(30 * time.Second), Jitter: -1}
	}, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.s.schedule.pauseWhenIdle = 2 * time.Minute
	te.lease(t0, map[string]time.Duration{"p-x1": time.Hour}, "p-x1")
	// Idle since t0, yet the end of the lease wakes the observer.
	if jobs, wake := te.tickProviders(at(30 * time.Minute)); len(jobs) != 0 || wake != at(time.Hour) {
		t.Fatalf("refreshes %v, wake %v; want the end of the lease", triggers(jobs), wake.Sub(t0))
	}
	jobs, _ := te.tickProviders(at(time.Hour))
	if len(jobs) != 1 || jobs[0].trigger != triggerExpiry {
		t.Fatalf("idle refreshes at the end %v", triggers(jobs))
	}
	te.onRefreshDone(jobs[0], refreshOutcome{err: "timeout"}, at(time.Hour))
	te.eventsOf(t, "")
	if jobs, wake := te.tickProviders(at(2 * time.Hour)); len(jobs) != 0 || !wake.IsZero() {
		t.Fatalf("idle retry %v, wake %v", triggers(jobs), wake.Sub(t0))
	}
	if skips := te.eventsOf(t, "provider_skip"); len(skips) != 1 || skips[0].Fields["reason"] != "idle" {
		t.Fatalf("skips %+v", skips)
	}
	expectRefresh(t, te, at(2*time.Hour+time.Second), "expiry")
}

// An answer that brings a lease ended already gives no more: no refresh is
// asked for it, which with a wrong clock would be a loop.
func TestExpiredOnArrivalStartsNoRefresh(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2")
	te.eventsOf(t, "")
	c := te.lease(at(time.Minute), map[string]time.Duration{"p-x1": -time.Second, "p-x2": time.Hour}, "p-x1", "p-x2")
	if c.leases != 2 || c.expired != 1 || !te.expired["p-x1"] {
		t.Fatalf("counts %+v", c)
	}
	if ev := te.lastOf(t, "provider_expired"); ev.Fields["outbound"] != "p-x1" || ev.Fields["renewal"] != "refused" {
		t.Fatalf("expired %v", ev.Fields)
	}
	for now := at(2 * time.Minute); now.Before(at(50 * time.Minute)); now = now.Add(time.Minute) {
		expectNoRefresh(t, te, now)
	}
}

// A later end that passed already is a new value but renews nothing: a
// lease that ended stays so without another provider_expired, and one that
// had not ended ends once. A backend whose ends trail its clock adds no
// renewal and no decision to the report on every answer.
func TestLaterEndThatPassedRenewsNothing(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute, "p-x2": time.Hour}, "p-x1", "p-x2")
	te.refreshAt(at(time.Minute))
	if !te.expired["p-x1"] {
		t.Fatal("setup: p-x1 did not end")
	}
	te.eventsOf(t, "")
	for i, now := range []time.Time{at(3 * time.Minute), at(4 * time.Minute)} {
		c := te.sameSet(now, map[string]time.Time{"p-x1": now.Add(-time.Second), "p-x2": at(time.Hour)})
		if c.renewed != 0 || c.expired != 1 || !te.expired["p-x1"] || !te.prov().leases["p-x1"].asked {
			t.Fatalf("answer %d: counts %+v, expired %v", i, c, te.expired)
		}
	}
	if evs := te.eventsOf(t, "provider_expired"); len(evs) != 0 {
		t.Fatalf("the ended lease ended again: %+v", evs)
	}
	// The clock of the backend runs ahead now: the later end of p-x2 is
	// past by it.
	c, _ := te.acceptLeases(te.prov(), te.prov().tags, leaseAnswer{
		ends: map[string]time.Time{"p-x1": at(4 * time.Minute), "p-x2": at(90 * time.Minute)}, serverTime: at(2 * time.Hour)}, at(5*time.Minute))
	if evs := te.eventsOf(t, "provider_expired"); c.renewed != 0 || c.expired != 2 || len(evs) != 1 ||
		evs[0].Fields["outbound"] != "p-x2" || evs[0].Fields["renewal"] != "refused" {
		t.Fatalf("counts %+v, expired events %+v; want one of p-x2", c, evs)
	}
}

// An end centuries off the clock it counts from, such as the zero time a
// backend sends for a time it never set, ends the lease on arrival as any
// end in the past does. The answer arrives a moment before it is applied,
// as over HTTP: what is left of the lease is counted down at once.
func TestLeaseEndCenturiesOffEndsOnArrival(t *testing.T) {
	for _, tc := range []struct {
		name        string
		end, server time.Time
	}{
		{"zero end", time.Time{}, at(time.Minute)},
		{"end in 1700", time.Date(1700, 1, 1, 0, 0, 0, 0, time.UTC), at(time.Minute)},
		{"server time in 9999", at(time.Hour), time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2")
			te.eventsOf(t, "")
			now := at(time.Minute)
			tags := []string{"p-x1", "p-x2"}
			c, _ := te.acceptLeases(te.prov(), tags, leaseAnswer{
				ends:       map[string]time.Time{"p-x1": tc.end, "p-x2": tc.server.Add(time.Hour)},
				serverTime: tc.server, at: now.Add(-time.Millisecond)}, now)
			te.applyProviderSet(te.prov(), providerChange{tags: tags}, now)
			if c.leases != 2 || c.expired != 1 || !te.expired["p-x1"] || te.expired["p-x2"] {
				t.Fatalf("counts %+v, expired %v", c, te.expired)
			}
			if ev := te.lastOf(t, "provider_expired"); ev.Fields["outbound"] != "p-x1" || ev.Fields["renewal"] != "refused" {
				t.Fatalf("expired %v", ev.Fields)
			}
			later := at(time.Hour)
			for _, p := range te.status(later).Paths {
				if p.Outbound == "p-x1" && (p.ExpiresInMs == nil || *p.ExpiresInMs >= 0 || !p.Expired) {
					t.Fatalf("status of p-x1 an hour later: expires in %v, expired %v", p.ExpiresInMs, p.Expired)
				}
			}
			for now := at(2 * time.Minute); now.Before(at(50 * time.Minute)); now = now.Add(time.Minute) {
				expectNoRefresh(t, te, now)
			}
		})
	}
}

func TestExpiryRefreshUsesExhaustedMinIntervalAndRunsUnderLowMemory(t *testing.T) {
	te := leaseEngine(t, func(p *ProviderConfig) {
		p.Limits.MinInterval = int64(5 * time.Minute)
		p.Limits.ExhaustedMinInterval = int64(30 * time.Second)
	}, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": 10 * time.Second}, "p-x1")
	te.requestRefresh("fresh")
	j := expectRefresh(t, te, t0, "host")
	te.onRefreshDone(j, refreshOutcome{ok: true}, t0)
	te.setMemoryLow(true, at(time.Second))
	expectNoRefresh(t, te, at(29*time.Second))
	expectRefresh(t, te, at(30*time.Second), "expiry")
}

// An exhausted balancer waits for traffic; an expiry does not, and the
// first must not hide the second.
func TestIdleExhaustedDoesNotHideExpiry(t *testing.T) {
	te := leaseEngine(t, func(p *ProviderConfig) { p.Triggers.SkipWhenExhausted = false },
		&SelectorConfig{Costs: costs("p-", 1), SwitchWithoutProbe: true}, "p-x1", "p-x2")
	te.s.schedule.pauseWhenIdle = 2 * time.Minute
	te.s.schedule.quarantine = backoff{initial: 2 * time.Hour, multiplier: 2, max: 4 * time.Hour}
	te.lease(t0, map[string]time.Duration{"p-x1": time.Hour}, "p-x1", "p-x2")
	te.failN(3, "p-x1", failDial, at(time.Minute))
	te.failN(3, "p-x2", failDial, at(time.Minute))
	if !te.sel.exhausted {
		t.Fatalf("setup: active %q", te.sel.current())
	}
	te.eventsOf(t, "")
	jobs, _ := te.tickProviders(at(time.Hour))
	if len(jobs) != 1 || jobs[0].trigger != triggerExpiry {
		t.Fatalf("idle refreshes %v, want expiry", triggers(jobs))
	}
	if skips := te.eventsOf(t, "provider_skip"); len(skips) != 1 || skips[0].Fields["trigger"] != "exhausted" ||
		skips[0].Fields["reason"] != "idle" {
		t.Fatalf("skips %+v", skips)
	}
}

// A renewed lease makes the outbound a live candidate again, and the
// balancer returns to it the usual way.
func TestRenewedIsACandidateAgain(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute}, "p-x1", "p-x2")
	te.refreshAt(at(time.Minute))
	te.run(t, at(time.Minute), nil)
	if te.sel.current() != "p-x2" {
		t.Fatalf("setup: active %q", te.sel.current())
	}
	c := te.sameSet(at(2*time.Minute), map[string]time.Time{"p-x1": at(time.Hour)})
	if c.renewed != 1 || c.expired != 0 || te.expired["p-x1"] {
		t.Fatalf("counts %+v, expired %v", c, te.expired["p-x1"])
	}
	if te.upgradeTargetFor(te.sel, at(2*time.Minute)) != "p-x1" || te.sel.upgradeAt.IsZero() {
		t.Fatalf("no return planned to the renewed p-x1")
	}
	for now := at(2 * time.Minute); now.Before(at(10*time.Minute)) && te.sel.current() != "p-x1"; now = now.Add(time.Second) {
		te.traffic(now)
		te.run(t, now, nil)
	}
	if sw := te.lastOf(t, "switch"); te.sel.current() != "p-x1" || sw.Fields["reason"] != "upgrade" {
		t.Fatalf("active %q after %v", te.sel.current(), sw.Fields)
	}
}

func TestNetworkChangeKeepsLeases(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute, "p-x2": time.Hour}, "p-x1", "p-x2")
	te.refreshAt(at(time.Minute))
	te.onNetworkChange("cafe", at(2*time.Minute))
	if !te.expired["p-x1"] || te.prov().leases["p-x2"] == nil || te.prov().leases["p-x2"].left(at(2*time.Minute)) != 58*time.Minute {
		t.Fatalf("leases after a network change: expired %v, %+v", te.expired, te.prov().leases)
	}
}

// A lease goes with its outbound: out of the set, after the drain, and when
// an answer stops naming an end.
func TestLeaseGoesWithTheOutbound(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2, "p-x3", 3)}, "p-x1", "p-x2", "p-x3")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute, "p-x2": time.Minute, "p-x3": time.Minute}, "p-x1", "p-x2", "p-x3")
	te.refreshAt(at(time.Minute))
	if len(te.expired) != 3 {
		t.Fatalf("setup: expired %v", te.expired)
	}
	// p-x1 leaves the set, p-x2 has no end any more.
	c, moved := te.acceptLeases(te.prov(), []string{"p-x2", "p-x3"},
		leaseAnswer{ends: map[string]time.Time{"p-x3": at(time.Minute)}, serverTime: at(2 * time.Minute)}, at(2*time.Minute))
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x2", "p-x3"}, retire: []string{"p-x1"}}, at(2*time.Minute))
	if !moved || c.leases != 1 || c.expired != 1 || len(te.prov().leases) != 1 || fmt.Sprint(te.expired) != "map[p-x3:true]" {
		t.Fatalf("counts %+v, leases %v, expired %v", c, te.prov().leases, te.expired)
	}
	te.dropProviderOutbound("p-x3", at(3*time.Minute))
	if len(te.prov().leases) != 0 || len(te.expired) != 0 {
		t.Fatalf("after the drain: leases %v, expired %v", te.prov().leases, te.expired)
	}
}

// A refresh for any trigger asks about the leases that ended: a failed one
// puts them back to be asked about, an answer settles them.
func TestAnyRefreshAsksAboutEndedLeases(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Minute, "p-x2": time.Hour}, "p-x1", "p-x2")
	te.prov().fetching = true
	te.refreshAt(at(time.Minute))
	te.prov().fetching = false
	te.requestRefresh("fresh")
	j := expectRefresh(t, te, at(2*time.Minute), "host")
	if fmt.Sprint(j.asked) != fmt.Sprint([]askedLease{{"p-x1", at(time.Minute)}}) {
		t.Fatalf("the host refresh asks about %+v", j.asked)
	}
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(2*time.Minute))
	expectNoRefresh(t, te, at(3*time.Minute))
}

func TestLeaseInStatusAndBody(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-x1", 1, "p-x2", 2)}, "p-x1", "p-x2", "p-x3")
	server := at(time.Hour)
	te.acceptLeases(te.prov(), []string{"p-x1", "p-x2", "p-x3"}, leaseAnswer{
		ends: map[string]time.Time{"p-x1": server.Add(time.Minute), "p-x2": server.Add(time.Hour)}, serverTime: server}, t0)
	te.applyProviderSet(te.prov(), providerChange{tags: []string{"p-x1", "p-x2", "p-x3"}}, t0)
	te.path("p-x3")
	now := at(2 * time.Minute)
	te.expireLeases(te.prov(), now)
	st := te.status(now)
	left := map[string]string{}
	for _, p := range st.Paths {
		if p.ExpiresInMs != nil {
			left[p.Outbound] = fmt.Sprint(*p.ExpiresInMs, p.Expired)
		}
	}
	if fmt.Sprint(left) != "map[p-x1:-60000 true p-x2:3480000 false]" {
		t.Fatalf("paths %v", left)
	}
	if pv := st.Providers[0]; pv.Expired != 1 || pv.SkewMs != 3_600_000 || pv.Clock != "server" {
		t.Fatalf("provider %+v", pv)
	}
	m, _, _ := requestOf(t, te, now)
	var got []string
	for _, o := range m["outbounds"].([]any) {
		got = append(got, fmt.Sprint(o.(map[string]any)["tag"], ":", o.(map[string]any)["expiresInMs"]))
	}
	if !slices.Equal(got, []string{"x1:-60000", "x2:3.48e+06", "x3:<nil>"}) {
		t.Fatalf("body outbounds %v", got)
	}
}

// Leases do not grow with answers: one per outbound of the set at most,
// however tags rotate.
func TestLeasesStayWithinTheSet(t *testing.T) {
	te := leaseEngine(t, nil, &SelectorConfig{Costs: costs("p-", 1)}, "p-x0")
	limit := te.prov().cfg.maxOutbounds
	for i := 0; i < 10_000; i++ {
		now := at(time.Duration(i) * time.Second)
		tags := make([]string, limit)
		ttl := map[string]time.Duration{}
		for k := range tags {
			tags[k] = fmt.Sprintf("p-%d-%d", i, k)
			ttl[tags[k]] = time.Duration(k%3-1) * time.Minute
		}
		te.lease(now, ttl, tags...)
		if len(te.prov().leases) > limit || len(te.expired) > limit {
			t.Fatalf("answer %d: %d leases, %d expired", i, len(te.prov().leases), len(te.expired))
		}
		te.events.drain()
	}
}

func leaseBody(version string, ends map[string]time.Time, server time.Time) []byte {
	body := map[string]any{"outbounds": []fakeOutbound{{Tag: "x1", Protocol: "vless", V: 1}, {Tag: "x2", Protocol: "vless", V: 1}},
		"version": version, "serverTime": server.UTC().Format(time.RFC3339)}
	exp := map[string]string{}
	for tag, end := range ends {
		exp[tag] = end.UTC().Format(time.RFC3339)
	}
	body["expiresAt"] = exp
	raw, _ := json.Marshal(body)
	return raw
}

// The most common answer while leases run is the same set with later ends:
// it renews them without touching a handler.
func TestSameVersionRenewsWithoutRebuild(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	now := time.Now()
	a, err := parseAnswer(leaseBody("v1", map[string]time.Time{"x1": now.Add(time.Minute), "x9": now.Add(time.Hour)}, now))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, a); !changed || err != nil {
		t.Fatalf("first apply: %v, %v", changed, err)
	}
	if ev := f.lastApply(t); ev["leases"] != 1.0 || ev["renewed"] != 0.0 || ev["expired"] != 0.0 || ev["clock"] != "server" {
		t.Fatalf("first apply %v; want one lease, the end of a tag outside the set ignored", ev)
	}
	x1 := f.m.GetHandler("p-x1")
	f.created = nil
	a, _ = parseAnswer(leaseBody("v1", map[string]time.Time{"x1": now.Add(2 * time.Hour), "x2": now.Add(2 * time.Hour)}, now))
	if changed, err := f.apply(t, a); changed || err != nil {
		t.Fatalf("same version: %v, %v", changed, err)
	}
	ev := f.lastApply(t)
	if ev["ok"] != true || ev["changed"] != false || ev["leases"] != 2.0 || ev["renewed"] != 1.0 {
		t.Fatalf("same version %v", ev)
	}
	if len(f.created) != 0 || f.m.GetHandler("p-x1") != x1 {
		t.Fatalf("the same set built %d handlers", len(f.created))
	}
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	if l := f.rt.p.leases["p-x1"]; l == nil || l.left(now) < 119*time.Minute {
		t.Fatalf("lease of p-x1 %+v", l)
	}
}

// A refused answer changes no lease, also one whose set did not build.
func TestMalformedTimeRejectsAndKeepsOldLeases(t *testing.T) {
	for body, want := range map[string]string{
		`{"outbounds": [{"tag": "x1"}], "expiresAt": {"x1": "tomorrow"}}`:            `expiresAt of x1: "tomorrow" is not a time`,
		`{"outbounds": [{"tag": "x1"}], "expiresAt": {"x1": "2026-10-09T12:00:00"}}`: "expiresAt of x1",
		`{"outbounds": [{"tag": "x1"}], "expiresAt": {"x1": 1760000000}}`:            "expiresAt of x1: 1760000000",
		`{"outbounds": [{"tag": "x1"}], "expiresAt": ["x1"]}`:                        "expiresAt:",
		`{"outbounds": [{"tag": "x1"}], "serverTime": "noon"}`:                       `serverTime: "noon" is not a time`,
		`{"outbounds": [{"tag": "x1"}], "serverTime": 5}`:                            "serverTime: 5",
	} {
		_, err := parseAnswer([]byte(body))
		if err == nil || !isAnswerError(err) || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want a refusal about %q", body, err, want)
		}
	}
	a, err := parseAnswer([]byte(`{"outbounds": [{"tag": "x1"}], "serverTime": "2026-10-08T12:00:00.25+03:00",
		"expiresAt": {"x1": "2026-10-09T12:00:00Z"}}`))
	if err != nil || a.serverTime.UTC().Format(time.RFC3339Nano) != "2026-10-08T09:00:00.25Z" ||
		!a.expiresAt["x1"].Equal(time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("parsed %+v, %v", a, err)
	}

	f := newApplyFixture(t, time.Minute)
	now := time.Now()
	a, _ = parseAnswer(leaseBody("v1", map[string]time.Time{"x1": now.Add(time.Hour)}, now))
	if _, err := f.apply(t, a); err != nil {
		t.Fatal(err)
	}
	broken := fmt.Sprintf(`{"version": "v2", "serverTime": %q, "expiresAt": {"x1": %q},
		"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}, {"tag": "x3", "protocol": "vless", "bad": true}]}`,
		now.UTC().Format(time.RFC3339), now.Add(-time.Hour).UTC().Format(time.RFC3339))
	a, err = parseAnswer([]byte(broken))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.apply(t, a); err == nil {
		t.Fatal("a set that does not build applied")
	}
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	if l := f.rt.p.leases["p-x1"]; l == nil || l.expired || l.left(now) < 59*time.Minute || len(f.o.e.expired) != 0 {
		t.Fatalf("the refused answer changed the lease: %+v", l)
	}
}

// renewing turns renewal on with the defaults, or as mod sets it.
func renewing(mod func(*ExpiryTrigger)) func(*ProviderConfig) {
	return func(p *ProviderConfig) {
		p.Triggers.Expiry = &ExpiryTrigger{}
		if mod != nil {
			mod(p.Triggers.Expiry)
		}
	}
}

// firstRefresh ticks with traffic every second from from until a refresh
// starts, and returns it and when.
func (te *testEngine) firstRefresh(t *testing.T, from, until time.Time) (*refreshJob, time.Time) {
	t.Helper()
	for now := from; now.Before(until); now = now.Add(time.Second) {
		if jobs, _ := te.refreshAt(now); len(jobs) > 0 {
			return jobs[0], now
		}
	}
	t.Fatalf("no refresh until %v", until.Sub(t0))
	return nil, time.Time{}
}

func TestRenewalAtShareOfShortestLease(t *testing.T) {
	te := leaseEngine(t, renewing(nil), &SelectorConfig{Costs: costs("p-", 1)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Hour, "p-x2": 2 * time.Hour}, "p-x1", "p-x2")
	if in := te.status(at(time.Minute)).Providers[0].RenewInMs; in == nil ||
		*in != (te.prov().leases["p-x1"].renewIn-time.Minute).Milliseconds() {
		t.Fatalf("status renew_in_ms %v, want the renewal of p-x1", in)
	}
	j, when := te.firstRefresh(t, t0, at(time.Hour))
	if d := when.Sub(t0); j.trigger != triggerExpiry || d < 27*time.Minute || d > 33*time.Minute {
		t.Fatalf("%s refresh after %v, want expiry at half of the 1 h lease +-10%%", j.trigger, d)
	}
	if fmt.Sprint(j.asked) != fmt.Sprint([]askedLease{{"p-x1", at(time.Hour)}}) {
		t.Fatalf("the renewal asks about %+v, want the lease of p-x1 alone", j.asked)
	}
	c := te.sameSet(when, map[string]time.Time{"p-x1": when.Add(time.Hour), "p-x2": at(2 * time.Hour)})
	te.onRefreshDone(j, refreshOutcome{ok: true}, when)
	if c.renewed != 1 || te.prov().leases["p-x1"].asked {
		t.Fatalf("counts %+v; the renewed lease must be a new value", c)
	}
}

// A lease is renewed without user traffic: a sleeping device would lose its
// outbounds otherwise. It waits for the network.
func TestRenewalIgnoresIdleWaitsForNetwork(t *testing.T) {
	te := leaseEngine(t, renewing(nil), &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.s.schedule.pauseWhenIdle = 2 * time.Minute
	te.lease(t0, map[string]time.Duration{"p-x1": time.Hour}, "p-x1")
	renew := t0.Add(te.prov().leases["p-x1"].renewIn)
	if jobs, wake := te.tickProviders(at(10 * time.Minute)); len(jobs) != 0 || !wake.Equal(renew) {
		t.Fatalf("idle: refreshes %v, wake %v; want the renewal at %v", triggers(jobs), wake.Sub(t0), renew.Sub(t0))
	}
	te.onNetworkLost(renew.Add(-time.Minute))
	te.eventsOf(t, "")
	if jobs, _ := te.tickProviders(renew); len(jobs) != 0 {
		t.Fatalf("refresh %v without a network", triggers(jobs))
	}
	if skips := te.eventsOf(t, "provider_skip"); len(skips) != 1 || skips[0].Fields["reason"] != "network_down" {
		t.Fatalf("skips %+v", skips)
	}
	te.onNetworkChange("lte", renew.Add(time.Minute))
	if jobs, _ := te.tickProviders(renew.Add(time.Minute)); len(jobs) != 1 || jobs[0].trigger != triggerExpiry {
		t.Fatalf("idle refreshes after the network came back %v", triggers(jobs))
	}
}

// While memory is low a planned renewal waits, and goes once it is urgent.
func TestRenewalWaitsForMemoryUntilUrgent(t *testing.T) {
	te := leaseEngine(t, renewing(nil), &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Hour}, "p-x1")
	te.setMemoryLow(true, at(time.Minute))
	l := te.prov().leases["p-x1"]
	renew, urgent := t0.Add(l.renewIn), t0.Add(l.urgentIn)
	if urgent != at(48*time.Minute) {
		t.Fatalf("urgent at %v, want 0.8 of the lease", urgent.Sub(t0))
	}
	te.eventsOf(t, "")
	if jobs, wake := te.refreshAt(renew); len(jobs) != 0 || !wake.Equal(urgent) {
		t.Fatalf("refreshes %v, wake %v; want to wait for the urgent point", triggers(jobs), wake.Sub(t0))
	}
	if skips := te.eventsOf(t, "provider_skip"); len(skips) != 1 || skips[0].Fields["reason"] != "memory" {
		t.Fatalf("skips %+v", skips)
	}
	expectNoRefresh(t, te, urgent.Add(-time.Second))
	expectRefresh(t, te, urgent, "expiry")
}

// A backend that does not renew a lease gets one request for it, not one
// at the renewal and more until the end.
func TestRenewalOncePerLeaseValue(t *testing.T) {
	te := leaseEngine(t, renewing(nil), &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Hour}, "p-x1")
	starts := 0
	for now := t0; now.Before(at(3 * time.Hour)); now = now.Add(10 * time.Second) {
		jobs, _ := te.refreshAt(now)
		for _, j := range jobs {
			starts++
			te.sameSet(now, map[string]time.Time{"p-x1": at(time.Hour)})
			te.onRefreshDone(j, refreshOutcome{ok: true}, now)
		}
	}
	if ended := te.eventsOf(t, "provider_expired"); starts != 1 || len(ended) != 1 || ended[0].Fields["renewal"] != "refused" {
		t.Fatalf("%d refreshes for one lease value, expired %+v", starts, ended)
	}
}

// An answer for another trigger with the same end leaves the renewal where
// it was; repeated answers do not put it off.
func TestSameEndKeepsTheRenewalPoint(t *testing.T) {
	te := leaseEngine(t, renewing(nil), &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Hour}, "p-x1")
	renew := t0.Add(te.prov().leases["p-x1"].renewIn)
	for now := at(5 * time.Minute); now.Before(renew.Add(-time.Minute)); now = now.Add(5 * time.Minute) {
		te.sameSet(now, map[string]time.Time{"p-x1": at(time.Hour)})
		if _, wake := te.refreshAt(now); !wake.Equal(renew) {
			t.Fatalf("after an answer at %v the renewal is at %v, want %v", now.Sub(t0), wake.Sub(t0), renew.Sub(t0))
		}
	}
}

// An answer may shorten a lease: the shorter end is its value from then on.
// The backend answered about it, so it is not asked about again, and a
// later answer that extends it past that end, though not past the end it
// had before, renews it.
func TestShorterEndIsTheValueALaterAnswerRenews(t *testing.T) {
	te := leaseEngine(t, renewing(func(e *ExpiryTrigger) { e.Jitter = -1 }), &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": 10 * time.Hour}, "p-x1")
	j := expectRefresh(t, te, at(5*time.Hour), "expiry")
	c := te.sameSet(at(5*time.Hour), map[string]time.Time{"p-x1": at(6 * time.Hour)})
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(5*time.Hour))
	l := te.prov().leases["p-x1"]
	if c.renewed != 0 || !l.end.Equal(at(6*time.Hour)) || !l.asked || l.left(at(5*time.Hour)) != time.Hour {
		t.Fatalf("shortened: counts %+v, end %v, asked %v, left %v", c, l.end.Sub(t0), l.asked, l.left(at(5*time.Hour)))
	}
	for now := at(5*time.Hour + time.Minute); now.Before(at(5*time.Hour + 30*time.Minute)); now = now.Add(time.Minute) {
		expectNoRefresh(t, te, now)
	}
	c = te.sameSet(at(5*time.Hour+30*time.Minute), map[string]time.Time{"p-x1": at(8 * time.Hour)})
	if l = te.prov().leases["p-x1"]; c.renewed != 1 || !l.end.Equal(at(8*time.Hour)) || l.asked {
		t.Fatalf("extended past the shorter end: counts %+v, end %v, asked %v", c, l.end.Sub(t0), l.asked)
	}
	j, when := te.firstRefresh(t, at(5*time.Hour+31*time.Minute), at(8*time.Hour))
	if when != at(6*time.Hour+45*time.Minute) || j.trigger != triggerExpiry ||
		fmt.Sprint(j.asked) != fmt.Sprint([]askedLease{{"p-x1", at(8 * time.Hour)}}) {
		t.Fatalf("%s refresh at %v asks about %+v; want the renewal of the 8 h value at half of it", j.trigger, when.Sub(t0), j.asked)
	}
}

// Any answer renews what it names, and a refresh of any trigger asks about
// the leases due: no separate renewal follows.
func TestAnyRefreshRenews(t *testing.T) {
	te := leaseEngine(t, renewing(nil), &SelectorConfig{Costs: costs("p-", 1)}, "p-x1", "p-x2")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Hour, "p-x2": time.Hour}, "p-x1", "p-x2")
	// An answer before the renewal point with a later end is a new value.
	te.requestRefresh("fresh")
	j := expectRefresh(t, te, at(10*time.Minute), "host")
	if len(j.asked) != 0 {
		t.Fatalf("a refresh before the renewal asks about %+v", j.asked)
	}
	te.sameSet(at(10*time.Minute), map[string]time.Time{"p-x1": at(2 * time.Hour), "p-x2": at(time.Hour)})
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(10*time.Minute))
	if in := te.prov().leases["p-x1"].renewIn; in < 49*time.Minute || in > 61*time.Minute {
		t.Fatalf("renewal of the new value in %v from the answer, want half of 110 min +-10%%", in)
	}
	// A host refresh while the planned renewal of p-x2 waits for memory.
	te.setMemoryLow(true, at(20*time.Minute))
	expectNoRefresh(t, te, at(40*time.Minute))
	te.requestRefresh("fresh")
	j = expectRefresh(t, te, at(41*time.Minute), "host")
	if fmt.Sprint(j.asked) != fmt.Sprint([]askedLease{{"p-x2", at(time.Hour)}}) {
		t.Fatalf("the host refresh asks about %+v", j.asked)
	}
	te.sameSet(at(41*time.Minute), map[string]time.Time{"p-x1": at(2 * time.Hour), "p-x2": at(3 * time.Hour)})
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(41*time.Minute))
	expectNoRefresh(t, te, at(55*time.Minute))
}

// A planned renewal waits for minInterval after another refresh; once it is
// urgent only exhaustedMinInterval holds it.
func TestUrgentRenewalWaitsOnlyForTheShorterInterval(t *testing.T) {
	te := leaseEngine(t, func(p *ProviderConfig) {
		renewing(nil)(p)
		p.Limits.MinInterval = int64(30 * time.Minute)
	}, &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Hour}, "p-x1")
	te.requestRefresh("fresh")
	j := expectRefresh(t, te, at(26*time.Minute), "host")
	te.onRefreshDone(j, refreshOutcome{ok: true}, at(26*time.Minute))
	if _, wake := te.refreshAt(at(34 * time.Minute)); wake != at(48*time.Minute) {
		t.Fatalf("a planned renewal held by minInterval wakes at %v, want the urgent point", wake.Sub(t0))
	}
	expectNoRefresh(t, te, at(48*time.Minute-time.Second))
	expectRefresh(t, te, at(48*time.Minute), "expiry")
}

func TestRenewAtZeroTurnsRenewalOffButNotExpiry(t *testing.T) {
	te := leaseEngine(t, renewing(func(e *ExpiryTrigger) { e.RenewAt = -1 }), &SelectorConfig{Costs: costs("p-", 1)}, "p-x1")
	te.lease(t0, map[string]time.Duration{"p-x1": time.Hour}, "p-x1")
	for now := t0; now.Before(at(time.Hour)); now = now.Add(time.Minute) {
		expectNoRefresh(t, te, now)
	}
	expectRefresh(t, te, at(time.Hour), "expiry")
	if ev := te.lastOf(t, "provider_expired"); ev.Fields["renewal"] != "off" {
		t.Fatalf("expired %v", ev.Fields)
	}
}

func TestExpiryTriggerDefaultsAndCeilings(t *testing.T) {
	for _, tc := range []struct {
		in                    *ExpiryTrigger
		renew, urgent, spread float64
	}{
		{nil, 0.5, 0.8, 0.1},
		{&ExpiryTrigger{RenewAt: 0.3, UrgentAt: 0.6, Jitter: 0.2}, 0.3, 0.6, 0.2},
		{&ExpiryTrigger{RenewAt: -1}, 0, 0, 0},
		{&ExpiryTrigger{Jitter: -1}, 0.5, 0.8, 0},
		// urgentAt is never before renewAt, neither goes past 0.95.
		{&ExpiryTrigger{RenewAt: 0.9}, 0.9, 0.9, 0.1},
		{&ExpiryTrigger{RenewAt: 2, UrgentAt: 3, Jitter: 2}, maxRenewAt, maxRenewAt, maxRenewJitter},
	} {
		p := newProviderSettings(quietProvider(func(p *ProviderConfig) { p.Triggers.Expiry = tc.in }))
		if p.renewAt != tc.renew || p.urgentAt != tc.urgent || p.renewJitter != tc.spread {
			t.Errorf("%+v: renewAt %v, urgentAt %v, jitter %v", tc.in, p.renewAt, p.urgentAt, p.renewJitter)
		}
	}
}

// 1000 clients that got the same leases at once must not renew them at
// once: no minute may get more than 5% of the renewals.
func TestRenewalsOfManyClientsSpreadOut(t *testing.T) {
	const clients = 1000
	pc := quietProvider(renewing(nil))
	byMinute := map[int]int{}
	for c := 0; c < clients; c++ {
		e := newEngine(newSettings(&Config{Providers: []*ProviderConfig{pc},
			Schedule: &ScheduleConfig{PauseWhenIdle: int64(24 * time.Hour)}}), int64(c+1))
		p := e.providers[0]
		e.tickProviders(t0)
		e.acceptLeases(p, []string{"p-x1"}, leaseAnswer{ends: map[string]time.Time{"p-x1": at(24 * time.Hour)}, serverTime: t0}, t0)
		_, wake := e.tickProviders(t0)
		jobs, _ := e.tickProviders(wake)
		if len(jobs) != 1 || jobs[0].trigger != triggerExpiry {
			t.Fatalf("client %d: refreshes %v at its wake %v", c, triggers(jobs), wake.Sub(t0))
		}
		if d := wake.Sub(t0); d < 648*time.Minute || d > 792*time.Minute {
			t.Fatalf("client %d renews after %v, want 12 h +-10%%", c, d)
		}
		byMinute[int(wake.Sub(t0)/time.Minute)]++
	}
	peak, minute := 0, 0
	for m, n := range byMinute {
		if n > peak {
			peak, minute = n, m
		}
	}
	if peak > clients*5/100 {
		t.Fatalf("minute %d got %d renewals of %d clients", minute, peak, clients)
	}
	t.Logf("busiest minute %d with %d renewals (%.1f%%)", minute, peak, float64(peak)*100/clients)
}
