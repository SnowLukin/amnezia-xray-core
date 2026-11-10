package health

import (
	"context"
	"encoding/json"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

// silenceFixture is an observer with the balancer auto over a1 and b1, a1
// the cheaper, whose loop the test turns by hand on a clock of its own.
// Each connection it opens tells by its name when the observer ends it.
type silenceFixture struct {
	t     *testing.T
	o     *Observer
	clk   *fakeClock
	ended map[string]bool
	conns uint32
}

func newSilenceFixture(t *testing.T, sc *SelectorConfig) *silenceFixture {
	t.Helper()
	if sc == nil {
		sc = &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3), Features: probeFirst(false)}
	}
	declared := fixtureSelector(sc, []string{"a1", "b1"})
	cfg := &Config{Paths: declared.Paths,
		Schedule: tune(&ScheduleConfig{}, scheduleKnobs{idleAfter: time.Hour})}
	f := &silenceFixture{t: t, o: newObserver(context.Background(), legacyScenarioConfig(cfg)), clk: &fakeClock{t: t0}, ended: map[string]bool{}}
	t.Cleanup(func() { f.o.Close() })
	f.o.s = legacyScenarioSettings(cfg)
	f.o.e.s = f.o.s
	f.o.clock = f.clk.now
	f.o.base = t0.Add(-time.Second)
	// The tests sweep by hand.
	f.o.sweeping = true
	f.o.mu.Lock()
	s := f.o.e.addSelector("auto", legacyScenarioSelector(declared, cfg))
	f.o.e.noteDemand(t0)
	f.o.e.updateCandidates(s, []string{"a1", "b1"}, t0)
	f.o.mu.Unlock()
	return f
}

// open starts a connection through tag that has received n bytes, and
// returns its tracker and the counter of what it reads.
func (f *silenceFixture) open(tag, name string, n int64) (*connTracker, *trackerReadCounter) {
	tr := f.o.StartDial(context.Background(), tag, net.Destination{}, func() { f.ended[name] = true }).(*connTracker)
	r, _ := tr.Dialed(nil, nil, nil, nil)
	r.Add(n)
	return tr, r.(*trackerReadCounter)
}

func (f *silenceFixture) engine(do func(e *engine)) {
	f.o.mu.Lock()
	do(f.o.e)
	f.o.mu.Unlock()
}

// failStrong fails the dials of three connections through tag now: evidence
// about the path, which rules it out without a probe.
func (f *silenceFixture) failStrong(tag string) {
	f.engine(func(e *engine) {
		for range 3 {
			f.conns++
			e.onFailure(tag, failDial, "", f.conns, f.clk.now())
		}
	})
}

// failWeak has three connections through tag get no answer from two
// destinations: evidence that may be about the destinations.
func (f *silenceFixture) failWeak(tag string) {
	f.engine(func(e *engine) {
		for _, target := range []string{"x.test", "y.test", "x.test"} {
			f.conns++
			e.onFailure(tag, failNoResponse, target, f.conns, f.clk.now())
		}
	})
}

// probeFails runs a probe of tag for reason that fails at the time of the
// clock, as the observer does for any reason but a confirmation, which
// needs a suspicion first.
func (f *silenceFixture) probeFails(tag string, reason probeReason) {
	var j *probeJob
	f.engine(func(e *engine) {
		e.nextID++
		j = &probeJob{id: e.nextID, tag: tag, reason: reason, selector: "auto", generation: e.generation}
		e.begin(j, f.clk.now())
	})
	f.answer(j, false)
}

// leave moves the balancer off a1 to b1 while a1 still works, as a pin or
// a return from a dearer outbound left behind would.
func (f *silenceFixture) leave() {
	f.engine(func(e *engine) { e.switchTo(e.selectors["auto"], "b1", "pin", f.clk.now()) })
}

// turn runs one turn of the loop at the time of the clock: what is due, and
// what it asks of the connections. It returns the probes that started and
// the wake.
func (f *silenceFixture) turn() ([]*probeJob, time.Time) {
	f.o.mu.Lock()
	jobs, _, wake := f.o.e.step(f.clk.now())
	downs := f.o.e.takeDowns()
	f.o.mu.Unlock()
	for _, d := range downs {
		f.o.closePending(d)
	}
	return jobs, wake
}

// answer applies the result of a probe at the time of the clock.
func (f *silenceFixture) answer(j *probeJob, ok bool) {
	f.engine(func(e *engine) { e.onProbeResult(j, probeResult{ok: ok, err: "x"}, f.clk.now()) })
}

func (f *silenceFixture) status(tag string) pathStatus {
	var s pathStatus
	f.engine(func(e *engine) { s = e.path(tag).status })
	return s
}

func (f *silenceFixture) events(typ string) []Event {
	f.t.Helper()
	raw, _ := f.o.DrainEvents()
	var evs []Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		f.t.Fatal(err)
	}
	var out []Event
	for _, e := range evs {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

func (f *silenceFixture) check(want map[string]bool) {
	f.t.Helper()
	for name := range want {
		if f.ended[name] != want[name] {
			f.t.Errorf("%s: ended %v, want %v", name, f.ended[name], want[name])
		}
	}
	for name := range f.ended {
		if _, ok := want[name]; !ok {
			f.t.Errorf("%s: ended, unexpected", name)
		}
	}
}

// An outbound found down on evidence about the path: stallTimeout after the
// decision, its connections from before that received nothing since are
// ended, TCP and UDP alike, as one pending_closed with silent. Those that
// received anything since are kept, also once their count is past the bytes
// whose time the tracker takes, and so are those of other outbounds and
// those that started after the decision. One timer, then none.
func TestSilentConnectionsEndStallTimeoutAfterADown(t *testing.T) {
	f := newSilenceFixture(t, nil)
	f.open("a1", "quiet", 100)
	udp, _ := f.open("a1", "quiet udp", 0)
	udp.udp = true
	udp.readCounter.Add(60)
	_, talking := f.open("a1", "talking", 100)
	_, big := f.open("a1", "talking past successBytes", 30_000)
	f.open("a1", "pending", 0)
	spliced, _ := f.open("a1", "spliced, kernel unknown", 100)
	spliced.spliced.Store(true)
	f.open("b1", "other outbound", 100)

	f.clk.advance(time.Second)
	decided := f.clk.now()
	f.failStrong("a1")
	_, wake := f.turn()
	if want := decided.Add(defaultStallTimeout); !wake.Equal(want) {
		t.Fatalf("wake %v after the decision, want stallTimeout %v", wake.Sub(decided), defaultStallTimeout)
	}
	f.check(map[string]bool{"pending": true})
	if evs := f.events("pending_closed"); len(evs) != 1 || evs[0].Fields["is_silent"] != nil {
		t.Fatalf("at the decision: %+v, want one pending_closed without silent", evs)
	}

	f.clk.advance(time.Second)
	f.open("a1", "after the decision", 100)
	talking.Add(1)
	big.Add(1)
	f.clk.t = decided.Add(defaultStallTimeout - time.Millisecond)
	f.turn()
	f.check(map[string]bool{"pending": true})

	f.clk.t = decided.Add(defaultStallTimeout)
	_, wake = f.turn()
	f.check(map[string]bool{"pending": true, "quiet": true, "quiet udp": true})
	evs := f.events("pending_closed")
	if len(evs) != 1 || evs[0].Fields["is_silent"] != true || evs[0].Fields["connections"] != float64(2) ||
		evs[0].Fields["outbound"] != "a1" || evs[0].Fields["reason"] != "traffic" {
		t.Fatalf("at the check: %+v, want one pending_closed of a1, silent, for two connections", evs)
	}
	var report struct {
		PendingClosed int64 `json:"pending_closed_count"`
	}
	if err := json.Unmarshal(f.o.PeekReport(), &report); err != nil || report.PendingClosed != 3 {
		t.Fatalf("report pending_closed %d (%v), want 3: both kinds count there", report.PendingClosed, err)
	}
	f.engine(func(e *engine) {
		if len(e.silences) != 0 || len(e.downs) != 0 {
			t.Fatalf("after the check: silences %+v, downs %+v", e.silences, e.downs)
		}
	})
	if !wake.IsZero() && wake.Before(decided.Add(time.Minute)) {
		t.Fatalf("a wake %v after the decision is left", wake.Sub(decided))
	}
	// Nothing ends twice.
	f.clk.advance(time.Minute)
	f.turn()
	if evs := f.events("pending_closed"); len(evs) != 0 {
		t.Fatalf("later: %+v", evs)
	}
}

// Whatever rules an outbound out, its connections from before the decision
// that received nothing since are ended stallTimeout after it, as one
// pending_closed with silent and the reason of the down: failures about the
// path, failures that may be about destinations with or without a probe
// that confirms them, and a failed probe alone, for every reason the
// observer probes, of an outbound a balancer chooses or of one none does.
// A connection that received anything since the decision is kept.
func TestSilentConnectionsEndAfterEveryDown(t *testing.T) {
	confirmAlways := &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3), Features: probeFirst(false), ConfirmWithProbe: ConfirmMode_ALWAYS}
	confirmNever := &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3), Features: probeFirst(false), ConfirmWithProbe: ConfirmMode_NEVER}
	// confirmed fails the probe that confirms the suspicion.
	confirmed := func(fail func(*silenceFixture)) func(*silenceFixture) {
		return func(f *silenceFixture) {
			fail(f)
			jobs, _ := f.turn()
			if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
				f.t.Fatalf("probes %+v, want one that confirms", jobs)
			}
			f.answer(jobs[0], false)
		}
	}
	probe := func(r probeReason) func(*silenceFixture) {
		return func(f *silenceFixture) { f.probeFails("a1", r) }
	}
	for _, tc := range []struct {
		name   string
		sc     *SelectorConfig
		left   bool
		decide func(*silenceFixture)
		reason string
	}{
		{name: "path failures", decide: func(f *silenceFixture) { f.failStrong("a1") }, reason: "traffic"},
		{name: "path failures confirmed by a probe", sc: confirmAlways,
			decide: confirmed(func(f *silenceFixture) { f.failStrong("a1") }), reason: "probe"},
		{name: "destination failures without a probe", sc: confirmNever,
			decide: func(f *silenceFixture) { f.failWeak("a1") }, reason: "traffic"},
		{name: "destination failures confirmed by a probe",
			decide: confirmed(func(f *silenceFixture) { f.failWeak("a1") }), reason: "probe"},
		{name: "network change probe of the chosen outbound", decide: probe(reasonNetwork), reason: "probe"},
		{name: "host probe of the chosen outbound", decide: probe(reasonHost), reason: "host_probe"},
		{name: "recheck", left: true, decide: probe(reasonRecheck), reason: "probe"},
		{name: "return to cheaper", left: true, decide: probe(reasonReturnToCheaper), reason: "return_probe"},
		{name: "failover candidate", left: true, decide: probe(reasonFailover), reason: "probe"},
		{name: "network change probe", left: true, decide: probe(reasonNetwork), reason: "probe"},
		{name: "remembered", left: true, decide: probe(reasonRemembered), reason: "probe"},
		{name: "group recheck", left: true, decide: probe(reasonGroup), reason: "probe"},
		{name: "host probe", left: true, decide: probe(reasonHost), reason: "host_probe"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSilenceFixture(t, tc.sc)
			if tc.left {
				f.leave()
			}
			f.open("a1", "quiet", 100)
			_, talking := f.open("a1", "talking", 100)
			f.clk.advance(time.Second)
			decided := f.clk.now()
			tc.decide(f)
			if s := f.status("a1"); s != statusDown {
				t.Fatalf("setup: a1 is %v, want down", s)
			}
			_, wake := f.turn()
			if due := decided.Add(defaultStallTimeout); wake.IsZero() || wake.After(due) {
				t.Fatalf("wake %v after the decision, want the check at stallTimeout %v", wake.Sub(decided), defaultStallTimeout)
			}
			f.check(map[string]bool{})
			f.events("pending_closed")

			f.clk.advance(time.Second)
			talking.Add(1)
			f.clk.t = decided.Add(defaultStallTimeout - time.Millisecond)
			f.turn()
			f.check(map[string]bool{})

			f.clk.t = decided.Add(defaultStallTimeout)
			f.turn()
			f.check(map[string]bool{"quiet": true, "talking": false})
			evs := f.events("pending_closed")
			if len(evs) != 1 || evs[0].Fields["is_silent"] != true || evs[0].Fields["connections"] != float64(1) ||
				evs[0].Fields["outbound"] != "a1" || evs[0].Fields["reason"] != tc.reason {
				t.Fatalf("at the check: %+v, want one pending_closed of a1, silent, reason %s, for one connection",
					evs, tc.reason)
			}
			f.engine(func(e *engine) {
				if len(e.silences) != 0 {
					t.Fatalf("checks left: %+v", e.silences)
				}
			})
		})
	}
}

// Nothing is ended for silence when it proves nothing about the path or
// nobody asked for it: the outbound came back, also when the probe that was
// confirming a suspicion of it when it went down passed, or a balancer chose
// it again before the check, the decision was provisional, the network
// failed, changed or is in doubt at the check, or shouldKeepSilentOnDown.
// A failed probe is held to the same rules as failures about the path.
func TestSilenceKeepsConnectionsWhenItProvesNothing(t *testing.T) {
	strong := func(f *silenceFixture) { f.failStrong("a1") }
	probe := func(f *silenceFixture) { f.probeFails("a1", reasonHost) }
	// The doubt ends before the check, the outbound still down.
	outOfDoubt := func(f *silenceFixture) { f.engine(func(e *engine) { e.doubt = false }) }
	// The probe confirming weak evidence when strong evidence came.
	var confirming *probeJob
	for _, tc := range []struct {
		name       string
		keepSilent bool
		left       bool
		decide     func(*silenceFixture)
		between    func(*silenceFixture)
	}{
		{name: "back up", decide: strong, between: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.markUp(e.path("a1"), "probe", f.clk.now()) })
		}},
		{name: "back up after a probe", left: true, decide: probe, between: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.markUp(e.path("a1"), "probe", f.clk.now()) })
		}},
		{name: "rescued", decide: func(f *silenceFixture) {
			f.failWeak("a1")
			jobs, _ := f.turn()
			if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
				f.t.Fatalf("probes %+v, want one that confirms", jobs)
			}
			confirming = jobs[0]
			f.failStrong("a1")
		}, between: func(f *silenceFixture) { f.answer(confirming, true) }},
		{name: "chosen again", decide: strong, between: func(f *silenceFixture) {
			f.engine(func(e *engine) {
				if !e.pin("auto", "a1", f.clk.now()) {
					f.t.Fatal("setup: pin refused")
				}
			})
		}},
		{name: "chosen again after a probe", left: true, decide: probe, between: func(f *silenceFixture) {
			f.engine(func(e *engine) {
				if !e.pin("auto", "a1", f.clk.now()) {
					f.t.Fatal("setup: pin refused")
				}
			})
		}},
		{name: "provisional", decide: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.doubt = true })
			f.failStrong("a1")
		}, between: outOfDoubt},
		{name: "provisional probe", left: true, decide: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.doubt = true })
			probe(f)
		}, between: outOfDoubt},
		{name: "network in doubt at the check", left: true, decide: probe, between: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.doubt = true })
		}},
		{name: "network lost meanwhile", decide: strong, between: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.onNetworkLost(f.clk.now()) })
		}},
		{name: "network lost after a probe", left: true, decide: probe, between: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.onNetworkLost(f.clk.now()) })
		}},
		{name: "network changed meanwhile", decide: strong, between: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.onNetworkChange("lte", f.clk.now()) })
		}},
		{name: "network changed after a probe", left: true, decide: probe, between: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.onNetworkChange("lte", f.clk.now()) })
		}},
		{name: "closeSilent off", keepSilent: true, decide: strong},
		{name: "closeSilent off after a probe", keepSilent: true,
			left: true, decide: probe},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var sc *SelectorConfig
			if tc.keepSilent {
				sc = &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3),
					Features: &SelectorFeatureConfig{ProbeBeforeSwitch: proto.Bool(false), CloseSilent: proto.Bool(false)}}
			}
			f := newSilenceFixture(t, sc)
			if tc.left {
				f.leave()
			}
			f.open("a1", "quiet", 100)
			f.clk.advance(time.Second)
			decided := f.clk.now()
			tc.decide(f)
			if s := f.status("a1"); s != statusDown {
				t.Fatalf("setup: a1 is %v, want down", s)
			}
			f.turn()
			if tc.between != nil {
				f.clk.advance(3 * time.Second)
				tc.between(f)
				f.turn()
			}
			f.clk.t = decided.Add(defaultStallTimeout + time.Second)
			f.turn()
			f.check(map[string]bool{"quiet": false})
			f.engine(func(e *engine) {
				if len(e.silences) != 0 {
					t.Fatalf("checks left: %+v", e.silences)
				}
			})
		})
	}
}

// While a failover away from the dead outbound still probes its candidates,
// new connections still go to it, and those ended now would come back to
// it: the check waits for the balancer to move off, without a timer, and
// runs then.
func TestSilenceWaitsForTheFailover(t *testing.T) {
	f := newSilenceFixture(t, &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3)})
	f.open("a1", "quiet", 100)
	f.clk.advance(time.Second)
	decided := f.clk.now()
	f.failStrong("a1")
	jobs, _ := f.turn()
	if len(jobs) != 1 || jobs[0].tag != "b1" || jobs[0].reason != reasonFailover {
		t.Fatalf("probes %+v, want the failover probe of b1", jobs)
	}
	f.clk.t = decided.Add(defaultStallTimeout)
	_, wake := f.turn()
	f.check(map[string]bool{})
	if !wake.Equal(decided.Add(auditWindow + time.Nanosecond)) {
		t.Fatalf("wake %v after the decision, want the audit deadline", wake.Sub(decided))
	}
	f.engine(func(e *engine) {
		if len(e.silences) != 1 || !e.silences[0].held || len(e.queue) != 1 || e.queue[0].reason != reasonAudit || e.queue[0].tag != "a1" {
			t.Fatalf("held checks %+v, queued %+v, want held silence and audit:a1", e.silences, e.queue)
		}
	})
	f.clk.advance(4 * time.Second)
	f.answer(jobs[0], true)
	f.turn()
	f.check(map[string]bool{"quiet": true})
	if evs := f.events("pending_closed"); len(evs) != 1 || evs[0].Fields["is_silent"] != true {
		t.Fatalf("events %+v, want the silent close at the switch", evs)
	}
	held := newSilenceFixture(t, &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3)})
	held.open("a1", "quiet", 100)
	held.clk.advance(time.Second)
	held.failStrong("a1")
	if jobs, _ := held.turn(); len(jobs) != 1 || jobs[0].reason != reasonFailover || jobs[0].tag != "b1" {
		t.Fatalf("probes %+v, want the held failover of b1", jobs)
	}
	for _, now := range []time.Time{decided.Add(auditWindow + time.Nanosecond), decided.Add(time.Minute)} {
		held.clk.t = now
		if jobs, wake := held.turn(); len(jobs) != 0 || !wake.IsZero() {
			t.Fatalf("after audit expiry probes %+v, wake %v, want neither", jobs, wake)
		}
		held.check(map[string]bool{})
		held.engine(func(e *engine) {
			if len(e.queue) != 0 || len(e.silences) != 1 || !e.silences[0].held || e.report.counts.AuditSkipped != 1 {
				t.Fatalf("queued %+v, silence %+v, skipped audits %d, want one expired audit and held silence", e.queue, e.silences, e.report.counts.AuditSkipped)
			}
		})
	}
}

// A spliced connection moves its bytes past the counters, and its socket
// tells what it received: one whose kernel received nothing since the
// decision is ended, one that received is kept.
func TestSilenceOfSplicedConnectionsByTheKernel(t *testing.T) {
	quiet, _ := loopbackPair(t, 10*1024)
	if _, ok := kernelReceived(rawConnOf(quiet)); !ok {
		t.Skip("the kernel does not report received bytes here")
	}
	talking, srv := loopbackPair(t, 10*1024)
	f := newSilenceFixture(t, nil)
	splice := func(c net.Conn, name string) {
		tr := f.o.StartDial(context.Background(), "a1", net.Destination{}, func() { f.ended[name] = true }).(*connTracker)
		r, w := tr.Dialed(c, nil, nil, nil)
		w.Add(400)
		r.Add(2048)
		r.(*trackerReadCounter).Spliced()
	}
	splice(quiet, "quiet")
	splice(talking, "talking")
	f.clk.advance(time.Second)
	decided := f.clk.now()
	f.failStrong("a1")
	f.turn()
	if _, err := srv.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	f.clk.t = decided.Add(defaultStallTimeout)
	f.turn()
	f.check(map[string]bool{"quiet": true, "talking": false})
}
