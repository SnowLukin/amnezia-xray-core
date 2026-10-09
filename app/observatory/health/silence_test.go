package health

import (
	"context"
	"encoding/json"
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

func newSilenceFixture(t *testing.T, passive *PassiveConfig, sc *SelectorConfig) *silenceFixture {
	t.Helper()
	if sc == nil {
		sc = &SelectorConfig{Costs: costs("a", 1, "b", 3), SwitchWithoutProbe: true}
	}
	cfg := &Config{SubjectSelector: []string{"a", "b"}, Passive: passive,
		Schedule: &ScheduleConfig{PauseWhenIdle: int64(time.Hour)}}
	f := &silenceFixture{t: t, o: newObserver(context.Background(), cfg), clk: &fakeClock{t: t0}, ended: map[string]bool{}}
	t.Cleanup(func() { f.o.Close() })
	f.o.clock = f.clk.now
	f.o.base = t0.Add(-time.Second)
	// The tests sweep by hand.
	f.o.sweeping = true
	f.o.mu.Lock()
	s := f.o.e.addSelector("auto", newSelectorSettings(sc))
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
	f := newSilenceFixture(t, nil, nil)
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
	if evs := f.events("pending_closed"); len(evs) != 1 || evs[0].Fields["silent"] != nil {
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
	if len(evs) != 1 || evs[0].Fields["silent"] != true || evs[0].Fields["connections"] != float64(2) ||
		evs[0].Fields["outbound"] != "a1" || evs[0].Fields["reason"] != "traffic" {
		t.Fatalf("at the check: %+v, want one pending_closed of a1, silent, for two connections", evs)
	}
	var report struct {
		PendingClosed int64 `json:"pending_closed"`
	}
	if err := json.Unmarshal(f.o.Report(), &report); err != nil || report.PendingClosed != 3 {
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

// Nothing is ended for silence when it proves nothing about the path or
// nobody asked for it: the outbound came back, also when the probe that was
// confirming a suspicion of it when it went down passed, or a balancer chose
// it again before the check, the evidence was about destinations (also when
// a probe confirmed it), the decision was provisional, the network failed
// meanwhile, or keepSilentOnDown.
func TestSilenceKeepsConnectionsWhenItProvesNothing(t *testing.T) {
	confirmAlways := &SelectorConfig{Costs: costs("a", 1, "b", 3), SwitchWithoutProbe: true, Confirm: ConfirmMode_ALWAYS}
	confirmNever := &SelectorConfig{Costs: costs("a", 1, "b", 3), SwitchWithoutProbe: true, Confirm: ConfirmMode_NEVER}
	strong := func(f *silenceFixture) { f.failStrong("a1") }
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
	// The probe confirming weak evidence when strong evidence came.
	var confirming *probeJob
	for _, tc := range []struct {
		name    string
		passive *PassiveConfig
		sc      *SelectorConfig
		decide  func(*silenceFixture)
		between func(*silenceFixture)
		end     bool
	}{
		{name: "strong", decide: strong, end: true},
		{name: "strong confirmed by a probe", sc: confirmAlways, decide: confirmed(strong), end: true},
		{name: "back up", decide: strong, between: func(f *silenceFixture) {
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
		{name: "weak", sc: confirmNever, decide: func(f *silenceFixture) { f.failWeak("a1") }},
		{name: "weak confirmed by a probe", decide: confirmed(func(f *silenceFixture) { f.failWeak("a1") })},
		{name: "provisional", decide: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.doubt = true })
			f.failStrong("a1")
		}},
		{name: "network lost meanwhile", decide: strong, between: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.onNetworkLost(f.clk.now()) })
		}},
		{name: "network changed meanwhile", decide: strong, between: func(f *silenceFixture) {
			f.engine(func(e *engine) { e.onNetworkChange("lte", f.clk.now()) })
		}},
		{name: "keepSilentOnDown", passive: &PassiveConfig{KeepSilentOnDown: true}, decide: strong},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSilenceFixture(t, tc.passive, tc.sc)
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
			f.check(map[string]bool{"quiet": tc.end})
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
	f := newSilenceFixture(t, nil, &SelectorConfig{Costs: costs("a", 1, "b", 3)})
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
	if !wake.IsZero() && wake.Before(decided.Add(time.Minute)) {
		t.Fatalf("the held check left a wake %v after the decision", wake.Sub(decided))
	}
	f.clk.advance(4 * time.Second)
	f.answer(jobs[0], true)
	f.turn()
	f.check(map[string]bool{"quiet": true})
	if evs := f.events("pending_closed"); len(evs) != 1 || evs[0].Fields["silent"] != true {
		t.Fatalf("events %+v, want the silent close at the switch", evs)
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
	f := newSilenceFixture(t, nil, nil)
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
