package health

import (
	"context"
	"encoding/json"
	gonet "net"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

// idleFixture is a balancer that chose the WireGuard-like w1 from t0 until
// the test leaves it for the cheaper a1. Connections the test opens through
// w1 come as those of a WireGuard handler do: a tracker of the observer and
// a connection its proxy processes. Each tells by its name when the
// observer ends it. The loop turns by hand on a clock of the test, and
// nothing sweeps.
type idleFixture struct {
	*sleepFixture
	t     *testing.T
	conns map[string]*idleConn
}

type idleConn struct {
	tr    *connTracker
	r     *trackerReadCounter
	ended bool
	gone  bool
}

func newIdleFixture(t *testing.T, after, closeIdle time.Duration) *idleFixture {
	t.Helper()
	cfg := &Config{Sleep: &SleepConfig{AfterLastUse: int64(after), CloseIdleAfter: int64(closeIdle)}}
	sf := newSleepFixture(t, cfg, &SelectorConfig{Costs: costs("a", 1, "w", 3), SkipProbeBeforeSwitch: true}, "a1", "w1")
	sf.o.sweeping = true
	f := &idleFixture{sleepFixture: sf, t: t, conns: map[string]*idleConn{}}
	f.engine(func(e *engine) { e.switchTo(e.selectors["auto"], "w1", "failover", t0) })
	f.turn(t0)
	return f
}

// open starts a connection through w1 at the time of the clock that
// receives n bytes.
func (f *idleFixture) open(name string, n int64) *idleConn {
	c := &idleConn{}
	c.tr = f.o.StartDial(context.Background(), "w1", net.Destination{}, func() { c.ended = true }).(*connTracker)
	r, _ := c.tr.Dialed(nil, nil, nil, nil)
	c.r = r.(*trackerReadCounter)
	c.r.Add(n)
	f.o.ProcessStarted("w1")
	f.conns[name] = c
	return c
}

// splice starts a connection through w1 over conn that received some bytes
// and was then spliced: the kernel tells what it receives.
func (f *idleFixture) splice(name string, conn gonet.Conn) *idleConn {
	c := &idleConn{}
	c.tr = f.o.StartDial(context.Background(), "w1", net.Destination{}, func() { c.ended = true }).(*connTracker)
	r, w := c.tr.Dialed(conn, nil, nil, nil)
	w.Add(400)
	r.Add(2048)
	r.(*trackerReadCounter).Spliced()
	c.r = r.(*trackerReadCounter)
	f.o.ProcessStarted("w1")
	f.conns[name] = c
	return c
}

// leave moves the balancer off w1 at now.
func (f *idleFixture) leave(now time.Time) {
	f.now = now
	f.engine(func(e *engine) { e.switchTo(e.selectors["auto"], "a1", "return", now) })
}

// finish ends at now, as the handler reports it, every connection the
// observer ended that has not ended yet.
func (f *idleFixture) finish(now time.Time) {
	f.now = now
	for _, c := range f.conns {
		if c.ended && !c.gone {
			c.gone = true
			c.tr.Finished(context.Canceled)
			f.o.ProcessEnded("w1")
		}
	}
}

// check tells whether exactly the connections named were ended.
func (f *idleFixture) check(ended ...string) {
	f.t.Helper()
	want := map[string]bool{}
	for _, name := range ended {
		want[name] = true
	}
	for name, c := range f.conns {
		if c.ended != want[name] {
			f.t.Errorf("%s: ended %v, want %v", name, c.ended, want[name])
		}
	}
}

func (f *idleFixture) events(typ string) []Event {
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

// Once no balancer has chosen the outbound for sleep.afterLastUse, its connections
// that received nothing for closeIdleAfter are ended, TCP and UDP alike,
// also one that never received anything and one past the bytes whose time
// the tracker takes: as one idle_closed, counted in the report. Those that
// received within closeIdleAfter are kept and ended once they fall silent
// that long, each at the one wake planned for it. The outbound sleeps with
// reason idle the shortest sleep.afterLastUse after the last is gone, not
// sleep.afterLastUse: the ends restart no such wait. Then no wake is left.
func TestIdleConnectionsEndOnceLeftAndSilent(t *testing.T) {
	const after, n = 5 * time.Minute, 2 * time.Minute
	f := newIdleFixture(t, after, n)
	f.now = at(time.Second)
	f.open("quiet", 100)
	udp := f.open("quiet udp", 0)
	udp.tr.udp = true
	udp.r.Add(60)
	talking := f.open("talking", 100)
	f.open("big quiet", 30_000)
	bigTalking := f.open("big talking", 30_000)
	f.open("pending", 0)

	left := at(10 * time.Second)
	f.leave(left)
	closeAt := left.Add(after)
	if wake := f.turn(left); !wake.Equal(closeAt) {
		t.Fatalf("wake %v after the switch off w1, want sleep.afterLastUse %v", wake.Sub(left), after)
	}
	f.now = left.Add(4 * time.Minute)
	talking.r.Add(1)
	bigTalking.r.Add(1)
	f.turn(closeAt.Add(-time.Millisecond))
	f.check()

	wake := f.turn(closeAt)
	f.check("quiet", "quiet udp", "big quiet", "pending")
	evs := f.events("idle_closed")
	if len(evs) != 1 || evs[0].Fields["outbound"] != "w1" || evs[0].Fields["connections"] != float64(4) {
		t.Fatalf("idle_closed %+v, want one for the four silent connections of w1", evs)
	}
	var report struct {
		IdleClosed    int64 `json:"idle_closed_count"`
		PendingClosed int64 `json:"pending_closed_count"`
		Decisions     int64 `json:"switches"`
	}
	// The report shows once there was a decision: the switch off w1 is one.
	if err := json.Unmarshal(f.o.Report(), &report); err != nil || report.IdleClosed != 4 || report.PendingClosed != 0 {
		t.Fatalf("report idle_closed %d, pending_closed %d (%v), want 4 and 0", report.IdleClosed, report.PendingClosed, err)
	}
	// talking received 4 minutes after the switch; big talking, past the
	// bytes the tracker times, received after the note at the switch and is
	// noted anew now.
	if want := left.Add(4*time.Minute + n); !wake.Equal(want) {
		t.Fatalf("wake %v after the switch, want %v: talking silent for closeIdleAfter", wake.Sub(left), want.Sub(left))
	}

	// The ends restart neither the wait for sleep.afterLastUse nor the silence.
	f.finish(closeAt.Add(100 * time.Millisecond))
	if wake := f.turn(f.now); !wake.Equal(left.Add(4*time.Minute + n)) {
		t.Fatalf("after the ends: wake %v after the switch", wake.Sub(left))
	}
	if f.proxies["w1"].asked() != 0 {
		t.Fatal("asked to sleep under kept connections")
	}
	wake = f.turn(left.Add(4*time.Minute + n))
	f.check("quiet", "quiet udp", "big quiet", "pending", "talking")
	if want := closeAt.Add(n); !wake.Equal(want) {
		t.Fatalf("wake %v after the switch, want %v: big talking silent since the note", wake.Sub(left), want.Sub(left))
	}
	f.finish(f.now.Add(time.Second))
	f.turn(f.now)
	f.turn(closeAt.Add(n))
	f.check("quiet", "quiet udp", "big quiet", "pending", "talking", "big talking")
	if evs := f.events("idle_closed"); len(evs) != 2 {
		t.Fatalf("idle_closed %+v, want one for each check that ended one", evs)
	}
	ended := closeAt.Add(n + time.Second)
	f.finish(ended)
	select {
	case <-f.o.kick:
	default:
		t.Fatal("the end of the last connection did not turn the loop")
	}
	if wake := f.turn(ended); !wake.Equal(ended.Add(minSleepAfter)) {
		t.Fatalf("wake %v after the last end, want the shortest sleep.afterLastUse %v", wake.Sub(ended), minSleepAfter)
	}
	f.turn(ended.Add(minSleepAfter - time.Millisecond))
	if len(f.sleeps(t)) != 0 {
		t.Fatal("slept while the last connection may still be closing")
	}
	if wake := f.turn(ended.Add(minSleepAfter)); !wake.IsZero() {
		t.Fatalf("a wake %v after the sleep is left", wake.Sub(ended))
	}
	slept := f.sleeps(t)
	if len(slept) != 1 || slept[0].Fields["reason"] != "idle" || !f.asleep(t, "w1") {
		t.Fatalf("after the last end: sleeps %+v, want one with reason idle", slept)
	}
}

// The check comes at the later of sleep.afterLastUse after the outbound was left
// and closeIdleAfter after the silence of the connection began, not before.
func TestIdleCloseWaitsForBothThresholds(t *testing.T) {
	for _, tc := range []struct {
		name       string
		after, n   time.Duration
		lastRead   time.Duration
		wantFromLt time.Duration
	}{
		{"sleep.afterLastUse last", 5 * time.Minute, 2 * time.Minute, -5 * time.Second, 5 * time.Minute},
		{"silence last", time.Minute, 2 * time.Minute, -5 * time.Second, 2*time.Minute - 5*time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newIdleFixture(t, tc.after, tc.n)
			left := at(time.Hour)
			f.now = at(time.Second)
			c := f.open("quiet", 100)
			f.now = left.Add(tc.lastRead)
			c.r.Add(10)
			f.leave(left)
			due := left.Add(tc.wantFromLt)
			if wake := f.turn(left); !wake.Equal(due) {
				t.Fatalf("wake %v after the switch, want %v", wake.Sub(left), tc.wantFromLt)
			}
			if wake := f.turn(due.Add(-time.Millisecond)); !wake.Equal(due) {
				t.Fatalf("before it: wake %v after the switch", wake.Sub(left))
			}
			f.check()
			f.turn(due)
			f.check("quiet")
		})
	}
}

// A use of the outbound restarts the wait for sleep.afterLastUse, here the end of
// another connection through it: the check it brings notes the connections
// at once and ends nothing, also one long silent, before sleep.afterLastUse after
// the use.
func TestIdleCloseWaitsSleepAfterAfterAUse(t *testing.T) {
	f := newIdleFixture(t, 5*time.Minute, time.Minute)
	f.now = at(time.Second)
	f.open("quiet", 100)
	other := f.open("other", 100)
	left := at(10 * time.Second)
	f.leave(left)
	f.turn(left)
	used := left.Add(4 * time.Minute)
	f.now = used
	other.tr.Finished(nil)
	f.o.ProcessEnded("w1")
	if wake := f.turn(used); !wake.Equal(used.Add(5 * time.Minute)) {
		t.Fatalf("wake %v after the use, want sleep.afterLastUse", wake.Sub(used))
	}
	f.turn(left.Add(5 * time.Minute))
	f.check()
	f.turn(used.Add(5 * time.Minute))
	f.check("quiet")
}

// With closeIdleAfter zero nothing is ended, nothing is planned for it, and
// the outbound stays awake under its connections, as before.
func TestIdleCloseOffKeepsEverything(t *testing.T) {
	f := newIdleFixture(t, 5*time.Minute, 0)
	f.now = at(time.Second)
	f.open("quiet", 100)
	left := at(10 * time.Second)
	f.leave(left)
	for _, now := range []time.Time{left, left.Add(5 * time.Minute), left.Add(time.Hour)} {
		if wake := f.turn(now); !wake.IsZero() {
			t.Fatalf("at %v: wake %v", now.Sub(left), wake.Sub(now))
		}
	}
	f.check()
	if len(f.events("idle_closed")) != 0 || f.proxies["w1"].asked() != 0 {
		t.Fatal("ended or asked to sleep with closeIdleAfter off")
	}
}

// Whatever but its connections holds the outbound keeps them all: a
// balancer that chose it again or another one pinned to it, a return to it,
// a failover that tried it, a probe that runs or waits, a provider request
// through it, a request of the host that waits for it. No check is planned
// for them meanwhile.
func TestIdleCloseKeepsWhatIsHeld(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold func(t *testing.T, e *engine, s *selector, now time.Time)
	}{
		{"chosen again", func(t *testing.T, e *engine, s *selector, now time.Time) {
			e.switchTo(s, "w1", "failover", now)
		}},
		{"pinned by another balancer", func(t *testing.T, e *engine, s *selector, now time.Time) {
			other := e.addSelector("other", newSelectorSettings(&SelectorConfig{Costs: costs("a", 1, "w", 3)}))
			e.updateCandidates(other, []string{"a1", "w1"}, now)
			if !e.pin("other", "w1", now) {
				t.Fatal("setup: pin refused")
			}
		}},
		{"return target", func(t *testing.T, e *engine, s *selector, now time.Time) {
			s.returnToCheaperTarget = "w1"
		}},
		{"tried by a failover under way", func(t *testing.T, e *engine, s *selector, now time.Time) {
			s.failoverTried = map[string]bool{"w1": true, "w2": true}
			s.failoverProbing = map[string]bool{"w2": true}
		}},
		{"queued probe", func(t *testing.T, e *engine, s *selector, now time.Time) {
			e.enqueue("w1", reasonRecheck, "auto", now.Add(time.Hour))
		}},
		{"running probe", func(t *testing.T, e *engine, s *selector, now time.Time) {
			e.running[1<<40] = &probeJob{id: 1 << 40, tag: "w1", reason: reasonHost}
		}},
		{"provider request through it", func(t *testing.T, e *engine, s *selector, now time.Time) {
			e.fetchingVia("w1", 1, now)
		}},
		{"host request", func(t *testing.T, e *engine, s *selector, now time.Time) {
			if _, _, err := e.requestProbe("auto", []string{"w1"}, now); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newIdleFixture(t, time.Minute, time.Minute)
			f.now = at(time.Second)
			f.open("quiet", 100)
			left := at(10 * time.Second)
			f.leave(left)
			f.turn(left)
			f.now = left.Add(30 * time.Second)
			var noted uint64
			f.engine(func(e *engine) {
				noted = e.paths["w1"].idleUses
				tc.hold(t, e, e.selectors["auto"], f.now)
			})
			for _, now := range []time.Time{f.now, left.Add(time.Minute), left.Add(time.Hour)} {
				f.turn(now)
				var checked uint64
				f.engine(func(e *engine) { checked = e.paths["w1"].idleUses })
				if checked != noted {
					t.Fatalf("at %v: checked since the hold", now.Sub(left))
				}
			}
			f.check()
		})
	}
}

// A balancer that chooses the outbound again and leaves it restarts the
// wait: the check comes sleep.afterLastUse after the second switch off it.
func TestIdleCloseCountsFromTheLastSwitchOff(t *testing.T) {
	f := newIdleFixture(t, time.Minute, time.Minute)
	f.now = at(time.Second)
	f.open("quiet", 100)
	f.leave(at(10 * time.Second))
	f.turn(f.now)
	back := at(50 * time.Second)
	f.now = back
	f.engine(func(e *engine) { e.switchTo(e.selectors["auto"], "w1", "failover", back) })
	f.turn(back)
	left := at(80 * time.Second)
	f.leave(left)
	if wake := f.turn(left); !wake.Equal(left.Add(time.Minute)) {
		t.Fatalf("wake %v after the second switch off, want sleep.afterLastUse", wake.Sub(left))
	}
	f.turn(at(10*time.Second + time.Minute))
	f.check()
	f.turn(left.Add(time.Minute))
	f.check("quiet")
}

// Silence proves an application idle only while the network works: while
// it is down or in doubt nothing is ended and nothing is planned, and after
// a network change or the end of an outage the check waits until the
// network has worked for closeIdleAfter.
func TestIdleCloseWaitsForASettledNetwork(t *testing.T) {
	const n = 2 * time.Minute
	for _, tc := range []struct {
		name string
		// upset changes the network at the time given, settle brings it
		// back; nil when it settles by itself.
		upset, settle func(e *engine, now time.Time)
	}{
		{"network change", func(e *engine, now time.Time) { e.onNetworkChange("lte", now) }, nil},
		{"network change ignored by the balancers", func(e *engine, now time.Time) {
			e.selectors["auto"].cfg.onNetworkChange = NetworkChangeMode_IGNORE
			e.onNetworkChange("lte", now)
		}, nil},
		{"network lost", func(e *engine, now time.Time) { e.onNetworkLost(now) },
			func(e *engine, now time.Time) { e.onNetworkChange("lte", now) }},
		{"network check failing", func(e *engine, now time.Time) { e.networkDown = true },
			func(e *engine, now time.Time) { e.endPause(now) }},
		{"network check in doubt", func(e *engine, now time.Time) { e.doubt = true },
			func(e *engine, now time.Time) { e.doubt = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newIdleFixture(t, 5*time.Minute, n)
			f.now = at(time.Second)
			f.open("quiet", 100)
			left := at(10 * time.Second)
			f.leave(left)
			f.turn(left)
			upset := left.Add(4 * time.Minute)
			f.now = upset
			f.engine(func(e *engine) { tc.upset(e, upset) })
			wake := f.turn(upset)
			settled := upset
			if tc.settle != nil {
				if !wake.IsZero() {
					t.Fatalf("a wake %v later while the network is not there", wake.Sub(upset))
				}
				f.turn(left.Add(10 * time.Minute))
				f.check()
				settled = left.Add(20 * time.Minute)
				f.now = settled
				f.engine(func(e *engine) { tc.settle(e, settled) })
				wake = f.turn(settled)
			}
			if !wake.Equal(settled.Add(n)) {
				t.Fatalf("wake %v after the network settled, want closeIdleAfter %v", wake.Sub(settled), n)
			}
			f.turn(settled.Add(n - time.Millisecond))
			f.check()
			f.turn(settled.Add(n))
			f.check("quiet")
		})
	}
}

// A spliced connection moves its bytes past the counters and is judged by
// what its socket received since the note of the previous check: one whose
// kernel received nothing is ended, one that received is kept and noted
// anew. One on a platform whose kernel does not count is kept, and plans no
// wake.
func TestIdleCloseOfSplicedConnectionsByTheKernel(t *testing.T) {
	quiet, _ := loopbackPair(t, 10*1024)
	if _, ok := kernelReceived(rawConnOf(quiet)); !ok {
		t.Skip("the kernel does not report received bytes here")
	}
	talking, srv := loopbackPair(t, 10*1024)
	f := newIdleFixture(t, time.Minute, time.Minute)
	f.now = at(time.Second)
	f.splice("quiet", quiet)
	f.splice("talking", talking)
	unknown := f.open("unknown", 100)
	unknown.tr.spliced.Store(true)
	left := at(10 * time.Second)
	f.leave(left)
	if wake := f.turn(left); !wake.Equal(left.Add(time.Minute)) {
		t.Fatalf("wake %v after the switch off w1, want a minute", wake.Sub(left))
	}
	if _, err := srv.Write(make([]byte, 100)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	wake := f.turn(left.Add(time.Minute))
	f.check("quiet")
	if want := left.Add(2 * time.Minute); !wake.Equal(want) {
		t.Fatalf("wake %v after the switch, want %v: talking noted anew", wake.Sub(left), want.Sub(left))
	}
	f.turn(left.Add(2 * time.Minute))
	f.check("quiet", "talking")
	f.finish(left.Add(2*time.Minute + time.Second))
	if wake := f.turn(f.now); !wake.IsZero() {
		t.Fatalf("a wake %v later for the connection that cannot tell", wake.Sub(f.now))
	}
	f.turn(left.Add(time.Hour))
	f.check("quiet", "talking")
	if f.proxies["w1"].asked() != 0 {
		t.Fatal("asked to sleep under the connection that cannot tell")
	}
}

// Once a check ended its idle connections, the outbound sleeps soon after
// the rest are gone. A new connection through it is a use like any other:
// after its end the outbound waits sleep.afterLastUse again.
func TestIdleCloseThenANewConnectionWaitsSleepAfter(t *testing.T) {
	f := newIdleFixture(t, time.Minute, time.Minute)
	f.now = at(time.Second)
	f.open("quiet", 100)
	left := at(10 * time.Second)
	f.leave(left)
	f.turn(left)
	f.turn(left.Add(time.Minute))
	f.check("quiet")
	started := left.Add(time.Minute + time.Second)
	f.now = started
	f.open("new", 100)
	f.finish(started)
	f.turn(started)
	ended := started.Add(time.Second)
	f.now = ended
	f.conns["new"].tr.Finished(context.Canceled)
	f.o.ProcessEnded("w1")
	if wake := f.turn(ended); !wake.Equal(ended.Add(time.Minute)) {
		t.Fatalf("wake %v after the end of the new connection, want sleep.afterLastUse", wake.Sub(ended))
	}
	if len(f.sleeps(t)) != 0 {
		t.Fatal("slept at once after a new connection")
	}
	f.turn(ended.Add(time.Minute))
	if slept := f.sleeps(t); len(slept) != 1 || slept[0].Fields["reason"] != "idle" {
		t.Fatalf("sleeps %+v, want one sleep.afterLastUse after the new connection", slept)
	}
}

// What takes the outbound between the plan and the check keeps its
// connections: the check looks again under the lock right before it ends
// anything. A balancer that chooses it, which is a use, and a probe queued
// for it or a network lost or changed, which are not, and which the loop
// has not turned for yet.
func TestIdleCloseTakenAfterThePlan(t *testing.T) {
	for _, tc := range []struct {
		name string
		take func(e *engine, now time.Time)
	}{
		{"chosen", func(e *engine, now time.Time) { e.switchTo(e.selectors["auto"], "w1", "failover", now) }},
		{"probe queued", func(e *engine, now time.Time) { e.enqueue("w1", reasonRecheck, "auto", now) }},
		{"network lost", func(e *engine, now time.Time) { e.onNetworkLost(now) }},
		{"network changed", func(e *engine, now time.Time) { e.onNetworkChange("lte", now) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newIdleFixture(t, time.Minute, time.Minute)
			f.now = at(time.Second)
			f.open("quiet", 100)
			left := at(10 * time.Second)
			f.leave(left)
			f.turn(left)
			due := left.Add(time.Minute)
			f.now = due
			f.o.mu.Lock()
			f.o.e.step(due)
			batch := f.o.e.takeSleeps()
			tc.take(f.o.e, due)
			f.o.mu.Unlock()
			if len(batch) != 1 || !batch[0].idleCheck {
				t.Fatalf("setup: batch %+v, want the idle check of w1", batch)
			}
			f.o.sleepOutbounds(batch)
			f.check()
		})
	}
}

// An outbound that cannot sleep keeps its connections.
func TestIdleCloseOnlyForSleepers(t *testing.T) {
	cfg := &Config{Sleep: &SleepConfig{AfterLastUse: int64(time.Minute), CloseIdleAfter: int64(time.Minute)}}
	f := newSleepFixture(t, cfg, &SelectorConfig{Costs: costs("a", 1, "w", 3), SkipProbeBeforeSwitch: true}, "a1", "a2", "w1")
	f.o.sweeping = true
	f.engine(func(e *engine) { e.switchTo(e.selectors["auto"], "a2", "failover", t0) })
	f.turn(t0)
	ended := false
	f.now = at(time.Second)
	tr := f.o.StartDial(context.Background(), "a2", net.Destination{}, func() { ended = true }).(*connTracker)
	r, _ := tr.Dialed(nil, nil, nil, nil)
	r.Add(100)
	f.o.ProcessStarted("a2")
	f.now = at(10 * time.Second)
	f.engine(func(e *engine) { e.switchTo(e.selectors["auto"], "a1", "return", f.now) })
	for _, now := range []time.Time{at(10 * time.Second), at(time.Hour)} {
		if wake := f.turn(now); !wake.IsZero() {
			t.Fatalf("a wake %v for an outbound that cannot sleep", wake.Sub(now))
		}
	}
	if ended {
		t.Fatal("ended a connection of an outbound that cannot sleep")
	}
}
