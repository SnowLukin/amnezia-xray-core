package health

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
)

// GetOutbound and Sleep make fakeHandler look like a handler of the outbound
// manager: it has Sleep whatever its proxy and passes the call on.
func (h *fakeHandler) GetOutbound() proxy.Outbound {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.looked++
	return h.out
}

func (h *fakeHandler) Sleep() bool {
	h.mu.Lock()
	h.slept++
	out := h.out
	h.mu.Unlock()
	if s, ok := out.(outbound.Sleeper); ok {
		return s.Sleep()
	}
	return true
}

func (h *fakeHandler) counts() (looked, slept int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.looked, h.slept
}

// plainProxy holds nothing to release.
type plainProxy struct{}

func (plainProxy) Process(context.Context, *transport.Link, internet.Dialer) error { return nil }

// sleepyProxy stands for WireGuard: it sleeps unless busy.
type sleepyProxy struct {
	plainProxy
	mu    sync.Mutex
	calls int
	busy  bool
	// during runs inside Sleep, as a proxy that calls back would.
	during func()
}

func (p *sleepyProxy) Sleep() bool {
	p.mu.Lock()
	p.calls++
	busy, during := p.busy, p.during
	p.mu.Unlock()
	if during != nil {
		during()
	}
	return !busy
}

func (p *sleepyProxy) asked() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *sleepyProxy) setBusy(busy bool) {
	p.mu.Lock()
	p.busy = busy
	p.mu.Unlock()
}

// sleepFixture is an observer whose loop the test turns by hand on a clock
// of its own. Outbounds named w... can sleep, a... cannot.
type sleepFixture struct {
	o        *Observer
	m        *fakeManager
	now      time.Time
	handlers map[string]*fakeHandler
	proxies  map[string]*sleepyProxy
}

func newSleepFixture(t *testing.T, cfg *Config, sc *SelectorConfig, candidates ...string) *sleepFixture {
	t.Helper()
	if cfg == nil {
		cfg = &Config{}
	}
	cfg.SubjectSelector = append(cfg.SubjectSelector, "a", "w")
	if cfg.Schedule == nil {
		cfg.Schedule = &ScheduleConfig{PauseWhenIdle: int64(time.Hour)}
	}
	if sc == nil {
		sc = &SelectorConfig{Costs: costs("a", 1, "w", 3)}
	}
	f := &sleepFixture{m: newFakeManager(), now: t0, handlers: map[string]*fakeHandler{}, proxies: map[string]*sleepyProxy{}}
	f.o = newObserver(context.Background(), cfg)
	t.Cleanup(func() { f.o.Close() })
	f.o.clock = func() time.Time { return f.now }
	f.o.ohm = f.m
	for _, tag := range candidates {
		f.add(tag)
	}
	f.o.mu.Lock()
	s := f.o.e.addSelector("auto", newSelectorSettings(sc))
	f.o.e.noteDemand(t0)
	f.o.e.updateCandidates(s, candidates, t0)
	f.o.mu.Unlock()
	return f
}

func (f *sleepFixture) add(tag string) {
	h := &fakeHandler{tag: tag, out: plainProxy{}}
	if strings.HasPrefix(tag, "w") {
		p := &sleepyProxy{}
		h.out = p
		f.proxies[tag] = p
	}
	f.handlers[tag] = h
	f.m.handlers[tag] = h
}

// turn runs one turn of the observer loop at now, the batch of sleeps
// included, and returns its wake. The end of a batch kicks the loop, which
// turns again.
func (f *sleepFixture) turn(now time.Time) time.Time {
	f.now = now
	f.o.mu.Lock()
	_, _, wake := f.o.e.step(now)
	batch := f.o.e.takeSleeps()
	f.o.mu.Unlock()
	if len(batch) == 0 {
		return wake
	}
	f.o.sleepOutbounds(batch)
	return f.turn(now)
}

// use marks a use of tag at now, as a probe or a provider request through
// it would, and turns the loop.
func (f *sleepFixture) use(tag string, now time.Time) time.Time {
	f.now = now
	f.o.mu.Lock()
	f.o.e.used(tag, now)
	f.o.mu.Unlock()
	return f.turn(now)
}

func (f *sleepFixture) engine(do func(e *engine)) {
	f.o.mu.Lock()
	do(f.o.e)
	f.o.mu.Unlock()
}

func (f *sleepFixture) sleeps(t *testing.T) []Event {
	t.Helper()
	raw, _ := f.o.DrainEvents()
	var evs []Event
	if err := json.Unmarshal(raw, &evs); err != nil {
		t.Fatal(err)
	}
	var out []Event
	for _, e := range evs {
		if e.Type == "outbound_sleep" {
			out = append(out, e)
		}
	}
	return out
}

func (f *sleepFixture) asleep(t *testing.T, tag string) bool {
	t.Helper()
	var st statusView
	if err := json.Unmarshal(f.o.Status(), &st); err != nil {
		t.Fatal(err)
	}
	for _, p := range st.Paths {
		if p.Outbound == tag {
			return p.Asleep
		}
	}
	return false
}

// A probe wakes an outbound the balancer does not use; it sleeps once
// sleep.after passed since, with one call and one event, and is not asked
// again.
func TestSleepsAfterIdle(t *testing.T) {
	f := newSleepFixture(t, nil, nil, "a1", "w1")
	f.turn(t0)
	f.engine(func(e *engine) { e.enqueue("w1", reasonNetwork, "", at(time.Second)) })
	f.now = at(time.Second)
	f.o.mu.Lock()
	jobs, _, _ := f.o.e.step(at(time.Second))
	if len(jobs) != 1 || jobs[0].tag != "w1" {
		f.o.mu.Unlock()
		t.Fatalf("setup: probes %+v", jobs)
	}
	f.o.e.onProbeResult(jobs[0], probeResult{ok: true, rtt: time.Millisecond}, at(2*time.Second))
	f.o.mu.Unlock()
	used := at(2 * time.Second)
	if wake := f.turn(used); !wake.Equal(used.Add(defaultSleepAfter)) {
		t.Fatalf("wake at %v after the probe, want %v", wake.Sub(t0), used.Add(defaultSleepAfter).Sub(t0))
	}
	f.turn(used.Add(defaultSleepAfter - time.Second))
	if n := f.proxies["w1"].asked(); n != 0 {
		t.Fatalf("asked to sleep %d times before sleep.after", n)
	}
	if wake := f.turn(used.Add(defaultSleepAfter)); !wake.IsZero() {
		t.Fatalf("wake at %v once everything sleeps", wake.Sub(t0))
	}
	evs := f.sleeps(t)
	if n := f.proxies["w1"].asked(); n != 1 || len(evs) != 1 {
		t.Fatalf("Sleep called %d times, %d events; want one of each", n, len(evs))
	}
	if fl := evs[0].Fields; fl["outbound"] != "w1" || fl["reason"] != "idle" || fl["idle_ms"] != float64(defaultSleepAfter.Milliseconds()) {
		t.Fatalf("event %v", fl)
	}
	if !f.asleep(t, "w1") || f.asleep(t, "a1") {
		t.Fatalf("status: w1 asleep %v, a1 asleep %v", f.asleep(t, "w1"), f.asleep(t, "a1"))
	}
	f.turn(at(3 * time.Hour))
	if n := f.proxies["w1"].asked(); n != 1 {
		t.Fatalf("asleep, yet asked again: %d calls", n)
	}
	// The next use wakes it, and it sleeps again after its own wait.
	f.use("w1", at(4*time.Hour))
	if f.asleep(t, "w1") {
		t.Fatal("used, yet shown asleep")
	}
	f.turn(at(4*time.Hour + defaultSleepAfter))
	if n := f.proxies["w1"].asked(); n != 2 {
		t.Fatalf("Sleep called %d times after the second use, want 2", n)
	}
}

// Whatever uses an outbound or is about to keeps it awake.
func TestNeverSleepsChosenPinnedTargetProbedQueuedOrFetchVia(t *testing.T) {
	for _, tc := range []struct {
		name string
		hold func(t *testing.T, e *engine, s *selector, now time.Time)
	}{
		{"chosen", func(t *testing.T, e *engine, s *selector, now time.Time) {
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
			s.upgradeTarget = "w1"
		}},
		{"tried by a failover under way", func(t *testing.T, e *engine, s *selector, now time.Time) {
			s.failoverTried = map[string]bool{"a1": true, "w1": true, "w2": true}
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSleepFixture(t, nil, nil, "a1", "w1", "w2")
			f.use("w1", at(time.Second))
			f.engine(func(e *engine) { tc.hold(t, e, e.selectors["auto"], at(2*time.Second)) })
			f.turn(at(2 * time.Second))
			f.turn(at(2*time.Second + defaultSleepAfter))
			f.turn(at(time.Hour))
			if n := f.proxies["w1"].asked(); n != 0 || len(f.sleeps(t)) != 0 {
				t.Fatalf("asked to sleep %d times", n)
			}
		})
	}
}

// A probe request of the host keeps the outbound it waits for awake, also
// while nothing probes it: here the network is down, so its probe waits and
// the request alone holds the outbound, until it ends after its timeout.
func TestHostRequestKeepsItsOutboundAwake(t *testing.T) {
	f := newSleepFixture(t, &Config{Sleep: &SleepConfig{After: int64(10 * time.Second)}}, nil, "a1", "w1")
	f.use("w1", at(time.Second))
	f.engine(func(e *engine) {
		e.onNetworkLost(at(2 * time.Second))
		if _, _, err := e.requestProbe("auto", []string{"w1"}, at(2*time.Second)); err != nil {
			t.Fatal(err)
		}
	})
	for _, now := range []time.Time{at(2 * time.Second), at(15 * time.Second), at(time.Minute)} {
		f.turn(now)
		if n := f.proxies["w1"].asked(); n != 0 {
			t.Fatalf("at %v, while the request waits: asked to sleep %d times", now.Sub(t0), n)
		}
	}
	var running int
	f.engine(func(e *engine) { running = len(e.running) })
	if running != 0 {
		t.Fatalf("setup: %d probes run while the network is down", running)
	}
	// hostProbes.timeout of two minutes ends the request.
	f.turn(at(2*time.Minute + 3*time.Second))
	if n := f.proxies["w1"].asked(); n != 1 || len(f.sleeps(t)) != 1 {
		t.Fatalf("after the request: asked to sleep %d times", n)
	}
}

// What tick holds back keeps nothing awake: a probe that waits for the
// network, for traffic or for memory, and a return while memory is low. The
// probe wakes the outbound once it starts.
func TestSleepsUnderAProbeThatWaits(t *testing.T) {
	for _, tc := range []struct {
		name   string
		hold   func(e *engine, now time.Time)
		at     time.Duration
		reason string
	}{
		{"network down", func(e *engine, now time.Time) {
			e.enqueue("w1", reasonRecheck, "auto", now)
			e.onNetworkLost(now)
		}, defaultSleepAfter, "idle"},
		// Idle from an hour on: a probe after a network change waits for
		// the next connection.
		{"idle", func(e *engine, now time.Time) {
			e.enqueue("w1", reasonNetwork, "", now)
		}, defaultSleepAfter, "idle"},
		{"memory low", func(e *engine, now time.Time) {
			e.selectors["auto"].upgradeTarget = "w1"
			e.enqueue("w1", reasonUpgrade, "auto", now)
			e.setMemoryLow(true, now)
		}, 0, "memory"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSleepFixture(t, nil, nil, "a1", "w1")
			from := at(time.Second)
			if tc.name == "idle" {
				from = at(2 * time.Hour)
			}
			f.use("w1", from)
			f.engine(func(e *engine) { tc.hold(e, from) })
			f.turn(from)
			f.turn(from.Add(tc.at))
			evs := f.sleeps(t)
			if n := f.proxies["w1"].asked(); n != 1 || len(evs) != 1 || evs[0].Fields["reason"] != tc.reason {
				t.Fatalf("asked to sleep %d times, events %+v", n, evs)
			}
			var queued int
			f.engine(func(e *engine) { queued = len(e.queue) })
			if queued == 0 {
				t.Fatal("setup: the probe did not wait")
			}
		})
	}
}

// A connection the observer sees no dial of may wake an outbound that can
// sleep: a rule straight at its tag, a fallbackTag or a dialerProxy chain.
// Its handler tells of every connection: the outbound is awake in Status
// again, does not sleep while one runs, and sleeps sleep.after after the
// last one ended, which kicks the loop to plan it.
func TestProcessWakesAndHoldsASleepingOutbound(t *testing.T) {
	f := newSleepFixture(t, nil, nil, "a1", "w1")
	f.use("w1", at(time.Second))
	f.turn(at(time.Second + defaultSleepAfter))
	if !f.asleep(t, "w1") || f.proxies["w1"].asked() != 1 {
		t.Fatal("setup: w1 does not sleep")
	}
	kicked := func() bool {
		select {
		case <-f.o.kick:
			return true
		default:
			return false
		}
	}
	kicked()
	f.now = at(time.Hour)
	f.o.ProcessStarted("w1")
	f.o.ProcessStarted("w1")
	if f.asleep(t, "w1") || !kicked() {
		t.Fatal("a connection through w1 left it asleep, or the loop unkicked")
	}
	f.turn(at(time.Hour))
	f.turn(at(time.Hour + defaultSleepAfter))
	f.now = at(2 * time.Hour)
	f.o.ProcessEnded("w1")
	if kicked() {
		t.Fatal("kicked while a connection still runs")
	}
	f.turn(at(3 * time.Hour))
	if n := f.proxies["w1"].asked(); n != 1 {
		t.Fatalf("asked to sleep %d times while a connection runs", n)
	}
	f.now = at(3 * time.Hour)
	f.o.ProcessEnded("w1")
	if !kicked() {
		t.Fatal("the last connection ended without a kick")
	}
	if wake := f.turn(at(3 * time.Hour)); !wake.Equal(at(3*time.Hour + defaultSleepAfter)) {
		t.Fatalf("wake at %v, want sleep.after past the last connection", wake.Sub(t0))
	}
	f.turn(at(3*time.Hour + defaultSleepAfter))
	if n := f.proxies["w1"].asked(); n != 2 || !f.asleep(t, "w1") {
		t.Fatalf("after the last connection: asked %d times, asleep %v", n, f.asleep(t, "w1"))
	}
	// An end the observer cannot match counts for nothing.
	f.o.ProcessEnded("w1")
	f.o.ProcessEnded("w9")
	f.engine(func(e *engine) {
		if p := e.paths["w1"]; p == nil || p.processes != 0 || p.awake {
			t.Fatalf("w1 after unmatched ends: %+v", p)
		}
	})
}

func TestNeverSleepsRetiringReplacedOrConnectivity(t *testing.T) {
	for _, tc := range []struct {
		name string
		cfg  *Config
		tag  string
		hold func(e *engine)
	}{
		{"retiring", nil, "w1", func(e *engine) { e.retiring["w1"] = true }},
		{"replaced", nil, "w1", func(e *engine) { e.replacing["w1"] = true }},
		{"network check", &Config{Connectivity: &ConnectivityConfig{Outbound: "w9", Destination: "https://check.test/"}}, "w9", nil},
		{"witness", &Config{Connectivity: &ConnectivityConfig{Outbound: "a9", Destination: "https://check.test/",
			Witnesses: []string{"w8"}}}, "w8", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSleepFixture(t, tc.cfg, nil, "a1", "w1")
			if tc.tag != "w1" {
				f.add(tc.tag)
			}
			f.use(tc.tag, at(time.Second))
			if tc.hold != nil {
				f.engine(tc.hold)
			}
			f.turn(at(time.Hour))
			if n := f.proxies[tc.tag].asked(); n != 0 || len(f.sleeps(t)) != 0 {
				t.Fatalf("%s asked to sleep %d times", tc.tag, n)
			}
		})
	}
}

// An outbound nothing used holds nothing yet: its handler is not even asked
// whether it can sleep, and no timer runs for it.
func TestNeverSleepsAnOutboundNeverUsed(t *testing.T) {
	f := newSleepFixture(t, nil, nil, "a1", "w1", "w2")
	for _, now := range []time.Time{t0, at(10 * time.Minute), at(2 * time.Hour)} {
		if wake := f.turn(now); !wake.IsZero() {
			t.Fatalf("wake at %v", wake.Sub(t0))
		}
	}
	for _, tag := range []string{"w1", "w2"} {
		if looked, slept := f.handlers[tag].counts(); looked != 0 || slept != 0 || f.proxies[tag].asked() != 0 {
			t.Fatalf("%s never used: looked at %d times, slept %d times", tag, looked, slept)
		}
	}
}

func TestSleepsAtOnceUnderLowMemory(t *testing.T) {
	f := newSleepFixture(t, nil, nil, "a1", "w1")
	f.use("w1", at(time.Second))
	f.now = at(2 * time.Second)
	f.o.SetMemoryLow(true)
	f.turn(at(2 * time.Second))
	evs := f.sleeps(t)
	if n := f.proxies["w1"].asked(); n != 1 || len(evs) != 1 || evs[0].Fields["reason"] != "memory" {
		t.Fatalf("under low memory: Sleep called %d times, events %+v", n, evs)
	}
	if evs[0].Fields["idle_ms"] != float64(1000) {
		t.Fatalf("idle_ms %v, want 1000", evs[0].Fields["idle_ms"])
	}
}

// A refusal is asked again after sleep.after, then twice as long each time,
// up to 30 minutes; a use starts the count again.
func TestBusySleepRetriesDoublingUpTo30m(t *testing.T) {
	f := newSleepFixture(t, nil, nil, "a1", "w1")
	f.proxies["w1"].setBusy(true)
	f.use("w1", t0)
	now := t0.Add(defaultSleepAfter)
	for i, want := range []time.Duration{5 * time.Minute, 10 * time.Minute, 20 * time.Minute, 30 * time.Minute, 30 * time.Minute} {
		wake := f.turn(now)
		if n := f.proxies["w1"].asked(); n != i+1 {
			t.Fatalf("at %v: Sleep called %d times, want %d", now.Sub(t0), n, i+1)
		}
		if got := wake.Sub(now); got != want {
			t.Fatalf("refusal %d: asked again in %v, want %v", i+1, got, want)
		}
		f.turn(wake.Add(-time.Second))
		now = wake
	}
	f.use("w1", now)
	if wake := f.turn(now.Add(time.Second)); wake.Sub(now) != defaultSleepAfter {
		t.Fatalf("after a use: asked again in %v, want sleep.after", wake.Sub(now))
	}
	f.proxies["w1"].setBusy(false)
	f.turn(now.Add(defaultSleepAfter))
	if len(f.sleeps(t)) != 1 || !f.asleep(t, "w1") {
		t.Fatal("no sleep once the outbound let go")
	}
}

// A handler has Sleep whatever its proxy; one whose proxy holds nothing is
// looked at once and never asked to sleep, and costs no wake.
func TestNonSleeperAskedOnce(t *testing.T) {
	f := newSleepFixture(t, nil, &SelectorConfig{Costs: costs("a", 1, "w", 3), SwitchWithoutProbe: true}, "a1", "a2", "w1")
	f.turn(t0)
	f.engine(func(e *engine) { e.switchTo(e.selectors["auto"], "a2", "failover", at(time.Second)) })
	if wake := f.turn(at(time.Second)); !wake.IsZero() {
		t.Fatalf("wake at %v for outbounds that cannot sleep", wake.Sub(t0))
	}
	f.use("a1", at(time.Minute))
	f.turn(at(time.Hour))
	looked, slept := f.handlers["a1"].counts()
	if looked != 1 || slept != 0 {
		t.Fatalf("a1 looked at %d times, Sleep called %d times; want once and never", looked, slept)
	}
}

// A use that comes while the handler is asked keeps the outbound awake: the
// answer is set aside and it sleeps after its own wait.
func TestUseAfterTheDecisionKeepsItAwake(t *testing.T) {
	f := newSleepFixture(t, nil, nil, "a1", "w1")
	f.use("w1", t0)
	f.proxies["w1"].during = func() {
		f.o.mu.Lock()
		f.o.e.used("w1", f.now)
		f.o.mu.Unlock()
	}
	now := t0.Add(defaultSleepAfter)
	wake := f.turn(now)
	if len(f.sleeps(t)) != 0 || f.asleep(t, "w1") {
		t.Fatal("asleep although used while asked")
	}
	if !wake.Equal(now.Add(defaultSleepAfter)) {
		t.Fatalf("asked again in %v, want sleep.after from the use", wake.Sub(now))
	}
}

// Sleep may take long or call back into the observer: it runs without the
// observer lock.
func TestSleepRunsOutsideTheLock(t *testing.T) {
	f := newSleepFixture(t, nil, nil, "a1", "w1")
	f.use("w1", t0)
	f.proxies["w1"].during = func() { f.o.Status() }
	done := make(chan struct{})
	go func() {
		f.turn(t0.Add(defaultSleepAfter))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Sleep ran under the observer lock")
	}
	if f.proxies["w1"].asked() != 1 {
		t.Fatal("setup: Sleep was not called")
	}
}

// Once every used outbound sleeps, the observer keeps no timer for them.
func TestNoWakeOnceEverythingSleeps(t *testing.T) {
	f := newSleepFixture(t, nil, nil, "a1", "w1", "w2")
	f.use("w1", at(time.Minute))
	if wake := f.use("w2", at(2*time.Minute)); !wake.Equal(at(time.Minute + defaultSleepAfter)) {
		t.Fatalf("wake at %v, want the sleep of w1", wake.Sub(t0))
	}
	if wake := f.turn(at(time.Minute + defaultSleepAfter)); !wake.Equal(at(2*time.Minute + defaultSleepAfter)) {
		t.Fatalf("wake at %v, want the sleep of w2", wake.Sub(t0))
	}
	if wake := f.turn(at(2*time.Minute + defaultSleepAfter)); !wake.IsZero() {
		t.Fatalf("wake at %v once everything sleeps", wake.Sub(t0))
	}
	if len(f.sleeps(t)) != 2 {
		t.Fatal("setup: not both asleep")
	}
}

func TestSleepOffWithZero(t *testing.T) {
	f := newSleepFixture(t, &Config{Sleep: &SleepConfig{After: -1}}, nil, "a1", "w1")
	f.use("w1", t0)
	f.engine(func(e *engine) { e.updateCandidates(e.selectors["auto"], []string{"a1"}, at(time.Second)) })
	for _, now := range []time.Time{at(time.Second), at(time.Hour)} {
		if wake := f.turn(now); !wake.IsZero() {
			t.Fatalf("wake at %v with sleep off", wake.Sub(t0))
		}
	}
	if looked, _ := f.handlers["w1"].counts(); looked != 0 || f.proxies["w1"].asked() != 0 {
		t.Fatalf("sleep off, yet w1 looked at %d times and asked %d times", looked, f.proxies["w1"].asked())
	}
	var kept bool
	f.engine(func(e *engine) { _, kept = e.paths["w1"] })
	if kept {
		t.Fatal("sleep off, yet w1 is kept although no balancer offers it")
	}
}

// Collecting what no balancer offers keeps an outbound that may hold a
// tunnel until it sleeps: forgotten, it would hold it for good.
func TestGCKeepsAnAwakeSleeperUntilItSleeps(t *testing.T) {
	f := newSleepFixture(t, nil, nil, "a1", "w1")
	f.use("w1", at(time.Second))
	f.engine(func(e *engine) { e.updateCandidates(e.selectors["auto"], []string{"a1"}, at(2*time.Second)) })
	f.now = at(3 * time.Second)
	f.o.SetMemoryLow(true)
	var kept bool
	f.engine(func(e *engine) { _, kept = e.paths["w1"] })
	if !kept {
		t.Fatal("w1 forgotten while it may hold its tunnel")
	}
	f.turn(at(3 * time.Second))
	if f.proxies["w1"].asked() != 1 || len(f.sleeps(t)) != 1 {
		t.Fatal("w1 did not sleep")
	}
	f.turn(at(3*time.Second + staleAfter))
	f.engine(func(e *engine) { _, kept = e.paths["w1"] })
	if kept {
		t.Fatal("w1 asleep and offered by no balancer, yet kept")
	}
}

// The drain of an outbound whose connections the observer cannot see goes
// on while the outbound refuses to sleep, and ends once it lets go.
func TestDrainAsksAnOutboundThatCanSleep(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	build := f.o.newHandler
	var tunnel *sleepyProxy
	f.o.newHandler = func(c *core.OutboundHandlerConfig) (outbound.Handler, error) {
		h, err := build(c)
		if err == nil && c.Tag == "p-x1" {
			tunnel = &sleepyProxy{busy: true}
			h.(*fakeHandler).out = tunnel
		}
		return h, err
	}
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1}, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	f.follow("p-x1", "p-x2")
	x1 := f.m.GetHandler("p-x1").(*fakeHandler)
	x2 := f.m.GetHandler("p-x2").(*fakeHandler)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 1})); err != nil {
		t.Fatal(err)
	}
	if tunnel.asked() != 1 || x1.closes() != 0 || len(f.rt.draining) != 1 {
		t.Fatalf("busy tunnel: asked %d times, closed %d, %d draining", tunnel.asked(), x1.closes(), len(f.rt.draining))
	}
	tunnel.setBusy(false)
	f.o.SetMemoryLow(true)
	if tunnel.asked() != 2 || x1.closes() != 1 || len(f.rt.draining) != 0 || x2.closes() != 0 {
		t.Fatalf("idle tunnel: asked %d times, closed %d, %d draining", tunnel.asked(), x1.closes(), len(f.rt.draining))
	}
	if _, slept := x2.counts(); slept != 0 {
		t.Fatal("asked an outbound of the set to sleep on a drain")
	}
}

func TestSleepRetryIsBounded(t *testing.T) {
	var got []time.Duration
	for tries := 1; tries <= 8; tries++ {
		got = append(got, sleepRetry(10*time.Second, tries))
	}
	want := []time.Duration{time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 16 * time.Minute,
		30 * time.Minute, 30 * time.Minute, 30 * time.Minute}
	if !slices.Equal(got, want) {
		t.Fatalf("retries %v, want %v", got, want)
	}
	if d := sleepRetry(defaultSleepAfter, 1<<30); d != maxSleepRetry {
		t.Fatalf("after a billion refusals %v", d)
	}
}

// One batch is under way at a time, so a slow Sleep does not pile up
// goroutines; its end kicks the loop, which plans what waited.
func TestOneSleepBatchAtATime(t *testing.T) {
	f := newSleepFixture(t, nil, nil, "a1", "w1", "w2")
	f.turn(t0)
	var first, second []sleepJob
	f.engine(func(e *engine) {
		e.used("w1", at(time.Second))
		e.step(at(time.Second))
		first = e.takeSleeps()
		e.used("w2", at(2*time.Second))
		// Turns go on while the batch is under way.
		for _, now := range []time.Time{at(2 * time.Second), at(3 * time.Second)} {
			e.step(now)
			second = append(second, e.takeSleeps()...)
		}
	})
	if len(first) != 1 || first[0].tag != "w1" || len(second) != 0 {
		t.Fatalf("batches of %d and %d jobs while the first is under way", len(first), len(second))
	}
	f.now = at(3 * time.Second)
	f.o.sleepOutbounds(first)
	f.engine(func(e *engine) {
		e.step(at(3 * time.Second))
		second = e.takeSleeps()
	})
	if len(second) != 1 || second[0].tag != "w2" {
		t.Fatalf("after the first batch: %d jobs, want the one of w2", len(second))
	}
}
