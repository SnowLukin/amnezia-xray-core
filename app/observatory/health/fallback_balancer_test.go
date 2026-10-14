package health

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"google.golang.org/protobuf/proto"
)

type fallbackFixture struct {
	o              *Observer
	source, target *selector
	h              *selectorHandle
	now            time.Time
}

func newFallbackFixture(t *testing.T) *fallbackFixture {
	t.Helper()
	f := &fallbackFixture{now: t0}
	paths := []*PathDefinition{{Tag: "ai1", Balancer: "ai", Cost: 1, Probe: "check"}, {Tag: "main1", Balancer: "main", Cost: 1, Probe: "check"}}
	f.o = newObserver(context.Background(), &Config{Paths: paths, Probes: []*ProbeProfile{{Name: "check", Destination: "http://127.0.0.1/probe"}}, Features: &FeatureConfig{Events: proto.Bool(true), Report: proto.Bool(true), Pinning: proto.Bool(true)}})
	f.o.clock = func() time.Time { return f.now }
	t.Cleanup(func() { f.o.Close() })
	f.o.e.report.begin(t0)
	f.source = f.o.e.addSelectorAt("ai", newSelectorSettings(&SelectorConfig{Paths: paths[:1], FallbackBalancer: "main"}), t0)
	f.target = f.o.e.addSelectorAt("main", newSelectorSettings(&SelectorConfig{Paths: paths[1:]}), t0)
	f.o.e.updateCandidates(f.source, []string{"ai1"}, t0)
	f.o.e.updateCandidates(f.target, []string{"main1"}, t0)
	f.h = &selectorHandle{o: f.o, s: f.source}
	return f
}

func (f *fallbackFixture) exhaust() {
	p := f.o.e.path("ai1")
	p.setStatus(statusDown, "traffic", f.now)
	p.quarantineUntil = f.now.Add(2 * time.Minute)
	f.o.e.exhaust(f.source, f.now)
}

func fallbackEvents(t *testing.T, o *Observer) []Event {
	t.Helper()
	raw, _ := o.DrainEvents()
	var all []Event
	if err := json.Unmarshal(raw, &all); err != nil {
		t.Fatal(err)
	}
	var result []Event
	for _, ev := range all {
		if ev.Type == "fallback_balancer" {
			result = append(result, ev)
		}
	}
	return result
}

func TestFallbackBalancerStartsOnFirstSelectedConnectionAndEndsOnRecovery(t *testing.T) {
	f := newFallbackFixture(t)
	if f.h.FallbackBalancer() != "" {
		t.Fatal("healthy source offered fallback")
	}
	f.h.FallbackSelected("main")
	if f.source.fallingBack {
		t.Fatal("healthy source entered fallback")
	}
	f.exhaust()
	if f.h.FallbackBalancer() != "main" || f.source.fallingBack {
		t.Fatal("exhaustion alone started fallback")
	}
	f.now = at(time.Second)
	f.h.FallbackSelected("main")
	f.h.FallbackSelected("main")
	var status statusView
	if err := json.Unmarshal(f.o.Status(), &status); err != nil {
		t.Fatal(err)
	}
	for _, b := range status.Balancers {
		if b.Balancer == "ai" && (!b.IsFallingBack || b.FallbackBalancer != "main") {
			t.Fatalf("status %+v", b)
		}
	}
	if !status.Features["balancers.ai.fallback_balancer"] {
		t.Fatal("effective feature missing")
	}
	f.now = at(6 * time.Second)
	f.o.e.path("ai1").quarantineUntil = time.Time{}
	f.o.e.switchTo(f.source, "ai1", "failover", f.now)
	if f.h.FallbackBalancer() != "" || f.source.fallingBack {
		t.Fatal("recovered source kept fallback")
	}
	events := fallbackEvents(t, f.o)
	if len(events) != 2 || events[0].Fields["is_active"] != true || events[1].Fields["is_active"] != false || events[0].Fields["to"] != "main" {
		t.Fatalf("events %+v", events)
	}
	r := f.o.e.snapshotReport(f.now)
	if r.FallbackBalancerCount != 1 || r.FallbackBalancerMs["ai"] != 5000 {
		t.Fatalf("report %+v", r.reportCounts)
	}
}

func TestFallbackBalancerReportWindowsAndSnapshots(t *testing.T) {
	f := newFallbackFixture(t)
	f.o.s.reportInterval = 3 * time.Second
	f.exhaust()
	f.now = at(time.Second)
	f.h.FallbackSelected("main")
	first := f.o.e.snapshotReport(at(4 * time.Second))
	second := f.o.e.snapshotReport(at(4 * time.Second))
	if first.FallbackBalancerMs["ai"] != 3000 || second.FallbackBalancerMs["ai"] != 3000 || f.o.e.report.counts.FallbackBalancerMs != nil {
		t.Fatal("snapshot advanced or changed accounting")
	}
	first.FallbackBalancerMs["ai"] = 999999
	if f.o.e.snapshotReport(at(4 * time.Second)).FallbackBalancerMs["ai"] != 3000 {
		t.Fatal("snapshot map aliases live counters")
	}
	taken := f.o.e.takeReport(false, at(4*time.Second))
	if taken == nil || taken.FallbackBalancerCount != 1 || taken.FallbackBalancerMs["ai"] != 3000 {
		t.Fatalf("first window %+v", taken)
	}
	if next := f.o.e.snapshotReport(at(7 * time.Second)); next.FallbackBalancerCount != 0 || next.FallbackBalancerMs["ai"] != 3000 {
		t.Fatalf("ongoing window %+v", next)
	}
	f.o.e.path("ai1").quarantineUntil = time.Time{}
	f.o.e.switchTo(f.source, "ai1", "failover", at(8*time.Second))
	final := f.o.e.takeReport(true, at(9*time.Second))
	if final == nil || final.FallbackBalancerCount != 0 || final.FallbackBalancerMs["ai"] != 4000 {
		t.Fatalf("final window %+v", final)
	}
	if taken.FallbackBalancerMs["ai"] != 3000 {
		t.Fatal("take report changed after end")
	}
}

func TestFallbackBalancerEndsOnPinRemovalTargetChangeAndForget(t *testing.T) {
	for _, action := range []string{"pin", "remove", "target", "forget"} {
		t.Run(action, func(t *testing.T) {
			f := newFallbackFixture(t)
			f.exhaust()
			f.now = at(time.Second)
			f.h.FallbackSelected("main")
			f.now = at(4 * time.Second)
			switch action {
			case "pin":
				if !f.o.e.pin("ai", "ai1", f.now) {
					t.Fatal("pin rejected")
				}
			case "remove":
				f.o.e.keepSelectorsAt(map[string]bool{"main": true}, f.now)
			case "target":
				raw := proto.Clone(f.source.cfg.raw).(*SelectorConfig)
				raw.FallbackBalancer = "other"
				f.o.e.addSelectorAt("ai", newSelectorSettings(raw), f.now)
			case "forget":
				f.source.cfg.onNetworkChange = NetworkChangeMode_FORGET
				f.o.e.onNetworkChange("mobile", f.now)
			}
			if f.source.fallingBack {
				t.Fatal("lifecycle change retained fallback")
			}
			if action == "pin" || action == "remove" {
				if f.h.FallbackBalancer() != "" {
					t.Fatal("pinned or stale selector offers fallback")
				}
				f.h.FallbackSelected("main")
				if f.source.fallingBack {
					t.Fatal("stale hook revived fallback")
				}
			}
			r := f.o.e.snapshotReport(f.now)
			if r.FallbackBalancerCount != 1 || r.FallbackBalancerMs["ai"] != 3000 {
				t.Fatalf("report %+v", r.reportCounts)
			}
			events := fallbackEvents(t, f.o)
			if len(events) != 2 || events[1].Fields["to"] != "main" {
				t.Fatalf("events %+v", events)
			}
		})
	}
}

func TestFallbackBalancerDemandKeepsQuarantineRecheckAndProviderExhaustion(t *testing.T) {
	f := newFallbackFixture(t)
	f.o.s.schedule.spacing = 0
	f.exhaust()
	pc := quietProvider(nil)
	pc.Apply.Balancers = []string{"ai"}
	pc.Features.Exhausted = proto.Bool(true)
	p := newProvider(newProviderSettings(pc))
	p.cfg.exhaustedDelay = durationRange{}
	f.o.e.providers = []*provider{p}
	f.now = at(3 * time.Minute)
	if !f.o.e.idle(f.now) {
		t.Fatal("setup was not idle")
	}
	if got := f.h.Select([]string{"ai1"}); got != "" {
		t.Fatalf("source selected %q", got)
	}
	f.h.FallbackSelected("main")
	if f.o.e.idle(f.now) {
		t.Fatal("fallback traffic did not register source demand")
	}
	jobs, _ := f.o.e.tick(f.now)
	found := false
	for _, j := range jobs {
		found = found || j.tag == "ai1" && j.reason == reasonRecheck
	}
	if !found {
		t.Fatalf("source quarantine was not rechecked: %+v", jobs)
	}
	want, _, _ := f.o.e.wantedRefresh(p, f.now)
	if !want[triggerExhausted] {
		t.Fatal("fallback suppressed provider exhausted trigger")
	}
	for _, j := range jobs {
		if j.tag == "ai1" {
			f.o.e.onProbeResult(j, probeResult{ok: true}, f.now.Add(time.Second))
		}
	}
	if f.source.fallingBack || f.source.current() != "ai1" {
		t.Fatal("probe recovery did not return to source")
	}
}

func TestFallbackBalancerOnlyAfterOnDemandCandidateFails(t *testing.T) {
	te := newTestEngine(t, &Config{}, &SelectorConfig{FallbackBalancer: "main", Paths: []*PathDefinition{{Tag: "a", Cost: 1}, {Tag: "b", Cost: 2, IsLazy: true}}}, "a1", "b1")
	te.failN(3, "a1", failDial, t0)
	if te.sel.exhausted {
		t.Fatal("source exhausted before on-demand candidate")
	}
	jobs, _ := te.tick(at(time.Second))
	if len(jobs) != 1 || jobs[0].tag != "b1" {
		t.Fatalf("on-demand candidate not tried %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(2*time.Second))
	if !te.sel.exhausted {
		t.Fatal("failed on-demand candidate did not exhaust source")
	}
}

func TestFallbackBalancerConcurrentHooksAndStatus(t *testing.T) {
	f := newFallbackFixture(t)
	f.exhaust()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				f.h.FallbackBalancer()
				f.h.FallbackSelected("main")
				f.o.Status()
				f.o.PeekReport()
			}
		}()
	}
	wg.Wait()
	if f.o.e.report.counts.FallbackBalancerCount != 1 {
		t.Fatal("concurrent hooks started duplicate transition")
	}
}

func TestFallbackBalancerTargetTrafficObservesOnlyTargetPath(t *testing.T) {
	f := newFallbackFixture(t)
	f.exhaust()
	f.h.FallbackSelected("main")
	tracker := f.o.StartDial(context.Background(), "main1", net.TCPDestination(net.LocalHostIP, 443), nil)
	if tracker == nil {
		t.Fatal("target traffic has no observer")
	}
	read, write := tracker.Dialed(nil, nil, nil, nil)
	write.Add(100)
	read.Add(32768)
	tracker.Finished(nil)
	if f.o.e.path("main1").stats.connsOK != 1 || f.o.e.path("ai1").stats.connsOK != 0 || !f.source.fallingBack {
		t.Fatal("target success changed or counted as source")
	}
	r := f.o.e.snapshotReport(f.now)
	if r.Paths["main1"].BytesDown != 32768 || r.Paths["ai1"].BytesDown != 0 {
		t.Fatalf("target bytes attributed incorrectly: %+v", r.Paths)
	}
}

func TestFallbackBalancerRequiresExhaustionBeyondSelectSafetyGuards(t *testing.T) {
	f := newFallbackFixture(t)
	f.o.s.paths.Store("ai1", pathSettings{probe: "check", isProbeOnly: true})
	if f.h.Select([]string{"ai1"}) != "" {
		t.Fatal("probe-only guard failed")
	}
	if f.h.FallbackBalancer() != "" {
		t.Fatal("non-exhausted safety rejection triggered fallback")
	}
	f.h.FallbackSelected("main")
	if f.source.fallingBack {
		t.Fatal("non-exhausted safety rejection started interval")
	}
}

func TestFallbackBalancerExhaustedEventNamesTarget(t *testing.T) {
	f := newFallbackFixture(t)
	f.exhaust()
	raw, _ := f.o.DrainEvents()
	var events []Event
	if err := json.Unmarshal(raw, &events); err != nil {
		t.Fatal(err)
	}
	for _, ev := range events {
		if ev.Type == "exhausted" {
			if ev.Fields["fallback_balancer"] != "main" {
				t.Fatalf("exhausted lacks configured target %+v", ev.Fields)
			}
			return
		}
	}
	t.Fatal("exhausted event missing")
}

func TestFallbackBalancerStaleTargetConfirmationAfterReload(t *testing.T) {
	f := newFallbackFixture(t)
	f.exhaust()
	selectedTarget := f.h.FallbackBalancer()
	if selectedTarget != "main" {
		t.Fatalf("source read target=%q", selectedTarget)
	}
	f.now = at(time.Second)
	raw := proto.Clone(f.source.cfg.raw).(*SelectorConfig)
	raw.FallbackBalancer = "other"
	f.o.e.addSelectorAt("ai", newSelectorSettings(raw), f.now)
	// The router finished choosing through the old captured target.
	f.h.FallbackSelected(selectedTarget)
	if f.source.fallingBack || f.o.e.report.counts.FallbackBalancerCount != 0 {
		t.Fatal("stale route confirmation started the reloaded target")
	}
	if events := fallbackEvents(t, f.o); len(events) != 0 {
		t.Fatalf("stale confirmation emitted %+v", events)
	}
	currentTarget := f.h.FallbackBalancer()
	if currentTarget != "other" {
		t.Fatalf("reloaded target=%q", currentTarget)
	}
	f.h.FallbackSelected(currentTarget)
	events := fallbackEvents(t, f.o)
	if !f.source.fallingBack || f.o.e.report.counts.FallbackBalancerCount != 1 || len(events) != 1 || events[0].Fields["to"] != "other" {
		t.Fatalf("current confirmation did not start exactly the new target: %+v", events)
	}
}
