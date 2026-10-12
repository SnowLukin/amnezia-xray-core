package health

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

func udpEngineFixture(t *testing.T) *testEngine {
	t.Helper()
	te := newTestEngine(t, &Config{Schedule: &ScheduleConfig{IdleAfter: int64(time.Hour)}}, nil, "a1", "a2")
	te.s.udpProbe = &udpSettings{requests: 5, interval: time.Millisecond, timeout: time.Second, perHour: 6}
	te.s.schedule.spacing = 0
	te.s.schedule.perMinute = 100
	te.s.schedule.probesPerServerNamePerMinute = 100
	te.s.schedule.hostProbes.perMinute = 100
	te.sel.returnToCheaperAt = time.Time{}
	return te
}

func TestUDPOutcomesLeaveHealthAndSelectionUnchanged(t *testing.T) {
	for _, state := range []string{"ok", "lossy", "dead", "unsupported"} {
		t.Run(state, func(t *testing.T) {
			te := udpEngineFixture(t)
			p := te.path("a1")
			p.status, p.reason = statusSuspect, "traffic"
			p.quarantineUntil = at(time.Minute)
			p.quarantineLevel = 3
			p.lastProbe, p.lastProbeEnd = at(time.Second), at(2*time.Second)
			p.failures = []failure{{at: t0, kind: failNoResponse, target: "first"}}
			beforeFailures := append([]failure(nil), p.failures...)
			beforeStatus, beforeReason, beforeQuarantine := p.status, p.reason, p.quarantineUntil
			beforeChoice, beforeLastProbe, beforeEnd := te.sel.current(), p.lastProbe, p.lastProbeEnd
			j := te.enqueueUDP("a1", reasonUDPSuspect, "auto", at(3*time.Second))
			te.begin(j, at(3*time.Second))
			te.onUDPProbeResult(j, udpProbeResult{state: state, answered: 2, sent: 5, rtt: 20 * time.Millisecond}, at(4*time.Second))
			if p.status != beforeStatus || p.reason != beforeReason || p.quarantineUntil != beforeQuarantine || p.quarantineLevel != 3 ||
				!reflect.DeepEqual(beforeFailures, p.failures) || te.sel.current() != beforeChoice || p.lastProbe != beforeLastProbe || p.lastProbeEnd != beforeEnd || p.stats.probes != 0 {
				t.Fatalf("UDP %s changed path health or choice: %+v", state, p)
			}
			if p.udp.state != state || p.stats.udpProbes != 1 || len(te.running) != 0 {
				t.Fatal("UDP result was not recorded independently")
			}
			te.onUDPProbeResult(j, udpProbeResult{state: state}, at(5*time.Second))
			if p.stats.udpProbes != 1 {
				t.Fatal("duplicate result was counted")
			}
		})
	}
}

func TestUDPWaitsForQueuedAndRunningTCP(t *testing.T) {
	te := udpEngineFixture(t)
	udp := te.enqueueUDP("a1", reasonUDPSwitch, "auto", t0)
	tcp := te.enqueue("a2", reasonNetwork, "auto", at(time.Minute))
	if jobs, _ := te.tick(at(time.Second)); len(jobs) != 0 {
		t.Fatal("UDP passed a future TCP job")
	}
	te.queue = []*probeJob{udp}
	te.begin(tcp, at(time.Second))
	if jobs, _ := te.tick(at(2 * time.Second)); len(jobs) != 0 {
		t.Fatal("UDP passed a running TCP job")
	}
	te.onProbeResult(tcp, probeResult{ok: true, state: statusUp}, at(3*time.Second))
	jobs, _ := te.tick(at(4 * time.Second))
	if len(jobs) != 1 || jobs[0] != udp {
		t.Fatalf("UDP did not resume after TCP: %+v", jobs)
	}
}

func TestUDPWaitsForCandidateAndAudit(t *testing.T) {
	te := probeOnlyTestEngine(t, 1, &PathDefinition{Tag: "real", Cost: 1}, &PathDefinition{Tag: "candidate", Cost: 2, IsProbeOnly: true})
	te.s.udpProbe = &udpSettings{requests: 5, perHour: 6}
	te.enqueueUDP("real", reasonUDPSwitch, "auto", t0)
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0].reason != reasonCandidate || jobs[0].isUDP {
		t.Fatalf("UDP passed a candidate: %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true, state: statusUp}, at(time.Second))
	p := te.path("real")
	p.status = statusDown
	p.downID++
	audit := te.enqueue("real", reasonAudit, "auto", at(10*time.Second))
	audit.auditDown = p.downID
	jobs, _ = te.tick(at(2 * time.Second))
	if len(jobs) != 0 {
		t.Fatal("UDP passed a future audit")
	}
}

func TestUDPRollingBudgetSurvivesNetworkAndReservesTCP(t *testing.T) {
	te := udpEngineFixture(t)
	for i := range 6 {
		now := at(time.Duration(i) * time.Minute)
		j := te.enqueueUDP("a1", reasonUDPSuspect, "auto", now)
		jobs, _ := te.tick(now)
		if len(jobs) != 1 || jobs[0] != j {
			t.Fatalf("UDP budget fixture did not start request %d: %+v", i, jobs)
		}
		te.onUDPProbeResult(j, udpProbeResult{state: "ok", sent: 5, answered: 5}, now)
	}
	te.onNetworkChange("new", at(6*time.Minute))
	te.queue = nil
	te.s.schedule.perMinute = 4
	te.s.schedule.probesPerServerNamePerMinute = 4
	j := te.enqueueUDP("a1", reasonUDPSwitch, "auto", at(7*time.Minute))
	if jobs, _ := te.tick(at(7 * time.Minute)); len(jobs) != 0 {
		t.Fatal("network reset the global UDP hour limit")
	}
	te.starts = []time.Time{at(60 * time.Minute), at(60*time.Minute + time.Second)}
	te.startsByName[te.serverName("a1")] = append([]time.Time(nil), te.starts...)
	te.udpStarts = nil
	j.notBefore = at(60*time.Minute + 2*time.Second)
	if wait := te.budgetWait(j, j.notBefore); wait <= 0 {
		t.Fatal("UDP consumed the urgent TCP reserve")
	}
	tcp := &probeJob{tag: "a1", reason: reasonConfirm}
	if wait := te.budgetWait(tcp, j.notBefore); wait != 0 {
		t.Fatal("urgent TCP could not use its reserved budget")
	}
}

func TestUDPSwitchStartsOnlyAfterUserWriteOncePerPathAndNetwork(t *testing.T) {
	te := udpEngineFixture(t)
	p := te.path("a1")
	if jobs, _ := te.tick(at(30 * time.Second)); len(jobs) != 0 {
		t.Fatal("selection alone started UDP")
	}
	if !te.onUDPUserWrite(p, at(time.Second)) {
		t.Fatal("first user write did not schedule UDP")
	}
	if jobs, _ := te.tick(at(20 * time.Second)); len(jobs) != 0 {
		t.Fatal("UDP started before twenty seconds")
	}
	jobs, _ := te.tick(at(21 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonUDPSwitch {
		t.Fatal("due switch probe did not start")
	}
	te.onUDPProbeResult(jobs[0], udpProbeResult{state: "dead"}, at(22*time.Second))
	te.switchTo(te.sel, "a2", "test", at(23*time.Second))
	te.switchTo(te.sel, "a1", "test", at(24*time.Second))
	if te.onUDPUserWrite(p, at(25*time.Second)) {
		t.Fatal("return to a path repeated its switch probe")
	}
	te.sel.cfg.onNetworkChange = NetworkChangeMode_IGNORE
	generation := te.generation
	te.onNetworkChange("ignored", at(26*time.Second))
	if te.generation != generation || p.udp.state != "" || p.udpSwitchDone {
		t.Fatal("IGNORE did not reset only UDP observation")
	}
	if !te.onUDPUserWrite(p, at(27*time.Second)) {
		t.Fatal("new network did not permit switch observation")
	}
}

func TestUDPStaleNetworkReplacementAndCanceledResultsAreDropped(t *testing.T) {
	for _, invalidate := range []string{"network", "replacement", "canceled", "removed"} {
		t.Run(invalidate, func(t *testing.T) {
			te := udpEngineFixture(t)
			te.sel.cfg.onNetworkChange = NetworkChangeMode_IGNORE
			p := te.path("a1")
			j := te.enqueueUDP("a1", reasonHost, "auto", t0)
			te.begin(j, t0)
			canceled := false
			j.cancel = func() { canceled = true }
			switch invalidate {
			case "network":
				te.onNetworkChange("new", at(time.Second))
			case "replacement":
				te.resetPath("a1", at(time.Second))
			case "removed":
				te.forget("a1", at(time.Second))
			}
			te.onUDPProbeResult(j, udpProbeResult{state: "dead", canceled: invalidate == "canceled"}, at(2*time.Second))
			if p.stats.udpProbes != 0 || p.udp.state != "" || len(te.running) != 0 {
				t.Fatal("stale UDP result was published")
			}
			if invalidate != "canceled" && !canceled {
				t.Fatal("old UDP job was not canceled")
			}
		})
	}
}

func TestUDPBlockedWorkDoesNotWakeIdleOrSleepingPath(t *testing.T) {
	for _, why := range []string{"idle", "memory", "asleep", "down", "network_down", "replacing", "retiring"} {
		t.Run(why, func(t *testing.T) {
			te := udpEngineFixture(t)
			p := te.path("a1")
			p.udpSwitchPending = true
			p.udpSwitchDue = at(time.Second)
			switch why {
			case "idle":
				te.lastDemand.Store(0)
			case "memory":
				te.memoryLow = true
			case "asleep":
				p.asleep = true
				p.awake = false
			case "down":
				p.status = statusDown
			case "network_down":
				te.networkDown = true
			case "replacing":
				te.replacing[p.tag] = true
			case "retiring":
				te.retiring[p.tag] = true
			}
			uses := p.uses
			if next := te.scheduleUDP(at(2 * time.Second)); !next.IsZero() || len(te.queue) != 0 || p.uses != uses {
				t.Fatal("blocked optional work scheduled an idle wake or used the path")
			}
		})
	}
}

func TestUDPSuspicionIsRateLimitedAndDoesNotChangeHealth(t *testing.T) {
	te := udpEngineFixture(t)
	p := te.path("a1")
	before := p.status
	if !te.onUDPSuspect(p, 3, 2, t0) || te.onUDPSuspect(p, 3, 2, at(9*time.Minute)) {
		t.Fatal("UDP suspicion cooldown was not enforced")
	}
	if p.status != before || te.sel.current() != p.tag {
		t.Fatal("UDP suspicion changed health or selection")
	}
	if !te.onUDPSuspect(p, 3, 2, at(10*time.Minute)) {
		t.Fatal("UDP suspicion did not resume after cooldown")
	}
}

func TestProbeOnlyCredentialsAuthorizeOnlyMandatoryHTTPJobs(t *testing.T) {
	te := probeOnlyTestEngine(t, 1, &PathDefinition{Tag: "real", Cost: 1}, &PathDefinition{Tag: "candidate", Cost: 2, IsProbeOnly: true})
	o := &Observer{s: te.s, e: te.engine}
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0].reason != reasonCandidate {
		t.Fatal("candidate fixture did not start")
	}
	j := jobs[0]
	ctx := o.probeJobContext(context.Background(), j)
	var bytes atomic.Int64
	generic := withProbe(ctx, &bytes)
	if o.AllowDispatch(generic, "candidate", true) {
		t.Fatal("generic probe context authorized probe-only dispatch")
	}
	state := probeStateFrom(generic)
	state.credential, _ = ctx.Value(probeCredentialKey{}).(*probeCredential)
	if !o.AllowDispatch(generic, "candidate", true) {
		t.Fatal("mandatory candidate credential rejected")
	}
	te.onNetworkChange("new", at(time.Second))
	if o.AllowDispatch(generic, "candidate", true) {
		t.Fatal("old network credential survived")
	}
	j = te.enqueue("candidate", reasonHost, "auto", at(2*time.Second))
	te.begin(j, at(2*time.Second))
	ctx = o.probeJobContext(context.Background(), j)
	generic = withProbe(ctx, &bytes)
	probeStateFrom(generic).credential, _ = ctx.Value(probeCredentialKey{}).(*probeCredential)
	if !o.AllowDispatch(generic, "candidate", true) {
		t.Fatal("mandatory host credential rejected")
	}
	te.s.paths.Store("candidate", pathSettings{})
	if o.AllowDispatch(generic, "candidate", true) {
		t.Fatal("old probe-only handler remained usable after promotion")
	}
	te.s.paths.Store("real", pathSettings{isProbeOnly: true})
	if o.AllowDispatch(withProbe(context.Background(), &bytes), "real", false) {
		t.Fatal("generic real job survived demotion")
	}
}

func TestUDPObservationSurvivesHTTPProfileReset(t *testing.T) {
	te := udpEngineFixture(t)
	p := te.path("a1")
	j := te.enqueueUDP("a1", reasonUDPSwitch, "auto", t0)
	te.begin(j, t0)
	incarnation := p.udpIncarnation
	te.resetProbeProfile("a1", at(time.Second))
	if j.invalidated || p.udpIncarnation != incarnation {
		t.Fatal("HTTP profile reset invalidated UDP transport")
	}
	te.onUDPProbeResult(j, udpProbeResult{state: "ok", answered: 5, sent: 5}, at(2*time.Second))
	if p.udp.state != "ok" {
		t.Fatal("HTTP profile reset dropped UDP observation")
	}
}

func TestQueuedUDPDoesNotKeepUnusedTunnelAwake(t *testing.T) {
	te := udpEngineFixture(t)
	p := te.path("a2")
	p.awake, p.sleeper = true, sleeperYes
	te.enqueueUDP("a2", reasonHost, "auto", t0)
	if te.heldAwake("a2", false) {
		t.Fatal("queued optional UDP held an unused tunnel awake")
	}
}

func TestProbeTrafficDoesNotScheduleUDPSwitchOrRefreshDemand(t *testing.T) {
	te := udpEngineFixture(t)
	o := &Observer{s: te.s, e: te.engine}
	p := te.path("a1")
	var bytes atomic.Int64
	ctx := withProbe(context.Background(), &bytes)
	tracker := o.StartDial(ctx, "a1", net.Destination{}, nil)
	_, write := tracker.Dialed(nil, nil, nil, nil)
	write.Add(500)
	if !p.udpSwitchDue.IsZero() || te.lastDemand.Load() != t0.UnixNano() || bytes.Load() != 500 {
		t.Fatal("probe bytes became user demand")
	}
}
