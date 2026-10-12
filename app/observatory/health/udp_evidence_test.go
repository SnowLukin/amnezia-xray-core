package health

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
)

func udpEvidenceObserver(t *testing.T) (*Observer, *time.Time) {
	t.Helper()
	te := newTestEngine(t, nil, nil, "a1", "b1")
	o := newUDPObserver(t)
	o.e, o.s = te.engine, te.engine.s
	o.s.passive.sweepInterval = 0
	o.e.paths["a1"].setStatus(statusUp, "traffic", t0)
	now := t0
	o.base = t0.Add(-time.Second)
	o.clock = func() time.Time { return now }
	t.Cleanup(func() { o.Close() })
	return o, &now
}

func evidenceFlow(o *Observer, tag, host string, port net.Port) *udpFlow {
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{Target: net.UDPDestination(net.ParseAddress(host), port)}})
	return o.StartUDPFlow(ctx, tag, port).(*udpFlow)
}

func sweepEvidence(o *Observer, now time.Time) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.sweepUDPEvidenceLocked(now)
}

func udpSuspectEvents(o *Observer) int64 { return o.e.report.counts.UDPSuspectCount }

func TestUDPEvidenceRequiresThreeFlowsTwoAddressPortsAndTCP(t *testing.T) {
	for _, tc := range []struct {
		name   string
		flows  int
		ports  []net.Port
		writes int64
		tcp    bool
		want   int64
	}{
		{"three flows two ports", 3, []net.Port{53, 3478, 53}, 3, true, 1},
		{"two flows", 2, []net.Port{53, 3478}, 3, true, 0},
		{"one target", 3, []net.Port{53, 53, 53}, 3, true, 0},
		{"two writes", 3, []net.Port{53, 3478, 53}, 2, true, 0},
		{"no TCP", 3, []net.Port{53, 3478, 53}, 3, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, now := udpEvidenceObserver(t)
			p := o.e.paths["a1"]
			if tc.tcp {
				p.lastTCPSuccess = *now
			}
			for i := 0; i < tc.flows; i++ {
				evidenceFlow(o, "a1", "target.example", tc.ports[i]).Uplink(30, tc.writes)
			}
			*now = t0.Add(5*time.Second - time.Nanosecond)
			if !sweepEvidence(o, *now) || udpSuspectEvents(o) != 0 {
				t.Fatal("flow classified before reply deadline")
			}
			*now = t0.Add(5 * time.Second)
			sweepEvidence(o, *now)
			if got := udpSuspectEvents(o); got != tc.want {
				t.Fatalf("suspect events %d, want %d", got, tc.want)
			}
			if p.status != statusUp || o.e.selectors["auto"].current() != "a1" {
				t.Fatal("UDP evidence changed path health or selection")
			}
			if p.stats.udp.flows != 0 {
				t.Fatal("in-flight evidence changed finish-only counters")
			}
			sweepEvidence(o, *now)
			if udpSuspectEvents(o) != tc.want {
				t.Fatal("repeated sweep emitted duplicate suspicion")
			}
		})
	}
}

func TestUDPEvidenceICEAndAnsweredFlowsSuppressSuspicion(t *testing.T) {
	for _, dead := range []int{1, 4} {
		o, now := udpEvidenceObserver(t)
		p := o.e.paths["a1"]
		p.lastTCPSuccess = *now
		for i := 0; i < dead; i++ {
			evidenceFlow(o, "a1", "target.example", net.Port(50000+i)).Uplink(30, 3)
		}
		alive := evidenceFlow(o, "a1", "live.example", 3478)
		alive.Uplink(10, 1)
		*now = t0.Add(time.Second)
		alive.Downlink(1, 1)
		if len(p.udpEvidence) != 1 || !p.udpEvidence[0].answered {
			t.Fatal("first byte did not immediately mark answered flow")
		}
		*now = t0.Add(5 * time.Second)
		sweepEvidence(o, *now)
		if udpSuspectEvents(o) != 0 {
			t.Fatal("ICE noise with a working UDP flow became suspicion")
		}
	}
}

func TestUDPEvidenceLateReplyReplacesUnansweredAndBoundsRetention(t *testing.T) {
	o, now := udpEvidenceObserver(t)
	f := evidenceFlow(o, "a1", "target.example", 3478)
	f.Uplink(30, 3)
	*now = t0.Add(5 * time.Second)
	sweepEvidence(o, *now)
	if len(f.path.udpEvidence) != 1 || f.path.udpEvidence[0].answered {
		t.Fatal("missing unanswered evidence")
	}
	*now = t0.Add(6 * time.Second)
	f.Downlink(1, 1)
	if len(f.path.udpEvidence) != 1 || !f.path.udpEvidence[0].answered {
		t.Fatal("late answer did not replace unanswered evidence")
	}
	for i := 0; i < maxUDPEvidence+8; i++ {
		next := evidenceFlow(o, "a1", "target.example", net.Port(50000+i))
		next.Uplink(30, 3)
	}
	*now = t0.Add(11 * time.Second)
	f.path.lastTCPSuccess = *now
	sweepEvidence(o, *now)
	if len(f.path.udpEvidence) > maxUDPEvidence || udpSuspectEvents(o) != 0 {
		t.Fatal("bounded evidence lost an answered flow")
	}
	*now = t0.Add(72 * time.Second)
	sweepEvidence(o, *now)
	if len(f.path.udpEvidence) != 0 {
		t.Fatal("expired evidence retained destinations")
	}
}

func TestUDPClosedEvidenceKeepsOriginalDeadlineAndFinishWindow(t *testing.T) {
	o, now := udpEvidenceObserver(t)
	p := o.e.paths["a1"]
	p.lastTCPSuccess = *now
	for _, port := range []net.Port{53, 3478, 53} {
		f := evidenceFlow(o, "a1", "target.example", port)
		f.Uplink(30, 3)
		*now = t0.Add(time.Second)
		f.Finished()
	}
	if len(o.udpEvidencePending) != 3 || p.stats.udp.flows != 3 {
		t.Fatal("closed links lost pending evidence or finish totals")
	}
	*now = t0.Add(4 * time.Second)
	if !sweepEvidence(o, *now) || udpSuspectEvents(o) != 0 {
		t.Fatal("closed links did not wait until original deadline")
	}
	*now = t0.Add(6 * time.Second)
	if sweepEvidence(o, *now) || len(o.udpEvidencePending) != 0 || udpSuspectEvents(o) != 1 {
		t.Fatal("closed links lost deadline evidence")
	}
	if p.stats.udp.flows != 3 || p.stats.udp.unanswered != 3 {
		t.Fatal("deadline duplicated finish-only counters")
	}
}

func TestUDPClosedPendingIsBounded(t *testing.T) {
	o, now := udpEvidenceObserver(t)
	for i := 0; i < maxUDPEvidencePending+4; i++ {
		f := evidenceFlow(o, "a1", "target.example", 53)
		f.Uplink(30, 3)
		f.Finished()
	}
	if len(o.udpEvidencePending) != maxUDPEvidencePending {
		t.Fatalf("pending links %d", len(o.udpEvidencePending))
	}
	*now = t0.Add(5 * time.Second)
	sweepEvidence(o, *now)
	if len(o.udpEvidencePending) != 0 {
		t.Fatal("pending links survived their deadline")
	}
}

func TestUDPEvidenceOldFlowCannotCrossNetworkReplacementOrDrop(t *testing.T) {
	for _, action := range []string{"network", "replacement", "mode", "drop"} {
		t.Run(action, func(t *testing.T) {
			o, now := udpEvidenceObserver(t)
			var flows []*udpFlow
			for _, port := range []net.Port{53, 3478, 53} {
				f := evidenceFlow(o, "a1", "target.example", port)
				f.Uplink(30, 3)
				flows = append(flows, f)
			}
			p := flows[0].path
			o.mu.Lock()
			switch action {
			case "network":
				o.e.onNetworkChange("new", t0.Add(time.Second))
			case "replacement":
				o.e.invalidateUDP("a1", t0.Add(time.Second))
			case "mode":
				o.e.invalidateUDP("a1", t0.Add(time.Second))
				o.s.paths.Store("a1", pathSettings{isProbeOnly: true})
			case "drop":
				o.e.dropProviderOutbound("a1", t0.Add(time.Second))
			}
			o.mu.Unlock()
			*now = t0.Add(6 * time.Second)
			flows[0].Downlink(1, 1)
			for _, f := range flows {
				f.Finished()
			}
			sweepEvidence(o, *now)
			if len(p.udpEvidence) != 0 || len(o.udpEvidencePending) != 0 || udpSuspectEvents(o) != 0 {
				t.Fatal("stale flow published evidence")
			}
			if action == "drop" && o.e.paths["a1"] != nil {
				t.Fatal("stale flow restored removed path")
			}
		})
	}
}

func TestUDPEvidenceOffKeepsStageOneCounters(t *testing.T) {
	o, now := udpEvidenceObserver(t)
	o.s.passive.udpReplyTimeout = 0
	p := o.e.paths["a1"]
	p.lastTCPSuccess = *now
	for _, port := range []net.Port{53, 3478, 53} {
		f := evidenceFlow(o, "a1", "target.example", port)
		f.Uplink(30, 3)
		f.Finished()
	}
	*now = t0.Add(6 * time.Second)
	if sweepEvidence(o, *now) || len(p.udpEvidence) != 0 || len(o.udpEvidencePending) != 0 || udpSuspectEvents(o) != 0 {
		t.Fatal("disabled evidence performed work")
	}
	if p.stats.udp.flows != 3 || p.stats.udp.unanswered != 3 || p.stats.udp.writes != 9 {
		t.Fatal("disabling evidence changed stage-one totals")
	}
}

func TestUDPEvidenceLaterTCPSuccessReevaluatesAndUDPDoesNotSupplyTCP(t *testing.T) {
	o, now := udpEvidenceObserver(t)
	for _, port := range []net.Port{53, 3478, 53} {
		evidenceFlow(o, "a1", "target.example", port).Uplink(30, 3)
	}
	*now = t0.Add(5 * time.Second)
	sweepEvidence(o, *now)
	p := o.e.paths["a1"]
	tracker := &connTracker{o: o, tag: "a1", network: o.e.generation, udpPath: p, udpEpoch: o.e.udpEpoch, udpIncarnation: p.udpIncarnation, udp: true}
	o.success(tracker)
	if !p.lastTCPSuccess.IsZero() || udpSuspectEvents(o) != 0 {
		t.Fatal("UDP traffic success supplied TCP prerequisite")
	}
	tracker.udp = false
	*now = t0.Add(6 * time.Second)
	o.success(tracker)
	if p.lastTCPSuccess != *now || udpSuspectEvents(o) != 1 {
		t.Fatal("later TCP success did not reevaluate existing evidence")
	}
}

func TestUDPEvidenceSelectedPathAndRecentTCPSuccessOnly(t *testing.T) {
	for _, tag := range []string{"a1", "b1"} {
		o, now := udpEvidenceObserver(t)
		for _, port := range []net.Port{53, 3478, 53} {
			evidenceFlow(o, tag, "target.example", port).Uplink(30, 3)
		}
		p := o.e.paths[tag]
		p.lastTCPSuccess = t0.Add(-time.Minute)
		*now = t0.Add(5 * time.Second)
		sweepEvidence(o, *now)
		if udpSuspectEvents(o) != 0 {
			t.Fatal("expired TCP success supplied prerequisite")
		}
		p.lastTCPSuccess = *now
		sweepEvidence(o, *now)
		want := int64(0)
		if tag == "a1" {
			want = 1
		}
		if udpSuspectEvents(o) != want {
			t.Fatalf("selected-only evidence tag %s, events %d", tag, udpSuspectEvents(o))
		}
	}
}

func TestUDPClosedPendingInvalidatedBeforeDeadline(t *testing.T) {
	o, now := udpEvidenceObserver(t)
	f := evidenceFlow(o, "a1", "target.example", 53)
	f.Uplink(30, 3)
	f.Finished()
	if len(o.udpEvidencePending) != 1 {
		t.Fatal("fixture has no pending closed link")
	}
	o.e.onNetworkChange("new", t0.Add(time.Second))
	*now = t0.Add(2 * time.Second)
	if sweepEvidence(o, *now) || len(o.udpEvidencePending) != 0 || len(f.path.udpEvidence) != 0 {
		t.Fatal("old pending link survived network invalidation")
	}
}

func TestUDPEvidenceTenMinuteCooldownWithFreshFlows(t *testing.T) {
	o, now := udpEvidenceObserver(t)
	p := o.e.paths["a1"]
	for _, start := range []time.Duration{0, time.Minute, 10 * time.Minute} {
		*now = t0.Add(start)
		p.lastTCPSuccess = *now
		for _, port := range []net.Port{53, 3478, 53} {
			evidenceFlow(o, "a1", "target.example", port).Uplink(30, 3)
		}
		*now = t0.Add(start + 5*time.Second)
		sweepEvidence(o, *now)
		want := int64(1)
		if start == 10*time.Minute {
			want = 2
		}
		if udpSuspectEvents(o) != want {
			t.Fatalf("suspicions at %v: %d, want %d", start, udpSuspectEvents(o), want)
		}
	}
}

func TestUDPEvidenceOngoingRepliesSuppressLaterICEUntilRepliesExpire(t *testing.T) {
	o, now := udpEvidenceObserver(t)
	p := o.e.paths["a1"]
	alive := evidenceFlow(o, "a1", "live.example", 3478)
	alive.Uplink(30, 3)
	alive.Downlink(1, 1)
	if len(p.udpEvidence) != 1 || !p.udpEvidence[0].answered || p.udpEvidence[0].at != t0 {
		t.Fatal("first reply was not recorded immediately")
	}
	*now = t0.Add(65 * time.Second)
	for _, port := range []net.Port{50000, 50001, 50000} {
		evidenceFlow(o, "a1", "ice.example", port).Uplink(30, 3)
	}
	*now = t0.Add(70 * time.Second)
	alive.Downlink(1, 1)
	p.lastTCPSuccess = *now
	sweepEvidence(o, *now)
	if udpSuspectEvents(o) != 0 {
		t.Fatal("ongoing UDP replies allowed false ICE suspicion after the initial window")
	}
	answeredAt := time.Time{}
	for _, record := range p.udpEvidence {
		if record.answered {
			answeredAt = record.at
		}
	}
	if answeredAt != *now {
		t.Fatalf("answered record was not refreshed from new bytes: %v", answeredAt)
	}
	*now = t0.Add(125 * time.Second)
	for _, port := range []net.Port{50000, 50001, 50000} {
		evidenceFlow(o, "a1", "ice.example", port).Uplink(30, 3)
	}
	*now = t0.Add(130 * time.Second)
	alive.Downlink(0, 1)
	p.lastTCPSuccess = *now
	sweepEvidence(o, *now)
	if udpSuspectEvents(o) != 0 {
		t.Fatal("last real reply stopped suppressing at the inclusive window boundary")
	}
	*now = t0.Add(130*time.Second + time.Nanosecond)
	sweepEvidence(o, *now)
	if udpSuspectEvents(o) != 1 {
		t.Fatal("zero-byte packet or old cumulative bytes kept expired replies alive")
	}
}

func TestUDPEvidenceClosedAnsweredFlowDoesNotRefreshWithoutNewBytes(t *testing.T) {
	o, now := udpEvidenceObserver(t)
	p := o.e.paths["a1"]
	alive := evidenceFlow(o, "a1", "live.example", 3478)
	alive.Uplink(30, 3)
	alive.Downlink(1, 1)
	*now = t0.Add(70 * time.Second)
	alive.Finished()
	for _, port := range []net.Port{50000, 50001, 50000} {
		evidenceFlow(o, "a1", "ice.example", port).Uplink(30, 3)
	}
	*now = t0.Add(75 * time.Second)
	p.lastTCPSuccess = *now
	sweepEvidence(o, *now)
	if udpSuspectEvents(o) != 1 {
		t.Fatal("closing a previously answered link refreshed expired proof without new bytes")
	}
}
