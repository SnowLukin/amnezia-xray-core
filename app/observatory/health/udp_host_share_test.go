package health

import (
	"slices"
	"testing"
	"time"
)

func udpHostShareFixture(t *testing.T) *testEngine {
	t.Helper()
	te := udpEngineFixture(t)
	te.s.schedule.perMinute = 12
	te.s.schedule.hostProbes.perMinute = 4
	te.s.schedule.probesPerServerNamePerMinute = 3
	te.serverNames(map[string]string{"a1": "same.example", "a2": "same.example"})
	return te
}

func TestUDPCompletedAndCanceledStartDoesNotConsumeTCPHostHalfSNI(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		name := "completed"
		if canceled {
			name = "canceled"
		}
		t.Run(name, func(t *testing.T) {
			te := udpHostShareFixture(t)
			udp := te.enqueueUDP("a1", reasonUDPSwitch, "auto", t0)
			jobs, _ := te.tick(t0)
			if len(jobs) != 1 || jobs[0] != udp {
				t.Fatalf("UDP did not start: %+v", jobs)
			}
			te.onUDPProbeResult(udp, udpProbeResult{state: "ok", answered: 5, sent: 5, canceled: canceled}, at(3*time.Second))
			request := te.ask(t, at(4*time.Second), "a1")
			jobs, _ = te.tick(at(4 * time.Second))
			if len(jobs) != 1 || jobs[0].reason != reasonHost || jobs[0].isUDP || jobs[0].request != request.id {
				t.Fatalf("UDP consumed the TCP host share: %+v", jobs)
			}
			te.onProbeResult(jobs[0], probeResult{ok: true, state: statusUp}, at(5*time.Second))
			if len(te.startsByName["same.example"]) != 2 || len(te.udpStarts) != 1 {
				t.Fatal("actual starts were refunded")
			}
			optional := te.enqueueUDP("a1", reasonUDPSuspect, "auto", at(6*time.Second))
			jobs, wake := te.tick(at(6 * time.Second))
			if len(jobs) != 0 || wake != at(64*time.Second) || te.running[optional.id] != nil {
				t.Fatalf("UDP admission lost its previous half-SNI limit: jobs=%+v wake=%v", jobs, wake)
			}
		})
	}
}

func TestUDPPhysicalSNIQuotaStillCountsUDPAfterHostShareQuery(t *testing.T) {
	te := udpHostShareFixture(t)
	udp := te.enqueueUDP("a1", reasonUDPSwitch, "auto", t0)
	te.tick(t0)
	te.onUDPProbeResult(udp, udpProbeResult{state: "ok", answered: 5, sent: 5}, at(time.Second))
	for index := 1; index <= 2; index++ {
		now := at(time.Duration(index) * time.Second)
		tcp := te.enqueue("a2", reasonNetwork, "auto", now)
		jobs, _ := te.tick(now)
		if len(jobs) != 1 || jobs[0] != tcp {
			t.Fatalf("TCP setup failed: %+v", jobs)
		}
		te.onProbeResult(tcp, probeResult{ok: true, state: statusUp}, now)
	}
	before := slices.Clone(te.startsByName["same.example"])
	te.ask(t, at(20*time.Second), "a1")
	te.tick(at(20 * time.Second))
	if !slices.Equal(before, te.startsByName["same.example"]) || len(before) != 3 {
		t.Fatal("TCP host filtering changed the full physical SNI quota")
	}
	mandatory := &probeJob{tag: "a2", reason: reasonNetwork}
	if wait := te.budgetWait(mandatory, at(20*time.Second)); wait != 40*time.Second {
		t.Fatalf("total SNI cap3 no longer blocks mandatory TCP: %v", wait)
	}
}

func TestUDPHostHalfSNIKeepsPreviousTCPStarts(t *testing.T) {
	te := udpHostShareFixture(t)
	udp := te.enqueueUDP("a1", reasonUDPSwitch, "auto", t0)
	te.tick(t0)
	te.onUDPProbeResult(udp, udpProbeResult{state: "ok", answered: 5, sent: 5}, at(time.Second))
	tcp := te.enqueue("a2", reasonNetwork, "auto", at(time.Second))
	jobs, _ := te.tick(at(time.Second))
	if len(jobs) != 1 || jobs[0] != tcp {
		t.Fatalf("TCP setup failed: %+v", jobs)
	}
	te.onProbeResult(tcp, probeResult{ok: true, state: statusUp}, at(time.Second))
	te.ask(t, at(20*time.Second), "a1")
	jobs, wake := te.tick(at(20 * time.Second))
	if len(jobs) != 0 || wake != at(61*time.Second) {
		t.Fatalf("previous TCP did not retain its host half-SNI deadline: jobs=%+v wake=%v", jobs, wake)
	}
}

func TestUDPHostSharePruningDoesNotRefundHourlyStarts(t *testing.T) {
	te := udpHostShareFixture(t)
	te.s.udpProbe.perHour = 1
	udp := te.enqueueUDP("a1", reasonUDPSwitch, "auto", t0)
	te.tick(t0)
	te.onUDPProbeResult(udp, udpProbeResult{state: "ok", answered: 5, sent: 5}, at(time.Second))
	te.ask(t, at(61*time.Second), "a2")
	jobs, _ := te.tick(at(61 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonHost || len(te.udpStartsByName["same.example"]) != 0 || len(te.udpStarts) != 1 {
		t.Fatalf("minute receipts were not pruned independently: jobs=%+v UDP=%v", jobs, te.udpStarts)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true, state: statusUp}, at(62*time.Second))
	next := te.enqueueUDP("a1", reasonUDPSuspect, "auto", at(63*time.Second))
	jobs, wake := te.tick(at(63 * time.Second))
	if len(jobs) != 0 || wake != at(time.Hour) || te.running[next.id] != nil || !te.path("a1").udpSkips["per_hour"] {
		t.Fatalf("minute pruning refunded perHour: jobs=%+v wake=%v", jobs, wake)
	}
}

func TestUDPHostShareSubtractsEachEqualTimestampOnlyOnce(t *testing.T) {
	te := udpHostShareFixture(t)
	te.s.schedule.probesPerServerNamePerMinute = 6
	for range 2 {
		udp := te.enqueueUDP("a1", reasonUDPSuspect, "auto", t0)
		jobs, _ := te.tick(t0)
		if len(jobs) != 1 || jobs[0] != udp {
			t.Fatalf("UDP setup failed: %+v", jobs)
		}
		te.onUDPProbeResult(udp, udpProbeResult{state: "ok", answered: 5, sent: 5}, t0)
	}
	for range 3 {
		tcp := te.enqueue("a2", reasonNetwork, "auto", t0)
		jobs, _ := te.tick(t0)
		if len(jobs) != 1 || jobs[0] != tcp {
			t.Fatalf("TCP setup failed: %+v", jobs)
		}
		te.onProbeResult(tcp, probeResult{ok: true, state: statusUp}, t0)
	}
	before := slices.Clone(te.startsByName["same.example"])
	te.ask(t, at(20*time.Second), "a1")
	jobs, wake := te.tick(at(20 * time.Second))
	if len(jobs) != 0 || wake != at(time.Minute) || len(before) != 5 || !slices.Equal(before, te.startsByName["same.example"]) ||
		len(te.udpStartsByName["same.example"]) != 2 || len(te.udpStarts) != 2 {
		t.Fatalf("equal UDP receipts erased TCP slots: jobs=%+v wake=%v starts=%v", jobs, wake, te.startsByName["same.example"])
	}
}

func TestUDPSNIReceiptsPrunedWithoutTCPHostLookups(t *testing.T) {
	te := udpHostShareFixture(t)
	for index := range 6 {
		now := at(time.Duration(index) * 61 * time.Second)
		if _, _, err := te.requestProbeKind("auto", []string{"a1"}, true, now); err != nil {
			t.Fatal(err)
		}
		jobs, _ := te.tick(now)
		if len(jobs) != 1 || !jobs[0].isUDP {
			t.Fatalf("UDP did not start: %+v", jobs)
		}
		te.onUDPProbeResult(jobs[0], udpProbeResult{state: "ok", answered: 5, sent: 5}, now)
		if !slices.Equal(te.udpStartsByName["same.example"], []time.Time{now}) || len(te.udpStarts) != index+1 {
			t.Fatal("minute receipts grew without TCP host lookups or refunded hourly starts")
		}
	}
	te = udpHostShareFixture(t)
	te.s.schedule.probesPerServerNamePerMinute = 4
	for _, now := range []time.Time{t0, at(50 * time.Second)} {
		if _, _, err := te.requestProbeKind("auto", []string{"a1"}, true, now); err != nil {
			t.Fatal(err)
		}
		jobs, _ := te.tick(now)
		if len(jobs) != 1 || !jobs[0].isUDP {
			t.Fatalf("UDP GC setup failed: %+v", jobs)
		}
		te.onUDPProbeResult(jobs[0], udpProbeResult{state: "ok", answered: 5, sent: 5}, now)
	}
	te.gc(at(70*time.Second), staleAfter)
	if !slices.Equal(te.udpStartsByName["same.example"], []time.Time{at(50 * time.Second)}) || len(te.udpStarts) != 2 {
		t.Fatal("GC retained expired minute receipts next to a live tail or refunded hourly starts")
	}
}
