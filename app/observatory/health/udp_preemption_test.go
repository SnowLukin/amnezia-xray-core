package health

import (
	"context"
	"reflect"
	"slices"
	"testing"
	"time"
)

func TestUDPPreemptedByReadyTCPWorkWithoutUDPSpacing(t *testing.T) {
	for _, reason := range []probeReason{reasonConnectivity, reasonConfirm, reasonFailover} {
		t.Run(reason.String(), func(t *testing.T) {
			te := udpEngineFixture(t)
			te.s.schedule.spacing = 4 * time.Second
			te.s.schedule.spacingJitter = 0
			te.s.udpProbe.perHour = 1
			p := te.path("a1")
			p.udp = udpSnapshot{state: "ok", answered: 5, sent: 5, at: at(-time.Second), reason: reasonHost}
			previousSnapshot := p.udp
			udp := te.enqueueUDP("a1", reasonUDPSwitch, "auto", t0)
			if jobs, _ := te.tick(t0); len(jobs) != 1 || jobs[0] != udp {
				t.Fatalf("UDP did not start: %+v", jobs)
			}
			cancelCalls := 0
			udp.cancel = func() { cancelCalls++ }
			minuteStarts := slices.Clone(te.starts)
			hostStarts := slices.Clone(te.hostStarts)
			sniStarts := slices.Clone(te.startsByName[te.serverName("a1")])
			var tcp *probeJob
			switch reason {
			case reasonConnectivity:
				te.enqueueConnectivity(at(time.Second))
				tcp = te.queue[0]
			case reasonConfirm:
				p.status = statusSuspect
				tcp = te.enqueue("a1", reasonConfirm, "", at(time.Second))
			case reasonFailover:
				tcp = te.enqueue("a2", reasonFailover, "auto", at(time.Second))
				te.sel.failoverProbing = map[string]bool{"a2": true}
			}
			if jobs, _ := te.tick(at(time.Second)); len(jobs) != 0 {
				t.Fatalf("TCP ran before UDP cleanup: %+v", jobs)
			}
			te.tick(at(time.Second))
			if cancelCalls != 1 || !udp.invalidated || len(te.spacedUntil) != 0 || te.running[udp.id] != udp {
				t.Fatalf("preemption did not retain cleanup slot: cancels=%d running=%v spacing=%v", cancelCalls, te.running, te.spacedUntil)
			}
			if !slices.Equal(te.starts, minuteStarts) || !slices.Equal(te.hostStarts, hostStarts) ||
				!slices.Equal(te.startsByName[te.serverName("a1")], sniStarts) || len(te.udpStarts) != 1 {
				t.Fatal("preemption refunded actual-start quotas")
			}
			// A completion racing cancellation is stale even if it reports success.
			te.onUDPProbeResult(udp, udpProbeResult{state: "ok", answered: 5, sent: 5, canceled: reason == reasonConnectivity}, at(time.Second))
			if p.udp != previousSnapshot || p.stats.udpProbes != 0 || p.stats.udpProbesDead != 0 || len(eventsOf(drainEvents(te), "udp_probe")) != 0 {
				t.Fatal("canceled or stale UDP completion published observation")
			}
			jobs, _ := te.tick(at(time.Second + time.Millisecond))
			if len(jobs) != 1 || jobs[0] != tcp || jobs[0].isUDP {
				t.Fatalf("ready TCP retained UDP spacing: %+v", jobs)
			}
			te.onProbeResult(tcp, probeResult{ok: true, state: statusUp}, at(2*time.Second))
			nextTag := te.sel.current()
			next := te.enqueueUDP(nextTag, reasonUDPSwitch, "auto", at(3*time.Second))
			keep, _ := te.tickUDP(next, at(3*time.Second), false, nil)
			if !keep || len(te.udpStarts) != 1 || te.running[next.id] != nil || !te.path(nextTag).udpSkips["per_hour"] {
				t.Fatal("cancellation released the rolling-hour quota")
			}
		})
	}
}

func TestUDPPreemptionPreservesTCPSpacingWithEqualReceipts(t *testing.T) {
	te := udpEngineFixture(t)
	te.s.schedule.concurrent = 2
	te.s.schedule.spacing = 4 * time.Second
	te.s.schedule.spacingJitter = 0
	te.serverNames(map[string]string{"a1": "udp.example", "a2": "tcp.example"})
	first := te.enqueue("a2", reasonNetwork, "auto", t0)
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0] != first {
		t.Fatalf("first TCP did not start: %+v", jobs)
	}
	te.onProbeResult(first, probeResult{ok: true, state: statusUp}, t0)
	udp := te.enqueueUDP("a1", reasonUDPSwitch, "auto", t0)
	jobs, _ = te.tick(t0)
	if len(jobs) != 1 || jobs[0] != udp || len(te.spacedUntil) != 2 || te.spacedUntil[0] != te.spacedUntil[1] {
		t.Fatalf("equal TCP/UDP spacing fixture did not start: %+v %v", jobs, te.spacedUntil)
	}
	second := te.enqueue("a2", reasonNetwork, "auto", at(time.Second))
	jobs, _ = te.tick(at(time.Second))
	if len(jobs) != 1 || jobs[0] != second || !udp.invalidated {
		t.Fatalf("TCP failed to preempt UDP: %+v", jobs)
	}
	te.onUDPProbeResult(udp, udpProbeResult{state: "ok", answered: 5, sent: 5}, at(time.Second))
	third := te.enqueue("a1", reasonNetwork, "auto", at(time.Second+time.Millisecond))
	jobs, wake := te.tick(at(time.Second + time.Millisecond))
	if len(jobs) != 0 || wake != at(4*time.Second) {
		t.Fatalf("preemption lost previous TCP spacing: jobs=%+v wake=%v", jobs, wake)
	}
	te.onProbeResult(second, probeResult{ok: true, state: statusUp}, at(2*time.Second))
	jobs, _ = te.tick(at(4 * time.Second))
	if len(jobs) != 1 || jobs[0] != third {
		t.Fatalf("TCP did not resume at its original spacing deadline: %+v", jobs)
	}
}

func TestUDPPreemptionCancelsBlockedWorkerBeforeReleasingSlot(t *testing.T) {
	te := udpEngineFixture(t)
	udp := te.enqueueUDP("a1", reasonUDPSwitch, "auto", t0)
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0] != udp {
		t.Fatalf("UDP did not start: %+v", jobs)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	udp.cancel = cancel
	conn := newBlockingSTUNConn(0)
	results := make(chan udpProbeResult, 1)
	go func() {
		results <- runSTUN(ctx, conn, &udpSettings{requests: 5, interval: time.Millisecond, timeout: time.Hour})
	}()
	<-conn.entered
	tcp := te.enqueue("a2", reasonFailover, "auto", at(time.Second))
	te.sel.failoverProbing = map[string]bool{"a2": true}
	jobs, _ = te.tick(at(time.Second))
	if len(jobs) != 0 || te.running[udp.id] != udp {
		t.Fatal("UDP slot was freed before cleanup")
	}
	select {
	case r := <-results:
		if !r.canceled || r.sent != 0 || !conn.readExited.Load() {
			t.Fatalf("worker not canceled/joined: %+v", r)
		}
		select {
		case <-conn.writeDone:
		default:
			t.Fatal("blocked writer remains running")
		}
		te.onUDPProbeResult(udp, r, at(time.Second))
	case <-time.After(time.Second):
		_ = conn.Close()
		t.Fatal("preemption did not finish UDP worker")
	}
	jobs, _ = te.tick(at(time.Second + time.Millisecond))
	if len(jobs) != 1 || jobs[0] != tcp {
		t.Fatalf("TCP failover did not get the cleaned slot: %+v", jobs)
	}
	if te.path("a1").udp.state != "" || te.path("a1").stats.udpProbes != 0 || len(te.udpStarts) != 1 {
		t.Fatal("canceled worker published a result or refunded quota")
	}
}

func TestUDPNotPreemptedByTCPWorkThatIsNotReady(t *testing.T) {
	te := udpEngineFixture(t)
	udp := te.enqueueUDP("a1", reasonUDPSwitch, "auto", t0)
	te.tick(t0)
	canceled := false
	udp.cancel = func() { canceled = true }
	before := slices.Clone(te.spacedUntil)
	te.enqueue("a2", reasonNetwork, "auto", at(time.Minute))
	te.tick(at(time.Second))
	if canceled || udp.invalidated || !reflect.DeepEqual(before, te.spacedUntil) {
		t.Fatal("future TCP preempted UDP before becoming ready")
	}
	te.tick(at(time.Minute))
	if !canceled || !udp.invalidated {
		t.Fatal("due TCP did not preempt UDP")
	}
}
