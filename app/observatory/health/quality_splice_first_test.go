//go:build linux || darwin

package health

import (
	"bytes"
	"context"
	"errors"
	"io"
	gonet "net"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

// These tests use the splice marker and genuine kernel receive progress. They
// do not invoke Linux's splice syscall or replace the full splice stand case.
func qualitySpliceFirstSocket(t *testing.T) (gonet.Conn, gonet.Conn, syscall.RawConn) {
	t.Helper()
	const greeting = 4096
	client, peer := loopbackPair(t, greeting)
	if err := client.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// A transport consumes its preamble before handing out the traffic counter.
	// The legacy splice baseline must include these bytes, not a fake read hook.
	if _, err := io.ReadFull(client, make([]byte, greeting)); err != nil {
		t.Fatal(err)
	}
	if err := client.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	raw := rawConnOf(client)
	if raw == nil {
		t.Fatal("real TCP socket did not expose RawConn")
	}
	if rx, ok := kernelReceived(raw); !ok || rx == 0 {
		t.Skip("native socket does not expose the positive receive baseline needed by legacy splice tracking")
	}
	if info := kernelTCPQuality(raw); info.present&qualityHasRTT == 0 {
		t.Skip("native socket does not expose a decoded S1 RTT field")
	}
	return client, peer, raw
}

type qualitySpliceFirstFixture struct {
	o      *Observer
	clk    *fakeClock
	tr     *connTracker
	read   *trackerReadCounter
	client gonet.Conn
	now    int64
}

func qualitySpliceFirstProgress(t *testing.T) qualitySpliceFirstFixture {
	t.Helper()
	client, peer, raw := qualitySpliceFirstSocket(t)
	o, clk := qualityIntegrationObserver(false)
	tr, read, write := qualityOpenPath(o, clk, client, 20*time.Millisecond)
	write.Add(100)
	read.Spliced()
	base := tr.kernelBase.Load()
	if tr.socket.Load() == nil || base == 0 {
		t.Fatal("eligible real socket did not capture its splice receive baseline")
	}
	if _, err := peer.Write(make([]byte, 512)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		rx, ok := kernelReceived(raw)
		if !ok {
			t.Fatal("real receive counter became unavailable before the first-progress sweep")
		}
		if rx > base {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real socket did not show post-splice receive growth")
		}
		time.Sleep(time.Millisecond)
	}
	clk.advance(80 * time.Millisecond)
	return qualitySpliceFirstFixture{o: o, clk: clk, tr: tr, read: read, client: client, now: o.nowNano()}
}

func qualitySpliceFirstEnd(t *testing.T, v qualitySegmentView, bytes int64) {
	t.Helper()
	p := qualitySummaryPath(t, v, "a1")
	qualitySummaryInt(t, p.BytesDown, bytes)
	if p.BulkSplicedCount == nil || *p.BulkSplicedCount != 1 {
		t.Fatalf("kernel-first splice bulk count: %v", p.BulkSplicedCount)
	}
	if p.TTFBCount != 1 || p.DialCount != 1 || p.TCPCount != 1 || p.BulkCount != 0 {
		t.Fatalf("kernel-first splice summary lost/duplicated timing or invented speed: %+v", p)
	}
}

func TestQualitySpliceKernelFirstSample(t *testing.T) {
	// Keep this synchronous assertion separate from the paused-publication test:
	// restoring only sweep's old counted-source call must fail here, not by a
	// timeout waiting for a sampler that the old admission never enters.
	t.Run("synchronous", func(t *testing.T) {
		f := qualitySpliceFirstProgress(t)
		var calls int
		var sampled tcpQualitySnapshot
		f.o.qualityInfo = func(raw syscall.RawConn) tcpQualitySnapshot {
			calls++
			if !f.o.mu.TryLock() {
				t.Error("kernel S1 ran under the observer lock")
			} else {
				f.o.mu.Unlock()
			}
			if f.o.e.path("a1").lastAnsweredStart != 0 {
				t.Error("kernel S1 ran after answered updated legacy state")
			}
			sampled = kernelTCPQuality(raw)
			return sampled
		}
		f.tr.sweep(f.now)
		first := f.tr.quality.first.Load()
		if first == nil {
			t.Fatal("kernel progress failed to produce quality first snapshot")
		}
		if calls != 1 || first.at != f.now || first.bytes != 0 || first.ttfb != 80*time.Millisecond ||
			first.tcp != sampled || first.tcp.present&qualityHasRTT == 0 {
			t.Fatalf("kernel first snapshot: %+v, decoded %+v, calls %d", first, sampled, calls)
		}
		if f.tr.firstRead.Load() != f.now || f.tr.lastRead.Load() != f.now || f.tr.ttfb.Load() != 0 {
			t.Fatal("quality changed legacy kernel first-read timing or populated legacy ttfb")
		}
		p := qualitySummaryPath(t, f.o.e.quality.currentView(f.clk.now()), "a1")
		if p.TTFBCount != 1 || p.DialCount != 1 || p.TCPCount != 1 || p.BytesDown != nil {
			t.Fatalf("first kernel answer did not collect exactly one initial sample: %+v", p)
		}
		f.tr.sweep(f.now)
		if err := f.client.Close(); err != nil {
			t.Fatal(err)
		}
		// Model the final lump reported by CopyRawConnIfExist after its copy ends.
		// Its magnitude is accounting input, not a measured speed interval.
		const finalBytes = int64(80 << 20)
		f.read.Add(finalBytes)
		f.tr.Finished(nil)
		f.tr.Finished(nil)
		qualitySpliceFirstEnd(t, f.o.e.quality.currentView(f.clk.now()), finalBytes)
		end := f.tr.quality.end(finalBytes)
		if calls != 1 || f.tr.quality.first.Load() != first || end.hasBulk || end.s2.present != 0 ||
			end.s1 != sampled || !end.spliced || f.tr.ttfb.Load() != 0 || f.tr.quality.socket.Load() != nil {
			t.Fatalf("final splice counter changed the first sample or invented speed: %+v, calls %d", end, calls)
		}
	})
}

func TestQualitySpliceKernelSampleDefersEndUntilPublication(t *testing.T) {
	for _, boundary := range []string{"same_window", "report_cut", "network_change"} {
		t.Run(boundary, func(t *testing.T) {
			f := qualitySpliceFirstProgress(t)
			f.o.s.reportInterval = time.Second
			entered := make(chan tcpQualitySnapshot, 1)
			release, done := make(chan struct{}), make(chan struct{})
			var unblock sync.Once
			defer unblock.Do(func() { close(release) })
			var calls atomic.Int32
			f.o.qualityInfo = func(raw syscall.RawConn) tcpQualitySnapshot {
				// Decode the real socket before pausing. RawControl has returned;
				// neither the descriptor nor its poll reference escapes this point.
				info := kernelTCPQuality(raw)
				if calls.Add(1) == 1 {
					entered <- info
					<-release
				}
				return info
			}
			go func() { f.tr.sweep(f.now); close(done) }()
			var sampled tcpQualitySnapshot
			select {
			case sampled = <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("eligible kernel S1 did not reach decoded publication window")
			}
			if sampled.present&qualityHasRTT == 0 {
				t.Fatal("eligible kernel S1 lost its decoded RTT before publication")
			}
			if err := f.client.Close(); err != nil {
				t.Fatal(err)
			}
			const finalBytes = int64(4 << 20)
			f.read.Add(finalBytes)
			finished := make(chan struct{})
			go func() { f.tr.Finished(nil); f.tr.Finished(nil); close(finished) }()
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("Finished waited for pending decoded kernel S1")
			}
			before := f.o.e.quality.currentView(f.clk.now())
			if p, ok := before.Paths["a1"]; ok && (p.TTFBCount != 0 || p.BytesDown != nil) {
				t.Fatal("Finished consumed unpublished kernel S1 or end bytes")
			}
			if f.tr.quality.firstKernelRead(f.o, f.now+1, finalBytes, int64(80*time.Millisecond)) ||
				f.tr.quality.firstRead(f.o, f.now+1, finalBytes, int64(80*time.Millisecond)) || calls.Load() != 1 {
				t.Fatal("closed admission accepted a later counted/kernel first sample")
			}
			f.o.e.report.decide()
			f.clk.advance(2 * time.Second)
			var old *reportView
			if boundary == "report_cut" {
				old = f.o.e.takeReport(false, f.clk.now())
				if old == nil || len(old.Quality) != 0 {
					t.Fatal("pending kernel-S1 report-cut fixture was not empty and eligible")
				}
			}
			if boundary == "network_change" {
				f.o.NetworkChanged("wifi:next")
			}
			unblock.Do(func() { close(release) })
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("decoded kernel S1 did not publish and flush after release")
			}
			if calls.Load() != 1 || f.tr.quality.first.Load() == nil || f.tr.quality.first.Load().tcp != sampled ||
				f.tr.ttfb.Load() != 0 || f.tr.quality.socket.Load() != nil {
				t.Fatal("pending kernel S1 was duplicated, lost, or changed legacy ttfb")
			}
			view := f.o.e.quality.currentView(f.clk.now())
			if boundary == "network_change" {
				if len(view.Paths) != 0 || view.Witness != nil || view.SamplesDropped != 0 {
					t.Fatal("old kernel-S1 completion entered the new quality network")
				}
				return
			}
			qualitySpliceFirstEnd(t, view, finalBytes)
			if old != nil {
				if len(old.Quality) != 0 {
					t.Fatal("kernel S1/end mutated the already-taken empty report")
				}
				later := f.o.e.snapshotReport(f.clk.now())
				if later == nil || len(later.Quality) != 1 {
					t.Fatal("deferred kernel S1/end did not enter the available report window")
				}
				qualitySpliceFirstEnd(t, later.Quality[0], finalBytes)
			}
		})
	}
}

func TestQualitySpliceDoesNotDuplicatePendingCountedS1(t *testing.T) {
	client, _, _ := qualitySpliceFirstSocket(t)
	o, _ := qualityIntegrationObserver(false)
	q := o.newConnectionQuality(false, o.nowNano())
	q.dialed(client, o.nowNano())
	entered := make(chan tcpQualitySnapshot, 1)
	release, done := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	var calls atomic.Int32
	o.qualityInfo = func(raw syscall.RawConn) tcpQualitySnapshot {
		info := kernelTCPQuality(raw)
		if calls.Add(1) == 1 {
			entered <- info
			<-release
		}
		return info
	}
	const firstAt = int64(time.Second)
	go func() { q.firstRead(o, firstAt, 512, int64(80*time.Millisecond)); close(done) }()
	var sampled tcpQualitySnapshot
	select {
	case sampled = <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("counted S1 did not reach decoded publication window")
	}
	q.onSpliced()
	if q.firstKernelRead(o, firstAt+1, 0, int64(80*time.Millisecond)) ||
		q.firstRead(o, firstAt+1, 1024, int64(80*time.Millisecond)) || calls.Load() != 1 {
		t.Fatal("splice minted a kernel suppression token while a counted S1 already owned the claim")
	}
	unblock.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("counted S1 did not publish after splice")
	}
	first := q.first.Load()
	if first == nil || first.at != firstAt || first.bytes != 512 || first.tcp != sampled || calls.Load() != 1 {
		t.Fatal("splice lost or duplicated the admitted counted S1")
	}
	if q.firstKernelRead(o, firstAt+2, 0, int64(80*time.Millisecond)) || q.needsMark(4<<20) || calls.Load() != 1 {
		t.Fatal("published counted S1 left a reusable kernel token or splice milestone")
	}
	initial, ok := q.takeInitial()
	if !ok || !initial.hasTTFB || initial.ttfb != 80*time.Millisecond || initial.tcp != sampled {
		t.Fatal("admitted counted S1 was not preserved as the initial sample")
	}
	if _, ok := q.takeInitial(); ok {
		t.Fatal("admitted counted S1 supplied its initial sample twice")
	}
	if end := q.end(4 << 20); end.hasBulk || !end.spliced || end.s1 != sampled || end.s2.present != 0 {
		t.Fatalf("pending counted S1 splice end invented a milestone or speed: %+v", end)
	}
}

type qualitySpliceClockRead struct {
	phase string
	at    time.Time
}

type qualitySpliceLegacyPoint struct {
	phase                                   string
	started, dialed                         int64
	bytesRead, bytesWritten                 int64
	firstRead, firstWrite                   int64
	lastRead, lastWrite, pendingWrite, ttfb int64
	readValue, writeValue                   int64
	kernelBase, kernelRx                    uint64
	network                                 uint64
	state                                   int32
	reported                                uint32
	spliced, watched, socket, dialedConn    bool
	watching, drop                          bool
	choice                                  string
	status                                  pathStatus
	reason                                  string
	statusSince, quarantine                 time.Time
	lastAnsweredStart                       int64
	stats                                   pathStats
	queue                                   []probeJob
}

type qualitySpliceDecisionSide struct {
	o        *Observer
	clk      *fakeClock
	te       *testEngine
	tr       *connTracker
	read     *trackerReadCounter
	write    *trackerWriteCounter
	phase    string
	clocks   []qualitySpliceClockRead
	legacy   []qualitySpliceLegacyPoint
	samples  int
	decision qualityDecisionTrace
}

func qualitySpliceDecisionFixture(t *testing.T, skip, skipProbe bool, client gonet.Conn) *qualitySpliceDecisionSide {
	t.Helper()
	cfg := &Config{
		Passive:      &PassiveConfig{ShouldSkipQuality: skip},
		Schedule:     &ScheduleConfig{IdleAfter: int64(time.Hour)},
		Connectivity: &ConnectivityConfig{Outbound: "net", Destination: "https://x.test/204"},
	}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3), SkipProbeBeforeSwitch: skipProbe}, "a1", "b1")
	o, clk := newTestObserver()
	o.s, o.e = te.s, te.engine
	o.e.report.begin(clk.now())
	if !skip {
		o.e.quality = newQualitySummary(clk.now())
	}
	side := &qualitySpliceDecisionSide{o: o, clk: clk, te: te, phase: "dial"}
	o.clock = func() time.Time {
		now := clk.now()
		side.clocks = append(side.clocks, qualitySpliceClockRead{phase: side.phase, at: now})
		return now
	}
	o.qualityInfo = func(raw syscall.RawConn) tcpQualitySnapshot {
		side.samples++
		return kernelTCPQuality(raw)
	}
	side.tr, side.read, side.write = qualityOpenPath(o, clk, client, 20*time.Millisecond)
	return side
}

func (s *qualitySpliceDecisionSide) record(watching, drop bool) {
	p := s.te.path("a1")
	var queued []probeJob
	for _, job := range s.te.queue {
		queued = append(queued, *job)
	}
	s.legacy = append(s.legacy, qualitySpliceLegacyPoint{
		phase: s.phase, started: s.tr.started, dialed: s.tr.dialed.Load(),
		bytesRead: s.tr.bytesRead.Load(), bytesWritten: s.tr.bytesWritten.Load(),
		firstRead: s.tr.firstRead.Load(), firstWrite: s.tr.firstWrite.Load(),
		lastRead: s.tr.lastRead.Load(), lastWrite: s.tr.lastWrite.Load(),
		pendingWrite: s.tr.pendingWrite.Load(), ttfb: s.tr.ttfb.Load(),
		readValue: s.read.Value(), writeValue: s.write.Value(),
		kernelBase: s.tr.kernelBase.Load(), kernelRx: s.tr.kernelRx.Load(),
		network: s.tr.network, state: s.tr.state.Load(), reported: s.tr.reported.Load(),
		spliced: s.tr.spliced.Load(), watched: s.tr.watched.Load(),
		socket: s.tr.socket.Load() != nil, dialedConn: s.tr.dialedConn != nil,
		watching: watching, drop: drop, choice: s.te.sel.current(),
		status: p.status, reason: p.reason, statusSince: p.statusSince, quarantine: p.quarantineUntil,
		lastAnsweredStart: p.lastAnsweredStart, stats: p.stats, queue: queued,
	})
}

func TestQualitySpliceKernelProgressPreservesDecisionsAndFailoverTime(t *testing.T) {
	for _, skipProbe := range []bool{false, true} {
		t.Run(map[bool]string{false: "probe_before_switch", true: "switch_without_probe"}[skipProbe], func(t *testing.T) {
			client, peer, raw := qualitySpliceFirstSocket(t)
			// Both independent engines observe this one socket at the same fake
			// times. This makes the legacy receive counters identical without
			// asserting equal numeric TCP RTT on independently opened sockets.
			off := qualitySpliceDecisionFixture(t, true, skipProbe, client)
			on := qualitySpliceDecisionFixture(t, false, skipProbe, client)
			sides := []*qualitySpliceDecisionSide{off, on}
			for _, s := range sides {
				s.record(false, false)
				s.phase = "request"
				s.write.Add(100)
				s.record(false, false)
				s.phase = "splice_marker"
				s.read.Spliced()
				s.record(false, false)
				if s.tr.socket.Load() == nil || s.tr.kernelBase.Load() == 0 {
					t.Fatal("paired real socket did not capture an eligible splice baseline")
				}
			}
			if off.tr.quality != nil || off.o.e.quality != nil || on.tr.quality == nil {
				t.Fatal("paired fixture did not establish skipped and enabled quality states")
			}
			if off.tr.kernelBase.Load() != on.tr.kernelBase.Load() {
				t.Fatal("shared socket changed receive baseline before paired splice markers completed")
			}
			if _, err := peer.Write(make([]byte, 512)); err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				rx, ok := kernelReceived(raw)
				if !ok {
					t.Fatal("paired real socket lost its receive counter")
				}
				if rx > off.tr.kernelBase.Load() && rx > on.tr.kernelBase.Load() {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("paired real socket did not show post-splice receive growth")
				}
				time.Sleep(time.Millisecond)
			}
			for _, s := range sides {
				s.clk.advance(80 * time.Millisecond)
				s.phase = "kernel_first_progress"
				now := s.o.nowNano()
				watching, drop := s.tr.sweep(now)
				s.record(watching, drop)
				if s.tr.firstRead.Load() != now || s.tr.lastRead.Load() != now || s.tr.ttfb.Load() != 0 ||
					s.tr.kernelRx.Load() <= s.tr.kernelBase.Load() || len(s.te.queue) != 0 || s.te.sel.current() != "a1" {
					t.Fatal("paired first kernel progress did not preserve the legacy observation and quiet controller")
				}
				s.phase = "repeat_sweep"
				watching, drop = s.tr.sweep(s.o.nowNano())
				s.record(watching, drop)
			}
			if off.samples != 0 || on.samples != 1 || on.tr.quality.first.Load() == nil ||
				on.tr.quality.first.Load().tcp.present&qualityHasRTT == 0 {
				t.Fatal("paired kernel sweep did not collect useful S1 exactly once only with quality enabled")
			}
			initial := qualitySummaryPath(t, on.o.e.quality.currentView(on.clk.now()), "a1")
			if initial.TTFBCount != 1 || initial.TCPCount != 1 || initial.DialCount != 1 {
				t.Fatal("enabled paired kernel progress omitted its useful initial quality sample")
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			const finalBytes = int64(4 << 20)
			for _, s := range sides {
				s.clk.advance(time.Second)
				s.phase = "final_counter"
				s.read.Add(finalBytes)
				watching, drop := s.tr.sweep(s.o.nowNano())
				s.record(watching, drop)
				s.phase = "finished"
				s.tr.Finished(nil)
				s.tr.Finished(nil)
				watching, drop = s.tr.sweep(s.o.nowNano())
				s.record(watching, drop)
				if s.te.path("a1").status != statusUp || s.te.sel.current() != "a1" || len(s.te.queue) != 0 {
					t.Fatal("paired final splice accounting changed the healthy path or requested a probe")
				}
			}
			qualitySpliceFirstEnd(t, on.o.e.quality.currentView(on.clk.now()), finalBytes)
			// Use the normal refused Dialed callbacks and connectivity proof to
			// reach an actual controller failover in both independent engines.
			for _, phase := range []string{"first_refusal", "second_refusal", "third_refusal"} {
				for _, s := range sides {
					s.clk.advance(time.Second)
					s.phase = phase
					failed := s.o.StartDial(context.Background(), "a1", net.TCPDestination(net.DomainAddress("target.test"), 443), nil)
					failed.Dialed(nil, errors.New("connection refused"), nil, nil)
					failed.Finished(nil)
					s.record(false, false)
				}
			}
			for _, s := range sides {
				if _, pending := s.te.pendingSuspicion["a1"]; s.te.path("a1").status != statusSuspect || !pending {
					t.Fatal("paired refused dials did not reach ordinary connectivity proof")
				}
				s.phase = "connectivity_proof_and_failover"
				jobs := s.te.run(t, s.clk.now(), map[string]bool{"a1": false, "b1": true})
				if s.te.path("a1").status != statusDown || s.te.sel.current() != "b1" {
					t.Fatal("paired connectivity proof did not produce the actual failover")
				}
				s.record(false, false)
				var copied []probeJob
				for _, job := range jobs {
					copied = append(copied, *job)
				}
				events, _ := s.te.events.drain()
				s.decision = qualityDecisionTrace{events: events, choice: s.te.sel.current(), downs: s.te.takeDowns(),
					jobs: copied, quarantine: s.te.path("a1").quarantineUntil}
			}
			if off.samples != 0 || on.samples != 1 {
				t.Fatal("final accounting or refused dials added a quality socket sample")
			}
			if !reflect.DeepEqual(off.legacy, on.legacy) || !reflect.DeepEqual(off.clocks, on.clocks) {
				t.Fatalf("quality changed paired legacy atoms/counters, sweep results, controller snapshots or clock order:\noff=%+v\non=%+v\noff clocks=%+v\non clocks=%+v",
					off.legacy, on.legacy, off.clocks, on.clocks)
			}
			if !bytes.Equal(off.decision.events, on.decision.events) || off.decision.choice != on.decision.choice ||
				!reflect.DeepEqual(off.decision.downs, on.decision.downs) || !reflect.DeepEqual(off.decision.jobs, on.decision.jobs) ||
				!off.decision.quarantine.Equal(on.decision.quarantine) {
				t.Fatalf("quality changed paired controller event timestamps, choices, downs, probes or quarantine:\noff=%+v\non=%+v", off.decision, on.decision)
			}
		})
	}
}
