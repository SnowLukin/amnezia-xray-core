package health

import (
	"context"
	"errors"
	"google.golang.org/protobuf/proto"
	"math"
	gonet "net"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
)

// No socket is opened by these tests. The sampler observes this identity;
// Control fails so splice's unrelated kernel-progress reader cannot use it.
type qualityTestRawConn struct{ syscall.RawConn }

func (*qualityTestRawConn) Control(func(uintptr)) error { return errors.New("test socket") }

type qualityTestConn struct {
	gonet.Conn
	raw syscall.RawConn
	err error
}

func (c qualityTestConn) SyscallConn() (syscall.RawConn, error) { return c.raw, c.err }

func qualityOpenPath(o *Observer, clk *fakeClock, conn gonet.Conn, dial time.Duration) (*connTracker, *trackerReadCounter, *trackerWriteCounter) {
	tr := o.StartDial(context.Background(), "a1", net.Destination{}, nil).(*connTracker)
	clk.advance(dial)
	r, w := tr.Dialed(conn, nil, nil, nil)
	return tr, r.(*trackerReadCounter), w.(*trackerWriteCounter)
}

func qualityWitnessObserver(skip bool) (*Observer, *fakeClock) {
	o, clk := newTestObserver()
	o.s = newSettings(&Config{
		Features: &FeatureConfig{Quality: proto.Bool(!skip), Report: proto.Bool(true), Events: proto.Bool(true), Connectivity: proto.Bool(true), Witnesses: proto.Bool(true)},
		Paths:    observed("a1", "a2"),
		Passive:  &PassiveConfig{ShouldSkipQuality: skip},
		Connectivity: &ConnectivityConfig{
			Outbound: "direct", Destination: "https://x.test/204", Witnesses: []string{"direct"},
		},
	})
	o.e = newEngine(o.s, 1)
	return o, clk
}

func TestQualityStartCapturesEpochBeforeTimestamp(t *testing.T) {
	for _, witness := range []bool{false, true} {
		t.Run(map[bool]string{false: "path", true: "witness"}[witness], func(t *testing.T) {
			o, clk := qualityWitnessObserver(false)
			var changed atomic.Bool
			o.clock = func() time.Time {
				if !changed.Swap(true) {
					o.qualityEpoch.Add(1)
				}
				return clk.now()
			}
			tag := "a1"
			if witness {
				tag = "direct"
			}
			tr := o.StartDial(context.Background(), tag, net.Destination{}, nil)
			var q *connectionQuality
			switch tr := tr.(type) {
			case *connTracker:
				q = tr.quality
			case *witnessTracker:
				q = tr.quality
			}
			if q == nil || q.network != 0 || o.qualityEpoch.Load() != 1 || q.started != int64(clk.now().Sub(o.base)) {
				t.Fatalf("construction crossed a network change and relabelled the dial: %+v", q)
			}
		})
	}
}

func TestQualityInitialSampleUsesRequestAndSocket(t *testing.T) {
	o, clk := newTestObserver()
	rc := &qualityTestRawConn{}
	s1 := tcpQualitySnapshot{
		rtt: 30 * time.Millisecond, minRTT: 20 * time.Millisecond,
		rxOutOfOrder: 2, rxTotal: 100, txRetransmit: 0, txTotal: 300,
		present: qualityHasRTT | qualityHasMinRTT | qualityHasRX | qualityHasTX,
	}
	samples := 0
	o.qualityInfo = func(got syscall.RawConn) tcpQualitySnapshot {
		if got != rc {
			t.Error("S1 used a different socket")
		}
		if !o.mu.TryLock() {
			t.Error("S1 sampled under the observer lock")
		} else {
			if p := o.e.paths["a1"]; p != nil && p.lastAnsweredStart != 0 {
				t.Error("S1 sampled after answered already updated the observer")
			}
			o.mu.Unlock()
		}
		samples++
		return s1
	}
	tr, r, w := qualityOpenPath(o, clk, qualityTestConn{raw: rc}, 120*time.Millisecond)
	clk.advance(40 * time.Millisecond)
	w.Add(500)
	clk.advance(20 * time.Millisecond)
	w.Add(100) // The first request write remains the TTFB origin.
	clk.advance(80 * time.Millisecond)
	r.Add(1024)
	clk.advance(time.Second)
	r.Add(1024)
	if samples != 1 {
		t.Fatalf("first reply sampled %d times, want 1", samples)
	}
	if first := tr.quality.first.Load(); first == nil || first.bytes != 1024 || first.ttfb != 100*time.Millisecond {
		t.Fatalf("first reply point: %+v", first)
	}
	o.mu.Lock()
	initial, ok := tr.quality.takeInitial()
	o.mu.Unlock()
	if !ok || !initial.hasDial || initial.dial != 120*time.Millisecond || !initial.hasTTFB || initial.ttfb != 100*time.Millisecond || initial.tcp != s1 {
		t.Fatalf("initial sample: %+v, present %v", initial, ok)
	}
	tr.Finished(nil)
	if _, ok := tr.quality.takeInitial(); ok {
		t.Fatal("Finished produced a second initial sample")
	}
	if end := tr.quality.end(tr.bytesRead.Load()); end.hasBulk || end.s1 != s1 || end.s2 != (tcpQualitySnapshot{}) || end.lastTCP != s1 {
		t.Fatalf("small connection end sample: %+v", end)
	}
	if samples != 1 || tr.quality.socket.Load() != nil {
		t.Fatal("Finished sampled or retained the closed socket")
	}
}

func TestQualityKnownZeroRemainsPresent(t *testing.T) {
	o, clk := newTestObserver()
	s1 := tcpQualitySnapshot{present: qualityHasRTT | qualityHasMinRTT | qualityHasRX | qualityHasTX}
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot { return s1 }
	tr, r, w := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 0)
	w.Add(1)
	r.Add(1)
	initial, ok := tr.quality.takeInitial()
	if !ok || !initial.hasDial || initial.dial != 0 || !initial.hasTTFB || initial.ttfb != 0 || initial.tcp != s1 {
		t.Fatalf("known zeros became unavailable: %+v, present %v", initial, ok)
	}
	tr.Finished(nil)
}

func TestQualityPreservesLegacyTTFBOriginAfterAnswered(t *testing.T) {
	o, clk := newTestObserver()
	tr, r, w := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 50*time.Millisecond)
	clk.advance(20 * time.Millisecond)
	sampled := make(chan struct{})
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		close(sampled)
		return tcpQualitySnapshot{}
	}
	o.mu.Lock()
	held := true
	defer func() {
		if held {
			o.mu.Unlock()
		}
	}()
	done := make(chan struct{})
	go func() { r.Add(1); close(done) }()
	select {
	case <-sampled:
	case <-time.After(5 * time.Second):
		t.Fatal("first reply did not reach its socket sample")
	}
	// A first write arriving while answered waits was also visible to the
	// baseline's legacy TTFB origin. Keep that old, possibly negative value.
	// The write goes on its own goroutine: with UDP probes it takes the
	// observer's lock too, after it has noted its time.
	clk.advance(2 * time.Second)
	wrote := make(chan struct{})
	go func() { w.Add(1); close(wrote) }()
	for deadline := time.Now().Add(5 * time.Second); tr.firstWrite.Load() == 0; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("first write was not noted while answered waits")
		}
	}
	o.mu.Unlock()
	held = false
	for _, ch := range []chan struct{}{done, wrote} {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatal("first reply or write did not finish after releasing answered")
		}
	}
	if got := tr.ttfb.Load(); got != -int64(2*time.Second) {
		t.Fatalf("quality moved the legacy TTFB origin: %v", time.Duration(got))
	}
	initial, ok := tr.quality.takeInitial()
	if !ok || !initial.hasTTFB || initial.ttfb != 20*time.Millisecond {
		t.Fatalf("new quality lost its first-read observation: %+v", initial)
	}
	tr.Finished(nil)
}

func TestQualityDialRequiresOwnSocket(t *testing.T) {
	for _, tc := range []struct {
		name string
		conn gonet.Conn
	}{
		{"no_connection", nil},
		{"no_raw_socket", qualityTestConn{}},
		{"syscall_conn_error", qualityTestConn{raw: &qualityTestRawConn{}, err: errors.New("closed")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o, clk := newTestObserver()
			samples := 0
			o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
				samples++
				return tcpQualitySnapshot{present: qualityHasRTT}
			}
			tr, r, _ := qualityOpenPath(o, clk, tc.conn, 150*time.Millisecond)
			clk.advance(75 * time.Millisecond)
			r.Add(100)
			initial, ok := tr.quality.takeInitial()
			if !ok || initial.hasDial || !initial.hasTTFB || initial.ttfb != 75*time.Millisecond || initial.tcp.present != 0 || samples != 0 {
				t.Fatalf("socketless connection: %+v, present %v, samples %d", initial, ok, samples)
			}
			clk.advance(time.Second)
			r.Add((1 << 20) - 100)
			if end := tr.quality.end(tr.bytesRead.Load()); !end.hasBulk || end.s1.present != 0 || end.s2.present != 0 {
				t.Fatalf("socketless stream lost its counted throughput: %+v", end)
			}
			tr.Finished(nil)
		})
	}
}

func TestQualitySuccessfulDialWithoutAnswer(t *testing.T) {
	o, clk := newTestObserver()
	samples := 0
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot { samples++; return tcpQualitySnapshot{present: qualityHasRTT} }
	tr, _, w := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 90*time.Millisecond)
	w.Add(200)
	clk.advance(time.Minute)
	tr.Finished(context.Canceled)
	initial, ok := tr.quality.takeInitial()
	if !ok || !initial.hasDial || initial.dial != 90*time.Millisecond || initial.hasTTFB || initial.tcp.present != 0 {
		t.Fatalf("unanswered successful dial: %+v, present %v", initial, ok)
	}
	if _, ok := tr.quality.takeInitial(); ok {
		t.Fatal("unanswered dial contributed twice")
	}
	if end := tr.quality.end(0); !end.hasBytes || end.bytesDown != 0 || end.hasBulk || end.spliced || end.lastTCP.present != 0 || samples != 0 {
		t.Fatalf("unanswered dial invented traffic or a socket sample: %+v, samples %d", end, samples)
	}
}

func TestQualityUnavailableTCPInfoKeepsDialAndThroughput(t *testing.T) {
	o, clk := newTestObserver()
	samples := 0
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		samples++
		return tcpQualitySnapshot{}
	}
	tr, r, _ := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 50*time.Millisecond)
	clk.advance(100 * time.Millisecond)
	r.Add(1024)
	clk.advance(2 * time.Second)
	r.Add((1 << 20) - 1024)
	tr.Finished(nil)
	initial, ok := tr.quality.takeInitial()
	end := tr.quality.end(tr.bytesRead.Load())
	if !ok || !initial.hasDial || initial.dial != 50*time.Millisecond || !initial.hasTTFB || initial.ttfb != 100*time.Millisecond || initial.tcp.present != 0 {
		t.Fatalf("unavailable TCP_INFO erased independent timing: %+v, present %v", initial, ok)
	}
	if samples != 2 || !end.hasBulk || end.bulkKbps != 4190 || end.s1.present != 0 || end.s2.present != 0 || end.lastTCP.present != 0 {
		t.Fatalf("unavailable TCP_INFO erased throughput or invented RTT: %+v, snapshots %d", end, samples)
	}
}

func TestQualityFailedDialDoesNotContribute(t *testing.T) {
	o, clk := newTestObserver()
	tr := o.StartDial(context.Background(), "a1", net.Destination{}, nil).(*connTracker)
	clk.advance(90 * time.Millisecond)
	r, w := tr.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, errors.New("refused"), nil, nil)
	if r != nil || w != nil {
		t.Fatal("failed dial returned counters")
	}
	if initial, ok := tr.quality.takeInitial(); ok || initial.hasDial || initial.hasTTFB || initial.tcp.present != 0 {
		t.Fatalf("failed dial contributed an initial sample: %+v, present %v", initial, ok)
	}
	if end := tr.quality.end(0); end.hasBytes {
		t.Fatal("failed dial invented a measured zero byte total")
	}
}

func TestQualityMilestonesAndUntimedTail(t *testing.T) {
	o, clk := newTestObserver()
	samples := 0
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		samples++
		return tcpQualitySnapshot{rtt: time.Duration(samples) * time.Millisecond, present: qualityHasRTT}
	}
	tr, r, _ := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 50*time.Millisecond)
	clk.advance(100 * time.Millisecond)
	r.Add(1024)
	r.Add(o.s.passive.successBytes - 1024)
	clockReads := 0
	o.clock = func() time.Time { clockReads++; return clk.now() }
	for i, milestone := range []struct{ bytes, kbps int64 }{
		{1 << 20, 4190}, {4 << 20, 8386}, {16 << 20, 22368}, {64 << 20, 67107},
	} {
		boundary := milestone.bytes
		clk.advance(time.Second)
		r.Add(boundary - tr.bytesRead.Load() - 1)
		if clockReads != i || samples != i+1 {
			t.Fatalf("before %d MiB: clocks %d, snapshots %d", boundary>>20, clockReads, samples)
		}
		clk.advance(time.Second)
		r.Add(1)
		point := tr.quality.bulk.Load()
		if clockReads != i+1 || samples != i+2 || point == nil || point.bytes != boundary || point.at != int64(clk.now().Sub(o.base)) {
			t.Fatalf("at %d MiB: clocks %d, snapshots %d, point %+v", boundary>>20, clockReads, samples, point)
		}
		end := tr.quality.end(tr.bytesRead.Load())
		if !end.hasBulk || end.bulkKbps != milestone.kbps || end.s1.rtt != time.Millisecond || end.s2.rtt != time.Duration(i+2)*time.Millisecond || end.lastTCP != end.s2 {
			t.Fatalf("at %d MiB: end %+v, want speed %d", boundary>>20, end, milestone.kbps)
		}
	}
	measured := tr.quality.end(tr.bytesRead.Load())
	clk.advance(2 * time.Minute)
	r.Add(256 << 20)
	r.Add(0)
	r.Add(-1)
	if clockReads != 4 || samples != 5 {
		t.Fatalf("after 64 MiB: clocks %d, snapshots %d", clockReads, samples)
	}
	clk.advance(time.Hour)
	tr.Finished(nil)
	end := tr.quality.end(tr.bytesRead.Load())
	if end.bytesDown != 320<<20 || end.bulkKbps != measured.bulkKbps || end.s2 != measured.s2 || !end.hasBulk || samples != 5 {
		t.Fatalf("untimed tail changed the measurement: before %+v, after %+v", measured, end)
	}
	if next := tr.quality.next.Load(); next != math.MaxUint64 {
		t.Fatalf("a further milestone remains after 64 MiB: %d", next)
	}
}

func TestQualityOneReadCoalescesMilestones(t *testing.T) {
	o, clk := newTestObserver()
	samples := 0
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot { samples++; return tcpQualitySnapshot{present: qualityHasRTT} }
	tr, r, _ := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 0)
	r.Add(1024)
	r.Add(o.s.passive.successBytes - 1024)
	clockReads := 0
	o.clock = func() time.Time { clockReads++; return clk.now() }
	clk.advance(2 * time.Second)
	r.Add((80 << 20) - tr.bytesRead.Load())
	if clockReads != 1 || samples != 2 {
		t.Fatalf("one read crossing four marks: clocks %d, snapshots %d", clockReads, samples)
	}
	point := tr.quality.bulk.Load()
	if point == nil || point.bytes != 80<<20 || tr.quality.next.Load() != math.MaxUint64 {
		t.Fatalf("coalesced last mark: %+v", point)
	}
	end := tr.quality.end(tr.bytesRead.Load())
	if !end.hasBulk || end.bulkKbps != 335540 {
		t.Fatalf("coalesced speed %+v, want 335540 Kbit/s", end)
	}
	r.Add(80 << 20)
	if clockReads != 1 || samples != 2 || tr.quality.bulk.Load() != point {
		t.Fatal("read after the coalesced 64 MiB mark was timed")
	}
	tr.Finished(nil)
}

func TestQualityFirstReadCrossingMilestonesHasNoSpeedInterval(t *testing.T) {
	o, clk := newTestObserver()
	samples := 0
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot { samples++; return tcpQualitySnapshot{present: qualityHasRTT} }
	tr, r, _ := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 0)
	clk.advance(time.Second)
	r.Add(80 << 20)
	if end := tr.quality.end(tr.bytesRead.Load()); end.hasBulk || samples != 2 || end.s1.present == 0 || end.s2.present == 0 {
		t.Fatalf("first read invented a speed interval: %+v, snapshots %d", end, samples)
	}
	tr.Finished(nil)
}

func TestQualityFailedS2KeepsLastSuccessfulSnapshot(t *testing.T) {
	o, clk := newTestObserver()
	s1 := tcpQualitySnapshot{rtt: 10 * time.Millisecond, present: qualityHasRTT}
	s2 := tcpQualitySnapshot{rtt: 20 * time.Millisecond, rxTotal: 200, rxOutOfOrder: 3, present: qualityHasRTT | qualityHasRX}
	samples := 0
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		samples++
		switch samples {
		case 1:
			return s1
		case 2:
			return s2
		default:
			return tcpQualitySnapshot{}
		}
	}
	tr, r, _ := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 0)
	r.Add(100)
	clk.advance(time.Second)
	r.Add((1 << 20) - 100)
	clk.advance(time.Second)
	r.Add(3 << 20)
	end := tr.quality.end(tr.bytesRead.Load())
	if samples != 3 || end.s1 != s1 || end.s2 != s2 || end.lastTCP != s2 || !end.hasBulk || end.bulkKbps != 16776 {
		t.Fatalf("failed syscall erased S2 or its timing: %+v, samples %d", end, samples)
	}
	tr.Finished(nil)
}

func TestQualityExcludedTraffic(t *testing.T) {
	for _, tag := range []string{"a1", "direct"} {
		for _, exclusion := range []string{"udp", "skip_quality", "probe"} {
			t.Run(tag+"/"+exclusion, func(t *testing.T) {
				o, _ := qualityWitnessObserver(exclusion == "skip_quality")
				samples := 0
				o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot { samples++; return tcpQualitySnapshot{present: qualityHasRTT} }
				ctx := context.Background()
				var probeBytes atomic.Int64
				if exclusion == "udp" {
					ctx = session.ContextWithOutbounds(ctx, []*session.Outbound{{Target: net.UDPDestination(net.LocalHostIP, 53)}})
				}
				if exclusion == "probe" {
					ctx = withProbe(ctx, &probeBytes)
				}
				tr := o.StartDial(ctx, tag, net.Destination{}, nil)
				switch tr := tr.(type) {
				case *connTracker:
					if tr.quality != nil {
						t.Fatal("excluded path traffic has a quality tracker")
					}
				case *witnessTracker:
					if tr.quality != nil {
						t.Fatal("excluded witness traffic has a quality tracker")
					}
				case *probeTracker:
					if exclusion != "probe" {
						t.Fatal("user traffic became a probe")
					}
				default:
					t.Fatalf("unexpected tracker %T", tr)
				}
				r, w := tr.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
				if w != nil {
					w.Add(100)
				}
				r.Add(80 << 20)
				tr.Finished(nil)
				if samples != 0 {
					t.Fatalf("excluded traffic sampled %d sockets", samples)
				}
				if exclusion == "probe" && probeBytes.Load() != (80<<20)+100 {
					t.Fatal("probe byte counting changed")
				}
			})
		}
	}
}

func TestQualitySpliceDoesNotInventTiming(t *testing.T) {
	for _, before := range []int64{0, 1024, 1 << 20} {
		t.Run(map[int64]string{0: "before_reply", 1024: "after_reply", 1 << 20: "after_bulk_mark"}[before], func(t *testing.T) {
			o, clk := newTestObserver()
			samples := 0
			o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot { samples++; return tcpQualitySnapshot{present: qualityHasRTT} }
			tr, r, _ := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 20*time.Millisecond)
			if before > 0 {
				clk.advance(80 * time.Millisecond)
				r.Add(1024)
				if before > 1024 {
					clk.advance(time.Second)
					r.Add(before - 1024)
				}
			}
			beforeSamples := samples
			r.Spliced()
			clk.advance(time.Minute)
			r.Add((80 << 20) - before) // Splice's aggregated byte count at the end.
			tr.Finished(nil)
			initial, _ := tr.quality.takeInitial()
			end := tr.quality.end(tr.bytesRead.Load())
			if end.hasBulk || !end.spliced || end.bytesDown != 80<<20 || samples != beforeSamples {
				t.Fatalf("spliced end sample: %+v, snapshots before %d after %d", end, beforeSamples, samples)
			}
			if initial.hasTTFB != (before > 0) || (before == 0 && initial.tcp.present != 0) {
				t.Fatalf("aggregated splice bytes invented a first reply: %+v", initial)
			}
			if before > 0 && initial.ttfb != 80*time.Millisecond {
				t.Fatalf("splice changed measured TTFB: %+v", initial)
			}
		})
	}
}

func TestQualitySmallSpliceIsNotBulk(t *testing.T) {
	o, clk := newTestObserver()
	tr, r, _ := qualityOpenPath(o, clk, nil, 0)
	r.Spliced()
	r.Add((1 << 20) - 1)
	if end := tr.quality.end(tr.bytesRead.Load()); end.hasBulk || end.spliced {
		t.Fatalf("small splice counted as bulk: %+v", end)
	}
	tr.Finished(nil)
}

func TestQualityWitnessMeasurementsDoNotChangeNetworkEvidence(t *testing.T) {
	o, clk := qualityWitnessObserver(false)
	baseline, baselineClock := qualityWitnessObserver(true)
	o.e.onConnectivity(probeResult{ok: false, err: "down"}, clk.now())
	baseline.e.onConnectivity(probeResult{ok: false, err: "down"}, baselineClock.now())
	samples := 0
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		samples++
		return tcpQualitySnapshot{rtt: time.Minute, rxOutOfOrder: 100, rxTotal: 100, txRetransmit: 100, txTotal: 100, present: qualityHasRTT | qualityHasRX | qualityHasTX}
	}
	tr := o.StartDial(context.Background(), "direct", net.Destination{}, nil).(*witnessTracker)
	plain := baseline.StartDial(context.Background(), "direct", net.Destination{}, nil).(*witnessTracker)
	clk.advance(50 * time.Millisecond)
	baselineClock.advance(50 * time.Millisecond)
	r, w := tr.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
	plainRead, _ := plain.Dialed(nil, nil, nil, nil)
	clk.advance(20 * time.Millisecond)
	baselineClock.advance(20 * time.Millisecond)
	w.Add(200)
	clk.advance(80 * time.Millisecond)
	baselineClock.advance(80 * time.Millisecond)
	r.Add(100)
	plainRead.Add(100)
	if !o.e.networkDown || tr.done.Load() {
		t.Fatal("S1 changed witness network evidence before successBytes")
	}
	r.Add(o.s.passive.successBytes - 100)
	plainRead.Add(baseline.s.passive.successBytes - 100)
	if o.e.networkDown || !tr.done.Load() || !o.e.queued("", reasonConnectivity) {
		t.Fatal("witness success no longer resumes the network")
	}
	clockReads := 0
	o.clock = func() time.Time { clockReads++; return clk.now() }
	for i, boundary := range []int64{1 << 20, 4 << 20, 16 << 20, 64 << 20} {
		clk.advance(time.Second)
		baselineClock.advance(time.Second)
		n := boundary - tr.qualityRead.Load() - 1
		r.Add(n)
		plainRead.Add(n)
		if clockReads != i || samples != i+1 {
			t.Fatalf("witness between marks: clocks %d snapshots %d", clockReads, samples)
		}
		clk.advance(time.Second)
		baselineClock.advance(time.Second)
		r.Add(1)
		plainRead.Add(1)
		if clockReads != i+1 || samples != i+2 {
			t.Fatalf("witness at mark: clocks %d snapshots %d", clockReads, samples)
		}
	}
	clk.advance(time.Minute)
	baselineClock.advance(time.Minute)
	r.Add(128 << 20)
	plainRead.Add(128 << 20)
	tr.Finished(nil)
	plain.Finished(nil)
	initial, ok := tr.quality.takeInitial()
	end := tr.quality.end(tr.qualityRead.Load())
	if !ok || !initial.hasDial || initial.dial != 50*time.Millisecond || !initial.hasTTFB || initial.ttfb != 80*time.Millisecond || !end.hasBulk || end.bytesDown != 192<<20 || end.s1.present == 0 || end.s2.present == 0 {
		t.Fatalf("witness measurements: initial %+v (%v), end %+v", initial, ok, end)
	}
	if end.bulkKbps != 67108 || clockReads != 4 || samples != 5 || tr.quality.socket.Load() != nil {
		t.Fatalf("witness bulk %+v, want 67108 Kbit/s, clocks %d samples %d", end, clockReads, samples)
	}
	if _, ok := tr.quality.takeInitial(); ok {
		t.Fatal("witness initial sample consumed twice")
	}
	if o.e.networkDown != baseline.e.networkDown || o.e.lastSuccess != baseline.e.lastSuccess || !reflect.DeepEqual(o.e.queue, baseline.e.queue) || !reflect.DeepEqual(o.e.events.events, baseline.e.events.events) || tr.bytes.Load() != plain.bytes.Load() {
		t.Fatal("quality measurements changed network decisions, witness evidence, or events")
	}
}

func TestQualityWitnessSpliceHasNoFakeFirstReply(t *testing.T) {
	o, clk := qualityWitnessObserver(false)
	samples := 0
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot { samples++; return tcpQualitySnapshot{present: qualityHasRTT} }
	tr := o.StartDial(context.Background(), "direct", net.Destination{}, nil).(*witnessTracker)
	clk.advance(30 * time.Millisecond)
	r, w := tr.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
	w.(*witnessWriteCounter).Spliced()
	r.(*witnessCounter).Spliced()
	clk.advance(time.Minute)
	r.Add(64 << 20)
	tr.Finished(nil)
	initial, ok := tr.quality.takeInitial()
	end := tr.quality.end(tr.qualityRead.Load())
	if !ok || !initial.hasDial || initial.hasTTFB || initial.tcp.present != 0 || end.hasBulk || !end.spliced || samples != 0 {
		t.Fatalf("witness splice invented timing: initial %+v, end %+v, snapshots %d", initial, end, samples)
	}
}

func TestQualityWitnessSuccessfulDialWithoutAnswer(t *testing.T) {
	o, clk := qualityWitnessObserver(false)
	o.e.onConnectivity(probeResult{ok: false, err: "down"}, clk.now())
	samples := 0
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		samples++
		return tcpQualitySnapshot{present: qualityHasRTT}
	}
	tr := o.StartDial(context.Background(), "direct", net.Destination{}, nil).(*witnessTracker)
	clk.advance(60 * time.Millisecond)
	_, w := tr.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
	w.Add(200)
	clk.advance(time.Minute)
	tr.Finished(context.Canceled)
	tr.Finished(context.Canceled)
	initial, ok := tr.quality.takeInitial()
	if !ok || !initial.hasDial || initial.dial != 60*time.Millisecond || initial.hasTTFB || initial.tcp.present != 0 || samples != 0 {
		t.Fatalf("unanswered witness dial: %+v, present %v, snapshots %d", initial, ok, samples)
	}
	if _, ok := tr.quality.takeInitial(); ok {
		t.Fatal("unanswered witness dial contributed twice")
	}
	if !o.e.networkDown || tr.done.Load() || !o.e.lastSuccess.IsZero() || len(o.e.queue) != 0 {
		t.Fatal("witness dial timing became network evidence without a reply")
	}
}

func TestQualityConcurrentFirstReplySamplesOnce(t *testing.T) {
	o, _ := newTestObserver()
	q := o.newConnectionQuality(false, 1)
	q.dialed(qualityTestConn{raw: &qualityTestRawConn{}}, 2)
	var samples atomic.Int32
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		samples.Add(1)
		return tcpQualitySnapshot{present: qualityHasRTT}
	}
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); q.firstRead(o, 100, 128, 90) }()
	}
	workers.Wait()
	if samples.Load() != 1 || q.first.Load() == nil {
		t.Fatalf("concurrent first reply: snapshots %d point %+v", samples.Load(), q.first.Load())
	}
	if initial, ok := q.takeInitial(); !ok || !initial.hasTTFB {
		t.Fatalf("missing concurrent first reply: %+v", initial)
	}
	if _, ok := q.takeInitial(); ok {
		t.Fatal("concurrent first reply supplied multiple initial samples")
	}
}

func TestQualityConcurrentMarksCannotReplaceNewerPoint(t *testing.T) {
	o, _ := newTestObserver()
	q := o.newConnectionQuality(false, 1)
	q.dialed(qualityTestConn{raw: &qualityTestRawConn{}}, 2)
	entered := make(chan struct{})
	release := make(chan struct{})
	var samples atomic.Int32
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		n := samples.Add(1)
		if n == 1 {
			close(entered)
			<-release
		}
		return tcpQualitySnapshot{rtt: time.Duration(n) * time.Millisecond, present: qualityHasRTT}
	}
	done := make(chan struct{})
	go func() { q.mark(o, 100, 1<<20); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		close(release)
		t.Fatal("first milestone did not enter the sampler")
	}
	q.mark(o, 200, 64<<20)
	close(release)
	<-done
	point := q.bulk.Load()
	if samples.Load() != 2 || point == nil || point.bytes != 64<<20 || point.at != 200 || q.end(64<<20).s2.rtt != 2*time.Millisecond {
		t.Fatalf("older S2 replaced a newer observation: snapshots %d, point %+v", samples.Load(), point)
	}
	var workers sync.WaitGroup
	for i := 0; i < 32; i++ {
		workers.Add(1)
		go func() { defer workers.Done(); q.mark(o, 300, 128<<20) }()
	}
	workers.Wait()
	if samples.Load() != 2 || q.bulk.Load() != point {
		t.Fatal("finished milestone budget allowed more S2 samples")
	}
}

func TestQualityLateSuccessfulS2SurvivesNewerFailedMark(t *testing.T) {
	o, _ := newTestObserver()
	q := o.newConnectionQuality(false, 1)
	q.dialed(qualityTestConn{raw: &qualityTestRawConn{}}, 2)
	s1 := tcpQualitySnapshot{rtt: 10 * time.Millisecond, present: qualityHasRTT}
	successfulS2 := tcpQualitySnapshot{
		rtt: 20 * time.Millisecond, loadRTT: 50 * time.Millisecond,
		rxOutOfOrder: 5, rxTotal: 400, txRetransmit: 7, txTotal: 800,
		present: qualityHasRTT | qualityHasLoadRTT | qualityHasRX | qualityHasTX,
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	var samples atomic.Int32
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		switch samples.Add(1) {
		case 1:
			return s1
		case 2:
			close(entered)
			<-release
			return successfulS2
		default:
			return tcpQualitySnapshot{}
		}
	}
	q.firstRead(o, int64(time.Second), 1024, int64(50*time.Millisecond))
	done := make(chan struct{})
	go func() { q.mark(o, int64(3*time.Second), 1<<20); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("1 MiB mark did not enter the sampler")
	}
	// The later failed sample publishes its byte/time observation while the
	// earlier, successful socket call is still in flight.
	q.mark(o, int64(5*time.Second), 4<<20)
	before := q.end(4 << 20)
	bulk := q.bulk.Load()
	unblock.Do(func() { close(release) })
	<-done
	end := q.end(4 << 20)
	if before.s2.present != 0 || before.lastTCP != s1 {
		t.Fatalf("in-flight socket sample became visible early: %+v", before)
	}
	if bulk == nil || bulk.bytes != 4<<20 || bulk.at != int64(5*time.Second) || q.bulk.Load() != bulk {
		t.Fatalf("late successful socket sample replaced the newer timing point: %+v", q.bulk.Load())
	}
	if end.s1 != s1 || end.s2 != successfulS2 || end.lastTCP != successfulS2 || samples.Load() != 3 {
		t.Fatalf("newer failed mark discarded the late successful S2: %+v, snapshots %d", end, samples.Load())
	}
	if !before.hasBulk || before.bulkKbps != 8386 || !end.hasBulk || end.bulkKbps != 8386 || end.bytesDown != 4<<20 {
		t.Fatalf("socket completion changed the 4 MiB throughput interval: before %+v, after %+v", before, end)
	}
}

func TestQualityConcurrentSuccessfulS2StaysOrderedThroughFinalFailure(t *testing.T) {
	o, _ := newTestObserver()
	q := o.newConnectionQuality(false, 1)
	q.dialed(qualityTestConn{raw: &qualityTestRawConn{}}, 2)
	s1 := tcpQualitySnapshot{rtt: 10 * time.Millisecond, present: qualityHasRTT}
	one := tcpQualitySnapshot{rtt: 20 * time.Millisecond, rxTotal: 100, present: qualityHasRTT | qualityHasRX}
	four := tcpQualitySnapshot{rtt: 25 * time.Millisecond, rxTotal: 400, present: qualityHasRTT | qualityHasRX}
	sixteen := tcpQualitySnapshot{rtt: 30 * time.Millisecond, rxTotal: 1600, rxOutOfOrder: 3, present: qualityHasRTT | qualityHasRX}
	entered := make(chan struct{})
	release := make(chan struct{})
	var unblock sync.Once
	defer unblock.Do(func() { close(release) })
	var samples atomic.Int32
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		switch samples.Add(1) {
		case 1:
			return s1
		case 2:
			return one
		case 3:
			close(entered)
			<-release
			return four
		case 4:
			return sixteen
		default:
			return tcpQualitySnapshot{}
		}
	}
	q.firstRead(o, int64(time.Second), 1024, int64(50*time.Millisecond))
	q.mark(o, int64(3*time.Second), 1<<20)
	done := make(chan struct{})
	go func() { q.mark(o, int64(5*time.Second), 4<<20); close(done) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("4 MiB mark did not enter the sampler")
	}
	q.mark(o, int64(7*time.Second), 16<<20)
	afterSixteen := q.end(16 << 20)
	q.mark(o, int64(9*time.Second), 64<<20)
	afterFailure := q.end(64 << 20)
	bulk := q.bulk.Load()
	unblock.Do(func() { close(release) })
	<-done
	end := q.end(96 << 20)
	if afterSixteen.s2 != sixteen || afterFailure.s2 != sixteen || end.s2 != sixteen || end.lastTCP != sixteen {
		t.Fatalf("S2 lost successful milestone order: after 16 %+v, after failure %+v, after late 4 %+v", afterSixteen, afterFailure, end)
	}
	if bulk == nil || bulk.bytes != 64<<20 || bulk.at != int64(9*time.Second) || q.bulk.Load() != bulk {
		t.Fatalf("late successful 4 MiB mark replaced the 64 MiB timing point: %+v", q.bulk.Load())
	}
	if samples.Load() != 5 || !afterFailure.hasBulk || afterFailure.bulkKbps != 67107 || !end.hasBulk || end.bulkKbps != 67107 || end.bytesDown != 96<<20 {
		t.Fatalf("final failed sample changed throughput or its bounded sample count: %+v, snapshots %d", end, samples.Load())
	}
}
