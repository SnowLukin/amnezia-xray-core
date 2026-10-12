package health

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

func TestQualityEndCutoffClosesAdmissionBeforeCounterSnapshot(t *testing.T) {
	o, clk := qualityIntegrationObserver(false)
	var socketReads int
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		socketReads++
		return tcpQualitySnapshot{rtt: 20 * time.Millisecond, present: qualityHasRTT}
	}
	q := o.newConnectionQuality(false, clk.now().UnixNano())
	q.dialed(qualityTestConn{raw: &qualityTestRawConn{}}, clk.now().UnixNano())
	firstAt := clk.now().Add(20 * time.Millisecond).UnixNano()
	q.firstRead(o, firstAt, 512, int64(20*time.Millisecond))
	var bytesDown atomic.Int64
	bytesDown.Store(512)
	loads := 0
	load := func() int64 {
		loads++
		cutoff := bytesDown.Load()
		// Model a counter increment/mark after this Load but before the
		// collector can finish registering its end. It must not introduce a
		// sampled bulk total beyond the saved completion byte count.
		lateTotal := bytesDown.Add(4 << 20)
		q.mark(o, firstAt+int64(2*time.Second), lateTotal)
		return cutoff
	}
	request, ready := q.requestEnd(qualityEndRequest{}, load)
	if !ready || request.bytesDown != 512 || loads != 1 {
		t.Fatal("completion cutoff missing or counter sampled more than once")
	}
	end := q.end(request.bytesDown)
	if end.hasBulk || end.s2.present != 0 || socketReads != 1 {
		t.Fatal("completion sampled bytes beyond its fixed byte cutoff")
	}
	bytesDown.Add(4 << 20)
	if q.beginMarkSample(bytesDown.Load()) || q.beginFirstSample(false) {
		t.Fatal("completion admitted a sample after its byte cutoff")
	}
	if _, ready := q.requestEnd(qualityEndRequest{}, load); ready || loads != 1 {
		t.Fatal("repeated completion recaptured its original byte cutoff")
	}
}

// The injected reader has already obtained its scalar TCP snapshot and holds
// no RawControl/poll reference while paused. This models the real publication
// window after Control returns; socket Close is not a join of counter Add.
func TestQualityFinishDefersOnlyQualityUntilPublication(t *testing.T) {
	for _, witness := range []bool{false, true} {
		for _, milestone := range []bool{false, true} {
			for _, boundary := range []string{"same_window", "report_cut", "final_cut", "network_change"} {
				t.Run(fmt.Sprintf("witness_%v/milestone_%v/%s", witness, milestone, boundary), func(t *testing.T) {
					o, clk := qualityIntegrationObserver(false)
					o.s.reportInterval = time.Second
					tag := "a1"
					if witness {
						tag = "direct"
					}
					tr := o.StartDial(context.Background(), tag, net.Destination{}, nil)
					clk.advance(10 * time.Millisecond)
					read, write := tr.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
					write.Add(100)
					entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
					var unblock sync.Once
					defer unblock.Do(func() { close(release) })
					var calls atomic.Int32
					o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
						n := calls.Add(1)
						info := tcpQualitySnapshot{rtt: 20 * time.Millisecond, minRTT: 10 * time.Millisecond,
							rxOutOfOrder: 1, rxTotal: 100, txRetransmit: 1, txTotal: 100,
							present: qualityHasRTT | qualityHasMinRTT | qualityHasRX | qualityHasTX}
						if milestone && n > 1 {
							info.loadRTT = 400 * time.Millisecond
							info.present |= qualityHasLoadRTT
							info.rxOutOfOrder = 11
							info.rxTotal = 1100
							info.txRetransmit = 10
							info.txTotal = 200
						}
						if (!milestone && n == 1) || (milestone && n == 2) {
							close(entered)
							<-release
						}
						return info
					}
					clk.advance(20 * time.Millisecond)
					counted := int64(512)
					if milestone {
						read.Add(512)
						clk.advance(2 * time.Second)
						counted = 4 << 20
					}
					chunk := counted
					if milestone {
						chunk -= 512
					}
					go func() { read.Add(chunk); close(done) }()
					select {
					case <-entered:
					case <-time.After(5 * time.Second):
						t.Fatal("sampler did not reach publication window")
					}
					finished := make(chan struct{})
					go func() { tr.Finished(nil); tr.Finished(nil); close(finished) }()
					select {
					case <-finished:
					case <-time.After(time.Second):
						t.Fatal("quality waited for sampler in core Finished")
					}
					view := o.e.quality.currentView(clk.now())
					var initial *qualityPathView
					if witness {
						initial = view.Witness
					} else if p, ok := view.Paths[tag]; ok {
						initial = &p
					}
					if initial != nil && initial.BytesDown != nil {
						t.Fatal("end consumed unpublished TCP data")
					}
					o.e.report.decide()
					if !milestone {
						clk.advance(2 * time.Second)
					}
					var old *reportView
					if boundary == "report_cut" || boundary == "final_cut" {
						old = o.e.takeReport(boundary == "final_cut", clk.now())
						if old == nil {
							t.Fatal("report-cut fixture not eligible")
						}
					}
					if boundary == "network_change" {
						o.NetworkChanged("wifi:next")
					}
					unblock.Do(func() { close(release) })
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Fatal("sampler did not publish/flush after release")
					}
					view = o.e.quality.currentView(clk.now())
					if boundary == "network_change" {
						if len(view.Paths) != 0 || view.Witness != nil || view.SamplesDropped != 0 {
							t.Fatal("old deferred completion entered new network")
						}
						return
					}
					var result qualityPathView
					if witness {
						if view.Witness == nil {
							t.Fatal("deferred witness missing")
						}
						result = *view.Witness
					} else {
						result = qualitySummaryPath(t, view, tag)
					}
					qualitySummaryInt(t, result.BytesDown, counted)
					if result.TTFBCount != 1 || result.DialCount != 1 || result.TCPCount != 1 || result.TXRetransmitFraction == nil {
						t.Fatal("deferred initial/end lost or duplicated snapshots")
					}
					wantTX := 0.01
					if milestone {
						wantTX = 0.05
						if result.BulkCount != 1 || result.BulkRTTP50Ms == nil || *result.BulkRTTP50Ms != 400 || result.RXOutOfOrderFraction == nil || *result.RXOutOfOrderFraction != 0.01 {
							t.Fatal("deferred latest S2/speed/RX lost")
						}
					}
					if *result.TXRetransmitFraction != wantTX {
						t.Fatal("deferred end used old/duplicated TX snapshot")
					}
					if old != nil {
						for _, segment := range old.Quality {
							for _, p := range segment.Paths {
								if p.BytesDown != nil {
									t.Fatal("deferred end mutated taken report")
								}
							}
							if segment.Witness != nil && segment.Witness.BytesDown != nil {
								t.Fatal("deferred witness mutated taken report")
							}
						}
						later := o.e.snapshotReport(clk.now())
						if boundary == "final_cut" {
							if later != nil || o.e.takeReport(true, clk.now()) != nil {
								t.Fatal("deferred quality reopened terminal report")
							}
						} else if later == nil || len(later.Quality) != 1 {
							t.Fatal("deferred data did not enter available window")
						}
					}
				})
			}
		}
	}
}

func TestQualityUploadSplicePreservesCountedDownload(t *testing.T) {
	for _, witness := range []bool{false, true} {
		for _, at := range []string{"before_first", "after_first", "after_bulk"} {
			t.Run(fmt.Sprintf("witness_%v/%s", witness, at), func(t *testing.T) {
				o, clk := qualityIntegrationObserver(false)
				o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
					return tcpQualitySnapshot{rtt: 20 * time.Millisecond, present: qualityHasRTT}
				}
				tag := "a1"
				if witness {
					tag = "direct"
				}
				tr := o.StartDial(context.Background(), tag, net.Destination{}, nil)
				read, write := tr.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
				splice := func() { write.(interface{ Spliced() }).Spliced() }
				if at == "before_first" {
					splice()
				}
				write.Add(100)
				clk.advance(20 * time.Millisecond)
				read.Add(512)
				if at == "after_first" {
					splice()
				}
				clk.advance(time.Second)
				read.Add((4 << 20) - 512)
				if at == "after_bulk" {
					splice()
				}
				tr.Finished(nil)
				v := o.e.quality.currentView(clk.now())
				var p qualityPathView
				if witness {
					if v.Witness == nil {
						t.Fatal("witness absent")
					}
					p = *v.Witness
				} else {
					p = qualitySummaryPath(t, v, tag)
				}
				if p.TTFBCount != 1 || p.TCPCount != 1 || p.BulkCount != 1 || p.BulkSplicedCount == nil || *p.BulkSplicedCount != 0 {
					t.Fatal("upload notification disabled/classified counted download")
				}
			})
		}
	}
}

func TestQualitySampleGateIsNotTakenBetweenMilestones(t *testing.T) {
	for _, first := range []int64{512, 128 << 10} {
		t.Run(fmt.Sprintf("first_bytes_%d", first), func(t *testing.T) {
			o, clk := qualityIntegrationObserver(false)
			tr, read, _ := qualityOpenPath(o, clk, qualityTestConn{}, 0)
			read.Add(first)
			q := tr.quality
			q.life.mu.Lock()
			locked := true
			defer func() {
				if locked {
					q.life.mu.Unlock()
				}
			}()
			done := make(chan struct{})
			go func() { read.Add(64); close(done) }()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("counted read between milestones waited for quality sample gate")
			}
			q.life.mu.Unlock()
			locked = false
			tr.Finished(nil)
		})
	}
}
