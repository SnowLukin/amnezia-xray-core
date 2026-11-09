package health

import (
	"encoding/json"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/features/outbound"
)

type qualityLimitedHandler struct {
	outbound.Handler
	up, down uint64
}

func (h *qualityLimitedHandler) SpeedLimit() (uint64, uint64) { return h.up, h.down }

// The kernel received 4 MiB in the first second, while the reader drained it
// only at the 1 MiB mark. Draining the buffer up to 4 MiB in 10 ms is no
// delivery speed; the 12 MiB of the next two seconds are.
func TestQualityPeakUsesKernelDelivery(t *testing.T) {
	o, clk := qualityIntegrationObserver(false)
	o.ohm = &v2Manager{h: map[string]outbound.Handler{"a1": &qualityLimitedHandler{down: 20 << 20 / 8}}}
	var kernel uint64
	loadRTT := 40 * time.Millisecond
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		return tcpQualitySnapshot{rxBytes: kernel, minRTT: 40 * time.Millisecond, loadRTT: loadRTT,
			present: qualityHasMinRTT | qualityHasLoadRTT | qualityHasRXBytes}
	}
	tr, r, _ := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 50*time.Millisecond)
	clk.advance(100 * time.Millisecond)
	kernel = 64 << 10
	r.Add(1024)
	r.Add(o.s.passive.successBytes - 1024)

	clk.advance(time.Second)
	kernel += 4 << 20
	r.Add(1<<20 - tr.bytesRead.Load())
	if end := tr.quality.end(tr.bytesRead.Load()); !end.hasPeak || end.peakKbps != 33554 {
		t.Fatalf("first interval: %+v, want 33554 kbps", end)
	}
	clk.advance(10 * time.Millisecond)
	kernel += 80 << 10
	r.Add(4<<20 - tr.bytesRead.Load())
	if end := tr.quality.end(tr.bytesRead.Load()); end.peakKbps != 33554 {
		t.Fatalf("draining the socket buffer became a speed: %d kbps", end.peakKbps)
	}
	clk.advance(2 * time.Second)
	kernel += 12 << 20
	loadRTT = 120 * time.Millisecond
	r.Add(16<<20 - tr.bytesRead.Load())
	end := tr.quality.end(tr.bytesRead.Load())
	if end.peakKbps != 50331 || !end.hasBulk || end.bulkKbps >= end.peakKbps {
		t.Fatalf("end %+v, want peak 50331 kbps above the average", end)
	}
	if load := qualityDownloadLoad(end); load != qualityLoadQueued {
		t.Fatalf("80 ms of queue above a 40 ms path is load %d", load)
	}

	tr.Finished(nil)
	v := o.e.quality.currentView(clk.now()).Paths["a1"]
	if v.BulkPeakCount != 1 || v.BulkSaturatedCount == nil || *v.BulkSaturatedCount != 1 || *v.BulkUnsaturatedCount != 0 {
		t.Fatalf("path view %+v", v)
	}
	qualitySummaryInt(t, v.BulkSaturatedKbpsP50, 80000)
	qualitySummaryInt(t, v.SpeedLimitDownKbps, 20971)
	if v.SpeedLimitUpKbps != nil {
		t.Fatal("an unlimited uplink was reported as a limit")
	}
}

func TestQualityPeakNeedsKernelCounter(t *testing.T) {
	o, clk := newTestObserver()
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot { return tcpQualitySnapshot{present: qualityHasRTT} }
	tr, r, _ := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 50*time.Millisecond)
	clk.advance(100 * time.Millisecond)
	r.Add(1024)
	clk.advance(time.Second)
	r.Add(4<<20 - tr.bytesRead.Load())
	if end := tr.quality.end(tr.bytesRead.Load()); end.hasPeak || !end.hasBulk || end.limit != (qualitySpeedLimit{}) {
		t.Fatalf("peak without a kernel counter or a limit without a handler: %+v", end)
	}
}

func TestQualityLoad(t *testing.T) {
	ms := time.Millisecond
	rtt := func(minRTT, loadRTT time.Duration) qualityLoadSignals {
		return qualityLoadSignals{minRTT: minRTT, loadRTT: loadRTT, hasRTT: true}
	}
	withLoss := func(s qualityLoadSignals, lost, total uint64) qualityLoadSignals {
		s.lost, s.total, s.hasLoss = lost, total, true
		return s
	}
	for name, c := range map[string]struct {
		s    qualityLoadSignals
		want qualityLoad
	}{
		"no signals":               {qualityLoadSignals{}, qualityLoadUnknown},
		"queue on a short path":    {rtt(20*ms, 55*ms), qualityLoadQueued},
		"small queue":              {rtt(20*ms, 45*ms), qualityLoadFree},
		"queue below half of long": {rtt(200*ms, 290*ms), qualityLoadFree},
		"queue above half of long": {rtt(200*ms, 300*ms), qualityLoadQueued},
		"queue overflowing":        {withLoss(rtt(20*ms, 200*ms), 5, 100), qualityLoadQueued},
		"policer":                  {withLoss(rtt(20*ms, 25*ms), 5, 100), qualityLoadPoliced},
		"managed queue drops":      {withLoss(rtt(20*ms, 25*ms), 2, 100), qualityLoadLossy},
		"loss without rtt":         {withLoss(qualityLoadSignals{}, 5, 100), qualityLoadLossy},
		"rare reordering":          {withLoss(qualityLoadSignals{}, 1, 1000), qualityLoadFree},
		"counter reset":            {withLoss(qualityLoadSignals{}, 0, 0), qualityLoadUnknown},
	} {
		if got := c.s.load(); got != c.want {
			t.Errorf("%s: load %d, want %d", name, got, c.want)
		}
	}
}

func TestQualityDownloadAndUploadLoadSignals(t *testing.T) {
	s := qualityEndSample{
		s1: tcpQualitySnapshot{minRTT: 40 * time.Millisecond, present: qualityHasMinRTT | qualityHasRX},
		s2: tcpQualitySnapshot{loadRTT: 45 * time.Millisecond, rxOutOfOrder: 10, rxTotal: 100, present: qualityHasLoadRTT | qualityHasRX},
		u1: tcpQualitySnapshot{minRTT: 60 * time.Millisecond, txRetransmit: 100, txTotal: 1000, present: qualityHasMinRTT | qualityHasTX},
		u2: tcpQualitySnapshot{rtt: 200 * time.Millisecond, txRetransmit: 100, txTotal: 101000, present: qualityHasRTT | qualityHasTX},
	}
	if load := qualityDownloadLoad(s); load != qualityLoadPoliced {
		t.Fatalf("10%% reordered with no queue: load %d", load)
	}
	// The upload takes the least RTT known, the 40 ms of the first reply.
	if load := qualityUploadLoad(s); load != qualityLoadQueued {
		t.Fatalf("160 ms of upload queue: load %d", load)
	}
	s.u2.rtt = 50 * time.Millisecond
	s.u2.txRetransmit = 100 + 5000
	if load := qualityUploadLoad(s); load != qualityLoadPoliced {
		t.Fatalf("5%% retransmitted with no queue: load %d", load)
	}
	s.u2.txTotal = 10
	if load := qualityUploadLoad(s); load != qualityLoadFree {
		t.Fatalf("a reset counter became loss: load %d", load)
	}
}

// Writes fill the socket buffer at once; the acknowledged bytes show 8 MiB
// went out in two seconds.
func TestQualityUploadPeakUsesAcknowledgedBytes(t *testing.T) {
	o, clk := qualityIntegrationObserver(false)
	var acked uint64
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		return tcpQualitySnapshot{txAcked: acked, rtt: 300 * time.Millisecond, minRTT: 40 * time.Millisecond,
			present: qualityHasRTT | qualityHasMinRTT | qualityHasTXAcked}
	}
	tr, _, w := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 50*time.Millisecond)
	clk.advance(10 * time.Millisecond)
	w.Add(1 << 20)
	w.Add(3 << 20)
	if end := tr.quality.end(0); end.hasUpPeak {
		t.Fatalf("a full socket buffer became an upload speed: %+v", end)
	}
	clk.advance(2 * time.Second)
	acked = 8 << 20
	w.Add(12 << 20)
	end := tr.quality.end(0)
	if !end.hasUpPeak || end.upPeakKbps != 33554 || end.hasPeak {
		t.Fatalf("end %+v, want an upload peak of 33554 kbps and no download peak", end)
	}
	tr.Finished(nil)
	v := o.e.quality.currentView(clk.now()).Paths["a1"]
	if v.UploadPeakCount != 1 || *v.UploadSaturatedCount != 1 || *v.UploadPolicedCount != 0 || v.BulkPeakCount != 0 {
		t.Fatalf("path view %+v", v)
	}
	qualitySummaryInt(t, v.UploadSaturatedKbpsP50, 50000)
}

func TestQualitySpeedHistogramsAddUp(t *testing.T) {
	a := &qualityAccumulator{}
	saturatedRTT := qualityEndSample{
		s1: tcpQualitySnapshot{minRTT: 20 * time.Millisecond, present: qualityHasMinRTT},
		s2: tcpQualitySnapshot{loadRTT: 200 * time.Millisecond, present: qualityHasLoadRTT},
	}
	for _, s := range []qualityEndSample{
		{hasBulk: true, bulkKbps: 900, hasPeak: true, peakKbps: 4000, s1: saturatedRTT.s1, s2: saturatedRTT.s2},
		{hasBulk: true, bulkKbps: 7000, hasPeak: true, peakKbps: 9000, s1: saturatedRTT.s1, s2: tcpQualitySnapshot{loadRTT: 25 * time.Millisecond, present: qualityHasLoadRTT}},
		{hasBulk: true, bulkKbps: 600000, hasPeak: true, peakKbps: 700000},
		{hasPeak: true, peakKbps: 1500,
			s1: tcpQualitySnapshot{minRTT: 20 * time.Millisecond, present: qualityHasMinRTT | qualityHasRX},
			s2: tcpQualitySnapshot{loadRTT: 22 * time.Millisecond, rxOutOfOrder: 10, rxTotal: 100, present: qualityHasLoadRTT | qualityHasRX}},
		{hasUpPeak: true, upPeakKbps: 3000},
		{hasBulk: true, bulkKbps: 100, limit: qualitySpeedLimit{downKbps: 8000, upKbps: 2000}},
	} {
		a.addEnd(s)
	}
	a.addInitial(qualityInitialSample{hasTTFB: true, ttfb: 70 * time.Millisecond, hasDial: true, dial: 15 * time.Millisecond})
	v := a.view()
	if v.BulkCount != 4 || v.BulkPeakCount != 4 || *v.BulkSaturatedCount != 2 || *v.BulkUnsaturatedCount != 1 || *v.BulkPolicedCount != 1 {
		t.Fatalf("download counts %+v", v)
	}
	// A peak with no signals counts neither as saturated nor as free.
	if v.UploadPeakCount != 1 || *v.UploadSaturatedCount != 0 || *v.UploadUnsaturatedCount != 0 || v.UploadSaturatedKbpsP50 != nil {
		t.Fatalf("upload counts %+v", v)
	}
	for name, h := range map[string]struct {
		buckets []uint64
		total   uint64
		bounds  int
	}{
		"bulk":      {v.BulkKbpsHist, v.BulkCount, len(qualitySpeedBounds)},
		"peak":      {v.BulkPeakKbpsHist, v.BulkPeakCount, len(qualitySpeedBounds)},
		"saturated": {v.BulkSaturatedKbpsHist, *v.BulkSaturatedCount, len(qualitySpeedBounds)},
		"policed":   {v.BulkPolicedKbpsHist, *v.BulkPolicedCount, len(qualitySpeedBounds)},
		"upload":    {v.UploadPeakKbpsHist, v.UploadPeakCount, len(qualitySpeedBounds)},
		"ttfb":      {v.TTFBMsHist, v.TTFBCount, len(qualityTimeBounds)},
		"dial":      {v.DialMsHist, v.DialCount, len(qualityTimeBounds)},
	} {
		var sum uint64
		for _, n := range h.buckets {
			sum += n
		}
		if len(h.buckets) != h.bounds+1 || sum != h.total {
			t.Errorf("%s buckets %v do not add up to %d", name, h.buckets, h.total)
		}
	}
	if v.BulkPeakKbpsHist[len(qualitySpeedBounds)] != 1 || v.BulkKbpsHist[0] != 1 {
		t.Fatalf("speeds outside the bounds lost their buckets: %v %v", v.BulkKbpsHist, v.BulkPeakKbpsHist)
	}
	qualitySummaryInt(t, v.BulkSaturatedKbpsP90, 5000)
	qualitySummaryInt(t, v.BulkPolicedKbpsP50, 2000)
	qualitySummaryInt(t, v.SpeedLimitDownKbps, 8000)
	qualitySummaryInt(t, v.SpeedLimitUpKbps, 2000)

	clone := cloneQualityPath(v)
	clone.BulkPeakKbpsHist[0] = 99
	if v.BulkPeakKbpsHist[0] == 99 {
		t.Fatal("a cloned view shares its buckets")
	}
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"bulk_kbps_hist", "bulk_peak_count", "bulk_peak_kbps_p50", "bulk_peak_kbps_p90", "bulk_peak_kbps_hist",
		"bulk_saturated_count", "bulk_unsaturated_count", "bulk_saturated_kbps_p50", "bulk_saturated_kbps_p90", "bulk_saturated_kbps_hist",
		"bulk_policed_count", "bulk_policed_kbps_p50", "bulk_policed_kbps_p90", "bulk_policed_kbps_hist",
		"upload_peak_count", "upload_peak_kbps_p50", "upload_peak_kbps_p90", "upload_peak_kbps_hist",
		"upload_saturated_count", "upload_unsaturated_count", "upload_policed_count",
		"ttfb_ms_hist", "dial_ms_hist", "speed_limit_down_kbps", "speed_limit_up_kbps"} {
		if fields[name] == nil {
			t.Errorf("report omits %s", name)
		}
	}
}

func TestQualitySpeedLimitOfHandler(t *testing.T) {
	if got := qualitySpeedLimitOf(nil); got != (qualitySpeedLimit{}) {
		t.Fatalf("missing handler has a limit: %+v", got)
	}
	if got := qualitySpeedLimitOf(&v2Handler{}); got != (qualitySpeedLimit{}) {
		t.Fatalf("handler without speed limits has one: %+v", got)
	}
	// 5mbps and 20mbps of the config, binary like brutalUp.
	if got := qualitySpeedLimitOf(&qualityLimitedHandler{up: 5 << 20 / 8, down: 20 << 20 / 8}); got != (qualitySpeedLimit{upKbps: 5242, downKbps: 20971}) {
		t.Fatalf("limit %+v", got)
	}
}
