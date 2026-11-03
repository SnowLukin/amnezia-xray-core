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
	if saturated, known := qualitySaturation(end); !saturated || !known {
		t.Fatal("80 ms of queue above a 40 ms path is not saturation")
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

func TestQualitySaturation(t *testing.T) {
	rtt := func(minRTT, loadRTT time.Duration) qualityEndSample {
		return qualityEndSample{
			s1: tcpQualitySnapshot{minRTT: minRTT, present: qualityHasMinRTT},
			s2: tcpQualitySnapshot{loadRTT: loadRTT, present: qualityHasLoadRTT},
		}
	}
	loss := func(outOfOrder, total uint64) qualityEndSample {
		return qualityEndSample{
			s1: tcpQualitySnapshot{present: qualityHasRX},
			s2: tcpQualitySnapshot{rxOutOfOrder: outOfOrder, rxTotal: total, present: qualityHasRX},
		}
	}
	for name, c := range map[string]struct {
		s                qualityEndSample
		saturated, known bool
	}{
		"no signals":               {qualityEndSample{}, false, false},
		"queue on a short path":    {rtt(20*time.Millisecond, 55*time.Millisecond), true, true},
		"small queue":              {rtt(20*time.Millisecond, 45*time.Millisecond), false, true},
		"queue below half of long": {rtt(200*time.Millisecond, 290*time.Millisecond), false, true},
		"queue above half of long": {rtt(200*time.Millisecond, 300*time.Millisecond), true, true},
		"loss":                     {loss(2, 100), true, true},
		"rare reordering":          {loss(1, 1000), false, true},
		"counter reset":            {loss(0, 0), false, false},
	} {
		if saturated, known := qualitySaturation(c.s); saturated != c.saturated || known != c.known {
			t.Errorf("%s: saturated %v known %v, want %v %v", name, saturated, known, c.saturated, c.known)
		}
	}
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
		{hasBulk: true, bulkKbps: 100, limit: qualitySpeedLimit{downKbps: 8000, upKbps: 2000}},
	} {
		a.addEnd(s)
	}
	v := a.view()
	if v.BulkCount != 4 || v.BulkPeakCount != 3 || *v.BulkSaturatedCount != 1 || *v.BulkUnsaturatedCount != 1 {
		t.Fatalf("counts %+v", v)
	}
	for name, h := range map[string]struct {
		buckets []uint64
		total   uint64
	}{"bulk": {v.BulkKbpsHist, v.BulkCount}, "peak": {v.BulkPeakKbpsHist, v.BulkPeakCount}, "saturated": {v.BulkSaturatedKbpsHist, *v.BulkSaturatedCount}} {
		var sum uint64
		for _, n := range h.buckets {
			sum += n
		}
		if len(h.buckets) != len(qualitySpeedBounds)+1 || sum != h.total {
			t.Errorf("%s buckets %v do not add up to %d", name, h.buckets, h.total)
		}
	}
	if v.BulkPeakKbpsHist[len(qualitySpeedBounds)] != 1 || v.BulkKbpsHist[0] != 1 {
		t.Fatalf("speeds outside the bounds lost their buckets: %v %v", v.BulkKbpsHist, v.BulkPeakKbpsHist)
	}
	qualitySummaryInt(t, v.BulkSaturatedKbpsP90, 5000)
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
		"speed_limit_down_kbps", "speed_limit_up_kbps"} {
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
