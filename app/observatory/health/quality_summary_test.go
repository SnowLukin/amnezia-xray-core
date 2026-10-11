package health

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func qualitySummaryJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func qualitySummaryPath(t *testing.T, v qualitySegmentView, tag string) qualityPathView {
	t.Helper()
	p, ok := v.Paths[tag]
	if !ok {
		t.Fatalf("missing path %q in %+v", tag, v)
	}
	return p
}

func qualitySummaryInt(t *testing.T, p *int64, want int64) {
	t.Helper()
	if p == nil {
		t.Fatalf("measurement absent, want %d", want)
	}
	if *p != want {
		t.Fatalf("measurement %d, want %d", *p, want)
	}
}

func qualitySummaryFraction(t *testing.T, p *float64, want float64) {
	t.Helper()
	if p == nil {
		t.Fatalf("fraction absent, want %.12g", want)
	}
	if math.IsNaN(*p) || math.IsInf(*p, 0) || math.Abs(*p-want) > 1e-12 {
		t.Fatalf("fraction %.12g, want %.12g", *p, want)
	}
}

func qualitySummaryFullInitial() qualityInitialSample {
	return qualityInitialSample{
		ttfb: 175 * time.Millisecond, dial: 80 * time.Millisecond, hasTTFB: true, hasDial: true,
		tcp: tcpQualitySnapshot{rtt: 45 * time.Millisecond, minRTT: 37 * time.Millisecond, present: qualityHasRTT | qualityHasMinRTT},
	}
}

func qualitySummaryFullEnd(n int64) qualityEndSample {
	s2 := tcpQualitySnapshot{
		loadRTT: 200 * time.Millisecond, rxOutOfOrder: 15, rxTotal: 1100, txRetransmit: 20, txTotal: 2000,
		present: qualityHasLoadRTT | qualityHasRX | qualityHasTX,
	}
	return qualityEndSample{
		bytesDown: n, bulkKbps: 2410, hasBulk: true,
		s1: tcpQualitySnapshot{rxOutOfOrder: 5, rxTotal: 100, present: qualityHasRX}, s2: s2, lastTCP: s2,
	}
}

func TestQualitySummaryHistogramBoundaries(t *testing.T) {
	var emptyTime qualityTimeHistogram
	var emptySpeed qualitySpeedHistogram
	if emptyTime.quantile(.5) != -1 || emptySpeed.quantile(.9) != -1 {
		t.Fatal("empty histogram supplied a measured quantile")
	}
	times := []time.Duration{
		10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond,
		50 * time.Millisecond, 60 * time.Millisecond, 80 * time.Millisecond, 100 * time.Millisecond,
		125 * time.Millisecond, 150 * time.Millisecond, 200 * time.Millisecond, 250 * time.Millisecond,
		300 * time.Millisecond, 400 * time.Millisecond, 500 * time.Millisecond, 750 * time.Millisecond,
		time.Second, 1500 * time.Millisecond, 2 * time.Second, 3 * time.Second, 5 * time.Second, 8 * time.Second,
	}
	for i, boundary := range times {
		var exact, above qualityTimeHistogram
		exact.add(boundary)
		above.add(boundary + time.Nanosecond)
		wantAbove := times[min(i+1, len(times)-1)].Milliseconds()
		if exact.quantile(.5) != boundary.Milliseconds() || above.quantile(.9) != wantAbove {
			t.Fatalf("time boundary %v: exact %d, above %d, want above %d", boundary, exact.quantile(.5), above.quantile(.9), wantAbove)
		}
	}
	speeds := []int64{128, 256, 512, 1000, 2000, 3000, 5000, 8000, 12000, 20000, 30000, 50000, 80000, 120000, 200000, 300000, 500000}
	for i, boundary := range speeds {
		var exact, above qualitySpeedHistogram
		exact.add(boundary)
		above.add(boundary + 1)
		wantAbove := speeds[min(i+1, len(speeds)-1)]
		if exact.quantile(.5) != boundary || above.quantile(.9) != wantAbove {
			t.Fatalf("speed boundary %d: exact %d, above %d, want above %d", boundary, exact.quantile(.5), above.quantile(.9), wantAbove)
		}
	}
	var floorTime qualityTimeHistogram
	for _, d := range []time.Duration{-time.Second, 0} {
		floorTime.add(d)
	}
	if floorTime.quantile(.9) != 10 {
		t.Fatal("known zero/negative duration did not enter the first bucket")
	}
}

func TestQualitySummaryHistogramRanksAndOverflow(t *testing.T) {
	var durations qualityTimeHistogram
	var speeds qualitySpeedHistogram
	for _, d := range []time.Duration{10, 20, 30, 40} {
		durations.add(d * time.Millisecond)
	}
	for _, kbps := range []int64{128, 256, 512, 1000} {
		speeds.add(kbps)
	}
	for _, tc := range []struct {
		q           float64
		time, speed int64
	}{{0, 10, 128}, {.1, 10, 128}, {.5, 20, 256}, {.9, 40, 1000}, {1, 40, 1000}} {
		if durations.quantile(tc.q) != tc.time || speeds.quantile(tc.q) != tc.speed {
			t.Fatalf("rank round(q*N), q=%v: time %d, speed %d", tc.q, durations.quantile(tc.q), speeds.quantile(tc.q))
		}
	}
	durations.add(50 * time.Millisecond)
	speeds.add(2000)
	if durations.quantile(.5) != 30 || durations.quantile(.9) != 50 || speeds.quantile(.5) != 512 || speeds.quantile(.9) != 2000 {
		t.Fatal("half-rank was not rounded upward for five samples")
	}
	var overflowTime qualityTimeHistogram
	var overflowSpeed qualitySpeedHistogram
	overflowTime.add(time.Hour)
	overflowSpeed.add(math.MaxInt64)
	if overflowTime.quantile(.9) != 8000 || overflowSpeed.quantile(.9) != 500000 {
		t.Fatal("overflow bucket invented a quantile above the last finite bound")
	}
	var floor qualitySpeedHistogram
	floor.add(-1)
	floor.add(0)
	if floor.quantile(.9) != 128 {
		t.Fatal("known zero/negative speed did not enter the first bucket")
	}
}

func TestQualitySummaryOptionalJSONAndKnownZeros(t *testing.T) {
	q := newQualitySummary(t0)
	q.addInitial(0, "ignored", false, qualityInitialSample{ttfb: time.Second, tcp: tcpQualitySnapshot{rtt: time.Second}})
	if views, _ := q.report(t0, false); len(views) != 0 {
		t.Fatal("values without presence flags admitted an empty segment")
	}
	q.addInitial(0, "initial", false, qualityInitialSample{hasTTFB: true, tcp: tcpQualitySnapshot{present: qualityHasMinRTT}})
	initial := qualitySummaryPath(t, q.currentView(t0), "initial")
	qualitySummaryInt(t, initial.TTFBP50Ms, 10)
	qualitySummaryInt(t, initial.TCPRTTMinMs, 0)
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(qualitySummaryJSON(t, initial), &fields); err != nil {
		t.Fatal(err)
	}
	for _, absent := range []string{"bytes_down", "dial_count", "dial_p50_ms", "tcp_count", "tcp_rtt_p50_ms", "bulk_count", "bulk_spliced_count", "rx_out_of_order_fraction", "tx_retransmit_fraction", "bytes_down_overflow"} {
		if _, exists := fields[absent]; exists {
			t.Errorf("unmeasured field %q was emitted", absent)
		}
	}
	if string(fields["tcp_rtt_min_ms"]) != "0" || string(fields["ttfb_count"]) != "1" {
		t.Fatalf("known zero/count disappeared: %s", qualitySummaryJSON(t, initial))
	}
	q.addInitial(0, "zero", false, qualityInitialSample{hasTTFB: true, hasDial: true, tcp: tcpQualitySnapshot{present: qualityHasRTT | qualityHasMinRTT}})
	q.addEnd(0, "zero", false, qualityEndSample{
		hasBulk: true, bulkKbps: 0,
		s1:      tcpQualitySnapshot{rxTotal: 100, present: qualityHasRX},
		s2:      tcpQualitySnapshot{rxTotal: 200, present: qualityHasRX | qualityHasLoadRTT},
		lastTCP: tcpQualitySnapshot{txTotal: 100, present: qualityHasTX},
	})
	zero := qualitySummaryPath(t, q.currentView(t0), "zero")
	qualitySummaryInt(t, zero.BytesDown, 0)
	qualitySummaryInt(t, zero.BulkKbpsMax, 0)
	qualitySummaryInt(t, zero.BulkRTTP50Ms, 10)
	qualitySummaryInt(t, zero.DialP50Ms, 10)
	qualitySummaryInt(t, zero.TCPRTTP50Ms, 10)
	qualitySummaryFraction(t, zero.RXOutOfOrderFraction, 0)
	qualitySummaryFraction(t, zero.TXRetransmitFraction, 0)
	if zero.BulkSplicedCount == nil || *zero.BulkSplicedCount != 0 || zero.BulkCount != 1 || zero.TCPCount != 1 {
		t.Fatalf("known zero bulk/TCP presence was lost: %+v", zero)
	}
	fields = nil
	if err := json.Unmarshal(qualitySummaryJSON(t, zero), &fields); err != nil {
		t.Fatal(err)
	}
	for _, knownZero := range []string{"bytes_down", "tcp_rtt_min_ms", "bulk_kbps_max", "bulk_spliced_count", "rx_out_of_order_fraction", "tx_retransmit_fraction"} {
		if string(fields[knownZero]) != "0" {
			t.Errorf("known zero field %q omitted or changed: %s", knownZero, fields[knownZero])
		}
	}
}

func TestQualitySummaryKnownZeroCompletionBeforeAndAfterTake(t *testing.T) {
	for _, take := range []bool{false, true} {
		t.Run(fmt.Sprintf("take_%v", take), func(t *testing.T) {
			q := newQualitySummary(t0)
			q.addInitial(0, "a", false, qualityInitialSample{hasDial: true, dial: 50 * time.Millisecond})
			if take {
				q.report(t0.Add(time.Second), true)
			}
			q.addEnd(0, "a", false, qualityEndSample{hasBytes: true})
			views, dropped := q.report(t0.Add(2*time.Second), false)
			if len(views) != 1 || dropped != 0 {
				t.Fatal("known zero completion did not produce its measured report phase")
			}
			p := qualitySummaryPath(t, views[0], "a")
			qualitySummaryInt(t, p.BytesDown, 0)
			if p.TTFBCount != 0 || p.BulkCount != 0 || (take && p.DialCount != 0) || (!take && p.DialCount != 1) {
				t.Fatalf("zero-byte completion invented/double-counted initial timing: %+v", p)
			}
			current := qualitySummaryPath(t, q.currentView(t0.Add(2*time.Second)), "a")
			qualitySummaryInt(t, current.BytesDown, 0)
			if current.DialCount != 1 {
				t.Fatal("zero-byte completion lost current-network dial history")
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(qualitySummaryJSON(t, p), &fields); err != nil {
				t.Fatal(err)
			}
			if string(fields["bytes_down"]) != "0" {
				t.Fatal("known zero completion disappeared from JSON")
			}
		})
	}
	q := newQualitySummary(t0)
	q.addInitial(0, "a", false, qualityInitialSample{hasDial: true})
	q.addEnd(0, "a", false, qualityEndSample{})
	if qualitySummaryPath(t, q.currentView(t0), "a").BytesDown != nil {
		t.Fatal("unmeasured empty End invented known bytes")
	}
	q.report(t0.Add(time.Second), true)
	q.addEnd(0, "a", false, qualityEndSample{})
	if views, _ := q.report(t0.Add(2*time.Second), false); len(views) != 0 {
		t.Fatal("unmeasured empty End created a new report segment")
	}
}

func TestQualitySummaryMetricDistributionsAndIndependentLoadRTT(t *testing.T) {
	q := newQualitySummary(t0)
	ttfb := []time.Duration{11, 20, 31, 50, 120, 500}
	dial := []time.Duration{0, 11, 80, 99, 149, 999}
	rtt := []time.Duration{15, 45, 90, 125, 130, 250}
	minRTT := []time.Duration{12000, 11000, 10000, 9000, 8000, 7350}
	speed := []int64{129, 1000, 2410, 2999, 7000, 600000}
	for i := range ttfb {
		q.addInitial(0, "a", false, qualityInitialSample{
			ttfb: ttfb[i] * time.Millisecond, dial: dial[i] * time.Millisecond, hasTTFB: true, hasDial: true,
			tcp: tcpQualitySnapshot{rtt: rtt[i] * time.Millisecond, minRTT: minRTT[i] * time.Microsecond, present: qualityHasRTT | qualityHasMinRTT},
		})
		q.addEnd(0, "a", false, qualityEndSample{
			bytesDown: 1 << 20, hasBulk: true, bulkKbps: speed[i],
			s2: tcpQualitySnapshot{loadRTT: 150 * time.Millisecond, present: qualityHasLoadRTT},
		})
	}
	// A counted bulk connection without a usable speed interval can still
	// contribute its successful S2 load RTT; it is not a new bulk speed sample.
	q.addEnd(0, "a", false, qualityEndSample{bytesDown: 1 << 20, s2: tcpQualitySnapshot{loadRTT: 50 * time.Millisecond, present: qualityHasLoadRTT}})
	p := qualitySummaryPath(t, q.currentView(t0), "a")
	if p.TTFBCount != 6 || p.DialCount != 6 || p.TCPCount != 6 || p.BulkCount != 6 {
		t.Fatalf("independent histogram counts: %+v", p)
	}
	qualitySummaryInt(t, p.TTFBP50Ms, 40)
	qualitySummaryInt(t, p.TTFBP90Ms, 125)
	qualitySummaryInt(t, p.DialP50Ms, 80)
	qualitySummaryInt(t, p.DialP90Ms, 150)
	qualitySummaryInt(t, p.TCPRTTP50Ms, 100)
	qualitySummaryInt(t, p.TCPRTTP90Ms, 150)
	qualitySummaryInt(t, p.TCPRTTMinMs, 7)
	qualitySummaryInt(t, p.BulkKbpsP50, 3000)
	qualitySummaryInt(t, p.BulkKbpsP90, 8000)
	qualitySummaryInt(t, p.BulkKbpsMax, 600000)
	qualitySummaryInt(t, p.BulkRTTP50Ms, 150)
	qualitySummaryInt(t, p.BytesDown, 7<<20)
}

func TestQualitySummaryWeightedFractionsUseDeltasAndLastTX(t *testing.T) {
	q := newQualitySummary(t0)
	s2 := tcpQualitySnapshot{rxOutOfOrder: 12, rxTotal: 200, txRetransmit: 5, txTotal: 10, present: qualityHasRX | qualityHasTX}
	q.addEnd(0, "a", false, qualityEndSample{
		bytesDown: 1 << 20,
		s1:        tcpQualitySnapshot{rxOutOfOrder: 2, rxTotal: 100, txRetransmit: 2, txTotal: 8, present: qualityHasRX | qualityHasTX},
		s2:        s2, lastTCP: s2,
	})
	s1 := tcpQualitySnapshot{rxTotal: 50, txRetransmit: 90, txTotal: 990, present: qualityHasRX | qualityHasTX}
	q.addEnd(0, "a", false, qualityEndSample{
		bytesDown: 1 << 20, s1: s1,
		s2: tcpQualitySnapshot{rxOutOfOrder: 3, rxTotal: 950, present: qualityHasRX}, lastTCP: s1,
	})
	p := qualitySummaryPath(t, q.currentView(t0), "a")
	qualitySummaryFraction(t, p.RXOutOfOrderFraction, .013) // (10+3)/(100+900).
	qualitySummaryFraction(t, p.TXRetransmitFraction, .095) // (5+90)/(10+990), once per connection.
	qualitySummaryInt(t, p.BytesDown, 2<<20)
}

func TestQualitySummaryInvalidRXAndZeroDenominatorsStayAbsent(t *testing.T) {
	valid := tcpQualitySnapshot{rxOutOfOrder: 10, rxTotal: 100, present: qualityHasRX}
	for _, tc := range []struct {
		name   string
		s1, s2 tcpQualitySnapshot
	}{
		{"missing_s1", tcpQualitySnapshot{}, valid},
		{"missing_s2", valid, tcpQualitySnapshot{}},
		{"decreasing_ooo", valid, tcpQualitySnapshot{rxOutOfOrder: 9, rxTotal: 200, present: qualityHasRX}},
		{"decreasing_total", valid, tcpQualitySnapshot{rxOutOfOrder: 20, rxTotal: 99, present: qualityHasRX}},
		{"zero_denominator", valid, tcpQualitySnapshot{rxOutOfOrder: 20, rxTotal: 100, present: qualityHasRX}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			q := newQualitySummary(t0)
			q.addEnd(0, "a", false, qualityEndSample{bytesDown: 1, s1: tc.s1, s2: tc.s2, lastTCP: tcpQualitySnapshot{txRetransmit: 3, present: qualityHasTX}})
			p := qualitySummaryPath(t, q.currentView(t0), "a")
			if p.RXOutOfOrderFraction != nil || p.TXRetransmitFraction != nil {
				t.Fatalf("unavailable/zero denominator became a fraction: %+v", p)
			}
			q.addEnd(0, "a", false, qualityEndSample{bytesDown: 1, s1: valid, s2: tcpQualitySnapshot{rxOutOfOrder: 20, rxTotal: 200, present: qualityHasRX}})
			qualitySummaryFraction(t, qualitySummaryPath(t, q.currentView(t0), "a").RXOutOfOrderFraction, .1)
		})
	}
}

func TestQualitySummaryCounterSumsDoNotWrapAtUint64(t *testing.T) {
	q := newQualitySummary(t0)
	for i := 0; i < 2; i++ {
		q.addEnd(0, "a", false, qualityEndSample{
			bytesDown: 1 << 20,
			s1:        tcpQualitySnapshot{present: qualityHasRX},
			s2:        tcpQualitySnapshot{rxOutOfOrder: math.MaxUint64 / 3, rxTotal: math.MaxUint64, present: qualityHasRX},
			lastTCP:   tcpQualitySnapshot{txRetransmit: math.MaxUint64 / 4, txTotal: math.MaxUint64, present: qualityHasTX},
		})
	}
	p := qualitySummaryPath(t, q.currentView(t0), "a")
	qualitySummaryFraction(t, p.RXOutOfOrderFraction, 1.0/3)
	qualitySummaryFraction(t, p.TXRetransmitFraction, .25)
}

func TestQualitySummaryBulkSpliceClassDoesNotInventSpeed(t *testing.T) {
	q := newQualitySummary(t0)
	q.addEnd(0, "small", false, qualityEndSample{bytesDown: (1 << 20) - 1})
	q.addEnd(0, "counted", false, qualityEndSample{bytesDown: 1 << 20})
	q.addEnd(0, "spliced", false, qualityEndSample{bytesDown: 1 << 20, spliced: true})
	current := q.currentView(t0)
	if qualitySummaryPath(t, current, "small").BulkSplicedCount != nil {
		t.Fatal("small connection created a bulk-splice measurement")
	}
	counted := qualitySummaryPath(t, current, "counted")
	if counted.BulkSplicedCount == nil || *counted.BulkSplicedCount != 0 || counted.BulkCount != 0 {
		t.Fatal("counted bulk without a speed interval lost its measured zero splice count")
	}
	spliced := qualitySummaryPath(t, current, "spliced")
	if spliced.BulkSplicedCount == nil || *spliced.BulkSplicedCount != 1 || spliced.BulkCount != 0 || spliced.BulkKbpsMax != nil {
		t.Fatal("spliced bulk invented a speed sample or omitted its splice count")
	}
}

func TestQualitySummaryWindowCutSplitsPhasesWithoutResettingCurrent(t *testing.T) {
	q := newQualitySummary(t0)
	epoch := q.networkChanged("wifi:private", true, t0)
	q.addInitial(epoch, "a", false, qualitySummaryFullInitial())
	windowA, dropped := q.report(t0.Add(20*time.Second), true)
	if len(windowA) != 1 || dropped != 0 || windowA[0].DurationMs != 20000 {
		t.Fatalf("initial window: %+v, dropped %d", windowA, dropped)
	}
	a := qualitySummaryPath(t, windowA[0], "a")
	if a.TTFBCount != 1 || a.DialCount != 1 || a.TCPCount != 1 || a.BytesDown != nil || a.BulkCount != 0 {
		t.Fatalf("initial phase went to the wrong window: %+v", a)
	}
	if empty, dropped := q.report(t0.Add(21*time.Second), false); len(empty) != 0 || dropped != 0 {
		t.Fatal("take left the old window populated")
	}
	q.addEnd(epoch, "a", false, qualitySummaryFullEnd(2<<20))
	windowB, _ := q.report(t0.Add(30*time.Second), false)
	if len(windowB) != 1 || windowB[0].DurationMs != 10000 {
		t.Fatalf("end window duration: %+v", windowB)
	}
	b := qualitySummaryPath(t, windowB[0], "a")
	if b.TTFBCount != 0 || b.DialCount != 0 || b.TCPCount != 0 || b.BulkCount != 1 {
		t.Fatalf("end phase double-counted the initial phase: %+v", b)
	}
	current := q.currentView(t0.Add(30 * time.Second))
	c := qualitySummaryPath(t, current, "a")
	qualitySummaryInt(t, c.BytesDown, 2<<20)
	if current.DurationMs != 30000 || c.TTFBCount != 1 || c.DialCount != 1 || c.TCPCount != 1 || c.BulkCount != 1 || q.epoch != epoch {
		t.Fatalf("take reset current-network history: %+v", current)
	}
	if qualitySummaryPath(t, windowA[0], "a").BytesDown != nil {
		t.Fatal("later completion mutated a previously taken report")
	}
}

func TestQualitySummaryAdmissionScopesAreIndependentAfterTake(t *testing.T) {
	q := newQualitySummary(t0)
	for i := 0; i < 64; i++ {
		q.addInitial(0, fmt.Sprintf("old%02d", i), false, qualityInitialSample{hasTTFB: true, ttfb: 20 * time.Millisecond})
	}
	q.report(t0.Add(time.Second), true)
	q.addInitial(0, "new00", false, qualityInitialSample{hasTTFB: true, ttfb: 30 * time.Millisecond})
	q.addEnd(0, "new00", false, qualityEndSample{bytesDown: 100})
	if len(q.current.paths) != 64 || q.current.paths["new00"] != nil || q.current.samplesDropped != 2 || len(q.open.paths) != 1 || q.open.samplesDropped != 0 {
		t.Fatal("full current scope prevented admission to the fresh window")
	}
	for i := 1; i < 64; i++ {
		q.addInitial(0, fmt.Sprintf("new%02d", i), false, qualityInitialSample{hasTTFB: true})
	}
	q.addEnd(0, "old00", false, qualityEndSample{bytesDown: 200})
	if q.open.paths["old00"] != nil || q.open.samplesDropped != 1 || q.current.samplesDropped != 65 {
		t.Fatalf("opposite scope admission failed: open drops %d, current drops %d", q.open.samplesDropped, q.current.samplesDropped)
	}
	current := qualitySummaryPath(t, q.currentView(t0.Add(2*time.Second)), "old00")
	qualitySummaryInt(t, current.BytesDown, 200)
	if current.TTFBCount != 1 {
		t.Fatal("returning current-admitted tag lost its earlier history")
	}
	views, _ := q.report(t0.Add(2*time.Second), true)
	if len(views) != 1 || views[0].SamplesDropped != 1 || views[0].PathsDropped != 60 || len(views[0].Paths) != 4 {
		t.Fatalf("window limits/drops mixed independent scopes: %+v", views)
	}
	if q.current.samplesDropped != 65 || len(q.current.paths) != 64 || len(q.open.paths) != 0 || q.open.samplesDropped != 0 {
		t.Fatal("take reset the current admission budget or kept the old window budget")
	}
}

func TestQualitySummaryStaleEpochIsIgnoredWithoutDrops(t *testing.T) {
	q := newQualitySummary(t0)
	old := q.networkChanged("cell:25001:private", true, t0)
	q.addInitial(old, "old", false, qualityInitialSample{hasTTFB: true})
	currentEpoch := q.networkChanged("wifi:new-private", false, t0.Add(time.Second))
	for i := 0; i < 64; i++ {
		q.addEnd(currentEpoch, fmt.Sprintf("p%02d", i), false, qualityEndSample{bytesDown: 1})
	}
	before := qualitySummaryJSON(t, q.currentView(t0.Add(2*time.Second)))
	q.addInitial(old, "stale", false, qualitySummaryFullInitial())
	q.addEnd(old, "stale", false, qualitySummaryFullEnd(1<<30))
	q.addInitial(old, "ignored-witness", true, qualitySummaryFullInitial())
	q.addEnd(old, "ignored-witness", true, qualitySummaryFullEnd(1<<30))
	if !bytes.Equal(before, qualitySummaryJSON(t, q.currentView(t0.Add(2*time.Second)))) || q.open.samplesDropped != 0 {
		t.Fatal("stale measurements changed current history or admission drop counts")
	}
	views, dropped := q.report(t0.Add(2*time.Second), false)
	if len(views) != 2 || dropped != 0 || len(views[0].Paths) != 1 || views[0].Witness != nil || views[1].Witness != nil {
		t.Fatalf("stale measurement reopened its closed segment: %+v", views)
	}
}

func TestQualitySummaryNetworkPrefixesDoNotExposeFingerprint(t *testing.T) {
	for i, tc := range []struct{ id, typ, operator string }{
		{"wifi:PRIVATE_SESSION", "wifi", ""}, {"eth:PRIVATE_SESSION", "eth", ""}, {"other:PRIVATE_SESSION", "other", ""},
		{"cell:25001:PRIVATE_SESSION", "cell", "25001"}, {"cell:310260:PRIVATE_SESSION:extra", "cell", "310260"},
		{"PRIVATE_SESSION", "unknown", ""}, {"wifi:", "unknown", ""}, {"WIFI:PRIVATE_SESSION", "unknown", ""},
		{"cell:2501:PRIVATE_SESSION", "unknown", ""}, {"cell:2500011:PRIVATE_SESSION", "unknown", ""},
		{"cell:2500x:PRIVATE_SESSION", "unknown", ""}, {"cell:+2501:PRIVATE_SESSION", "unknown", ""},
		{"cell:25001", "unknown", ""}, {"cell:25001:", "unknown", ""}, {"vpn:PRIVATE_SESSION", "unknown", ""},
		{"", "unknown", ""}, {":PRIVATE_SESSION", "unknown", ""}, {" wifi:PRIVATE_SESSION", "unknown", ""},
		{"cell:２５００１:PRIVATE_SESSION", "unknown", ""},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			q := newQualitySummary(t0)
			epoch := q.networkChanged(tc.id, i%2 == 0, t0)
			q.addInitial(epoch, "a", false, qualityInitialSample{hasTTFB: true})
			v := q.currentView(t0)
			if v.NetworkType != tc.typ || v.NetworkOperator != tc.operator || v.IsNetworkKnown != (i%2 == 0) {
				t.Fatalf("metadata from %q: %+v", tc.id, v)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(qualitySummaryJSON(t, v), &fields); err != nil {
				t.Fatal(err)
			}
			if fields["is_network_known"] == nil || string(fields["duration_ms"]) != "0" || (tc.operator == "" && fields["network_operator"] != nil) {
				t.Fatal("metadata JSON changed required known/duration presence or emitted an unknown operator")
			}
			views, _ := q.report(t0, false)
			if strings.Contains(string(qualitySummaryJSON(t, views)), "PRIVATE_SESSION") || strings.Contains(string(qualitySummaryJSON(t, v)), "PRIVATE_SESSION") {
				t.Fatal("opaque fingerprint leaked into public quality metadata")
			}
		})
	}
}

func TestQualitySummaryDurationsUseWallTimeAndClampBackwardClock(t *testing.T) {
	start := time.Now()
	q := newQualitySummary(start)
	q.addInitial(0, "a", false, qualityInitialSample{hasTTFB: true})
	if q.open.started != start.Round(0) || q.current.started != start.Round(0) {
		t.Fatal("collection retained monotonic-clock metadata")
	}
	for _, tc := range []struct {
		delta time.Duration
		want  int64
	}{{1500 * time.Microsecond, 1}, {-time.Second, 0}, {3 * time.Second, 3000}} {
		views, _ := q.report(start.Round(0).Add(tc.delta), false)
		if len(views) != 1 || views[0].DurationMs != tc.want || q.currentView(start.Round(0).Add(tc.delta)).DurationMs != tc.want {
			t.Fatalf("wall duration %v: %+v, want %d ms", tc.delta, views, tc.want)
		}
	}
	q.networkChanged("wifi:private", false, start.Round(0).Add(-time.Second))
	views, _ := q.report(start.Add(time.Hour), false)
	if len(views) != 1 || views[0].DurationMs != 0 {
		t.Fatal("backward network-change timestamp produced a negative/expanding closed segment")
	}
}

func TestQualitySummaryEmptySegmentsAndFifthDropCount(t *testing.T) {
	q := newQualitySummary(t0)
	for i := 0; i < 5; i++ {
		q.networkChanged(fmt.Sprintf("wifi:empty%d", i), false, t0.Add(time.Duration(i)*time.Second))
	}
	if views, dropped := q.report(t0.Add(5*time.Second), false); len(views) != 0 || dropped != 0 {
		t.Fatal("empty networks counted as report segments")
	}
	for i := 0; i < 4; i++ {
		q.addInitial(q.epoch, fmt.Sprintf("seg%d", i), false, qualityInitialSample{hasTTFB: true})
		q.networkChanged(fmt.Sprintf("wifi:next%d", i), false, t0.Add(time.Duration(6+i)*time.Second))
	}
	q.networkChanged("eth:empty-transition", true, t0.Add(10*time.Second))
	if views, dropped := q.report(t0.Add(11*time.Second), false); len(views) != 4 || dropped != 0 {
		t.Fatal("an empty fifth network evicted a measured segment")
	}
	q.addInitial(q.epoch, "latest", false, qualityInitialSample{hasTTFB: true})
	for i := 0; i < 3; i++ {
		views, dropped := q.report(t0.Add(12*time.Second), false)
		if len(views) != 4 || dropped != 1 || views[0].Paths["seg0"].TTFBCount != 0 || views[0].Paths["seg1"].TTFBCount != 1 || views[3].Paths["latest"].TTFBCount != 1 {
			t.Fatalf("latest nonempty four/drop snapshot: %+v, dropped %d", views, dropped)
		}
	}
	taken, dropped := q.report(t0.Add(12*time.Second), true)
	if len(taken) != 4 || dropped != 1 {
		t.Fatal("take did not return the accumulated segment drop exactly once")
	}
	if views, dropped := q.report(t0.Add(13*time.Second), false); len(views) != 0 || dropped != 0 {
		t.Fatal("take did not reset report segment/drop scope")
	}
	if q.currentView(t0.Add(13 * time.Second)).Paths["latest"].TTFBCount != 1 {
		t.Fatal("segment take cleared current-network history")
	}
}

func TestQualitySummaryLateTopFourPromotionAndRevivalKeepHistory(t *testing.T) {
	q := newQualitySummary(t0)
	for _, tag := range []string{"a", "b", "c", "d"} {
		q.addEnd(0, tag, false, qualityEndSample{bytesDown: 3 << 20})
	}
	q.addInitial(0, "z", false, qualityInitialSample{hasTTFB: true, ttfb: 500 * time.Millisecond})
	q.addEnd(0, "z", false, qualityEndSample{bytesDown: 1 << 20, hasBulk: true, bulkKbps: 128})
	before, _ := q.report(t0, false)
	if len(before) != 1 || before[0].PathsDropped != 1 || len(before[0].Paths) != 4 || before[0].Paths["z"].TTFBCount != 0 {
		t.Fatal("top-four did not initially retain the four busiest tags")
	}
	q.addInitial(0, "z", false, qualityInitialSample{hasTTFB: true, ttfb: 20 * time.Millisecond})
	q.addEnd(0, "z", false, qualityEndSample{bytesDown: 4 << 20, hasBulk: true, bulkKbps: 5000})
	promoted, _ := q.report(t0, false)
	z := qualitySummaryPath(t, promoted[0], "z")
	qualitySummaryInt(t, z.BytesDown, 5<<20)
	qualitySummaryInt(t, z.TTFBP50Ms, 20)
	qualitySummaryInt(t, z.TTFBP90Ms, 500)
	qualitySummaryInt(t, z.BulkKbpsP50, 128)
	qualitySummaryInt(t, z.BulkKbpsP90, 5000)
	if z.TTFBCount != 2 || z.BulkCount != 2 || promoted[0].Paths["d"].BytesDown != nil {
		t.Fatal("late promotion lost history or violated lexical tie order")
	}
	for _, tag := range []string{"a", "b", "c", "y"} {
		q.addEnd(0, tag, false, qualityEndSample{bytesDown: 10 << 20})
	}
	outside, _ := q.report(t0, false)
	if _, exists := outside[0].Paths["z"]; exists {
		t.Fatal("z remained top-four after four heavier paths arrived")
	}
	q.addInitial(0, "z", false, qualityInitialSample{hasTTFB: true, ttfb: 60 * time.Millisecond})
	q.addEnd(0, "z", false, qualityEndSample{bytesDown: 20 << 20, hasBulk: true, bulkKbps: 8000})
	revived, _ := q.report(t0, false)
	z = qualitySummaryPath(t, revived[0], "z")
	qualitySummaryInt(t, z.BytesDown, 25<<20)
	qualitySummaryInt(t, z.TTFBP50Ms, 60)
	qualitySummaryInt(t, z.TTFBP90Ms, 500)
	qualitySummaryInt(t, z.BulkKbpsP50, 5000)
	if z.TTFBCount != 3 || z.BulkCount != 3 || revived[0].PathsDropped != 2 {
		t.Fatalf("returning tag lost cumulative history: %+v", z)
	}
}

func TestQualitySummaryAdmissionAndChurnStayBounded(t *testing.T) {
	q := newQualitySummary(t0)
	for i := 0; i < 64; i++ {
		q.addInitial(0, fmt.Sprintf("p%02d", i), false, qualityInitialSample{hasTTFB: true, ttfb: 20 * time.Millisecond})
	}
	for i := 0; i < 1000; i++ {
		tag := fmt.Sprintf("rejected%04d", i)
		q.addInitial(0, tag, false, qualityInitialSample{hasTTFB: true})
		q.addEnd(0, tag, false, qualityEndSample{bytesDown: 1 << 30})
	}
	q.addInitial(0, "empty", false, qualityInitialSample{})
	q.addInitial(0, "empty", false, qualityInitialSample{tcp: tcpQualitySnapshot{rxTotal: 100, txTotal: 100, present: qualityHasRX | qualityHasTX}})
	q.addEnd(0, "empty", false, qualityEndSample{})
	q.addEnd(0, "empty", false, qualityEndSample{s2: tcpQualitySnapshot{rtt: time.Second, present: qualityHasRTT}})
	q.addInitial(1, "stale", false, qualitySummaryFullInitial())
	q.addEnd(1, "stale", false, qualitySummaryFullEnd(1<<30))
	q.addInitial(0, "p00", false, qualityInitialSample{hasTTFB: true, ttfb: 40 * time.Millisecond})
	q.addEnd(0, "p00", false, qualityEndSample{bytesDown: 42})
	q.addInitial(0, strings.Repeat("w", 20000), true, qualityInitialSample{hasTTFB: true})
	q.addEnd(0, "another-witness-tag", true, qualityEndSample{bytesDown: 123})
	for _, scope := range []*qualityCollection{&q.current, &q.open} {
		if len(scope.paths) != 64 || scope.tagBytes != 64*3 || scope.samplesDropped != 2000 || scope.witness == nil {
			t.Fatalf("churn exceeded retained budgets or miscounted dropped phases: %+v", scope)
		}
	}
	current := q.currentView(t0)
	if len(current.Paths) != 64 || current.PathsDropped != 0 || current.SamplesDropped != 2000 {
		t.Fatalf("current view omitted admitted paths: %+v", current)
	}
	p := qualitySummaryPath(t, current, "p00")
	qualitySummaryInt(t, p.BytesDown, 42)
	qualitySummaryInt(t, p.TTFBP90Ms, 40)
	if p.TTFBCount != 2 || current.Witness.TTFBCount != 1 {
		t.Fatal("budget exhaustion blocked existing keys or independent witness")
	}
	views, _ := q.report(t0, false)
	if len(views) != 1 || len(views[0].Paths) != 4 || views[0].PathsDropped != 60 || views[0].SamplesDropped != 2000 {
		t.Fatalf("report confused retained and rejected paths: %+v", views)
	}
	for _, tag := range []string{"p00", "p01", "p02", "p03"} {
		qualitySummaryPath(t, views[0], tag)
	}
}

func TestQualitySummaryTagByteBudgetPreservesExactIdentity(t *testing.T) {
	q := newQualitySummary(t0)
	a, b := strings.Repeat("a", 10000), strings.Repeat("b", 6384)
	q.addInitial(0, a, false, qualityInitialSample{hasTTFB: true})
	q.addInitial(0, b, false, qualityInitialSample{hasTTFB: true})
	rejected := a + "different-suffix"
	q.addInitial(0, rejected, false, qualityInitialSample{hasTTFB: true})
	q.addEnd(0, rejected, false, qualityEndSample{bytesDown: 999})
	q.addInitial(0, a, false, qualityInitialSample{hasTTFB: true})
	q.addEnd(0, a, false, qualityEndSample{bytesDown: 123})
	for _, scope := range []*qualityCollection{&q.current, &q.open} {
		if len(scope.paths) != 2 || scope.tagBytes != 16384 || scope.samplesDropped != 2 || scope.paths[rejected] != nil {
			t.Fatal("tag-byte limit truncated a key, freed identity, or exceeded its copied-byte cap")
		}
	}
	p := qualitySummaryPath(t, q.currentView(t0), a)
	qualitySummaryInt(t, p.BytesDown, 123)
	if p.TTFBCount != 2 {
		t.Fatal("an admitted tag stopped updating at the copied-byte limit")
	}
	other := newQualitySummary(t0)
	other.addInitial(0, strings.Repeat("x", 16385), false, qualityInitialSample{hasTTFB: true})
	dropOnly, _ := other.report(t0, false)
	if len(dropOnly) != 1 || dropOnly[0].SamplesDropped != 1 || dropOnly[0].PathsDropped != 0 || len(dropOnly[0].Paths) != 0 || other.current.tagBytes != 0 {
		t.Fatal("oversized rejected tag was retained or its missing coverage was hidden")
	}
	other.addEnd(0, "short", false, qualityEndSample{bytesDown: 10})
	if len(other.current.paths) != 1 || other.current.tagBytes != 5 {
		t.Fatal("rejection consumed admission capacity")
	}
}

func TestQualitySummaryTagByteScopesAreIndependentAfterTake(t *testing.T) {
	q := newQualitySummary(t0)
	old := strings.Repeat("o", 16384)
	q.addInitial(0, old, false, qualityInitialSample{hasTTFB: true})
	q.report(t0.Add(time.Second), true)
	q.addInitial(0, "new", false, qualityInitialSample{hasTTFB: true})
	q.addEnd(0, old, false, qualityEndSample{bytesDown: 1})
	if q.current.tagBytes != 16384 || q.current.samplesDropped != 1 || q.current.paths["new"] != nil || len(q.current.paths) != 1 {
		t.Fatal("current tag-byte admission budget changed at the report cut")
	}
	if q.open.tagBytes != 3 || q.open.samplesDropped != 1 || q.open.paths[old] != nil || len(q.open.paths) != 1 {
		t.Fatal("fresh window reused the current tag-byte budget or lost its independent rejection count")
	}
	p := qualitySummaryPath(t, q.currentView(t0.Add(2*time.Second)), old)
	qualitySummaryInt(t, p.BytesDown, 1)
	if p.TTFBCount != 1 {
		t.Fatal("admitted old tag lost its current history when rejected by the window")
	}
}

func TestQualitySummaryByteOverflowKeepsPositiveRankingAndSnapshot(t *testing.T) {
	q := newQualitySummary(t0)
	q.addEnd(0, "z", false, qualityEndSample{bytesDown: math.MaxInt64})
	prior := q.currentView(t0)
	priorJSON := qualitySummaryJSON(t, prior)
	for _, tag := range []string{"a", "b", "c", "d", "z"} {
		if tag != "z" {
			q.addEnd(0, tag, false, qualityEndSample{bytesDown: math.MaxInt64})
		}
		q.addEnd(0, tag, false, qualityEndSample{bytesDown: 1})
	}
	current := q.currentView(t0)
	for _, tag := range []string{"a", "b", "c", "d", "z"} {
		p := qualitySummaryPath(t, current, tag)
		qualitySummaryInt(t, p.BytesDown, math.MaxInt64)
		if !p.BytesDownOverflow {
			t.Fatalf("overflow of %q was not advertised", tag)
		}
	}
	if !bytes.Equal(priorJSON, qualitySummaryJSON(t, prior)) || prior.Paths["z"].BytesDownOverflow || strings.Contains(string(priorJSON), "bytes_down_overflow") {
		t.Fatal("overflow changed the earlier snapshot or emitted its absent flag")
	}
	views, _ := q.report(t0, false)
	if len(views) != 1 || len(views[0].Paths) != 4 || views[0].PathsDropped != 1 {
		t.Fatal("overflow changed top-four capacity")
	}
	for _, tag := range []string{"a", "b", "c", "d"} {
		qualitySummaryPath(t, views[0], tag)
	}
	if _, exists := views[0].Paths["z"]; exists {
		t.Fatal("saturated paths no longer tie by exact tag")
	}
}

func TestQualitySummarySixtyFourPathsAcrossFourSegmentsJSONSize(t *testing.T) {
	q := newQualitySummary(t0)
	epoch := q.networkChanged("cell:25001:private0", true, t0)
	for segment := 0; segment < 4; segment++ {
		for i := 0; i < 64; i++ {
			tag := fmt.Sprintf("p%02d", i)
			q.addInitial(epoch, tag, false, qualitySummaryFullInitial())
			q.addEnd(epoch, tag, false, qualitySummaryFullEnd(int64(i+1)<<20))
		}
		q.addInitial(epoch, "direct", true, qualitySummaryFullInitial())
		q.addEnd(epoch, "direct", true, qualitySummaryFullEnd(96<<20))
		if len(q.current.paths) != 64 || len(q.open.paths) != 64 {
			t.Fatal("ordinary 64-path fixture lost admitted histories")
		}
		if segment < 3 {
			epoch = q.networkChanged(fmt.Sprintf("cell:25001:private%d", segment+1), true, t0.Add(time.Duration(segment+1)*time.Minute))
		}
	}
	views, dropped := q.report(t0.Add(4*time.Minute), false)
	if len(views) != 4 || dropped != 0 || len(q.currentView(t0).Paths) != 64 {
		t.Fatal("64-path/four-segment fixture lost current or report scope")
	}
	for _, segment := range views {
		if len(segment.Paths) != 4 || segment.PathsDropped != 60 || segment.SamplesDropped != 0 || segment.Witness == nil {
			t.Fatalf("incorrect bounded segment: %+v", segment)
		}
	}
	encoded := qualitySummaryJSON(t, struct {
		Quality []qualitySegmentView `json:"quality"`
		Dropped uint64               `json:"quality_segments_dropped"`
	}{views, dropped})
	t.Logf("quality JSON: %d bytes for 64 measured paths in each of four segments; 16 emitted paths plus four witnesses", len(encoded))
	if len(encoded) >= 25*1024 {
		t.Fatalf("ordinary short-tag quality report is %d bytes, want <25 KiB", len(encoded))
	}
}

func qualityCorruptSummaryPath(v *qualityPathView) {
	fields := reflect.ValueOf(v).Elem()
	for i := 0; i < fields.NumField(); i++ {
		field := fields.Field(i)
		if field.Kind() != reflect.Pointer || field.IsNil() {
			continue
		}
		switch value := field.Elem(); value.Kind() {
		case reflect.Int64:
			value.SetInt(-999)
		case reflect.Uint64:
			value.SetUint(999)
		case reflect.Float64:
			value.SetFloat(999)
		}
	}
	v.TTFBCount = 999
}

func qualityCorruptSummarySegment(v *qualitySegmentView) {
	for tag, path := range v.Paths {
		qualityCorruptSummaryPath(&path)
		v.Paths[tag] = path
	}
	delete(v.Paths, "a")
	v.Paths["injected"] = qualityPathView{TTFBCount: 999}
	if v.Witness != nil {
		qualityCorruptSummaryPath(v.Witness)
	}
}

func TestQualitySummarySnapshotsAreDeeplyImmutable(t *testing.T) {
	q := newQualitySummary(t0)
	epoch := q.networkChanged("wifi:private", true, t0)
	for _, witness := range []bool{false, true} {
		q.addInitial(epoch, "a", witness, qualitySummaryFullInitial())
		q.addEnd(epoch, "a", witness, qualitySummaryFullEnd(2<<20))
	}
	epoch = q.networkChanged("cell:25001:private", false, t0.Add(time.Second))
	for _, witness := range []bool{false, true} {
		q.addInitial(epoch, "b", witness, qualitySummaryFullInitial())
		q.addEnd(epoch, "b", witness, qualitySummaryFullEnd(3<<20))
	}
	now := t0.Add(2 * time.Second)
	baseline, _ := q.report(now, false)
	baselineJSON := qualitySummaryJSON(t, baseline)
	baselineCurrent := q.currentView(now)
	currentJSON := qualitySummaryJSON(t, baselineCurrent)
	returned, _ := q.report(now, false)
	for i := range returned {
		qualityCorruptSummarySegment(&returned[i])
	}
	current := q.currentView(now)
	qualityCorruptSummarySegment(&current)
	again, _ := q.report(now, false)
	if !bytes.Equal(baselineJSON, qualitySummaryJSON(t, again)) || !bytes.Equal(currentJSON, qualitySummaryJSON(t, q.currentView(now))) {
		t.Fatal("mutating a returned map/optional scalar/witness changed retained history")
	}
	q.addInitial(epoch, "b", false, qualityInitialSample{hasTTFB: true, ttfb: time.Second})
	q.addEnd(epoch, "b", false, qualitySummaryFullEnd(4<<20))
	if !bytes.Equal(baselineJSON, qualitySummaryJSON(t, baseline)) || !bytes.Equal(baselineJSON, qualitySummaryJSON(t, again)) || !bytes.Equal(currentJSON, qualitySummaryJSON(t, baselineCurrent)) {
		t.Fatal("new measurements mutated an earlier snapshot")
	}
}
