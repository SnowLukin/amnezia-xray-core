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

func qualityProjectionJSON(t *testing.T, v any) map[string]json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func qualityProjectionObject(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func qualityProjectionSeed(e *engine, tag string, witness bool) {
	e.quality.addInitial(e.qualityEpoch, tag, witness, qualityInitialSample{
		hasTTFB: true, hasDial: true, tcp: tcpQualitySnapshot{present: qualityHasRTT | qualityHasMinRTT},
	})
	e.quality.addEnd(e.qualityEpoch, tag, witness, qualityEndSample{
		hasBytes: true,
		s1:       tcpQualitySnapshot{present: qualityHasRX},
		s2:       tcpQualitySnapshot{rxTotal: 100, present: qualityHasRX | qualityHasLoadRTT},
		lastTCP:  tcpQualitySnapshot{txTotal: 100, present: qualityHasTX},
	})
}

func TestQualityRequestProjectionPreservesAllFieldsAndOwnsPointers(t *testing.T) {
	view := qualityPathView{
		BytesDown: qualityScalar(int64(0)), BytesDownOverflow: true,
		TTFBCount: 1, TTFBP50Ms: qualityScalar(int64(10)), TTFBP90Ms: qualityScalar(int64(20)),
		DialCount: 2, DialP50Ms: qualityScalar(int64(30)), DialP90Ms: qualityScalar(int64(40)),
		TCPCount: 3, TCPRTTP50Ms: qualityScalar(int64(50)), TCPRTTP90Ms: qualityScalar(int64(60)), TCPRTTMinMs: qualityScalar(int64(0)),
		BulkCount: 4, BulkKbpsP50: qualityScalar(int64(128)), BulkKbpsP90: qualityScalar(int64(500000)), BulkKbpsMax: qualityScalar(int64(0)),
		BulkRTTP50Ms: qualityScalar(int64(90)), BulkSplicedCount: qualityScalar(uint64(0)),
		RXOutOfOrderFraction: qualityScalar(float64(0)), TXRetransmitFraction: qualityScalar(.125),
	}
	request := requestQualityOf(view)
	snake, camel := qualityProjectionJSON(t, view), qualityProjectionJSON(t, request)
	pairs := map[string]string{
		"bytes_down": "bytesDown", "bytes_down_overflow": "bytesDownOverflow",
		"ttfb_count": "ttfbCount", "ttfb_p50_ms": "ttfbP50Ms", "ttfb_p90_ms": "ttfbP90Ms",
		"dial_count": "dialCount", "dial_p50_ms": "dialP50Ms", "dial_p90_ms": "dialP90Ms",
		"tcp_count": "tcpCount", "tcp_rtt_p50_ms": "tcpRttP50Ms", "tcp_rtt_p90_ms": "tcpRttP90Ms", "tcp_rtt_min_ms": "tcpRttMinMs",
		"bulk_count": "bulkCount", "bulk_kbps_p50": "bulkKbpsP50", "bulk_kbps_p90": "bulkKbpsP90", "bulk_kbps_max": "bulkKbpsMax",
		"bulk_rtt_p50_ms": "bulkRttP50Ms", "bulk_spliced_count": "bulkSplicedCount",
		"rx_out_of_order_fraction": "rxOutOfOrderFraction", "tx_retransmit_fraction": "txRetransmitFraction",
	}
	if len(snake) != len(pairs) || len(camel) != len(pairs) {
		t.Fatal("projection omitted a present field or introduced a second spelling")
	}
	for from, to := range pairs {
		if camel[to] == nil || !bytes.Equal(snake[from], camel[to]) {
			t.Errorf("projection %s -> %s changed %s to %s", from, to, snake[from], camel[to])
		}
	}
	v, r := reflect.ValueOf(view), reflect.ValueOf(request).Elem()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.Kind() == reflect.Pointer && !field.IsNil() && field.Pointer() == r.FieldByName(v.Type().Field(i).Name).Pointer() {
			t.Errorf("request retained input pointer %s", v.Type().Field(i).Name)
		}
	}
	*request.BytesDown = 999
	*request.TCPRTTMinMs = 999
	*request.RXOutOfOrderFraction = .5
	if *view.BytesDown != 0 || *view.TCPRTTMinMs != 0 || *view.RXOutOfOrderFraction != 0 {
		t.Fatal("projection mutation changed the captured current-network view")
	}
	if fields := qualityProjectionJSON(t, requestQualityOf(qualityPathView{})); len(fields) != 0 {
		t.Fatalf("unknown measurements became known zeros: %v", fields)
	}
}

func TestQualityStatusProjectionPreservesZeroAndLegacyTTFB(t *testing.T) {
	o, _ := newTestObserver()
	o.e.quality = newQualitySummary(t0)
	o.e.qualityEpoch = o.e.quality.networkChanged("cell:25001:PRIVATE_SESSION", false, t0)
	o.e.networkID = "cell:25001:PRIVATE_SESSION"
	path := o.e.path("a1")
	path.stats.ttfb.add(180 * time.Millisecond)
	o.e.path("a2")
	qualityProjectionSeed(o.e, "a1", false)
	qualityProjectionSeed(o.e, "witness-name", true)
	o.e.quality.addEnd(o.e.qualityEpoch, "retired-measurement", false, qualityEndSample{bytesDown: 1 << 20})
	before := len(o.e.paths)
	status := o.e.status(t0.Add(time.Second))
	if len(o.e.paths) != before || status.Network != o.e.networkID || status.WitnessQuality == nil {
		t.Fatal("status projection changed policy paths or lost opaque network/witness state")
	}
	fields := qualityProjectionJSON(t, status)
	for _, absent := range []string{"network_type", "network_operator", "networkType", "networkOperator"} {
		if fields[absent] != nil {
			t.Fatalf("status interpreted opaque network id as %s", absent)
		}
	}
	for _, pv := range status.Paths {
		switch pv.Outbound {
		case "a1":
			if pv.Quality == nil || pv.Quality.TCPRTTMinMs == nil || *pv.Quality.TCPRTTMinMs != 0 || pv.TTFBP50Ms != 200 {
				t.Fatalf("quality changed legacy TTFB or omitted measured zero: %+v", pv)
			}
			q := qualityProjectionJSON(t, pv.Quality)
			for _, key := range []string{"bytes_down", "tcp_rtt_min_ms", "rx_out_of_order_fraction", "tx_retransmit_fraction"} {
				if string(q[key]) != "0" {
					t.Errorf("known zero %s missing: %s", key, q[key])
				}
			}
		case "a2":
			if pv.Quality != nil || qualityProjectionJSON(t, pv)["quality"] != nil {
				t.Fatal("unmeasured path acquired a quality object")
			}
		case "retired-measurement":
			t.Fatal("status invented a policy row for a measurement-only tag")
		}
	}
	newFields := []json.RawMessage{fields["paths"], fields["witness_quality"]}
	for _, raw := range newFields {
		if bytes.Contains(raw, []byte("PRIVATE_SESSION")) {
			t.Fatal("quality status fields exposed opaque network id")
		}
	}
}

func TestQualityProviderProjectionUsesOwnExactTagsAndCamelCase(t *testing.T) {
	te := setEngine(t)
	te.quality = newQualitySummary(t0)
	te.qualityEpoch = te.quality.networkChanged("cell:310260:PRIVATE_SESSION", true, t0)
	te.networkID = "cell:310260:PRIVATE_SESSION"
	te.networks[te.networkID] = map[string]string{}
	qualityProjectionSeed(te.engine, "p-x1", false)
	qualityProjectionSeed(te.engine, "foreign-provider-path", false)
	qualityProjectionSeed(te.engine, "direct", true)
	m, body, _ := requestOf(t, te, at(time.Minute))
	if m["witnessQuality"] == nil {
		t.Fatalf("provider omitted measured witness: %v", m)
	}
	for _, absent := range []string{"networkType", "networkOperator", "network_type", "network_operator"} {
		if m[absent] != nil {
			t.Fatalf("provider interpreted opaque network id as %s", absent)
		}
	}
	for _, outbound := range m["outbounds"].([]any) {
		row := outbound.(map[string]any)
		if row["tag"] == "x1" {
			q := row["quality"].(map[string]any)
			for _, key := range []string{"bytesDown", "tcpRttMinMs", "rxOutOfOrderFraction", "txRetransmitFraction"} {
				if q[key] != 0.0 {
					t.Errorf("known zero camelCase %s missing: %v", key, q)
				}
			}
			if q["ttfbCount"] != 1.0 || q["ttfbP50Ms"] != 10.0 || q["ttfb_count"] != nil {
				t.Fatal("provider quality did not preserve measured camelCase fields")
			}
		} else if row["quality"] != nil {
			t.Fatal("unmeasured provider outbound acquired a quality object")
		}
	}
	for _, secret := range []string{"PRIVATE_SESSION", "foreign-provider-path", "p-x1"} {
		if strings.Contains(string(body), secret) {
			t.Fatalf("provider quality body exposed %q", secret)
		}
	}
	network := m["network"].(map[string]any)
	if network["id"] != providerHash(te.prov().cfg.url, te.networkID) || network["isKnown"] != true || network["isUp"] != true {
		t.Fatal("provider lost its existing opaque network hash or known/up state")
	}
}

func TestQualityProjectionUnknownMeasurementsAndNetworkMetadataAreOmitted(t *testing.T) {
	te := setEngine(t)
	te.quality = newQualitySummary(t0)
	te.qualityEpoch = te.quality.networkChanged("malformed:PRIVATE_SESSION", false, t0)
	te.networkID = "malformed:PRIVATE_SESSION"
	s := qualityProjectionJSON(t, te.status(t0))
	r := qualityProjectionJSON(t, te.providerRequest(te.prov(), &refreshJob{refresh: 1, trigger: triggerHost}, t0))
	for _, fields := range []map[string]json.RawMessage{s, r} {
		for _, absent := range []string{"network_type", "networkType", "network_operator", "networkOperator", "witness_quality", "witnessQuality", "quality_samples_dropped", "qualitySamplesDropped"} {
			if fields[absent] != nil {
				t.Errorf("unknown/zero optional top field %s emitted", absent)
			}
		}
	}
	for _, outbound := range te.providerRequest(te.prov(), &refreshJob{}, t0).Outbounds {
		if outbound.Quality != nil {
			t.Fatal("unknown measurements emitted an empty quality object")
		}
	}
}

func TestQualityReportProjectionKeepsSnapshotsAndCurrentAcrossTake(t *testing.T) {
	o, clk := newTestObserver()
	o.e.quality = newQualitySummary(t0)
	o.e.qualityEpoch = o.e.quality.networkChanged("wifi:PRIVATE_SESSION", false, t0)
	o.e.networkID = "wifi:PRIVATE_SESSION"
	o.e.path("a1")
	o.e.report.begin(t0)
	o.s.reportInterval = time.Second
	qualityProjectionSeed(o.e, "a1", false)
	qualityProjectionSeed(o.e, "direct", true)
	// Merely collecting quality is no report decision and must not cut it.
	clk.advance(2 * time.Second)
	if o.TakeReport(false) != nil || o.Report() != nil {
		t.Fatal("quality measurement created a report decision")
	}
	if len(o.e.quality.open.paths) != 1 {
		t.Fatal("nil report consumed the quality window")
	}
	o.e.emit(clk.now(), "provider_restart_needed", map[string]any{"provider": "p"})
	snapshot := o.Report()
	again := o.Report()
	if !bytes.Equal(snapshot, again) {
		t.Fatal("repeated report snapshot changed measured data")
	}
	var snap map[string]json.RawMessage
	if err := json.Unmarshal(snapshot, &snap); err != nil {
		t.Fatal(err)
	}
	if snap["quality"] == nil || bytes.Contains(snap["quality"], []byte("PRIVATE_SESSION")) {
		t.Fatal("report quality omitted measured data or exposed its fingerprint")
	}
	for _, absent := range []string{"network_type", "network_operator"} {
		if bytes.Contains(snap["quality"], []byte(`"`+absent+`":`)) {
			t.Fatalf("report quality interpreted opaque network id as %s", absent)
		}
	}
	var segments []qualitySegmentView
	if err := json.Unmarshal(snap["quality"], &segments); err != nil {
		t.Fatal(err)
	}
	if len(segments) != 1 || segments[0].IsNetworkKnown || segments[0].DurationMs != 2000 || segments[0].Witness == nil || segments[0].Paths["a1"].BytesDown == nil || *segments[0].Paths["a1"].BytesDown != 0 {
		t.Fatal("report quality lost known zero/witness presence")
	}
	currentBefore, _ := json.Marshal(o.e.quality.currentView(clk.now()).Paths)
	taken := o.TakeReport(false)
	var take map[string]json.RawMessage
	if err := json.Unmarshal(taken, &take); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(snap["quality"], take["quality"]) || len(o.e.quality.open.paths) != 0 {
		t.Fatal("taking the due report lost or failed to reset its measured window")
	}
	currentAfter, _ := json.Marshal(o.e.quality.currentView(clk.now()).Paths)
	if !bytes.Equal(currentBefore, currentAfter) || o.e.status(clk.now()).Paths[0].Quality == nil {
		t.Fatal("report take cleared current-network status quality")
	}
	if fields := qualityProjectionJSON(t, o.e.reportView(reportSnapshot, clk.now(), false)); fields["quality"] != nil {
		t.Fatal("new empty report window retained old quality measurements")
	}
}

func TestQualityProjectionLossAndOverflowFieldsRemainOptional(t *testing.T) {
	te := setEngine(t)
	te.quality = newQualitySummary(t0)
	for i := 0; i < 64; i++ {
		tag := fmt.Sprintf("m%02d", i)
		if i == 0 {
			tag = "p-x1"
		}
		te.quality.addEnd(0, tag, false, qualityEndSample{bytesDown: 1})
	}
	te.quality.addInitial(0, "rejected", false, qualityInitialSample{hasTTFB: true})
	te.quality.addEnd(0, "rejected", false, qualityEndSample{hasBytes: true})
	te.quality.addEnd(0, "p-x1", false, qualityEndSample{bytesDown: math.MaxInt64})
	status := qualityProjectionJSON(t, te.status(t0))
	request := qualityProjectionJSON(t, te.providerRequest(te.prov(), &refreshJob{}, t0))
	if string(status["quality_samples_dropped"]) != "2" || string(request["qualitySamplesDropped"]) != "2" {
		t.Fatal("current admission loss was hidden by status/provider projection")
	}
	for _, outbound := range te.providerRequest(te.prov(), &refreshJob{}, t0).Outbounds {
		if outbound.Tag == "x1" && (outbound.Quality == nil || !outbound.Quality.BytesDownOverflow) {
			t.Fatal("provider projection lost saturated-byte loss flag")
		}
	}
	views, dropped := te.quality.report(t0, false)
	v := reportView{Quality: views, QualitySegmentsDropped: dropped}
	fields := qualityProjectionJSON(t, v)
	var segments []map[string]json.RawMessage
	if err := json.Unmarshal(fields["quality"], &segments); err != nil {
		t.Fatal(err)
	}
	if string(segments[0]["samples_dropped"]) != "2" || string(segments[0]["paths_dropped"]) != "60" {
		t.Fatal("report projection hid rejected phases or top-four coverage loss")
	}
	if fields["quality_segments_dropped"] != nil {
		t.Fatal("zero optional segment-drop count was emitted")
	}
	v.QualitySegmentsDropped = 3
	if string(qualityProjectionJSON(t, v)["quality_segments_dropped"]) != "3" {
		t.Fatal("nonzero report segment loss was omitted")
	}
	te.quality.report(t0, true)
	if te.status(t0).QualitySamplesDropped != 2 || te.providerRequest(te.prov(), &refreshJob{}, t0).QualitySamplesDropped != 2 {
		t.Fatal("report take reset current scope loss awareness")
	}
}

func TestQualityNetworkChangeEventKeepsOpaqueIDInBothPolicyModes(t *testing.T) {
	for _, mode := range []NetworkChangeMode{NetworkChangeMode_RECHECK, NetworkChangeMode_IGNORE} {
		te := newTestEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3), NetworkChangeAction: mode}, "a1", "b1")
		generation := te.generation
		for i, id := range []string{"cell:25001:PRIVATE_SESSION", "malformed:PRIVATE_SESSION"} {
			known := i == 1
			if known {
				te.networks[id] = map[string]string{}
			}
			te.onNetworkChange(id, at(time.Duration(i+1)*time.Second))
			ev := te.lastOf(t, "network_change")
			if ev.Fields["network"] != id || ev.Fields["is_known"] != known || te.networkKnown != known {
				t.Fatal("opaque network event lost its original id/known state")
			}
			for _, absent := range []string{"network_type", "network_operator", "networkType", "networkOperator"} {
				if _, present := ev.Fields[absent]; present {
					t.Fatalf("quality added interpreted %s to network event", absent)
				}
			}
			if mode == NetworkChangeMode_IGNORE && (te.generation != generation || ev.Fields["is_ignored"] != true) {
				t.Fatal("opaque id changed ignore policy behavior")
			}
			if mode == NetworkChangeMode_RECHECK && te.generation != generation+uint64(i+1) {
				t.Fatal("opaque id changed recheck policy generation")
			}
		}
	}
}

func TestQualityProviderCombinedRequestSizeIsBounded(t *testing.T) {
	tags := make([]string, 0, maxProviderOutbounds)
	tagBytes := 0
	for i := 0; i < maxProviderOutbounds; i++ {
		short := fmt.Sprintf("node-%03d", i)
		tags = append(tags, "p-"+short)
		tagBytes += len(short)
	}
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: pathSpecs("p-", 1)}, tags...)
	te.applyProviderSet(te.prov(), providerChange{tags: tags}, t0)
	te.quality = newQualitySummary(t0)
	for i, tag := range tags {
		p := te.path(tag)
		p.status, p.reason, p.statusSince = statusDown, "network_change", t0
		p.quarantineLevel, p.quarantineUntil = 9, at(time.Hour)
		for kind := range p.stats.failuresByKind {
			p.stats.failuresByKind[kind] = 1_000_000
		}
		p.stats.probes, p.lastProbeEnd, p.lastError, p.lastClass = 1, t0, strings.Repeat("e", 4096), "status_503"
		p.stats.connsOK, p.stats.connsFailed, p.stats.bytesUp, p.stats.bytesDown = 1_000_000, 1_000_000, 1<<40, 1<<40
		p.stats.activeTime = 24 * time.Hour
		p.stats.ttfb.add(10 * time.Second)
		if i < 64 {
			te.quality.addInitial(0, tag, false, qualityInitialSample{hasTTFB: true, hasDial: true, ttfb: 10 * time.Second, dial: 10 * time.Second, tcp: tcpQualitySnapshot{rtt: 10 * time.Second, minRTT: 10 * time.Second, present: qualityHasRTT | qualityHasMinRTT}})
			s2 := tcpQualitySnapshot{loadRTT: 10 * time.Second, rxTotal: 1000, rxOutOfOrder: 20, txTotal: 10000, txRetransmit: 70, present: qualityHasLoadRTT | qualityHasRX | qualityHasTX}
			te.quality.addEnd(0, tag, false, qualityEndSample{bytesDown: 1 << 40, hasBulk: true, bulkKbps: 3_000_000, s1: tcpQualitySnapshot{present: qualityHasRX}, s2: s2, lastTCP: s2})
		}
	}
	qualityProjectionSeed(te.engine, "direct", true)
	te.prov().lastFailedAt, te.prov().lastClass = t0, strings.Repeat("r", 1<<20)
	_, body, _ := requestOf(t, te, at(time.Minute))
	coreBytes := len(body) - tagBytes
	t.Logf("combined provider request: %d bytes total, %d core bytes, %d outbounds, 64 path quality records and one witness", len(body), coreBytes, len(tags))
	if coreBytes >= 128<<10 {
		t.Fatalf("bounded core request is %d bytes, want <128 KiB", coreBytes)
	}
	if len(te.quality.current.paths) != 64 {
		t.Fatal("size fixture did not retain all 64 measured paths")
	}
}
