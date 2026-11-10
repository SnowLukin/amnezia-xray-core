package health

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"testing"
	"time"
)

func measurementFeatures() *FeatureConfig {
	return &FeatureConfig{HostLabels: qualityScalar(true), LabelRequest: qualityScalar(true), PathLabels: qualityScalar(true), SessionLabels: qualityScalar(true), ProbeMeasurements: qualityScalar(true), Quality: qualityScalar(true), Events: qualityScalar(true), Report: qualityScalar(true), NetworkMemory: qualityScalar(true), MemoryPressure: qualityScalar(true), ProbeRequests: qualityScalar(true)}
}
func measurementConfig() *Config {
	return &Config{Features: measurementFeatures(), Network: &NetworkConfig{MaxNetworks: 2}, LabelRequest: &LabelRequestConfig{Outbound: "direct", Destinations: []string{"http://127.0.0.1/whoami"}, Labels: map[string]string{"asn": "X-Network-Asn"}, RequestsPerHour: 6}, Schedule: &ScheduleConfig{IdleAfter: int64(time.Hour)}}
}
func measurementEngine(t *testing.T) *testEngine {
	e := newTestEngine(t, measurementConfig(), &SelectorConfig{NetworkChangeAction: NetworkChangeMode_IGNORE}, "a1", "b1")
	e.quality = newQualitySummary(t0)
	e.onNetworkChange("home", t0)
	return e
}

func labelStart(t *testing.T, e *engine, now time.Time) *labelJob {
	t.Helper()
	e.noteDemand(now)
	e.sawSuccess("witness", now)
	e.noteLabelSuccess(e.qualityEpoch)
	job, _ := e.nextLabelRequest(context.Background(), now)
	if job == nil {
		t.Fatal("eligible network did not request labels")
	}
	return job
}

func TestMeasurementLabelFormat(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		ok          bool
	}{
		{"type", "wifi", true}, {strings.Repeat("a", 24), strings.Repeat("Z", 32), true},
		{"operator", "25001", true}, {"region", "RU-MOW", true}, {"k", "A.B_C:0-9", true},
		{"", "wifi", false}, {"Type", "wifi", false}, {"a-b", "wifi", false}, {"ключ", "wifi", false},
		{strings.Repeat("a", 25), "v", false}, {"k", "", false}, {"k", strings.Repeat("a", 33), false},
		{"k", "has space", false}, {"k", "a/b", false}, {"k", "\r\n", false},
	} {
		if got := ValidateLabels(map[string]string{tc.name: tc.value}, 8) == nil; got != tc.ok {
			t.Errorf("%q=%q: %v", tc.name, tc.value, got)
		}
	}
	labels := map[string]string{}
	for i := range 9 {
		labels[fmt.Sprintf("k%d", i)] = "v"
	}
	if ValidateLabels(labels, 8) == nil {
		t.Fatal("count limit accepted")
	}
	if got := filterLabels(labels, 8); len(got) != 8 || got["k8"] != "" {
		t.Fatal("host filtering is not bounded/deterministic")
	}
}

func TestMeasurementHostAPIAndClosedSegments(t *testing.T) {
	o := newObserver(context.Background(), &Config{Features: measurementFeatures()})
	defer o.Close()
	if err := o.NetworkChangedWithLabels("opaque", `{"type":"wifi","bad-key":"discard","operator":"25001"}`); err != nil {
		t.Fatal(err)
	}
	o.e.quality.addInitial(o.e.qualityEpoch, "a1", false, qualityInitialSample{hasDial: true, dial: time.Millisecond})
	snap, _ := o.e.quality.report(o.now(), false)
	if o.e.status(o.now()).Network.Host["type"] != "wifi" {
		t.Fatal("host labels absent")
	}
	// Labels that cannot be read are dropped and reported; the change stays.
	for i, bad := range []string{`null`, `[]`, `{"type":3}`, `{`, `{"type":"` + strings.Repeat("a", 8192) + `"}`} {
		id := fmt.Sprint("bad", i)
		if o.NetworkChangedWithLabels(id, bad) == nil || o.e.networkID != id || o.e.hostLabels != nil {
			t.Fatalf("labels %.12s: network %q, labels %v", bad, o.e.networkID, o.e.hostLabels)
		}
	}
	o.NetworkChanged("next")
	if o.e.status(o.now()).Network != nil {
		t.Fatal("old call kept host labels")
	}
	if snap[0].Network.Host["type"] != "wifi" {
		t.Fatal("snapshot changed")
	}
	closed, _ := o.e.quality.report(o.now(), false)
	if closed[0].Network.Host["type"] != "wifi" {
		t.Fatal("closed segment lost labels")
	}
	raw, _ := o.DrainEvents()
	var events []Event
	if err := json.Unmarshal(raw, &events); err != nil {
		t.Fatal(err)
	}
	changes := []Event{}
	for _, ev := range events {
		if ev.Type == "network_change" {
			changes = append(changes, ev)
		}
	}
	if changes[0].Fields["labels"].(map[string]any)["type"] != "wifi" {
		t.Fatal("network_change labels absent")
	}
}

func TestMeasurementEligibilityCacheRetryAndBudget(t *testing.T) {
	e := measurementEngine(t)
	if j, _ := e.nextLabelRequest(context.Background(), t0); j != nil {
		t.Fatal("no network proof")
	}
	e.labelProof = true
	e.lastDemand.Store(t0.Add(-2 * time.Hour).UnixNano())
	if j, _ := e.nextLabelRequest(context.Background(), t0); j != nil {
		t.Fatal("idle requested")
	}
	e.noteDemand(t0)
	e.networkDown = true
	if j, _ := e.nextLabelRequest(context.Background(), t0); j != nil {
		t.Fatal("lost network requested")
	}
	e.networkDown = false
	e.memoryLow = true
	if j, _ := e.nextLabelRequest(context.Background(), t0); j != nil {
		t.Fatal("low memory requested")
	}
	e.memoryLow = false
	j := labelStart(t, e.engine, t0)
	if extra, _ := e.nextLabelRequest(context.Background(), t0); extra != nil {
		t.Fatal("duplicate in-flight request")
	}
	e.labelResult(j, nil, "network", t0)
	if j, w := e.nextLabelRequest(context.Background(), t0.Add(9*time.Minute)); j != nil || !w.Equal(t0.Add(10*time.Minute)) {
		t.Fatal("retry delay")
	}
	j = labelStart(t, e.engine, t0.Add(10*time.Minute))
	e.labelResult(j, nil, "status_500", t0.Add(10*time.Minute))
	if j, _ := e.nextLabelRequest(context.Background(), t0.Add(20*time.Minute)); j != nil {
		t.Fatal("more than one retry")
	}
	e.onNetworkChange("mobile", t0.Add(21*time.Minute))
	j = labelStart(t, e.engine, t0.Add(21*time.Minute))
	e.labelResult(j, map[string]string{"asn": "8359"}, "", t0.Add(21*time.Minute))
	e.onNetworkChange("home", t0.Add(22*time.Minute))
	e.noteDemand(t0.Add(22 * time.Minute))
	e.sawSuccess("traffic", t0.Add(22*time.Minute))
	e.noteLabelSuccess(e.qualityEpoch)
	if j, _ := e.nextLabelRequest(context.Background(), t0.Add(22*time.Minute)); j != nil {
		t.Fatal("failed cached network requested again")
	}
	e.onNetworkChange("mobile", t0.Add(23*time.Minute))
	if e.networkLabels().IP["asn"] != "8359" {
		t.Fatal("cache miss")
	}
	if j, _ := e.nextLabelRequest(context.Background(), t0.Add(23*time.Minute)); j != nil {
		t.Fatal("cached labels requested again")
	}
	e.onNetworkChange("third", t0.Add(24*time.Minute))
	if len(e.labelCache) != 2 || e.labelCache["home"] != nil {
		t.Fatal("maxNetworks not enforced")
	}
	e.s.labelRequest.requestsPerHour = 3
	e.noteDemand(t0.Add(24 * time.Minute))
	e.sawSuccess("traffic", t0.Add(24*time.Minute))
	e.noteLabelSuccess(e.qualityEpoch)
	if j, w := e.nextLabelRequest(context.Background(), t0.Add(24*time.Minute)); j != nil || !w.Equal(t0.Add(time.Hour)) {
		t.Fatal("hourly budget")
	}
	j = labelStart(t, e.engine, t0.Add(time.Hour))
	e.labelResult(j, map[string]string{"asn": "123"}, "", t0.Add(time.Hour))
	e.s.labelRequest.requestsPerHour = 6
	e.onNetworkChange("", t0.Add(61*time.Minute))
	j = labelStart(t, e.engine, t0.Add(61*time.Minute))
	e.labelResult(j, map[string]string{"asn": "456"}, "", t0.Add(61*time.Minute))
	e.onNetworkChange("", t0.Add(62*time.Minute))
	j = labelStart(t, e.engine, t0.Add(62*time.Minute))
	e.labelResult(j, map[string]string{"asn": "789"}, "", t0.Add(62*time.Minute))
}

func TestMeasurementStaleResultAndNoKey(t *testing.T) {
	e := measurementEngine(t)
	j := labelStart(t, e.engine, t0)
	e.onNetworkChange("new", t0.Add(time.Second))
	if j.ctx.Err() == nil {
		t.Fatal("old request not canceled")
	}
	e.labelResult(j, map[string]string{"asn": "old"}, "", t0.Add(time.Second))
	if e.networkLabels() != nil {
		t.Fatal("stale labels published")
	}
	e.s.labelRequest = nil
	e.onNetworkChange("off", t0.Add(2*time.Second))
	e.sawSuccess("traffic", t0.Add(2*time.Second))
	e.noteLabelSuccess(e.qualityEpoch)
	if j, _ := e.nextLabelRequest(context.Background(), t0.Add(2*time.Second)); j != nil {
		t.Fatal("no key requested labels")
	}
}

func TestMeasurementPathsReportsProbeSegments(t *testing.T) {
	cfg := &Config{Features: measurementFeatures(), SessionLabels: map[string]string{"cohort": "a"}, Report: &ReportConfig{QuietShare: 1}}
	sc := &SelectorConfig{Paths: []*PathDefinition{{Tag: "a1", Group: "g", Labels: map[string]string{"node": "n1", "entry": "own"}}, {Tag: "b1", Cost: 2}}, Groups: []*PathGroup{{Name: "g", Cost: 1, Labels: map[string]string{"location": "NL", "entry": "group"}}}, NetworkChangeAction: NetworkChangeMode_IGNORE}
	e := newTestEngine(t, cfg, sc, "a1", "b1")
	e.report.begin(t0)
	e.report.quietSample = true
	e.quality = newQualitySummary(t0)
	e.onNetworkChangeWithLabels("private", map[string]string{"type": "cellular"}, t0)
	e.quality.addProbe(e.qualityEpoch, "b1", "return", probeResult{ok: true, steps: []probeStepResult{{ok: true, connect: 300 * time.Millisecond}, {ok: true, connect: 9 * time.Second}}})
	e.quality.addProbe(e.qualityEpoch, "b1", "host", probeResult{steps: []probeStepResult{{connect: time.Second}}})
	e.labelQualityPath("b1", e.qualityEpoch)
	e.quality.addProbe(e.qualityEpoch-1, "old", "host", probeResult{ok: true})
	v := e.reportView(reportSnapshot, t0.Add(time.Minute), false)
	if v.Labels["cohort"] != "a" || !reflect.DeepEqual(v.PathLabels["a1"], map[string]string{"node": "n1", "location": "NL", "entry": "own"}) || v.PathLabels["b1"] != nil {
		t.Fatal("path/report labels", v.PathLabels)
	}
	q := v.Quality[0].Paths["b1"]
	if q.ProbeCount != 2 || q.ProbeFailedCount != 1 || *q.ProbeConnectP50Ms != 300 || q.ProbeReasons["return"] != 1 || q.ProbeReasons["host"] != 1 {
		t.Fatalf("probe summary: %+v", q)
	}
	if v.Quality[0].Network.Host["type"] != "cellular" || v.Quality[0].Paths["old"].ProbeCount != 0 {
		t.Fatal("segment metadata/epoch")
	}
	v.PathLabels["a1"]["node"] = "mutated"
	v.Labels["cohort"] = "mutated"
	q.ProbeReasons["return"] = 99
	if e.s.pathLabels("a1")["node"] != "n1" || e.s.reportLabels["cohort"] != "a" || e.quality.currentView(t0).Paths["b1"].ProbeReasons["return"] != 1 {
		t.Fatal("snapshot alias")
	}
	// Labels only for paths retained in report, including quality-only tags.
	for i := range 65 {
		tag := fmt.Sprintf("z%02d", i)
		e.path(tag)
		e.s.paths.Store(tag, pathSettings{labels: map[string]string{"node": strings.Repeat("n", 32), "location": strings.Repeat("L", 32), "entry": strings.Repeat("e", 32), "kind": strings.Repeat("k", 32), "pool": strings.Repeat("p", 32), "variant": strings.Repeat("v", 32)}})
	}
	v = e.reportView(reportSnapshot, t0.Add(time.Minute), false)
	for tag := range v.PathLabels {
		if _, ok := v.Paths[tag]; !ok {
			t.Fatal("label for path absent from report", tag)
		}
	}
	raw, _ := json.Marshal(v)
	t.Logf("64 path report bytes=%d", len(raw))
	if len(v.Paths) != 64 || len(raw) > 64<<10 {
		t.Fatal("report bound", len(raw))
	}
}

func TestMeasurementProviderLabelsAndIsolation(t *testing.T) {
	cfg := &providerSettings{prefix: "p-", balancer: "auto", balancers: []string{"auto"}, maxOutbounds: 8}
	a := &providerAnswer{groupsRaw: json.RawMessage(`[{"name":"g","cost":1,"labels":{"location":"NL","entry":"group"}}]`), heads: []outboundHead{{Tag: "n1", Selection: json.RawMessage(`{"balancer":"auto","group":"g","labels":{"node":"n1","entry":"own"}}`)}}}
	if err := validateAnswerPaths(cfg, a); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mergeLabels(a.groups[0].Labels, a.paths[0].Labels), map[string]string{"location": "NL", "entry": "own", "node": "n1"}) {
		t.Fatal("provider merge")
	}
	a.heads[0].Selection = json.RawMessage(`{"balancer":"auto","cost":1,"labels":{"node":"bad space"}}`)
	if validateAnswerPaths(cfg, a) == nil {
		t.Fatal("invalid provider labels")
	}
	e := measurementEngine(t)
	e.hostLabels = map[string]string{"type": "wifi"}
	j := labelStart(t, e.engine, t0)
	e.labelResult(j, map[string]string{"asn": "8359"}, "", t0)
	p := &provider{cfg: cfg}
	req := e.providerRequest(p, &refreshJob{}, t0)
	if req.Network.Labels["type"] != "wifi" || req.Network.IPLabels["asn"] != "8359" {
		t.Fatal("provider network labels")
	}
	req.Network.Labels["type"] = "mutated"
	if e.hostLabels["type"] != "wifi" {
		t.Fatal("request alias")
	}
	if !reflect.DeepEqual(maps.Clone(e.hostLabels), map[string]string{"type": "wifi"}) {
		t.Fatal("unexpected network metadata")
	}
}

func TestMeasurementBlockedRequestDoesNotDelayFailover(t *testing.T) {
	var want []byte
	for _, enabled := range []bool{false, true} {
		cfg := &Config{Schedule: quarantine(time.Minute)}
		if enabled {
			cfg.Features = measurementFeatures()
			cfg.LabelRequest = measurementConfig().LabelRequest
		}
		e := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 2), SkipProbeBeforeSwitch: true, NetworkChangeAction: NetworkChangeMode_IGNORE}, "a1", "b1")
		// The same salt in both runs, so the hashed network compares.
		e.networkSalt = [16]byte{}
		e.onNetworkChange("home", t0)
		if enabled {
			e.hostLabels = map[string]string{"type": "wifi"}
			labelStart(t, e.engine, t0) // deliberately left unresolved throughout failover
		}
		before := time.Now()
		e.failN(3, "a1", failDial, at(time.Second))
		e.run(t, at(time.Second), map[string]bool{"a1": false, "b1": true})
		if time.Since(before) > time.Second {
			t.Fatal("blocked label request delayed failover")
		}
		if e.sel.current() != "b1" || e.path("a1").status != statusDown {
			t.Fatal("failover did not occur")
		}
		trace := struct {
			Active    string
			State     pathStatus
			DownSince time.Time
			Events    []Event
		}{Active: e.sel.current(), State: e.path("a1").status, DownSince: e.path("a1").statusSince}
		raw, _ := e.events.drain()
		if err := json.Unmarshal(raw, &trace.Events); err != nil {
			t.Fatal(err)
		}
		got, _ := json.Marshal(trace)
		if !enabled {
			want = got
		} else if string(got) != string(want) {
			t.Fatalf("labels changed controller trace\n%s\n%s", want, got)
		}
	}
}

func TestMeasurementDisabledFlags(t *testing.T) {
	cfg := measurementConfig()
	cfg.Features.LabelRequest = qualityScalar(false)
	cfg.Features.PathLabels = qualityScalar(false)
	cfg.Features.SessionLabels = qualityScalar(false)
	cfg.Features.HostLabels = qualityScalar(false)
	cfg.Paths = []*PathDefinition{{Tag: "a1", Labels: map[string]string{"node": "n1"}}}
	cfg.SessionLabels = map[string]string{"cohort": "a"}
	o := newObserver(context.Background(), cfg)
	defer o.Close()
	if err := o.NetworkChangedWithLabels("home", `{"type":"wifi"}`); err != nil {
		t.Fatal(err)
	}
	if o.s.labelRequest != nil || o.e.networkLabels() != nil || o.e.reportView(reportSnapshot, o.now(), false).PathLabels != nil || len(o.s.reportLabels) != 0 {
		t.Fatal("metadata enabled without own flag")
	}
	raw, _ := o.DrainEvents()
	if strings.Contains(string(raw), `"labels"`) {
		t.Fatal("disabled host labels in event")
	}
}

func TestMeasurementProbeNotProofAndCanceledRetry(t *testing.T) {
	e := measurementEngine(t)
	e.sawSuccess("probe", t0)
	if j, _ := e.nextLabelRequest(context.Background(), t0); j != nil {
		t.Fatal("probe supplied network proof")
	}
	j := labelStart(t, e.engine, t0)
	e.cancelLabelRequest(t0.Add(time.Second))
	e.labelResult(j, map[string]string{"asn": "late"}, "", t0.Add(time.Second))
	if j, w := e.nextLabelRequest(context.Background(), t0.Add(2*time.Second)); j != nil || !w.Equal(t0.Add(time.Second+10*time.Minute)) {
		t.Fatal("canceled request bypassed retry delay")
	}
	j = labelStart(t, e.engine, t0.Add(time.Second+10*time.Minute))
	e.labelResult(j, map[string]string{"asn": "new"}, "", t0.Add(time.Second+10*time.Minute))
}

func TestMeasurementRetiredQualityLabels(t *testing.T) {
	o, clk := qualityIntegrationObserver(false)
	o.s.features.PathLabels = true
	o.s.paths.Store("a1", pathSettings{labels: map[string]string{"node": "retired"}})
	o.e.quality.addInitial(o.e.qualityEpoch, "a1", false, qualityInitialSample{hasDial: true, dial: time.Millisecond})
	o.e.labelQualityPath("a1", o.e.qualityEpoch)
	delete(o.e.paths, "a1")
	o.s.paths.Delete("a1")
	o.e.quality.addEnd(o.e.qualityEpoch, "a1", false, qualityEndSample{hasBytes: true, bytesDown: 1024})
	o.e.labelQualityPath("a1", o.e.qualityEpoch)
	report := o.e.reportView(reportSnapshot, clk.now(), false)
	if report.PathLabels["a1"]["node"] != "retired" {
		t.Fatal("removed provider path lost labels")
	}
	o.NetworkChanged("next")
	report = o.e.reportView(reportSnapshot, clk.now(), false)
	if report.PathLabels["a1"]["node"] != "retired" {
		t.Fatal("closed segment lost retired labels")
	}
	report.PathLabels["a1"]["node"] = "mutation"
	if o.e.reportView(reportSnapshot, clk.now(), false).PathLabels["a1"]["node"] != "retired" {
		t.Fatal("retired labels alias snapshot")
	}
}
