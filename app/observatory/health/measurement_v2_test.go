package health

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/features/stats"
)

func TestMeasurementV2MetadataCombinationsWithoutUserQuality(t *testing.T) {
	for mask := 0; mask < 32; mask++ {
		t.Run(fmt.Sprintf("%02d", mask), func(t *testing.T) {
			f := v2NoEvidence()
			f.Report = qualityScalar(true)
			f.HostLabels = qualityScalar(mask&1 != 0)
			f.LabelRequest = qualityScalar(mask&2 != 0)
			f.PathLabels = qualityScalar(mask&4 != 0)
			f.SessionLabels = qualityScalar(mask&8 != 0)
			f.ProbeMeasurements = qualityScalar(mask&16 != 0)
			cfg := &Config{Features: f, Report: &ReportConfig{QuietShare: 1}, Paths: []*PathDefinition{{Tag: "a1", Labels: map[string]string{"node": "n1"}}}, SessionLabels: map[string]string{"cohort": "a"}, LabelRequest: measurementConfig().LabelRequest}
			o := newObserver(context.Background(), cfg)
			defer o.Close()
			if err := o.NetworkChangedWithLabels("home", `{"type":"wifi"}`); err != nil {
				t.Fatal(err)
			}
			now := o.now()
			o.e.noteDemand(now)
			o.e.sawSuccess("traffic", now)
			o.e.noteLabelSuccess(o.e.qualityEpoch)
			job, _ := o.e.nextLabelRequest(o.runCtx, now)
			if (job != nil) != (mask&2 != 0) {
				t.Fatal("labelRequest gate")
			}
			if job != nil {
				o.e.labelResult(job, map[string]string{"asn": "8359"}, "", now)
			}
			p := o.e.path("a1")
			probe := &probeJob{id: 1, tag: "a1", reason: reasonHost, generation: o.e.generation, qualityEpoch: o.e.qualityEpoch}
			o.e.running[1] = probe
			o.e.onProbeResult(probe, probeResult{ok: true, steps: []probeStepResult{{ok: true, connect: 300 * time.Millisecond}}}, now)
			status := o.e.status(now)
			if (status.Network != nil && len(status.Network.Host) > 0) != (mask&1 != 0) {
				t.Fatal("hostLabels gate")
			}
			if (status.Network != nil && len(status.Network.IP) > 0) != (mask&2 != 0) {
				t.Fatal("IP gate")
			}
			if (len(o.s.reportLabels) > 0) != (mask&8 != 0) {
				t.Fatal("sessionLabels gate")
			}
			if (o.e.quality != nil) != (mask&19 != 0) {
				t.Fatal("probe-only collector gate")
			}
			if mask&16 != 0 {
				q := o.e.quality.currentView(now).Paths["a1"]
				if q.ProbeCount != 1 || q.DialCount != 0 || q.TTFBCount != 0 || q.TCPCount != 0 || *q.ProbeConnectP50Ms != 300 {
					t.Fatalf("probe-only measurements: %+v", q)
				}
			}
			provider := &provider{cfg: &providerSettings{tag: "test", balancer: "auto", maxOutbounds: 8}, tags: []string{p.tag}}
			request := o.e.providerRequest(provider, &refreshJob{}, now)
			if len(request.Outbounds) != 1 || request.Outbounds[0].Quality != nil || request.WitnessQuality != nil {
				t.Fatal("user quality appeared in probe-only POST")
			}
			report := o.e.reportView(reportSnapshot, now, false)
			if (len(report.Labels) > 0) != (mask&8 != 0) || (len(report.PathLabels) > 0) != (mask&4 != 0) {
				t.Fatal("report metadata gates")
			}
		})
	}
}

func TestMeasurementV2DisabledHostLabelsStillChangesNetwork(t *testing.T) {
	o := newObserver(context.Background(), &Config{Features: v2NoEvidence()})
	defer o.Close()
	before := o.e.qualityEpoch
	for i, raw := range []string{`{"type":"wifi"}`, `{broken`, string(make([]byte, 9000))} {
		id := string(rune('a' + i))
		if err := o.NetworkChangedWithLabels(id, raw); err != nil {
			t.Fatal("metadata-off signal rejected", err)
		}
		before++
		if o.e.networkID != id || o.e.qualityEpoch != before || o.e.hostLabels != nil {
			t.Fatal("metadata-off lost base signal")
		}
	}
}

func TestMeasurementV2ProbeOnlyWithoutReport(t *testing.T) {
	f := v2NoEvidence()
	f.ProbeMeasurements = qualityScalar(true)
	o := newObserver(context.Background(), &Config{Features: f})
	defer o.Close()
	if o.e.quality == nil || o.e.quality.reporting || o.e.quality.current.started.IsZero() {
		t.Fatal("probe-only collector missing start")
	}
	o.e.quality.addProbe(0, "reserve", "host", probeResult{ok: true, steps: []probeStepResult{{ok: true, connect: time.Millisecond}}})
	if o.e.quality.currentView(o.now()).Paths["reserve"].ProbeCount != 1 || o.e.quality.open.nonempty() {
		t.Fatal("report-off collector retained report scope or lost probes")
	}
	if o.PeekReport() != nil {
		t.Fatal("report-off emitted report")
	}
}

// IGNORE preserves routing evidence while metadata belongs to a new network.
// An old reply must neither prove that network nor erase a newer valid proof.
func TestMeasurementV2ProofEpochWithoutUserQuality(t *testing.T) {
	for _, report := range []bool{false, true} {
		for _, tag := range []string{"a1", "direct"} {
			t.Run(fmt.Sprintf("report=%v/tag=%s", report, tag), func(t *testing.T) {
				f := v2NoEvidence()
				f.LabelRequest, f.Witnesses, f.Report = qualityScalar(true), qualityScalar(true), qualityScalar(report)
				cfg := &Config{Features: f, Paths: []*PathDefinition{{Tag: "a1", Balancer: "auto", Cost: 1}},
					Connectivity: &ConnectivityConfig{Witnesses: []string{"direct"}}, LabelRequest: measurementConfig().LabelRequest}
				o := newObserver(context.Background(), cfg)
				defer o.Close()
				o.e.addSelector("auto", newSelectorSettings(&SelectorConfig{Paths: cfg.Paths, Features: v2NoClosures()}))
				o.NetworkChanged("old")
				var old []stats.Counter
				for range 2 {
					tr := o.StartDial(context.Background(), tag, net.Destination{}, nil)
					if tr == nil {
						t.Fatal("label request lacks observation hook")
					}
					read, _ := tr.Dialed(nil, nil, nil, nil)
					old = append(old, read)
					defer tr.Finished(nil)
				}
				o.NetworkChanged("new")
				old[0].Add(o.s.passive.successBytes)
				if o.e.labelProof {
					t.Fatal("old connection proved new network")
				}
				tr := o.StartDial(context.Background(), tag, net.Destination{}, nil)
				read, _ := tr.Dialed(nil, nil, nil, nil)
				read.Add(o.s.passive.successBytes)
				if !o.e.labelProof {
					t.Fatal("fresh connection did not prove network")
				}
				if tag == "a1" && o.e.path("a1").status != statusUnknown {
					t.Fatal("metadata created controller evidence")
				}
				old[1].Add(o.s.passive.successBytes)
				if !o.e.labelProof {
					t.Fatal("old connection erased current proof")
				}
				o.e.noteDemand(o.now())
				if j, _ := o.e.nextLabelRequest(o.runCtx, o.now()); j == nil {
					t.Fatal("fresh proof did not allow request")
				}
				tr.Finished(nil)
			})
		}
	}
}

func TestMeasurementV2NetworkMetadataOnlySegments(t *testing.T) {
	f := v2NoEvidence()
	f.Report = qualityScalar(true)
	f.HostLabels = qualityScalar(true)
	o := newObserver(context.Background(), &Config{Features: f, Report: &ReportConfig{QuietShare: 1}})
	defer o.Close()
	o.NetworkChangedWithLabels("A", `{"type":"wifi"}`)
	o.NetworkChangedWithLabels("B", `{"type":"cellular"}`)
	var report struct {
		Quality []struct {
			Network *networkLabelsView         `json:"network"`
			Paths   map[string]json.RawMessage `json:"paths"`
		} `json:"quality"`
	}
	if err := json.Unmarshal(o.PeekReport(), &report); err != nil {
		t.Fatal(err)
	}
	if len(report.Quality) != 2 || report.Quality[0].Network.Host["type"] != "wifi" || report.Quality[1].Network.Host["type"] != "cellular" {
		t.Fatalf("metadata-only segments: %+v", report)
	}
	for _, segment := range report.Quality {
		if len(segment.Paths) != 0 {
			t.Fatal("metadata collected user quality")
		}
	}
}

func TestMeasurementV2LabelCacheLRU(t *testing.T) {
	e := measurementEngine(t)
	for i, id := range []string{"A", "B", "A", "C", "A"} {
		now := t0.Add(time.Duration(i) * time.Second)
		e.onNetworkChange(id, now)
		e.noteDemand(now)
		e.noteLabelSuccess(e.qualityEpoch)
		j, _ := e.nextLabelRequest(context.Background(), now)
		expected := i == 0 || i == 1 || i == 3
		if (j != nil) != expected {
			t.Fatalf("step%d/%s cache miss=%v want%v", i, id, j != nil, expected)
		}
		if j != nil {
			e.labelResult(j, map[string]string{"asn": "8359"}, "", now)
		}
	}
	if len(e.labelCache) != 2 || e.labelCache["A"] == nil || e.labelCache["C"] == nil || e.labelCache["B"] != nil {
		t.Fatal("did not evict least recently used network")
	}
}

func TestMeasurementV2PreparedProtoValidation(t *testing.T) {
	for _, lr := range []*LabelRequestConfig{
		{Destination: "ftp://host/a"},
		{Labels: map[string]string{"BAD": "X-Network-Asn"}},
		{Labels: map[string]string{"asn": "bad header"}},
		{RequestsPerHour: 61},
	} {
		if (&Config{Features: v2NoEvidence(), LabelRequest: lr}).ValidateMeasurementContext() == nil {
			t.Fatal("invalid prepared label request accepted", lr)
		}
	}
	if err := (&Config{Features: v2NoEvidence(), LabelRequest: &LabelRequestConfig{}}).ValidateMeasurementContext(); err != nil {
		t.Fatal("empty disabled preparation rejected", err)
	}
	o := newObserver(context.Background(), &Config{Features: v2NoEvidence()})
	defer o.Close()
	for _, cfg := range []*SelectorConfig{
		{Paths: []*PathDefinition{{Tag: "a1", Labels: map[string]string{"node": "bad value"}}}},
		{Groups: []*PathGroup{{Name: "g", Labels: map[string]string{"BAD": "v"}}}},
		{Features: &SelectorFeatureConfig{Groups: qualityScalar(true)}, Groups: []*PathGroup{{Name: "g", Labels: map[string]string{"k1": "v", "k2": "v", "k3": "v", "k4": "v", "k5": "v", "k6": "v"}}}, Paths: []*PathDefinition{{Tag: "a1", Group: "g", Labels: map[string]string{"k7": "v"}}}},
	} {
		if _, err := o.OutboundSelector("auto", serial.ToTypedMessage(cfg)); err == nil {
			t.Fatal("invalid selector labels accepted", cfg)
		}
	}
}

func TestMeasurementV2NetworkMetadataSegmentBound(t *testing.T) {
	f := v2NoEvidence()
	f.Report = qualityScalar(true)
	f.HostLabels = qualityScalar(true)
	o := newObserver(context.Background(), &Config{Features: f})
	defer o.Close()
	for i := range 5 {
		o.NetworkChangedWithLabels(fmt.Sprint(i), fmt.Sprintf(`{"type":"net%d"}`, i))
	}
	segments, dropped := o.e.quality.report(o.now(), false)
	if len(segments) != 4 || dropped != 1 || segments[0].Network.Host["type"] != "net1" || segments[3].Network.Host["type"] != "net4" {
		t.Fatalf("segment bound: %+v dropped=%d", segments, dropped)
	}
}
