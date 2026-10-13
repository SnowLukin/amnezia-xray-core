package health

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
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
			if (len(o.s.pathLabels("a1")) > 0) != (mask&4 != 0) {
				t.Fatal("pathLabels gate")
			}
			if (len(o.s.reportLabels) > 0) != (mask&8 != 0) {
				t.Fatal("sessionLabels gate")
			}
			if (o.e.quality != nil) != (mask&16 != 0) {
				t.Fatal("probe-only collector gate")
			}
			if o.e.quality != nil {
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
	for _, raw := range []string{`{"type":"wifi"}`, `{broken`, string(make([]byte, 9000))} {
		if err := o.NetworkChangedWithLabels("changed", raw); err != nil {
			t.Fatal("metadata-off signal rejected", err)
		}
		before++
		if o.e.networkID != "changed" || o.e.qualityEpoch != before || o.e.hostLabels != nil {
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
