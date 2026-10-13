package health

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"
)

func TestCensusObserverFinalReportInterruptsAndDoesNotRepeat(t *testing.T) {
	e := censusTestEngine(t, 1, &PathDefinition{Tag: "a", Cost: 1}, &PathDefinition{Tag: "b", Cost: 2})
	censusStartedJob(t, e, at(time.Second))
	o := &Observer{e: e, s: e.s, clock: func() time.Time { return at(2 * time.Second) }, run: "test-run"}
	raw := o.TakeReport(true)
	var v reportView
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	if v.Census == nil || v.Census.IsComplete || v.Census.Paths["a"].Reason != "close" || v.Census.Paths["b"].Reason != "close" {
		t.Fatalf("stop lost in-flight census: %s", raw)
	}
	if v.CensusProbes != 1 || v.Probes != 0 || v.Run != "test-run" {
		t.Fatalf("counter/run isolation: %s", raw)
	}
	if len(o.TakeReport(true)) != 0 || len(o.PeekReport()) != 0 {
		t.Fatal("closed report repeated")
	}
	status := e.status(at(2 * time.Second))
	if status.Census == nil || !status.Census.IsSelected || status.Census.IsRunning {
		t.Fatalf("stop status: %+v", status.Census)
	}
}

func TestCensusObserverPeekKeepsWindowThenTakeConsumesSnapshot(t *testing.T) {
	e := censusTestEngine(t, 1, &PathDefinition{Tag: "a", Cost: 1})
	e.s.reportInterval = time.Second
	j := censusStartedJob(t, e, at(time.Second))
	e.onProbeResult(j, probeResult{ok: false, bytes: 2048, class: "dial_timeout"}, at(2*time.Second))
	e.tick(at(2 * time.Second))
	if !e.report.decided || e.report.quietSample {
		t.Fatal("census must make report due independently of quiet sampling")
	}
	o := &Observer{e: e, s: e.s, clock: func() time.Time { return at(3 * time.Second) }}
	for i := 0; i < 2; i++ {
		var view reportView
		if err := json.Unmarshal(o.PeekReport(), &view); err != nil {
			t.Fatal(err)
		}
		if view.Census == nil || view.CensusProbeBytes != 2048 || view.ProbeBytes != 0 || view.Probes != 0 || view.Paths["a"].Probes != 0 {
			t.Fatalf("snapshot counters: %+v", view)
		}
	}
	var taken reportView
	if err := json.Unmarshal(o.TakeReport(false), &taken); err != nil {
		t.Fatal(err)
	}
	if taken.Census == nil || len(e.census.pending) != 0 {
		t.Fatal("take did not consume census")
	}
	if next := e.reportView(reportSnapshot, at(4*time.Second), false); next.Census != nil || next.CensusProbes != 0 || next.CensusProbeBytes != 0 {
		t.Fatalf("snapshot leaked next window: %+v", next)
	}
}

func TestCensusObserverProbeOnlyDispatchCredentialExpiresOnInterrupt(t *testing.T) {
	e := censusTestEngine(t, 1, &PathDefinition{Tag: "only", Cost: 1, IsProbeOnly: true})
	j := censusStartedJob(t, e, at(time.Second))
	o := &Observer{e: e, s: e.s}
	parent := o.probeJobContext(context.Background(), j)
	ctx := withProbe(parent, &atomic.Int64{})
	probeStateFrom(ctx).credential, _ = parent.Value(probeCredentialKey{}).(*probeCredential)
	if !o.AllowDispatch(ctx, "only", true) || o.AllowDispatch(context.Background(), "only", true) {
		t.Fatal("census credential scope")
	}
	e.interruptCensus("network_change", at(2*time.Second))
	if o.AllowDispatch(ctx, "only", true) {
		t.Fatal("interrupted census retained probe-only permission")
	}
}
