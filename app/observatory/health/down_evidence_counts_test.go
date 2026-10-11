package health

import (
	"testing"
	"time"
)

func TestDownEvidenceCountersFollowActualDecisions(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		mode                 ConfirmMode
		strong               bool
		pass                 bool
		wantStrong, wantWeak int64
	}{
		{"strong auto", ConfirmMode_AUTO, true, false, 1, 0},
		{"strong always", ConfirmMode_ALWAYS, true, false, 1, 0},
		{"weak auto", ConfirmMode_AUTO, false, false, 0, 1},
		{"weak always", ConfirmMode_ALWAYS, false, false, 0, 1},
		{"weak cleared", ConfirmMode_AUTO, false, true, 0, 0},
		{"strong cleared by always", ConfirmMode_ALWAYS, true, true, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := newReportEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3), ConfirmWithProbe: tc.mode}, "a1", "b1")
			if tc.strong {
				te.failN(3, "a1", failDialTimeout, at(time.Second))
			} else {
				suspectWeak(te, "a1", time.Second)
			}
			te.run(t, at(4*time.Second), map[string]bool{"a1": tc.pass})
			p := te.path("a1")
			if p.stats.downsStrong != tc.wantStrong || p.stats.downsWeak != tc.wantWeak {
				t.Fatalf("decision counters strong=%d weak=%d, want %d/%d", p.stats.downsStrong, p.stats.downsWeak, tc.wantStrong, tc.wantWeak)
			}
			if p.status == statusDown {
				te.failN(3, "a1", failDialTimeout, at(5*time.Second))
				if p.stats.downsStrong != tc.wantStrong || p.stats.downsWeak != tc.wantWeak {
					t.Fatal("repeated failures counted the same down twice")
				}
			}
			v := te.reportView(reportSnapshot, at(6*time.Second), false)
			if got := v.Paths["a1"]; got.DownsStrong != tc.wantStrong || got.DownsWeak != tc.wantWeak {
				t.Fatalf("reported decision counters %+v", got)
			}
		})
	}
}

func TestStrongEvidenceOvertakingWeakProbeCountsOneStrongDown(t *testing.T) {
	te := newReportEngine(t, nil, nil, "a1", "b1")
	suspectWeak(te, "a1", time.Second)
	jobs, _ := te.tick(at(4 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatalf("setup confirming jobs %+v", jobs)
	}
	te.failN(3, "a1", failDialTimeout, at(5*time.Second))
	te.onProbeResult(jobs[0], probeResult{ok: false, class: "timeout"}, at(6*time.Second))
	p := te.path("a1")
	if p.status != statusDown || p.stats.downsStrong != 1 || p.stats.downsWeak != 0 {
		t.Fatalf("strong overtook weak: status=%s strong=%d weak=%d", p.status, p.stats.downsStrong, p.stats.downsWeak)
	}
}

func TestAlwaysProbeUpgradesEvidenceWhileAlreadyRunning(t *testing.T) {
	te := newReportEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3), ConfirmWithProbe: ConfirmMode_ALWAYS}, "a1", "b1")
	suspectWeak(te, "a1", time.Second)
	jobs, _ := te.tick(at(4 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm {
		t.Fatalf("setup confirming jobs %+v", jobs)
	}
	te.failN(3, "a1", failDialTimeout, at(5*time.Second))
	if te.path("a1").status != statusSuspect {
		t.Fatal("always decided before its confirming probe")
	}
	te.onProbeResult(jobs[0], probeResult{ok: false, class: "timeout"}, at(6*time.Second))
	p := te.path("a1")
	if p.status != statusDown || p.stats.downsStrong != 1 || p.stats.downsWeak != 0 {
		t.Fatalf("upgraded evidence: status=%s strong=%d weak=%d", p.status, p.stats.downsStrong, p.stats.downsWeak)
	}
}

func TestAlwaysProbeUpgradesQueuedEvidenceBeforeStarting(t *testing.T) {
	te := newReportEngine(t, nil, &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3), ConfirmWithProbe: ConfirmMode_ALWAYS}, "a1", "b1")
	suspectWeak(te, "a1", time.Second)
	te.failN(3, "a1", failDialTimeout, at(2*time.Second))
	p := te.path("a1")
	if p.status != statusSuspect || p.stats.downsStrong != 0 || !te.queued("a1", reasonConfirm) {
		t.Fatal("always decided before its queued confirming probe")
	}
	jobs, _ := te.tick(at(4 * time.Second))
	if len(jobs) != 1 || jobs[0].reason != reasonConfirm || !jobs[0].evidenceStrong {
		t.Fatalf("queued confirmation did not preserve strong evidence: %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false, class: "timeout"}, at(5*time.Second))
	if p.status != statusDown || p.stats.downsStrong != 1 || p.stats.downsWeak != 0 {
		t.Fatalf("queued evidence decision: status=%s strong=%d weak=%d", p.status, p.stats.downsStrong, p.stats.downsWeak)
	}
}
