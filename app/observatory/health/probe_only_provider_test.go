package health

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/xtls/xray-core/features/outbound"
)

func TestProviderProbeOnlyPathBoolean(t *testing.T) {
	cfg := newProviderSettings(quietProvider(nil))
	for _, tc := range []struct {
		name, field string
		group       bool
		want, bad   bool
	}{
		{name: "missing"},
		{name: "false", field: `,"isProbeOnly":false`},
		{name: "true", field: `,"isProbeOnly":true`, want: true},
		{name: "group", field: `,"isProbeOnly":true`, group: true, want: true},
		{name: "null", field: `,"isProbeOnly":null`},
		{name: "string", field: `,"isProbeOnly":"true"`, bad: true},
		{name: "number", field: `,"isProbeOnly":1`, bad: true},
		{name: "object", field: `,"isProbeOnly":{}`, bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, groups := `"cost":1`, ""
			if tc.group {
				path = `"group":"relay"`
				groups = `,"groups":[{"name":"relay","cost":1}]`
			}
			a, err := parseAnswer([]byte(`{"outbounds":[{"tag":"x","protocol":"vless","selection":{"balancer":"auto",` + path + tc.field + `}}]` + groups + `}`))
			if err != nil {
				t.Fatal(err)
			}
			_, err = checkHeads(cfg, a)
			if tc.bad {
				if err == nil {
					t.Fatal("invalid isProbeOnly accepted")
				}
				return
			}
			if err != nil || len(a.paths) != 1 || a.paths[0].IsProbeOnly != tc.want {
				t.Fatalf("paths=%v err=%v", a.paths, err)
			}
		})
	}
}

func TestProviderRequestHasEveryProbeOnlyFlag(t *testing.T) {
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: []*PathDefinition{
		{Tag: "p-real", Balancer: "auto", Cost: 1}, {Tag: "p-probe", Balancer: "auto", Cost: 2, IsProbeOnly: true},
	}}, "p-real", "p-probe")
	te.prov().tags = []string{"p-real", "p-probe"}
	p := te.path("p-probe")
	p.lastProbeEnd, p.lastRTT = at(time.Second), 20*time.Millisecond
	p.lastProbeResult = probeResult{ok: true, state: statusUp, profile: "internal", reason: "candidate", steps: []probeStepResult{{name: "mandatory", ok: true, bytes: 32768}}}
	body, _, _ := requestOf(t, te, at(2*time.Second))
	outbounds := body["outbounds"].([]any)
	for i, want := range []bool{false, true} {
		o := outbounds[i].(map[string]any)
		if flag, present := o["isProbeOnly"]; !present || flag != want {
			t.Fatalf("outbound flag=%v present=%v want=%v", flag, present, want)
		}
	}
	probe := outbounds[1].(map[string]any)["probe"].(map[string]any)
	if probe["reason"] != "candidate" || probe["isPassed"] != true || probe["rttMs"] != 20.0 || probe["agoMs"] != 1000.0 || len(probe["steps"].([]any)) != 1 {
		t.Fatalf("probe fields=%v", probe)
	}
}

func TestMinHealthyWaitsForCandidateOutcome(t *testing.T) {
	for _, passed := range []bool{false, true} {
		t.Run(fmt.Sprint(passed), func(t *testing.T) {
			te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
				p.Triggers.MinHealthyOutbounds = 2
				p.Limits.MinInterval = -1
			}), &SelectorConfig{Paths: []*PathDefinition{
				{Tag: "p-real", Balancer: "auto", Cost: 1}, {Tag: "p-a", Balancer: "auto", Cost: 2, IsProbeOnly: true}, {Tag: "p-b", Balancer: "auto", Cost: 3, IsProbeOnly: true},
			}}, "p-real", "p-a", "p-b")
			te.prov().tags = []string{"p-real", "p-a", "p-b"}
			expectNoRefresh(t, te, t0)
			a := te.path("p-a")
			a.candidateChecked, a.candidateEpoch = true, te.candidateEpoch
			a.lastProbeResult = probeResult{ok: passed, reason: "candidate"}
			if passed {
				a.candidateVerifiedAt = at(time.Second)
			} else {
				expectNoRefresh(t, te, at(time.Second))
				b := te.path("p-b")
				b.candidateChecked, b.candidateEpoch = true, te.candidateEpoch
				b.lastProbeResult = probeResult{reason: "candidate"}
			}
			j := expectRefresh(t, te, at(2*time.Second), "min_healthy")
			te.onRefreshDone(j, refreshOutcome{ok: true, changed: true}, at(3*time.Second))
			expectNoRefresh(t, te, at(10*time.Minute))
			if got := te.healthy(te.sel); got != 1 {
				t.Fatalf("healthy=%d want=1", got)
			}
		})
	}
}

func TestMinHealthyExhaustedDoesNotWaitForCandidates(t *testing.T) {
	te := newProviderEngine(t, quietProvider(func(p *ProviderConfig) {
		p.Triggers.MinHealthyOutbounds = 2
	}), &SelectorConfig{Paths: []*PathDefinition{
		{Tag: "p-real", Balancer: "auto", Cost: 1}, {Tag: "p-probe", Balancer: "auto", Cost: 2, IsProbeOnly: true},
	}}, "p-real", "p-probe")
	te.exhaust(te.sel, t0)
	expectRefresh(t, te, t0, "min_healthy")
}

func TestProviderModeOnlyChangeReplacesHandlerAndDrains(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	for _, mode := range []bool{true, false, true} {
		old := f.m.GetHandler("p-x1")
		oldProbeOnly := f.o.e.isProbeOnly("p-x1")
		if old != nil && !oldProbeOnly {
			f.open("p-x1")
		}
		a := answer(t, fakeOutbound{Tag: "x1", V: 1, Path: json.RawMessage(fmt.Sprintf(`{"balancer":"auto","probe":"check","cost":1,"isProbeOnly":%t}`, mode))})
		// A reused version cannot hide a mode transition.
		a.version = "same-version"
		if changed, err := f.apply(t, a); !changed || err != nil {
			t.Fatalf("mode=%v changed=%v err=%v", mode, changed, err)
		}
		next := f.m.GetHandler("p-x1")
		if old != nil {
			if next == old {
				t.Fatal("mode change did not replace the old handler")
			}
			if oldProbeOnly {
				if old.(*fakeHandler).closes() != 1 || f.rt.draining[old] != nil {
					t.Fatal("idle probe handler was not closed")
				}
			} else {
				if f.rt.draining[old] == nil || f.rt.draining[old].registered {
					t.Fatal("old real handler did not drain its open connection")
				}
				if old.(*fakeHandler).closes() != 0 {
					t.Fatal("old handler closed before drain")
				}
			}
		}
		if f.o.e.isProbeOnly("p-x1") != mode {
			t.Fatalf("committed mode=%v want=%v", f.o.e.isProbeOnly("p-x1"), mode)
		}
		if mode && slices.Contains(f.o.e.selectors["auto"].candidates, "p-x1") {
			t.Fatal("demoted outbound still selectable")
		}
	}
}

func TestProviderPromotionReusesOnlyFreshCandidateProof(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		age                           time.Duration
		epoch, failed, profileChanged bool
		want                          bool
	}{
		{name: "fresh", age: time.Second, want: true},
		{name: "expired", age: 10 * time.Minute},
		{name: "network", age: time.Second, epoch: true},
		{name: "failed", age: time.Second, failed: true},
		{name: "profile", age: time.Second, profileChanged: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: []*PathDefinition{
				{Tag: "p-real", Balancer: "auto", Cost: 1}, {Tag: "p-probe", Balancer: "auto", Cost: 2, IsProbeOnly: true},
			}}, "p-real", "p-probe")
			te.prov().tags = []string{"p-real", "p-probe"}
			q := te.path("p-probe")
			now := at(time.Hour)
			q.candidateVerifiedAt, q.candidateEpoch = now.Add(-tc.age), te.candidateEpoch
			q.lastProbe, q.lastProbeEnd, q.lastRTT = q.candidateVerifiedAt.Add(-time.Second), q.candidateVerifiedAt, 20*time.Millisecond
			q.lastProbeResult = probeResult{ok: !tc.failed, reason: "candidate", state: statusUp, steps: []probeStepResult{{name: "mandatory", ok: true}}}
			q.failures = []failure{{at: t0, kind: failDial}}
			q.quarantineLevel = 3
			q.serverName = "old-server"
			if tc.epoch {
				te.candidateEpoch++
			}
			probe := ""
			if tc.profileChanged {
				probe = "changed"
			}
			te.exhaust(te.sel, now)
			te.applyProviderSet(te.prov(), providerChange{
				tags:  []string{"p-real", "p-probe"},
				paths: []*PathDefinition{{Tag: "p-real", Balancer: "auto", Cost: 1}, {Tag: "p-probe", Balancer: "auto", Cost: 2, Probe: probe}},
				reset: []string{"p-probe"},
			}, now)
			if !q.realSince.Equal(now) || q.freshDownReported || len(q.failures) != 0 || q.quarantineLevel != 0 || q.serverName != "" || !q.candidateVerifiedAt.IsZero() {
				t.Fatalf("promotion retained old history: %+v", q)
			}
			events := te.eventsOf(t, "probe_only_promoted")
			if tc.want {
				if len(events) != 1 || q.status != statusUp || q.lastProbeResult.reason != "candidate" || len(q.lastProbeResult.steps) != 1 || te.sel.current() != "p-probe" {
					t.Fatalf("fresh promotion: events=%v state=%v current=%v", events, q.status, te.sel.current())
				}
			} else if len(events) != 0 || !q.lastProbeEnd.IsZero() {
				t.Fatalf("stale proof reused: events=%v probeEnd=%v", events, q.lastProbeEnd)
			}
		})
	}
}

type demotionRegistrationManager struct {
	*fakeManager
	installed chan struct{}
	release   chan struct{}
}

func (m *demotionRegistrationManager) AddHandler(ctx context.Context, handler outbound.Handler) error {
	if err := m.fakeManager.AddHandler(ctx, handler); err != nil {
		return err
	}
	close(m.installed)
	<-m.release
	return nil
}

func TestProviderDemotionBlocksSelectBeforeCommit(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	h := &selectorHandle{o: f.o, s: f.o.e.selectors["auto"]}
	candidates := []string{"p-x1"}
	if got := h.Select(candidates); got != "p-x1" {
		t.Fatalf("initial selection=%q", got)
	}
	m := &demotionRegistrationManager{fakeManager: f.m, installed: make(chan struct{}), release: make(chan struct{})}
	f.o.ohm = m
	a := answer(t, fakeOutbound{Tag: "x1", V: 1, Path: json.RawMessage(`{"balancer":"auto","probe":"check","cost":1,"isProbeOnly":true}`)})
	done := make(chan error, 1)
	go func() {
		_, err := f.o.applySet(f.rt, a, &refreshJob{p: f.rt.p, trigger: triggerHost})
		done <- err
	}()
	select {
	case <-m.installed:
	case <-time.After(time.Second):
		close(m.release)
		t.Fatal("replacement not registered")
	}
	got := h.Select(candidates)
	close(m.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("demotion did not commit")
	}
	if got != "" {
		t.Fatalf("user traffic selected demoted handler before commit: %q", got)
	}
	if f.o.e.retiring["p-x1"] || !f.o.e.isProbeOnly("p-x1") {
		t.Fatal("temporary retirement survived demotion commit")
	}
}

func TestProviderDemotionRegistrationFailureRestoresMode(t *testing.T) {
	f := newApplyFixture(t, time.Hour)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	old := f.m.GetHandler("p-x1")
	f.m.failAdd = "p-x2"
	a := answer(t,
		fakeOutbound{Tag: "x1", V: 1, Path: json.RawMessage(`{"balancer":"auto","probe":"check","cost":1,"isProbeOnly":true}`)},
		fakeOutbound{Tag: "x2", V: 1},
	)
	if changed, err := f.apply(t, a); changed || err == nil {
		t.Fatalf("apply changed=%v err=%v", changed, err)
	}
	if f.m.GetHandler("p-x1") != old || f.o.e.isProbeOnly("p-x1") || f.o.e.retiring["p-x1"] {
		t.Fatal("failed demotion left mode or retirement changed")
	}
	if !slices.Contains(f.o.e.selectors["auto"].candidates, "p-x1") {
		t.Fatal("failed demotion did not restore real candidate")
	}
}

func TestProviderFreshPromotionRestoresDownGroup(t *testing.T) {
	paths := []*PathDefinition{
		{Tag: "p-real", Balancer: "auto", Group: "p-relay"},
		{Tag: "p-probe", Balancer: "auto", Group: "p-relay", IsProbeOnly: true},
	}
	groups := []*PathGroup{{Name: "p-relay", Cost: 1, FailedInRowToLeave: 3}}
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: paths, Groups: groups}, "p-real", "p-probe")
	te.prov().tags = []string{"p-real", "p-probe"}
	now := at(time.Minute)
	real := te.path("p-real")
	real.status, real.quarantineUntil = statusDown, now.Add(time.Hour)
	g := te.sel.groupFor("p-real")
	te.groupQuarantine(te.sel, g, now)
	te.exhaust(te.sel, now)
	probe := te.path("p-probe")
	probe.status = statusUp
	probe.candidateChecked, probe.candidateEpoch = true, te.candidateEpoch
	probe.candidateVerifiedAt, probe.lastProbeEnd = now.Add(-time.Second), now.Add(-time.Second)
	probe.lastProbeResult = probeResult{ok: true, state: statusUp, reason: "candidate"}
	if !g.down || te.sel.groupFor("p-probe") != nil {
		t.Fatal("candidate result changed the real group")
	}
	te.applyProviderSet(te.prov(), providerChange{
		tags:   []string{"p-real", "p-probe"},
		paths:  []*PathDefinition{{Tag: "p-real", Balancer: "auto", Group: "p-relay"}, {Tag: "p-probe", Balancer: "auto", Group: "p-relay"}},
		groups: groups,
		reset:  []string{"p-probe"},
	}, now)
	promotedGroup := te.sel.groupFor("p-probe")
	if promotedGroup == nil || promotedGroup.down || promotedGroup.failed != 0 || !promotedGroup.until.IsZero() {
		t.Fatalf("promoted group stayed down: %+v", promotedGroup)
	}
	if te.sel.current() != "p-probe" || te.sel.exhausted {
		t.Fatalf("fresh promotion not activated: current=%q exhausted=%v", te.sel.current(), te.sel.exhausted)
	}
	if real.status != statusDown || !real.quarantined(now) {
		t.Fatal("promotion erased the failed real member's quarantine")
	}
}
