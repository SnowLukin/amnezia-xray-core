package health

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
)

func TestProviderOnlyPathObservedAtHandlerConstruction(t *testing.T) {
	f := newApplyFixture(t, time.Minute, func(p *ProviderConfig) {
		p.Apply.AllowedProtocols = []string{"freedom"}
	})
	f.o.parseOutbounds = loadOutbounds
	if f.o.Observes("p-x1") {
		t.Fatal("provider path observed before its answer")
	}
	observedAtBuild := false
	f.o.newHandler = func(cfg *core.OutboundHandlerConfig) (outbound.Handler, error) {
		observedAtBuild = f.o.Observes(cfg.Tag)
		if s := f.o.e.selectors["auto"]; s.cfg.paths[cfg.Tag] != nil || s.groups["p-relay"] != nil || len(f.rt.p.tags) != 0 {
			t.Fatal("uncommitted path reached selector or provider set")
		}
		return &fakeHandler{tag: cfg.Tag, proxy: cfg.ProxySettings}, nil
	}
	a, err := parseAnswer([]byte(`{"outbounds":[{"tag":"x1","protocol":"freedom","path":{"balancer":"auto","group":"relay"}}],
		"groups":[{"name":"relay","cost":1}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, a); !changed || err != nil {
		t.Fatalf("apply %v, %v", changed, err)
	}
	if !observedAtBuild || !f.o.Observes("p-x1") {
		t.Fatal("provider path was not observed when its handler was built")
	}
}

func TestProviderPathStagingRollsBackOnFailure(t *testing.T) {
	for _, failure := range []string{"build", "register", "routing"} {
		t.Run(failure, func(t *testing.T) {
			f := newApplyFixture(t, time.Minute, func(p *ProviderConfig) {
				p.Apply.AllowedProtocols = []string{"freedom"}
			})
			f.o.parseOutbounds = loadOutbounds
			seen := false
			f.o.newHandler = func(cfg *core.OutboundHandlerConfig) (outbound.Handler, error) {
				if cfg.Tag == "p-x2" {
					seen = f.o.Observes(cfg.Tag)
					if failure == "build" {
						return nil, errors.New("cannot build")
					}
				}
				return &fakeHandler{tag: cfg.Tag, proxy: cfg.ProxySettings}, nil
			}
			first, err := parseAnswer([]byte(`{"version":"old","outbounds":[{"tag":"x1","protocol":"freedom","path":{"balancer":"auto","cost":1}}]}`))
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := f.apply(t, first); !changed || err != nil {
				t.Fatalf("first apply %v, %v", changed, err)
			}
			oldHandler, oldVersion := f.m.GetHandler("p-x1"), f.rt.version
			if failure == "register" {
				f.m.failAdd = "p-x2"
			}
			routing := ""
			if failure == "routing" {
				f.rt.cfg.routing = true
				f.o.replaceRules = func(*router.Config, bool) error { t.Fatal("invalid routing reached ReplaceRules"); return nil }
				routing = `,"routing":{"rules":[],"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{}}},{"tag":"auto","strategy":{"type":"leastCost","settings":{}}}]}`
			}
			second, err := parseAnswer([]byte(`{"version":"new","outbounds":[{"tag":"x2","protocol":"freedom","path":{"balancer":"auto","cost":2}}]` + routing + `}`))
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := f.apply(t, second); changed || err == nil {
				t.Fatalf("rejected apply %v, %v", changed, err)
			}
			if !seen || f.o.Observes("p-x2") || !f.o.Observes("p-x1") || f.m.GetHandler("p-x1") != oldHandler ||
				f.m.GetHandler("p-x2") != nil || f.rt.version != oldVersion {
				t.Fatalf("rollback: ctor observed=%v, new observed=%v, old observed=%v, handlers=%s, version=%s",
					seen, f.o.Observes("p-x2"), f.o.Observes("p-x1"), f.m.tags(), f.rt.version)
			}
		})
	}
}

func TestProviderGroupCannotJoinBaseGroup(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	base := &SelectorConfig{
		Paths:  []*PathDefinition{{Tag: "s1", Balancer: "auto", Group: "p-relay"}},
		Groups: []*PathGroup{{Name: "p-relay", Cost: 1, FailedInRowToLeave: 3}},
	}
	s := f.o.e.addSelector("auto", newSelectorSettings(base))
	s.baseRaw = base
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	old := f.m.GetHandler("p-x1")
	a, err := parseAnswer([]byte(`{"outbounds":[{"tag":"x2","protocol":"vless","v":1,"path":{"balancer":"auto","group":"relay"}}],"groups":[{"name":"relay","cost":2}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, a); changed || err == nil || !strings.Contains(err.Error(), "belongs to the base config") {
		t.Fatalf("apply %v, %v", changed, err)
	}
	if f.m.GetHandler("p-x1") != old || f.m.GetHandler("p-x2") != nil {
		t.Fatal("rejected group changed the active set")
	}
	if ev := f.lastApply(t); ev["is_ok"] != false {
		t.Fatal(ev)
	}
}

func TestProviderRefreshKeepsOwnGroupName(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	for _, tag := range []string{"x1", "x2"} {
		a, err := parseAnswer([]byte(`{"outbounds":[{"tag":"` + tag + `","protocol":"vless","v":1,"path":{"balancer":"auto","group":"relay"}}],"groups":[{"name":"relay","cost":2}]}`))
		if err != nil {
			t.Fatal(err)
		}
		if changed, err := f.apply(t, a); !changed || err != nil {
			t.Fatalf("refresh %s: %v, %v", tag, changed, err)
		}
	}
	f.o.e.updateCandidates(f.o.e.selectors["auto"], []string{"p-x2"}, time.Now())
	if g := f.o.e.selectors["auto"].groups["p-relay"]; g == nil || len(g.members) != 1 || g.members[0] != "p-x2" {
		t.Fatalf("group after refresh %+v", g)
	}
}

func TestProviderRoutingKeepsDeclaredPathsAndChangesSettings(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	base := &SelectorConfig{
		Paths:  []*PathDefinition{{Tag: "s2", Balancer: "auto", Group: "base"}, {Tag: "s1", Balancer: "auto", Group: "base"}},
		Groups: []*PathGroup{{Name: "base", Cost: 1, FailedInRowToLeave: 3}},
	}
	s := f.o.e.addSelector("auto", newSelectorSettings(base))
	s.baseRaw = base
	s.cfg.baseOrder = []string{"s2", "s1"}
	f.rt.cfg.routing = true
	var replaced *router.Config
	f.o.replaceRules = func(rc *router.Config, _ bool) error { replaced = rc; return nil }
	a, err := parseAnswer([]byte(`{"outbounds":[{"tag":"x1","protocol":"vless","v":1,"path":{"balancer":"auto","group":"relay"}}],
		"groups":[{"name":"relay","cost":2}],
		"routing":{"rules":[],"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{"minHoldTime":"7s","confirmWithProbe":"always"}}}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, a); !changed || err != nil {
		t.Fatalf("apply %v, %v", changed, err)
	}
	if replaced == nil || len(replaced.BalancingRule) != 1 {
		t.Fatalf("routing %+v", replaced)
	}
	inst, err := replaced.BalancingRule[0].StrategySettings.GetInstance()
	if err != nil {
		t.Fatal(err)
	}
	cfg := inst.(*SelectorConfig)
	if cfg.MinHoldTime != int64(7*time.Second) || cfg.ConfirmWithProbe != ConfirmMode_ALWAYS {
		t.Fatalf("settings %+v", cfg)
	}
	if len(cfg.Paths) != 3 || cfg.Paths[0].Tag != "s2" || cfg.Paths[1].Tag != "s1" || cfg.Paths[2].Tag != "p-x1" {
		t.Fatalf("paths %+v", cfg.Paths)
	}
	if len(cfg.Groups) != 2 || cfg.Groups[0].Name != "base" || cfg.Groups[1].Name != "p-relay" {
		t.Fatalf("groups %+v", cfg.Groups)
	}
	s = f.o.e.selectors["auto"]
	f.o.e.updateCandidates(s, []string{"p-x1", "s1", "s2"}, time.Now())
	if len(s.candidates) != 3 || s.candidates[0] != "s2" || s.candidates[1] != "s1" || s.candidates[2] != "p-x1" {
		t.Fatalf("candidate order %v", s.candidates)
	}
}

func TestProviderPathValidationLeavesPreviousSet(t *testing.T) {
	for _, tc := range []struct{ name, path, groups, want string }{
		{"missing path", "", ``, "path must be an object"},
		{"wrong balancer", `,"path":{"balancer":"other","cost":1}`, ``, "balancer must be auto"},
		{"no cost", `,"path":{"balancer":"auto"}`, ``, "needs cost above zero"},
		{"unknown group", `,"path":{"balancer":"auto","group":"missing"}`, ``, "unknown group missing"},
		{"group cost", `,"path":{"balancer":"auto","group":"relay","cost":1}`, `,"groups":[{"name":"relay","cost":2}]`, "cost is set by group relay"},
		{"group false", `,"path":{"balancer":"auto","group":"relay","isLazy":false}`, `,"groups":[{"name":"relay","cost":2}]`, "isLazy is set by group relay"},
		{"group zero interval", `,"path":{"balancer":"auto","group":"relay","returnInterval":"0s"}`, `,"groups":[{"name":"relay","cost":2}]`, "returnInterval is set by group relay"},
		{"group empty probe", `,"path":{"balancer":"auto","group":"relay","probe":""}`, `,"groups":[{"name":"relay","cost":2}]`, "probe is set by group relay"},
		{"group null", `,"path":{"balancer":"auto","group":"relay","cost":null}`, `,"groups":[{"name":"relay","cost":2}]`, "must not be null"},
		{"empty group name", `,"path":{"balancer":"auto","group":"","cost":1}`, ``, "group needs nonempty name"},
		{"unknown probe", `,"path":{"balancer":"auto","cost":1,"probe":"missing"}`, ``, "unknown probe missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newApplyFixture(t, time.Minute)
			if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
				t.Fatal(err)
			}
			old := f.m.GetHandler("p-x1")
			a, err := parseAnswer([]byte(`{"outbounds":[{"tag":"x2","protocol":"vless","v":1` + tc.path + `}]` + tc.groups + `}`))
			if err != nil {
				t.Fatal(err)
			}
			if changed, err := f.o.applySet(f.rt, a, &refreshJob{p: f.rt.p, trigger: triggerHost}); changed || err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("apply %v, %v; want %q", changed, err, tc.want)
			}
			if f.m.GetHandler("p-x1") != old || f.m.GetHandler("p-x2") != nil {
				t.Fatal("refused answer changed handlers")
			}
			if ev := f.lastApply(t); ev["is_ok"] != false {
				t.Fatal(ev)
			}
		})
	}
}

func TestProviderEmptyGroupDeclarationIsAllowed(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	a, err := parseAnswer([]byte(`{"outbounds":[{"tag":"x1","protocol":"vless","v":1,"path":{"balancer":"auto","cost":1}}],"groups":[{"name":"unused","cost":2}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, a); !changed || err != nil {
		t.Fatalf("apply %v, %v", changed, err)
	}
	if f.m.GetHandler("p-x1") == nil {
		t.Fatal("valid path was not registered")
	}
}

func TestProviderReordersGroupAfterRoutingResponse(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	f.rt.cfg.routing = true
	f.o.replaceRules = func(rc *router.Config, _ bool) error {
		inst, err := rc.BalancingRule[0].StrategySettings.GetInstance()
		if err != nil {
			return err
		}
		f.o.e.addSelector("auto", newSelectorSettings(inst.(*SelectorConfig)))
		return nil
	}
	first, err := parseAnswer([]byte(`{"version":"first","outbounds":[
		{"tag":"x1","protocol":"vless","v":1,"path":{"balancer":"auto","group":"relay"}},
		{"tag":"x2","protocol":"vless","v":1,"path":{"balancer":"auto","group":"relay"}}],
		"groups":[{"name":"relay","cost":1}],
		"routing":{"rules":[],"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{}}}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, first); !changed || err != nil {
		t.Fatalf("first apply %v, %v", changed, err)
	}
	s := f.o.e.selectors["auto"]
	f.o.e.updateCandidates(s, []string{"p-x1", "p-x2"}, time.Now())
	second, err := parseAnswer([]byte(`{"version":"second","outbounds":[
		{"tag":"x2","protocol":"vless","v":1,"path":{"balancer":"auto","group":"relay"}},
		{"tag":"x1","protocol":"vless","v":1,"path":{"balancer":"auto","group":"relay"}}],
		"groups":[{"name":"relay","cost":1}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, second); !changed || err != nil {
		t.Fatalf("second apply %v, %v", changed, err)
	}
	want := []string{"p-x2", "p-x1"}
	if !slices.Equal(s.candidates, want) {
		t.Fatalf("candidates %v, want %v", s.candidates, want)
	}
	g := s.groups["p-relay"]
	if g == nil || !slices.Equal(g.members, want) {
		t.Fatalf("group members %+v, want %v", g, want)
	}
}

func TestProviderRoutingDuplicateBalancerRejectsWholeSet(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	old := f.m.GetHandler("p-x1")
	oldVersion := f.rt.version
	oldLease := f.rt.p.leases["p-x1"]
	f.rt.cfg.routing = true
	called := false
	f.o.replaceRules = func(*router.Config, bool) error { called = true; return errors.New("duplicate balancer tag") }
	a, err := parseAnswer([]byte(`{"version":"duplicate","outbounds":[{"tag":"x2","protocol":"vless","v":1,"path":{"balancer":"auto","cost":2}}],
		"expiresAt":{"x2":"2026-10-11T12:00:00Z"},
		"routing":{"rules":[],"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{}}},{"tag":"auto","strategy":{"type":"leastCost","settings":{}}}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	changed, applyErr := f.apply(t, a)
	if changed || applyErr == nil || !strings.Contains(applyErr.Error(), "duplicate balancer tag") {
		t.Fatalf("apply %v, %v", changed, applyErr)
	}
	if called {
		t.Fatal("invalid routing reached ReplaceRules")
	}
	if f.m.GetHandler("p-x1") != old || f.m.GetHandler("p-x2") != nil || f.rt.version != oldVersion || f.rt.p.leases["p-x1"] != oldLease || f.rt.p.leases["p-x2"] != nil {
		t.Fatalf("rejected set changed handlers/version/leases: tags=%s version=%s leases=%v", f.m.tags(), f.rt.version, f.rt.p.leases)
	}
	if ev := f.lastApply(t); ev["is_ok"] != false {
		t.Fatalf("provider_apply %+v", ev)
	}
	now := time.Now()
	f.o.e.onRefreshDone(&refreshJob{p: f.rt.p, trigger: triggerHost}, refreshOutcome{err: applyErr.Error(), class: applyErr.Error()}, now)
	request := f.o.e.providerRequest(f.rt.p, &refreshJob{p: f.rt.p, trigger: triggerHost}, now.Add(time.Second))
	if request.LastFailure == nil || !strings.Contains(request.LastFailure.Error, "duplicate balancer tag") {
		t.Fatalf("next request lastFailure %+v", request.LastFailure)
	}
}

func TestProviderRoutingDuplicateRuleTagRejectsWholeSet(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	first, err := parseAnswer([]byte(`{"version":"old","outbounds":[{"tag":"x1","protocol":"vless","v":1,"path":{"balancer":"auto","group":"relay"}}],
		"groups":[{"name":"relay","cost":1}],"serverTime":"2026-10-10T12:00:00Z","expiresAt":{"x1":"2026-10-11T12:00:00Z"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, first); !changed || err != nil {
		t.Fatalf("first apply %v, %v", changed, err)
	}
	s := f.o.e.selectors["auto"]
	oldCfg, oldGroup := s.cfg, s.groups["p-relay"]
	oldMembers := slices.Clone(oldGroup.members)
	oldHandler, oldVersion, oldLease := f.m.GetHandler("p-x1"), f.rt.version, f.rt.p.leases["p-x1"]
	if oldLease == nil {
		t.Fatal("first set did not establish a lease")
	}
	f.rt.cfg.routing = true
	called := false
	f.o.replaceRules = func(*router.Config, bool) error { called = true; return nil }
	second, err := parseAnswer([]byte(`{"version":"new","outbounds":[{"tag":"x2","protocol":"vless","v":1,"path":{"balancer":"auto","group":"relay"}}],
		"groups":[{"name":"relay","cost":3}],"serverTime":"2026-10-10T12:00:00Z","expiresAt":{"x2":"2026-10-11T12:00:00Z"},
		"routing":{"rules":[{"ruleTag":"dup","domain":["full:a.test"],"balancerTag":"auto"},
		{"ruleTag":"dup","domain":["full:b.test"],"balancerTag":"auto"}],
		"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{"minHoldTime":"7s"}}}]}}`))
	if err != nil {
		t.Fatal(err)
	}
	changed, applyErr := f.apply(t, second)
	if changed || applyErr == nil || !strings.Contains(applyErr.Error(), "duplicate ruleTag") {
		t.Fatalf("apply %v, %v", changed, applyErr)
	}
	if called {
		t.Fatal("invalid routing reached ReplaceRules")
	}
	if f.m.GetHandler("p-x1") != oldHandler || f.m.GetHandler("p-x2") != nil || f.rt.version != oldVersion ||
		f.rt.p.leases["p-x1"] != oldLease || f.rt.p.leases["p-x2"] != nil {
		t.Fatalf("rejected set changed handlers/version/leases: tags=%s version=%s leases=%v", f.m.tags(), f.rt.version, f.rt.p.leases)
	}
	if s.cfg != oldCfg || s.groups["p-relay"] != oldGroup || !slices.Equal(oldGroup.members, oldMembers) {
		t.Fatalf("rejected set changed selector: cfg=%+v group=%+v", s.cfg, s.groups["p-relay"])
	}
	if ev := f.lastApply(t); ev["is_ok"] != false {
		t.Fatalf("provider_apply %+v", ev)
	}
	now := time.Now()
	f.o.e.onRefreshDone(&refreshJob{p: f.rt.p, trigger: triggerHost}, refreshOutcome{err: applyErr.Error(), class: applyErr.Error()}, now)
	request := f.o.e.providerRequest(f.rt.p, &refreshJob{p: f.rt.p, trigger: triggerHost}, now.Add(time.Second))
	if request.LastFailure == nil || !strings.Contains(request.LastFailure.Error, "duplicate ruleTag") {
		t.Fatalf("next request lastFailure %+v", request.LastFailure)
	}
}

func TestNewSelectorDoesNotAdoptProviderPathsAsBase(t *testing.T) {
	base := &PathDefinition{Tag: "s1", Balancer: "auto", Cost: 1}
	provided := &PathDefinition{Tag: "p-x1", Balancer: "auto", Cost: 2}
	e := newEngine(newSettings(&Config{Paths: []*PathDefinition{base}, Providers: []*ProviderConfig{quietProvider(nil)}}), 1)
	s := e.addSelector("auto", newSelectorSettings(&SelectorConfig{Paths: []*PathDefinition{base, provided}}))
	if !slices.Equal(s.cfg.baseOrder, []string{"s1"}) || len(s.baseRaw.Paths) != 1 || s.baseRaw.Paths[0].Tag != "s1" {
		t.Fatalf("base order %v, base paths %+v", s.cfg.baseOrder, s.baseRaw.Paths)
	}
}
