package health

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func groupConfig() *SelectorConfig {
	return &SelectorConfig{Paths: []*PathDefinition{{Tag: "a", Group: "a"}, {Tag: "b", Cost: 2}}, Groups: []*PathGroup{{Name: "a", Cost: 1, FailedInRowToLeave: 3}}, StartOutbound: "a2"}
}

func TestGroupStopsAfterConsecutiveFailures(t *testing.T) {
	te := newTestEngine(t, &Config{Schedule: quarantine(time.Minute)}, groupConfig(), "a1", "a2", "a3", "a4", "a5", "b1")
	te.markDown(te.path("a2"), "traffic", t0)
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0].tag != "a3" {
		t.Fatalf("first successor: %v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false}, t0)
	jobs, _ = te.tick(at(3 * time.Second))
	if len(jobs) != 1 || jobs[0].tag != "a4" {
		t.Fatalf("second successor: %v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false}, at(3*time.Second))
	jobs, _ = te.tick(at(6 * time.Second))
	if len(jobs) != 1 || jobs[0].tag != "b1" {
		t.Fatalf("group was not abandoned: %v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(6*time.Second))
	if te.sel.current() != "b1" {
		t.Fatal(te.sel.current())
	}
	if te.healthy(te.sel) != 1 {
		t.Fatalf("unknown members count as healthy: %d", te.healthy(te.sel))
	}
}

func TestGroupClientPositionsSpread(t *testing.T) {
	seen := map[string]bool{}
	for seed := int64(1); seed <= 50; seed++ {
		cfg := groupConfig()
		cfg.StartOutbound = ""
		te := newSeededTestEngine(t, seed, nil, cfg, "a1", "a2", "a3", "a4", "a5", "b1")
		seen[te.sel.current()] = true
	}
	if len(seen) < 4 {
		t.Fatal(fmt.Sprint(seen))
	}
}

func readyGroupEngine(t *testing.T) *testEngine {
	t.Helper()
	cfg := groupConfig()
	cfg.ReturnToCheaper = tune(&ReturnToCheaperConfig{Interval: int64(10 * time.Minute)}, returnKnobs{fixedPhase: true, successes: 2, spacing: time.Second})
	te := newTestEngine(t, &Config{Schedule: quarantine(time.Minute)}, cfg, "a1", "a2", "a3", "a4", "a5", "b1")
	te.s.schedule.spacing = 0
	te.s.schedule.spacingJitter = 0
	te.s.schedule.quarantine.jitter = 0
	te.s.schedule.perMinute = 1000
	te.s.schedule.probesPerServerNamePerMinute = 1000
	return te
}

func abandonGroup(t *testing.T, te *testEngine) {
	t.Helper()
	te.markDown(te.path("a2"), "traffic", t0)
	for _, tag := range []string{"a3", "a4", "b1"} {
		jobs, _ := te.tick(t0)
		if len(jobs) != 1 || jobs[0].tag != tag {
			t.Fatalf("want probe %s, got %+v", tag, jobs)
		}
		te.onProbeResult(jobs[0], probeResult{ok: tag == "b1", class: "timeout"}, t0)
	}
}

func TestGroupRecoveryProbesOneNextMemberAndKeepsPathQuarantine(t *testing.T) {
	te := readyGroupEngine(t)
	abandonGroup(t, te)
	te.path("a4").quarantineUntil = at(5 * time.Minute)
	if g := te.sel.groups["a"]; !g.down || g.failed != 3 {
		t.Fatalf("group %+v", g)
	}
	jobs, wake := te.tick(at(30 * time.Second))
	if len(jobs) != 0 || !wake.Equal(at(time.Minute)) {
		t.Fatalf("early probe or wrong wake: %v %v", jobs, wake)
	}
	jobs, _ = te.tick(at(time.Minute))
	if len(jobs) != 1 || jobs[0].tag != "a5" || jobs[0].reason != reasonGroup {
		t.Fatalf("recovery: %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(time.Minute))
	if te.sel.groups["a"].down || te.sel.groups["a"].failed != 0 {
		t.Fatal("group not recovered")
	}
	if te.path("a1").status != statusUnknown || !te.path("a4").quarantined(at(time.Minute)) {
		t.Fatal("recovery changed other members")
	}
	if te.sel.current() != "b1" {
		t.Fatal("recovery skipped return probes")
	}
	te.sel.returnToCheaperAt = at(2 * time.Minute)
	jobs, _ = te.tick(at(2 * time.Minute))
	if len(jobs) != 1 || jobs[0].tag != "a5" || jobs[0].reason != reasonReturnToCheaper {
		t.Fatalf("return target: %+v", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(2*time.Minute))
	if te.sel.current() != "b1" {
		t.Fatal("returned before required probes")
	}
	jobs, _ = te.tick(at(2*time.Minute + time.Second))
	if len(jobs) != 1 || jobs[0].tag != "a5" {
		t.Fatal("missing second return probe")
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(2*time.Minute+time.Second))
	if te.sel.current() != "a5" {
		t.Fatal("did not return")
	}
}

func TestGroupSuccessResetsFailures(t *testing.T) {
	for _, traffic := range []bool{false, true} {
		t.Run(fmt.Sprint(traffic), func(t *testing.T) {
			te := readyGroupEngine(t)
			te.markDown(te.path("a2"), "traffic", t0)
			jobs, _ := te.tick(t0)
			if traffic {
				te.onSuccess("a3", t0)
			} else {
				te.onProbeResult(jobs[0], probeResult{ok: true}, t0)
			}
			if te.sel.groups["a"].failed != 0 {
				t.Fatal("success did not reset failures")
			}
		})
	}
}

func TestGroupSamePriceBeforeExpensiveAndProviderCostKeepsMembership(t *testing.T) {
	cfg := groupConfig()
	cfg.Groups[0].FailedInRowToLeave = 1
	cfg.Paths = append(cfg.Paths, &PathDefinition{Tag: "c", Group: "c"})
	cfg.Groups = append(cfg.Groups, &PathGroup{Name: "c", Cost: 1, FailedInRowToLeave: 3})
	te := newTestEngine(t, nil, cfg, "a1", "a2", "b1", "c1", "c2")
	te.markDown(te.path("a2"), "traffic", t0)
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0].tag[0] != 'c' {
		t.Fatalf("same-price group not first: %v", jobs)
	}
	te.sel.cfg.paths["c1"].cost, te.sel.cfg.paths["c2"].cost = 9, 8
	if te.sel.groupFor("c1") != te.sel.groupFor("c2") {
		t.Fatal("price split the group")
	}
	ranked := te.ranked(te.sel, t0, nil)
	if len(ranked) != 3 || ranked[0] != "b1" || ranked[1] != "c2" {
		t.Fatal(ranked)
	}
}

func TestGroupSerializesDifferentNamesAndKeepsGlobalConcurrency(t *testing.T) {
	te := readyGroupEngine(t)
	te.s.schedule.concurrent = 3
	te.enqueue("a1", reasonNetwork, te.sel.tag, t0)
	te.enqueue("a3", reasonNetwork, te.sel.tag, t0)
	te.enqueue("b1", reasonNetwork, te.sel.tag, t0)
	jobs, _ := te.tick(t0)
	if len(jobs) != 2 {
		t.Fatalf("wanted one group member and independent path: %+v", jobs)
	}
	if jobs[0].tag != "a1" || jobs[1].tag != "b1" {
		t.Fatal(jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, t0)
	jobs, _ = te.tick(t0)
	if len(jobs) != 1 || jobs[0].tag != "a3" {
		t.Fatal(jobs)
	}
}

func TestGroupHostRequestCoversOneMember(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			te := readyGroupEngine(t)
			var tags []string
			if explicit {
				tags = []string{"a1", "a2", "a3", "a4", "a5", "b1"}
			}
			_, got, err := te.requestProbe(te.sel.tag, tags, t0)
			if err != nil || len(got) != 2 {
				t.Fatalf("got %v, error %v", got, err)
			}
			_, got, err = te.requestProbe(te.sel.tag, []string{"a4"}, t0)
			if err != nil || len(got) != 2 {
				t.Fatalf("joined duplicate member: %v %v", got, err)
			}
		})
	}
}

func TestGroupRecoveryWaitsForLifecycleGates(t *testing.T) {
	for _, gate := range []string{"idle", "memory", "pin", "lazy", "network"} {
		t.Run(gate, func(t *testing.T) {
			te := readyGroupEngine(t)
			abandonGroup(t, te)
			switch gate {
			case "idle":
				te.s.schedule.idleAfter = time.Second
			case "memory":
				te.memoryLow = true
			case "pin":
				te.sel.pinned = "b1"
			case "lazy":
				for _, p := range te.sel.cfg.paths {
					if p.groupName == "a" {
						p.lazy = true
					}
				}
			case "network":
				te.networkDown = true
			}
			jobs, wake := te.tick(at(time.Minute))
			if len(jobs) != 0 {
				t.Fatal("probe passed gate", jobs)
			}
			if !wake.IsZero() && !wake.After(at(time.Minute)) {
				t.Fatal("busy wake", wake)
			}
			switch gate {
			case "idle":
				te.noteDemand(at(2 * time.Minute))
			case "memory":
				te.memoryLow = false
			case "pin":
				te.sel.pinned = ""
			case "lazy":
				for _, p := range te.sel.cfg.paths {
					if p.groupName == "a" {
						p.lazy = false
					}
				}
			case "network":
				te.networkDown = false
			}
			jobs, _ = te.tick(at(2 * time.Minute))
			if len(jobs) != 1 || jobs[0].reason != reasonGroup {
				t.Fatalf("no recovery after gate: %v", jobs)
			}
		})
	}
}

func TestGroupRecoveryWithReturnDisabled(t *testing.T) {
	te := readyGroupEngine(t)
	te.sel.cfg.returnToCheaper.interval = 0
	abandonGroup(t, te)
	jobs, _ := te.tick(at(time.Minute))
	if len(jobs) != 1 || jobs[0].reason != reasonGroup {
		t.Fatal(jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(time.Minute))
	jobs, _ = te.tick(at(time.Hour / 2))
	if len(jobs) != 0 || te.sel.current() != "b1" {
		t.Fatal("disabled return ran")
	}
}

func TestGroupCandidateAndNetworkLifecycle(t *testing.T) {
	te := readyGroupEngine(t)
	abandonGroup(t, te)
	te.onNetworkChange("next", t0)
	if g := te.sel.groups["a"]; g.down || g.failed != 0 {
		t.Fatal("network kept group failure")
	}
	te.updateCandidates(te.sel, []string{"b1"}, t0)
	if len(te.sel.groups) != 0 {
		t.Fatal("empty group retained")
	}
	te.updateCandidates(te.sel, []string{"a1", "b1"}, t0)
	if g := te.sel.groups["a"]; g == nil || len(g.members) != 1 || g.down {
		t.Fatal("group not rebuilt")
	}
	te.keepSelectors(map[string]bool{})
	if len(te.selectors) != 0 {
		t.Fatal("removed selector retained")
	}
}

func TestGroupExpiredMemberIsLastReserve(t *testing.T) {
	te := readyGroupEngine(t)
	te.expired["a3"] = true
	te.markDown(te.path("a2"), "traffic", t0)
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0].tag != "a4" {
		t.Fatal("expired member taken before live ring member")
	}
}

func TestGroupViewsAndReportCount(t *testing.T) {
	te := readyGroupEngine(t)
	abandonGroup(t, te)
	views := te.status(t0).Groups
	if len(views) != 1 || views[0].Members != 5 || views[0].Healthy != 0 || views[0].Quarantined != 3 || views[0].Failed != 3 || views[0].State != "down" {
		t.Fatalf("views %+v", views)
	}
	report := te.snapshotReport(t0)
	if report.GroupDown != 1 || len(report.Groups) != 1 || reportPaths != 64 {
		t.Fatalf("report %+v", report)
	}
}

func TestGroupProviderSummaryUsesOwnMembersAndCamelCase(t *testing.T) {
	cfg := &SelectorConfig{Paths: []*PathDefinition{{Tag: "x", Group: "x"}}, Groups: []*PathGroup{{Name: "x", Cost: 1, FailedInRowToLeave: 1}}, StartOutbound: "p-x1"}
	te := newProviderEngine(t, quietProvider(nil), cfg, "p-x1", "p-x2", "other-x3")
	te.markDown(te.path("p-x1"), "traffic", t0)
	body, _, _ := requestOf(t, te, t0)
	groups, ok := body["groups"].([]any)
	if !ok || len(groups) != 1 {
		t.Fatalf("groups: %v", body["groups"])
	}
	g := groups[0].(map[string]any)
	if g["members"] != float64(2) || g["state"] != "down" || g["failed"] != float64(1) || g["healthy"] != float64(0) {
		t.Fatal(g)
	}
	if _, ok := g["clientPosition"]; !ok {
		t.Fatal(g)
	}
	if _, ok := g["quarantineMs"]; !ok {
		t.Fatal(g)
	}
	if _, ok := g["client_position"]; ok {
		t.Fatal(g)
	}
}

func TestGroupFailedRecoveryExtendsGroupRestAndMovesRing(t *testing.T) {
	te := readyGroupEngine(t)
	abandonGroup(t, te)
	jobs, _ := te.tick(at(time.Minute))
	if len(jobs) != 1 || jobs[0].tag != "a5" {
		t.Fatal(jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: false, class: "timeout"}, at(time.Minute))
	g := te.sel.groups["a"]
	if !g.down || !g.until.After(at(2*time.Minute)) {
		t.Fatalf("rest did not grow: %+v", g)
	}
	jobs, _ = te.tick(g.until)
	if len(jobs) != 1 || jobs[0].tag != "a1" {
		t.Fatal("failed recovery did not advance ring", jobs)
	}
}

func TestGroupSharedNamesRetainThreePerMinute(t *testing.T) {
	te := readyGroupEngine(t)
	te.s.schedule.probesPerServerNamePerMinute = 3
	te.serverNameFor = func(string) string { return "same.example" }
	te.markDown(te.path("a2"), "traffic", t0)
	for _, tag := range []string{"a3", "a4", "b1"} {
		jobs, _ := te.tick(t0)
		if len(jobs) != 1 || jobs[0].tag != tag {
			t.Fatal("wrong probe", jobs)
		}
		te.onProbeResult(jobs[0], probeResult{ok: tag == "b1"}, t0)
	}
	te.enqueue("a1", reasonNetwork, te.sel.tag, t0)
	jobs, wake := te.tick(t0)
	if len(jobs) != 0 || !wake.Equal(at(time.Minute)) {
		t.Fatal("name budget bypassed", jobs, wake)
	}
}

func TestGroupStaleAndCanceledProbesDoNotChangeGroup(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(fmt.Sprint(canceled), func(t *testing.T) {
			te := readyGroupEngine(t)
			abandonGroup(t, te)
			jobs, _ := te.tick(at(time.Minute))
			if len(jobs) != 1 {
				t.Fatal(jobs)
			}
			if !canceled {
				te.generation++
			}
			te.onProbeResult(jobs[0], probeResult{ok: true, canceled: canceled}, at(time.Minute))
			if !te.sel.groups["a"].down {
				t.Fatal("invalid result revived group")
			}
		})
	}
}

func TestGroupProvisionalRestEndsWithNetworkRecovery(t *testing.T) {
	te := readyGroupEngine(t)
	te.doubt = true
	abandonGroup(t, te)
	if !te.sel.groups["a"].down {
		t.Fatal("not down")
	}
	te.doubt = false
	te.restoreProvisional(t0)
	if te.sel.groups["a"].down {
		t.Fatal("group remained down after provisional paths recovered")
	}
}

func TestGroupHostCanCheckAfterGroupQuarantine(t *testing.T) {
	te := readyGroupEngine(t)
	abandonGroup(t, te)
	for _, p := range te.sel.cfg.paths {
		if p.groupName == "a" {
			p.lazy = true
		}
	}
	_, tags, err := te.requestProbe(te.sel.tag, []string{"a1", "a5"}, at(time.Minute))
	if err != nil || len(tags) != 1 {
		t.Fatal(tags, err)
	}
	jobs, _ := te.tick(at(time.Minute))
	if len(jobs) != 1 || jobs[0].tag != "a1" || jobs[0].reason != reasonHost {
		t.Fatal(jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(time.Minute))
	if te.sel.groups["a"].down || te.sel.current() != "b1" {
		t.Fatal("host recovery changed choice or stayed down")
	}
}

func TestGroupBoundedAcrossCandidateAndSelectorChanges(t *testing.T) {
	te := readyGroupEngine(t)
	for i := range 100 {
		tag := fmt.Sprintf("new%d", i)
		cfg := &SelectorConfig{Paths: []*PathDefinition{{Tag: tag, Group: tag}}, Groups: []*PathGroup{{Name: tag, Cost: 1}}}
		te.addSelector(te.sel.tag, legacyScenarioSelector(cfg, nil))
		te.updateCandidates(te.sel, []string{tag}, t0)
		if len(te.sel.groups) != 1 || len(te.sel.groups[tag].members) != 1 {
			t.Fatal("old groups retained")
		}
	}
}

func TestGroupHostLimitCountsGroupsNotMembers(t *testing.T) {
	tags := make([]string, 0, 25)
	for i := range 24 {
		tags = append(tags, fmt.Sprintf("a%02d", i))
	}
	tags = append(tags, "b1")
	cfg := groupConfig()
	cfg.StartOutbound = "a07"
	te := newTestEngine(t, nil, cfg, tags...)
	_, got, err := te.requestProbe(te.sel.tag, tags, t0)
	if err != nil || len(got) != 2 {
		t.Fatalf("24 member request: %v %v", got, err)
	}
}

func TestGroupCurrentFailureCountsOnceAndOldTrafficCannotReviveIt(t *testing.T) {
	te := readyGroupEngine(t)
	te.sel.groups["a"].limit = 1
	te.markDown(te.path("a2"), "traffic", t0)
	te.markDown(te.path("a2"), "traffic", t0)
	g := te.sel.groups["a"]
	if !g.down || g.failed != 1 {
		t.Fatal("failure counted more than once")
	}
	te.onSuccess("a2", t0)
	if !g.down || g.failed != 0 || !te.path("a2").quarantined(t0) {
		t.Fatal("old traffic revived failed dials or failed to reset count")
	}
}

func TestGroupRingWrapsAndQuarantineCancelsQueuedReturn(t *testing.T) {
	te := readyGroupEngine(t)
	te.switchTo(te.sel, "a5", "start", t0)
	te.markDown(te.path("a5"), "traffic", t0)
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0].tag != "a1" {
		t.Fatal("ring did not wrap", jobs)
	}
	te = readyGroupEngine(t)
	te.sel.groups["a"].limit = 1
	te.sel.cfg.paths["a2"].cost = 10
	te.sel.returnToCheaperTarget = "a1"
	te.enqueue("a1", reasonReturnToCheaper, te.sel.tag, t0)
	te.markDown(te.path("a2"), "traffic", t0)
	jobs, _ = te.tick(t0)
	if len(jobs) != 1 || jobs[0].tag != "b1" || te.sel.returnToCheaperTarget != "" {
		t.Fatal("abandoned group still probed", jobs)
	}
}

func TestGroupExhaustedRecoveryPrefersLiveLeaseAndCheaperGroup(t *testing.T) {
	cfg := &SelectorConfig{Paths: []*PathDefinition{{Tag: "a", Group: "a"}, {Tag: "c", Group: "c"}, {Tag: "b", Cost: 2}}, Groups: []*PathGroup{{Name: "a", Cost: 9, FailedInRowToLeave: 1}, {Name: "c", Cost: 1, FailedInRowToLeave: 1}}, StartOutbound: "a1"}
	te := newTestEngine(t, nil, cfg, "a1", "a2", "b1", "c1", "c2")
	te.sel.groups["a"].down = true
	te.sel.groups["c"].down = true
	te.expired["a1"] = true
	te.expired["a2"] = true
	te.expired["b1"] = true
	te.exhaust(te.sel, t0)
	jobs, _ := te.tick(t0)
	if len(jobs) != 1 || jobs[0].tag[0] != 'c' || jobs[0].reason != reasonGroup {
		t.Fatal("expired or expensive recovery displaced live cheap group", jobs)
	}
}

func TestGroupExpiredCandidatesRemainReserve(t *testing.T) {
	te := readyGroupEngine(t)
	for _, tag := range te.sel.groups["a"].members {
		te.expired[tag] = true
	}
	ranked := te.ranked(te.sel, t0, nil)
	if len(ranked) != 6 || ranked[0] != "b1" {
		t.Fatal(ranked)
	}
	te.path("b1").status = statusDown
	te.path("b1").quarantineUntil = at(time.Minute)
	ranked = te.ranked(te.sel, t0, nil)
	if len(ranked) != 5 || ranked[0] != "a2" {
		t.Fatal("expired group is not last reserve", ranked)
	}
}

func TestGroupDownHostSkipsExpiredRepresentative(t *testing.T) {
	te := readyGroupEngine(t)
	abandonGroup(t, te)
	g := te.sel.groups["a"]
	te.expired[g.members[g.position]] = true
	_, tags, err := te.requestProbe(te.sel.tag, nil, at(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(tags, func(tag string) bool { return te.sel.groupFor(tag) == g }) {
		t.Fatalf("host omitted group with live members: hash=%s tags=%v", g.members[g.position], tags)
	}
}

func TestGroupDownHostOrdering(t *testing.T) {
	cfg := &SelectorConfig{Paths: []*PathDefinition{{Tag: "a", Group: "a"}, {Tag: "c", Group: "c"}, {Tag: "b", Cost: 9}}, Groups: []*PathGroup{{Name: "a", Cost: 1}, {Name: "c", Cost: 2}}, StartOutbound: "b1"}
	te := newTestEngine(t, nil, cfg, "a1", "a2", "b1", "c1", "c2")
	te.s.schedule.hostProbes.maxOutbounds = 2
	for _, g := range te.sel.groups {
		g.down = true
		g.until = at(time.Minute)
	}
	for range 100 {
		tags := te.probePlan(te.sel, t0)
		if len(tags) != 2 || te.sel.groupFor(tags[1]) != te.sel.groups["a"] {
			t.Fatalf("cheap group omitted under limit: %v", tags)
		}
	}
}

func TestGroupDownHostSkipsLazyRepresentative(t *testing.T) {
	te := readyGroupEngine(t)
	abandonGroup(t, te)
	g := te.sel.groups["a"]
	te.sel.cfg.paths[g.members[g.position]].lazy = true
	_, tags, err := te.requestProbe(te.sel.tag, nil, at(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(tags, func(tag string) bool { return te.sel.groupFor(tag) == g && !te.sel.cfg.isLazy(tag) }) {
		t.Fatalf("host omitted group with eligible members: hash=%s tags=%v", g.members[g.position], tags)
	}
}

func TestGroupDownHostQuarantineThenActualProbe(t *testing.T) {
	te := readyGroupEngine(t)
	abandonGroup(t, te)
	g := te.sel.groups["a"]
	te.expired[g.members[g.position]] = true
	if !te.pin(te.sel.tag, "b1", t0) {
		t.Fatal("pin failed")
	}
	te.onSuccess("b1", at(30*time.Second))
	_, tags, err := te.requestProbe(te.sel.tag, nil, at(30*time.Second))
	if err != nil || len(tags) != 2 {
		t.Fatal(tags, err)
	}
	r := te.requests[len(te.requests)-1]
	it := r.items[1]
	if it.result != probeDown || it.source != "group_quarantine" || it.rest != 30*time.Second {
		t.Fatalf("before group rest: %+v", it)
	}
	if jobs, _ := te.tick(at(30 * time.Second)); len(jobs) != 0 {
		t.Fatal("host spent a probe during group quarantine", jobs)
	}
	te.onSuccess("b1", at(time.Minute))
	_, tags, err = te.requestProbe(te.sel.tag, nil, at(time.Minute))
	if err != nil || len(tags) != 2 {
		t.Fatal(tags, err)
	}
	jobs, _ := te.tick(at(time.Minute))
	if len(jobs) != 1 || jobs[0].tag != tags[1] || jobs[0].reason != reasonHost {
		t.Fatal("no actual host probe after quarantine", jobs)
	}
	te.onProbeResult(jobs[0], probeResult{ok: true}, at(time.Minute))
	r = te.requests[len(te.requests)-1]
	it = r.items[1]
	if it.result != probeOK || it.source != "probe" || g.down {
		t.Fatalf("actual result: %+v groupDown=%v", it, g.down)
	}
}

func TestGroupDownHostPreservesUsablePriorityAndActualPrices(t *testing.T) {
	cfg := &SelectorConfig{Paths: []*PathDefinition{{Tag: "a", Group: "a"}, {Tag: "c", Group: "c"}, {Tag: "b", Cost: 9}, {Tag: "d", Cost: 12}}, Groups: []*PathGroup{{Name: "a", Cost: 1}, {Name: "c", Cost: 2}}, StartOutbound: "b1"}
	te := newTestEngine(t, nil, cfg, "a1", "a2", "b1", "c1", "c2", "d1")
	te.s.schedule.hostProbes.maxOutbounds = 3
	for _, g := range te.sel.groups {
		g.down = true
		g.until = at(time.Minute)
	}
	cases := []struct {
		name   string
		prices map[string]float64
		want   string
	}{
		{"configured", nil, "a"},
		{"provider", map[string]float64{"a1": 8, "a2": 8, "c1": 3, "c2": 3}, "c"},
		{"equal", map[string]float64{"a1": 3, "a2": 3, "c1": 3, "c2": 3}, "a"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for tag, price := range tc.prices {
				te.sel.cfg.paths[tag].cost = price
			}
			for range 30 {
				tags := te.probePlan(te.sel, t0)
				if len(tags) != 3 || tags[0] != "b1" || tags[1] != "d1" || te.sel.groupFor(tags[2]) != te.sel.groups[tc.want] {
					t.Fatalf("current, usable, ordered quarantine: %v", tags)
				}
			}
		})
	}
}

func TestGroupDownHostPrefersMemberOutsideIndividualQuarantine(t *testing.T) {
	cfg := groupConfig()
	cfg.StartOutbound = "b1"
	te := newTestEngine(t, nil, cfg, "a1", "a2", "a3", "a4", "a5", "b1")
	g := te.sel.groups["a"]
	g.down = true
	g.until = t0
	first := g.members[g.position]
	te.path(first).status = statusDown
	te.path(first).quarantineUntil = at(time.Minute)
	tags := te.probePlan(te.sel, t0)
	if len(tags) != 2 || tags[1] == first || te.path(tags[1]).quarantined(t0) {
		t.Fatalf("individual quarantine hid an eligible member: %v", tags)
	}
}
