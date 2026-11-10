package health

import (
	"context"
	"encoding/json"
	"maps"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func statusOf(t *testing.T, o *Observer) statusView {
	t.Helper()
	var v statusView
	if err := json.Unmarshal(o.Status(), &v); err != nil {
		t.Fatal(err)
	}
	return v
}

func balancerOf(t *testing.T, v statusView, tag string) balancerStatusView {
	t.Helper()
	for _, b := range v.Balancers {
		if b.Balancer == tag {
			return b
		}
	}
	t.Fatalf("no balancer %s in %+v", tag, v.Balancers)
	return balancerStatusView{}
}

func pathOf(t *testing.T, v statusView, tag string) pathStatusView {
	t.Helper()
	for _, p := range v.Paths {
		if p.Outbound == tag {
			return p
		}
	}
	t.Fatalf("no path %s", tag)
	return pathStatusView{}
}

// Status shows where new connections of a balancer leave and the labels of
// that outbound: its choice, the outbound it holds when exhausted, the choice
// of its fallbackBalancer, or its fallbackTag. Labels need no pathLabels.
func TestStatusExitFollowsTheTraffic(t *testing.T) {
	now := t0
	paths := []*PathDefinition{
		{Tag: "ai1", Balancer: "ai", Cost: 1, Probe: "check", Labels: map[string]string{"country": "nl"}},
		{Tag: "main1", Balancer: "main", Cost: 1, Probe: "check", Labels: map[string]string{"country": "de"}},
	}
	o := newObserver(context.Background(), &Config{Paths: paths, Probes: []*ProbeProfile{stepProfile("check", "http://127.0.0.1/probe", 0)}, Features: &FeatureConfig{Events: proto.Bool(true), Report: proto.Bool(true)}})
	o.clock = func() time.Time { return now }
	t.Cleanup(func() { o.Close() })
	source := o.e.addSelectorAt("ai", newSelectorSettings(&SelectorConfig{Paths: paths[:1], FallbackBalancer: "main", FallbackTag: "block"}), t0)
	target := o.e.addSelectorAt("main", newSelectorSettings(&SelectorConfig{Paths: paths[1:], FallbackTag: "direct"}), t0)
	o.e.updateCandidates(source, []string{"ai1"}, t0)
	o.e.updateCandidates(target, []string{"main1"}, t0)
	h := &selectorHandle{o: o, s: source}
	exhaust := func(s *selector, tag string) {
		p := o.e.path(tag)
		p.setStatus(statusDown, "traffic", now)
		p.quarantineUntil = now.Add(time.Hour)
		o.e.exhaust(s, now)
	}
	check := func(step, exit, country string) {
		t.Helper()
		v := statusOf(t, o)
		b := balancerOf(t, v, "ai")
		if b.Exit != exit || b.ExitLabels["country"] != country {
			t.Fatalf("%s: exit %q labels %v, want %q %q", step, b.Exit, b.ExitLabels, exit, country)
		}
		if p := pathOf(t, v, "ai1"); p.Labels["country"] != "nl" {
			t.Fatalf("%s: path labels %v without pathLabels", step, p.Labels)
		}
	}

	check("chosen", "ai1", "nl")
	exhaust(source, "ai1")
	check("held", "ai1", "nl")
	if h.Current() != "ai1" {
		t.Fatal("Current does not hand out the held outbound")
	}
	now = at(2 * time.Minute)
	check("fallback balancer", "main1", "de")
	if h.Current() != "" {
		t.Fatal("Current hands out an outbound after the hold")
	}
	exhaust(target, "main1")
	now = at(4 * time.Minute)
	check("fallback tag of the fallback balancer", "direct", "")
	if b := balancerOf(t, statusOf(t, o), "main"); b.Exit != "direct" || b.ExitLabels != nil {
		t.Fatalf("target %+v", b)
	}
	source.cfg.fallbackBalancer = ""
	check("own fallback tag", "block", "")
}

// Current tells the router the choice without counting as demand.
func TestCurrentIsNotDemand(t *testing.T) {
	f := newFallbackFixture(t)
	f.o.e.lastDemand.Store(1)
	if f.h.Current() != "ai1" || f.o.e.lastDemand.Load() != 1 {
		t.Fatalf("current %q, demand %d", f.h.Current(), f.o.e.lastDemand.Load())
	}
}

// An answer with the same outbound and new labels keeps its handler; Status
// shows the new labels at once, the report none without pathLabels.
func TestProviderLabelsChangeInPlace(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	labeled := func(country string) fakeOutbound {
		return fakeOutbound{Tag: "x1", V: 1, Path: json.RawMessage(`{"balancer":"auto","probe":"check","cost":1,"labels":{"country":"` + country + `"}}`)}
	}
	if _, err := f.apply(t, answer(t, labeled("nl"))); err != nil {
		t.Fatal(err)
	}
	x1 := f.m.GetHandler("p-x1")
	if changed, err := f.apply(t, answer(t, labeled("de"))); !changed || err != nil {
		t.Fatalf("new labels: changed %v, %v", changed, err)
	}
	if f.m.GetHandler("p-x1") != x1 {
		t.Fatal("new labels replaced the handler")
	}
	if got := pathOf(t, statusOf(t, f.o), "p-x1").Labels["country"]; got != "de" {
		t.Fatalf("status labels %q", got)
	}
	f.o.mu.Lock()
	r := f.o.e.snapshotReport(f.o.now())
	f.o.mu.Unlock()
	if r.PathLabels != nil {
		t.Fatalf("report labels without pathLabels: %v", r.PathLabels)
	}
}

func TestLabelValuesThatAreAddressesAreRefused(t *testing.T) {
	for _, value := range []string{"192.168.1.10", "fe80::1", "2a02:6b8::1", "::ffff:10.0.0.1", "203.0.113.7:443", "[2a02::1]:443"} {
		if ValidateLabels(map[string]string{"node": value}, 6) == nil {
			t.Errorf("%s accepted", value)
		}
		if filterLabels(map[string]string{"node": value, "type": "wifi"}, 6)["node"] != "" {
			t.Errorf("%s kept", value)
		}
	}
	for _, value := range []string{"nl", "RU-MOW", "25001", "A.B_C:0-9", "1.2.3", "ab:cd"} {
		if err := ValidateLabels(map[string]string{"node": value}, 6); err != nil {
			t.Errorf("%s refused: %v", value, err)
		}
	}
	cfg := &providerSettings{prefix: "p-", balancer: "auto", balancers: []string{"auto"}, maxOutbounds: 8}
	a := &providerAnswer{heads: []outboundHead{{Tag: "n1", Selection: json.RawMessage(`{"balancer":"auto","cost":1,"labels":{"node":"10.1.2.3"}}`)}}}
	if validateAnswerPaths(cfg, a) == nil {
		t.Fatal("provider label with an address accepted")
	}
	o := newObserver(context.Background(), &Config{Features: &FeatureConfig{HostLabels: proto.Bool(true)}})
	defer o.Close()
	if err := o.NetworkChangedWithLabels("home", `{"type":"wifi","gateway":"192.168.1.1"}`); err != nil {
		t.Fatal(err)
	}
	if v := statusOf(t, o); v.Network == nil || !maps.Equal(v.Network.Host, map[string]string{"type": "wifi"}) {
		t.Fatalf("host labels %+v", v.Network)
	}
}

// The same network with new labels is not a change of network: what the
// balancer learned stays, only the labels change.
func TestSameNetworkOnlyUpdatesItsLabels(t *testing.T) {
	te := newTestEngine(t, &Config{Features: &FeatureConfig{HostLabels: proto.Bool(true)}}, nil, "a1", "b1")
	te.onNetworkChangeWithLabels("cell", map[string]string{"radio": "lte"}, at(time.Second))
	p := te.path("a1")
	p.setStatus(statusDown, "traffic", at(2*time.Second))
	p.quarantineLevel, p.quarantineUntil = 2, at(time.Hour)
	generation, epoch := te.generation, te.qualityEpoch

	te.onNetworkChangeWithLabels("cell", map[string]string{"radio": "nr"}, at(3*time.Second))
	if te.generation != generation || te.qualityEpoch != epoch || p.quarantineLevel != 2 || p.status != statusDown {
		t.Fatalf("labels reset the network: generation %d->%d, level %d, %s", generation, te.generation, p.quarantineLevel, p.status)
	}
	if te.hostLabels["radio"] != "nr" {
		t.Fatalf("labels %v", te.hostLabels)
	}

	te.onNetworkLost(at(4 * time.Second))
	te.onNetworkChangeWithLabels("cell", map[string]string{"radio": "nr"}, at(5*time.Second))
	if te.generation == generation || p.quarantineLevel != 0 {
		t.Fatal("the same network after a loss was not a change")
	}
}
