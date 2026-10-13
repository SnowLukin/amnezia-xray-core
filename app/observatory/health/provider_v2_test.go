package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"google.golang.org/protobuf/proto"
	"math"
	"strings"
	"testing"
	"time"
)

func v2ProviderConfig() *ProviderConfig {
	return &ProviderConfig{IsEnabled: proto.Bool(true), Tag: "fresh", Url: "https://provider.test/set", Apply: &ProviderApply{Balancer: "auto", TagPrefix: "p-"}, Features: &ProviderFeatureConfig{Exhausted: proto.Bool(false), LeaseRenewal: proto.Bool(false), BackendSchedule: proto.Bool(false)}}
}
func TestProviderV2DisabledHasNoRuntimeOrBackgroundWork(t *testing.T) {
	for _, flag := range []*bool{nil, proto.Bool(false)} {
		pc := v2ProviderConfig()
		pc.IsEnabled = flag
		pc.InitialAnswer = []byte(strings.Repeat("broken", 100000))
		o := newObserver(context.Background(), &Config{Providers: []*ProviderConfig{pc}})
		o.applyInitial()
		if len(o.providers) != 0 || len(o.e.providers) != 0 || len(o.s.providers) != 0 || o.e.quality != nil || o.e.events != nil {
			t.Fatal("disabled provider created runtime collectors or slots")
		}
		if n := testing.AllocsPerRun(100, func() {
			jobs, wake := o.e.tickProviders(at(100 * time.Hour))
			if len(jobs) != 0 || !wake.IsZero() {
				t.Fatal("disabled provider scheduled work")
			}
		}); n != 0 {
			t.Fatalf("disabled tick allocated %v", n)
		}
		if o.RequestProviderRefresh("fresh") {
			t.Fatal("disabled refresh accepted")
		}
		_, err := o.ProviderRefreshInfo("fresh")
		if CommandErrorOf(err).Code != "feature_disabled" {
			t.Fatal(err)
		}
		_, _, err = o.ExportProviderSet("fresh", 0)
		if CommandErrorOf(err).Code != "feature_disabled" {
			t.Fatal(err)
		}
		if err = o.SetProviderHeaders("fresh", map[string]string{"Authorization": "secret"}); CommandErrorOf(err).Code != "feature_disabled" {
			t.Fatal(err)
		}
		o.Close()
	}
}
func TestProviderV2EveryOptionalTriggerOffDoesNotSchedule(t *testing.T) {
	pc := v2ProviderConfig()
	pc.Features = &ProviderFeatureConfig{Exhausted: proto.Bool(false), LeaseRenewal: proto.Bool(false), BackendSchedule: proto.Bool(false)}
	pc.Triggers = &ProviderTriggers{Interval: int64(time.Minute), MinHealthyOutbounds: 5, MaxAge: int64(time.Minute), WhenCostAbove: &CostTrigger{Cost: 1, After: proto.Int64(0)}, NetworkChangeMode: RefreshOnNetwork_REFRESH_ANY, NetworkSettleDelay: proto.Int64(0)}
	e := newEngine(newSettings(&Config{Providers: []*ProviderConfig{pc}}), 1)
	s := e.addSelector("auto", newSelectorSettings(&SelectorConfig{Paths: []*PathDefinition{{Tag: "p-x", Balancer: "auto", Cost: 2}}}))
	p := e.providers[0]
	p.tags = []string{"p-x"}
	e.updateCandidates(s, []string{"p-x"}, t0)
	s.started = true
	s.setActive("p-x")
	s.activeSince = t0
	e.noteDemand(t0)
	e.scheduleRefresh(p, time.Minute, true, t0)
	e.providersOnNetworkChange(false, t0)
	e.acceptLeases(p, p.tags, leaseAnswer{ends: map[string]time.Time{"p-x": t0.Add(time.Minute)}, serverTime: t0, at: t0}, t0)
	for _, now := range []time.Time{t0, at(2 * time.Minute), at(time.Hour)} {
		e.noteDemand(now)
		jobs, wake := e.tickProviders(now)
		if len(jobs) != 0 || !p.nextAt.IsZero() || p.networkPending || p.aboveSince != (time.Time{}) {
			t.Fatalf("disabled trigger work: jobs=%v wake=%v provider=%+v", jobs, wake, p)
		}
	}
	if !e.expired["p-x"] {
		t.Fatal("lease expiration stopped with renewal trigger")
	}
}
func TestProviderV2BackendScheduleGateKeepsPeriodicIndependent(t *testing.T) {
	for _, backend := range []bool{false, true} {
		pc := v2ProviderConfig()
		pc.Features.Periodic = proto.Bool(true)
		pc.Features.BackendSchedule = proto.Bool(backend)
		pc.Triggers = &ProviderTriggers{Interval: int64(time.Hour)}
		p := newProvider(newProviderSettings(pc))
		e := newEngine(newSettings(nil), 1)
		e.scheduleRefresh(p, time.Minute, true, t0)
		want := triggerInterval
		if backend {
			want = triggerNextRefresh
		}
		if p.nextAt.IsZero() || p.nextTrigger != want {
			t.Fatalf("backend=%v: next=%v trigger=%v", backend, p.nextAt, p.nextTrigger)
		}
	}
}
func TestProviderV2ExpiredFallbackOnOff(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		pc := v2ProviderConfig()
		pc.Features.ExpiredLeaseFallback = proto.Bool(enabled)
		e := newEngine(newSettings(&Config{Providers: []*ProviderConfig{pc}}), 1)
		s := e.addSelector("auto", newSelectorSettings(&SelectorConfig{Paths: []*PathDefinition{{Tag: "p-x", Balancer: "auto", Cost: 1}}}))
		e.providers[0].tags = []string{"p-x"}
		e.updateCandidates(s, []string{"p-x"}, t0)
		e.expired["p-x"] = true
		ranked := e.ranked(s, t0, nil)
		if (len(ranked) > 0) != enabled {
			t.Fatalf("fallback=%v rank=%v", enabled, ranked)
		}
		s.setActive("")
		e.switchTo(s, "p-x", "start", t0)
		if (s.current() == "p-x") != enabled {
			t.Fatalf("fallback=%v current=%s", enabled, s.current())
		}
	}
}
func TestProviderV2SemanticMetadataAndShapeBounds(t *testing.T) {
	makeBody := func(tag, version string, extra string) []byte {
		return []byte(fmt.Sprintf(`{"protocol":1,"version":%q,"outbounds":[{"tag":%q,"protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":1}}]%s}`, version, tag, extra))
	}
	cfg := newProviderSettings(v2ProviderConfig())
	for _, n := range []int{126, 127} {
		a, err := parseAnswer(makeBody(strings.Repeat("x", n), strings.Repeat("v", 256), ""))
		if err != nil {
			t.Fatal(err)
		}
		_, err = checkHeads(cfg, a)
		if (err == nil) != (n == 126) {
			t.Fatalf("tag bytes=%d error=%v", n, err)
		}
	}
	if _, err := parseAnswer(makeBody("x", strings.Repeat("v", 257), "")); err == nil {
		t.Fatal("oversized version accepted")
	}
	if _, err := parseAnswer(makeBody("x", "v", `,"baseVersion":"`+strings.Repeat("b", 257)+`"`)); err == nil {
		t.Fatal("oversized baseVersion accepted")
	}
	for _, extra := range []string{`,"routing":{}`, `,"routing":null`, `,"dns":{}`} {
		if _, err := parseAnswer(makeBody("x", "v", extra)); err == nil {
			t.Fatal("forbidden response accepted")
		}
	}
	for _, selection := range []string{`{"balancer":"auto","probe":"check","cost":1,"isLazy":true}`, `{"balancer":"auto","probe":"check","cost":1,"returnInterval":"1h"}`, `{"balancer":"auto","probe":"check","cost":1,"probePolicy":"lazy"}`} {
		body := fmt.Sprintf(`{"outbounds":[{"tag":"x","protocol":"vless","selection":%s}]}`, selection)
		a, err := parseAnswer([]byte(body))
		if err == nil {
			_, err = checkHeads(cfg, a)
		}
		if err == nil {
			t.Fatal("retired response alias accepted")
		}
	}
	a, _ := parseAnswer(makeBody("x", "v", `,"groups":[{"name":"unused","probe":"check","cost":1}]`))
	if _, err := checkHeads(cfg, a); err == nil {
		t.Fatal("unused group accepted")
	}
}
func TestProviderV2MergedLabelsBoundedAndGroupPolicy(t *testing.T) {
	body := `{"outbounds":[{"tag":"x","protocol":"vless","selection":{"balancer":"auto","group":"g","labels":{"node":"member"}}}],"groups":[{"name":"g","probe":"check","cost":2,"probePolicy":"onDemand","returnToCheaper":{},"labels":{"node":"group","fleet":"a"}}]}`
	a, err := parseAnswer([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = checkHeads(newProviderSettings(v2ProviderConfig()), a); err != nil {
		t.Fatal(err)
	}
	if !a.groups[0].IsLazy || a.groups[0].ReturnIsEnabled == nil || !*a.groups[0].ReturnIsEnabled || a.paths[0].Labels["node"] != "member" || a.paths[0].Labels["fleet"] != "a" {
		t.Fatalf("group %v path %v", a.groups[0], a.paths[0])
	}
	for _, labels := range []string{`{"KEY":"x"}`, `{"node":"node.example/path"}`, `{"node":""}`, `{"node":"` + strings.Repeat("x", 33) + `"}`} {
		raw := strings.Replace(body, `{"node":"member"}`, labels, 1)
		a, _ := parseAnswer([]byte(raw))
		if _, err = checkHeads(newProviderSettings(v2ProviderConfig()), a); err == nil {
			t.Fatal("invalid label accepted")
		}
	}
}
func TestProviderV2PostTruncatesOnlyOptionalDetails(t *testing.T) {
	max := int64(math.MaxInt64)
	u := uint64(math.MaxUint64)
	fraction := .99
	q := &requestQuality{BytesDown: &max, TTFBCount: u, TTFBP50Ms: &max, TTFBP90Ms: &max, DialCount: u, DialP50Ms: &max, DialP90Ms: &max, TCPCount: u, TCPRTTP50Ms: &max, TCPRTTP90Ms: &max, TCPRTTMinMs: &max, BulkCount: u, BulkKbpsP50: &max, BulkKbpsP90: &max, BulkKbpsMax: &max, BulkRTTP50Ms: &max, BulkSplicedCount: &u, RXOutOfOrderFraction: &fraction, TXRetransmitFraction: &fraction}
	profile := strings.Repeat("p", 64)
	steps := []requestProbeStep{{Name: strings.Repeat("n", 64), IsPassed: true}, {Name: strings.Repeat("m", 64), IsPassed: true}, {Name: strings.Repeat("o", 64), IsPassed: true}, {Name: strings.Repeat("q", 64), IsPassed: true}}
	r := &providerRequest{Protocol: 1, Provider: "fresh", Version: "v1", BaseVersion: "base", ClientTime: t0, Network: requestNetwork{IsUp: true}, WitnessQuality: q, Balancer: requestBalancer{Tag: "auto", Current: "x", IsOwn: true}, Outbounds: []requestOutbound{}}
	for i := 0; i < 128; i++ {
		tag := fmt.Sprintf("%03d%s", i, strings.Repeat("\x00", 123))
		r.Groups = append(r.Groups, requestGroup{Group: tag, State: "up", Members: 1, Healthy: 1})
		expires := int64(1234)
		r.Outbounds = append(r.Outbounds, requestOutbound{Tag: tag, State: "up", IsProbeOnly: i%2 == 0, ExpiresInMs: &expires, Quality: q, Probe: &requestProbe{Profile: &profile, Reason: "host", State: "up", IsPassed: true, Steps: &steps}})
	}
	raw, _ := json.Marshal(r)
	if len(raw) <= maxProviderRequestBytes {
		t.Fatal("fixture is not rich enough")
	}
	o := &Observer{run: "private-run"}
	rt := &providerRuntime{cfg: newProviderSettings(v2ProviderConfig())}
	body, err := o.requestBody(rt, &refreshJob{request: r})
	if err != nil || len(body) > maxProviderRequestBytes {
		t.Fatalf("size=%d err=%v", len(body), err)
	}
	var bounded providerRequest
	if json.Unmarshal(body, &bounded) != nil || !bounded.IsTruncated || bounded.WitnessQuality != nil || len(bounded.Outbounds) != 128 {
		t.Fatal("unmarked truncation")
	}
	for i, item := range bounded.Outbounds {
		original := r.Outbounds[i]
		if item.Tag != original.Tag || item.State != original.State || item.IsProbeOnly != original.IsProbeOnly || item.ExpiresInMs == nil || *item.ExpiresInMs != *original.ExpiresInMs || item.Probe == nil || item.Probe.State != "up" {
			t.Fatal("essential state or lease changed")
		}
		if original.Quality == nil || original.Probe.Profile == nil || original.Probe.Steps == nil {
			t.Fatal("kept diagnostic measurement changed")
		}
	}
	r.Outbounds[0].Tag = strings.Repeat("essential", maxProviderRequestBytes)
	if body, err = o.requestBody(rt, &refreshJob{request: r}); body != nil || !errors.Is(err, errProviderRequestTooLarge) {
		t.Fatalf("essential overflow size=%d err=%v", len(body), err)
	}
}
func TestProviderV2OversizedEssentialNeverFetchesOrChangesSet(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 1})); err != nil {
		t.Fatal(err)
	}
	saved, seq := f.providerSet(t, 0)
	old := f.m.GetHandler("p-x1")
	now := f.o.now()
	f.o.e.noteDemand(now)
	j := f.o.e.beginRefresh(f.rt.p, triggerHost, now)
	j.request = &providerRequest{Outbounds: []requestOutbound{{Tag: strings.Repeat("x", maxProviderRequestBytes)}}}
	f.o.runRefresh(j)
	if f.rt.p.lastClass != "request_too_large" || f.rt.p.refreshes != 1 || f.rt.p.failures != 1 || old != f.m.GetHandler("p-x1") {
		t.Fatalf("overflow outcome %+v", f.rt.p)
	}
	after, next := f.providerSet(t, 0)
	if seq != next || fmt.Sprint(saved) != fmt.Sprint(after) {
		t.Fatal("overflow changed accepted provider set")
	}
	f.o.e.requestRefresh("fresh")
	jobs, _ := f.o.e.tickProviders(now)
	if len(jobs) != 0 {
		t.Fatal("manual request bypassed overflow backoff")
	}
}
func TestProviderV2ProbeTicketRejectsRestartIDCollision(t *testing.T) {
	makeObserver := func() *Observer {
		o := newObserver(context.Background(), &Config{Features: &FeatureConfig{ProbeRequests: proto.Bool(true)}, Paths: []*PathDefinition{{Tag: "a", Balancer: "auto", Cost: 1}}})
		s := o.e.addSelector("auto", newSelectorSettings(&SelectorConfig{Paths: []*PathDefinition{{Tag: "a", Balancer: "auto", Cost: 1}}}))
		o.e.updateCandidates(s, []string{"a"}, o.now())
		return o
	}
	old := makeObserver()
	ticket, err := old.RequestProbe(ProbeRequest{Balancer: "auto"})
	if err != nil {
		t.Fatal(err)
	}
	old.Close()
	fresh := makeObserver()
	defer fresh.Close()
	current, err := fresh.RequestProbe(ProbeRequest{Balancer: "auto"})
	if err != nil || current.ID != ticket.ID || current.Run == ticket.Run {
		t.Fatalf("collision fixture %+v %+v err=%v", ticket, current, err)
	}
	if raw, _, err := fresh.ProbeResult(ticket.Run, ticket.ID); raw != nil || CommandErrorOf(err).Code != "stale_run" {
		t.Fatal("old ticket read a new run")
	}
	if raw, _, err := fresh.ProbeResult(current.Run, current.ID); raw == nil || err != nil {
		t.Fatal(err)
	}
}
func TestProviderV2FailureCodeNeverExportsRawError(t *testing.T) {
	if providerFailureCode("dial https://secret.example/token") != "unknown" || providerFailureCode("invalid_answer") != "invalid_answer" {
		t.Fatal("raw error exported as failure code")
	}
}

func TestProviderV2ResultOmissionForTypedFailure(t *testing.T) {
	o := &Observer{run: "r"}
	body := MarshalResult(o, ProbeTicket{}, NewCommandError("feature_disabled", "UDP probes are disabled"))
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil {
		t.Fatal(string(body))
	}
	if _, present := fields["data"]; present {
		t.Fatal("failed typed Result includes a zero ticket")
	}
	body = MarshalResult(o, json.RawMessage(nil), nil)
	if json.Unmarshal(body, &fields) != nil {
		t.Fatal(string(body))
	}
	if _, present := fields["data"]; present {
		t.Fatal("nil success data is not omitted")
	}
}
func TestProviderV2SwitchHistoryKeepsDelimiters(t *testing.T) {
	r := report{}
	r.begin(t0)
	r.count("switch", map[string]any{"from": "a>one:two", "to": "b:three>four", "reason": "failover"})
	if len(r.counts.SwitchHistory) != 1 || r.counts.SwitchHistory[0].From != "a>one:two" || r.counts.SwitchHistory[0].To != "b:three>four" {
		t.Fatal("delimiter changed typed switch history")
	}
}

func TestProviderV2RetiredExpiredOwnershipSurvivesDrain(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		f := newApplyFixture(t, time.Minute, func(p *ProviderConfig) { p.Features.ExpiredLeaseFallback = proto.Bool(fallback) })
		first := answer(t, fakeOutbound{Tag: "x1", V: 1})
		first.expiresAt = map[string]time.Time{"x1": f.o.now().Add(-time.Second)}
		if _, err := f.apply(t, first); err != nil {
			t.Fatal(err)
		}
		f.follow("p-x1")
		f.open("p-x1")
		old := f.m.GetHandler("p-x1")
		if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x2", V: 1})); err != nil {
			t.Fatal(err)
		}
		if !f.rt.p.retiredExpired["p-x1"] || f.o.e.expiredLeaseUsable("p-x1") != fallback {
			t.Fatalf("fallback=%v expired drain lost exact owner", fallback)
		}
		if f.m.GetHandler("p-x1") != old {
			t.Fatal("old connection drain ended early")
		}
		// A protected base tag in the same textual namespace is not inferred owned.
		f.o.e.expired["p-protected-base"] = true
		if !f.o.e.expiredLeaseUsable("p-protected-base") {
			t.Fatal("prefix inferred ownership of a protected base tag")
		}
		// A fresh replacement under the exact tag ends its old lease ownership.
		if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", V: 2})); err != nil {
			t.Fatal(err)
		}
		if f.rt.p.retiredExpired["p-x1"] || !f.o.e.expiredLeaseUsable("p-x1") {
			t.Fatal("revived tag retained expired tombstone")
		}
	}
}
func TestProviderV2EachRefreshTriggerHasAnEffectiveGate(t *testing.T) {
	cases := []struct {
		name    string
		trigger refreshTrigger
		enable  func(*ProviderFeatureConfig)
	}{
		{"periodic", triggerInterval, func(f *ProviderFeatureConfig) { f.Periodic = proto.Bool(true) }},
		{"minHealthy", triggerMinHealthy, func(f *ProviderFeatureConfig) { f.MinHealthy = proto.Bool(true) }},
		{"costAbove", triggerCostAbove, func(f *ProviderFeatureConfig) { f.CostAbove = proto.Bool(true) }},
		{"maxAge", triggerMaxAge, func(f *ProviderFeatureConfig) { f.MaxAge = proto.Bool(true) }},
		{"network", triggerNetwork, func(f *ProviderFeatureConfig) { f.NetworkChange = proto.Bool(true) }},
		{"backend", triggerNextRefresh, func(f *ProviderFeatureConfig) { f.BackendSchedule = proto.Bool(true) }},
		{"expiry", triggerExpiry, func(f *ProviderFeatureConfig) { f.LeaseRenewal = proto.Bool(true) }},
		{"exhausted", triggerExhausted, func(f *ProviderFeatureConfig) { f.Exhausted = proto.Bool(true) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for _, enabled := range []bool{false, true} {
				pc := v2ProviderConfig()
				if enabled {
					tc.enable(pc.Features)
				}
				pc.Triggers = &ProviderTriggers{Interval: int64(time.Minute), MinHealthyOutbounds: 2, MaxAge: int64(time.Minute), WhenCostAbove: &CostTrigger{Cost: 1, After: proto.Int64(0)}, NetworkChangeMode: RefreshOnNetwork_REFRESH_ANY, NetworkSettleDelay: proto.Int64(0)}
				e := newEngine(newSettings(&Config{Providers: []*ProviderConfig{pc}}), 1)
				p := e.providers[0]
				p.started = true
				p.setAt = t0
				p.tags = []string{"p-x"}
				s := e.addSelector("auto", newSelectorSettings(&SelectorConfig{Paths: []*PathDefinition{{Tag: "p-x", Balancer: "auto", Cost: 2}}}))
				e.updateCandidates(s, p.tags, t0)
				s.started = true
				s.setActive("p-x")
				s.activeSince = t0
				if tc.trigger == triggerExhausted {
					s.exhausted = true
				}
				e.acceptLeases(p, p.tags, leaseAnswer{ends: map[string]time.Time{"p-x": t0.Add(time.Minute)}, serverTime: t0, at: t0}, t0)
				e.providersOnNetworkChange(false, t0)
				e.scheduleRefresh(p, time.Minute, tc.trigger == triggerNextRefresh, t0)
				now := at(2 * time.Minute)
				e.noteDemand(now)
				want, _, _ := e.wantedRefresh(p, now)
				if want[tc.trigger] != enabled {
					t.Fatalf("enabled=%v trigger=%s wanted=%v", enabled, tc.trigger.String(), want)
				}
			}
		})
	}
}
func TestProviderV2ManualRefreshReportsGuardsAndResumes(t *testing.T) {
	cases := []struct {
		name    string
		block   func(*Observer, *provider, *selector, time.Time)
		release func(*Observer, *provider, *selector, time.Time)
	}{
		{"idle", func(o *Observer, p *provider, s *selector, now time.Time) { o.e.lastDemand.Store(0) }, func(o *Observer, p *provider, s *selector, now time.Time) { o.e.noteDemand(now) }},
		{"network_down", func(o *Observer, p *provider, s *selector, now time.Time) { o.e.networkDown = true }, func(o *Observer, p *provider, s *selector, now time.Time) { o.e.networkDown = false }},
		{"pinned", func(o *Observer, p *provider, s *selector, now time.Time) { s.pinned = "a" }, func(o *Observer, p *provider, s *selector, now time.Time) { s.pinned = "" }},
		{"memory", func(o *Observer, p *provider, s *selector, now time.Time) { o.e.memoryLow = true }, func(o *Observer, p *provider, s *selector, now time.Time) { o.e.memoryLow = false }},
		{"auth", func(o *Observer, p *provider, s *selector, now time.Time) {
			p.authFailed = true
			p.authRetryAt = now.Add(time.Hour)
		}, func(o *Observer, p *provider, s *selector, now time.Time) { p.authFailed = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pc := v2ProviderConfig()
			o := newObserver(context.Background(), &Config{Providers: []*ProviderConfig{pc}, Paths: []*PathDefinition{{Tag: "a", Balancer: "auto", Cost: 1}}})
			defer o.Close()
			now := t0
			o.clock = func() time.Time { return now }
			p := o.e.providers[0]
			s := o.e.addSelector("auto", newSelectorSettings(&SelectorConfig{Paths: []*PathDefinition{{Tag: "a", Balancer: "auto", Cost: 1}}}))
			o.e.updateCandidates(s, []string{"a"}, now)
			o.e.noteDemand(now)
			tc.block(o, p, s, now)
			data, err := o.ProviderRefreshInfo("fresh")
			if err != nil || !data.IsQueued || data.Reason != tc.name {
				t.Fatalf("deferred=%+v err=%v", data, err)
			}
			jobs, _ := o.e.tickProviders(now)
			if len(jobs) != 0 || !p.host {
				t.Fatal("manual request bypassed guard or was lost")
			}
			if tc.name == "auth" && data.RetryInMs != time.Hour.Milliseconds() {
				t.Fatal("auth retry delay missing")
			}
			tc.release(o, p, s, now)
			jobs, _ = o.e.tickProviders(now)
			if len(jobs) != 1 || jobs[0].trigger != triggerHost {
				t.Fatal("deferred manual request did not resume")
			}
		})
	}
}
