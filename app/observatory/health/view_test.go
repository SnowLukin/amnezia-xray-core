package health

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

// viewFixture is an observer whose user balancer auto has a1 and b1, and
// whose balancer spare has s1.
type viewFixture struct {
	o           *Observer
	now         time.Time
	auto, spare *selector
}

func newViewFixture(t *testing.T, auto *SelectorConfig) *viewFixture {
	t.Helper()
	f := &viewFixture{now: t0}
	paths := []*PathDefinition{
		{Tag: "a1", Balancer: "auto", Cost: 1, Probe: "check", Protocol: "vless", Labels: map[string]string{"country": "nl"}},
		{Tag: "b1", Balancer: "auto", Cost: 2, Probe: "check", Protocol: "wireguard"},
		{Tag: "s1", Balancer: "spare", Cost: 1, Probe: "check", Protocol: "trojan"},
	}
	f.o = newObserver(context.Background(), &Config{
		Paths: paths, Probes: []*ProbeProfile{stepProfile("check", "http://127.0.0.1/probe", 0)},
		View: &ViewConfig{Balancer: "auto"}, Features: &FeatureConfig{Pinning: proto.Bool(true)},
	})
	f.o.clock = func() time.Time { return f.now }
	t.Cleanup(func() { f.o.Close() })
	auto.Paths, auto.FallbackTag = paths[:2], "block"
	f.auto = f.o.e.addSelectorAt("auto", newSelectorSettings(auto), t0)
	f.spare = f.o.e.addSelectorAt("spare", newSelectorSettings(&SelectorConfig{Paths: paths[2:], FallbackTag: "direct"}), t0)
	return f
}

// connect plays the first connection through each balancer: the router
// brings its candidates and the balancer chooses.
func (f *viewFixture) connect() *viewFixture {
	f.o.e.updateCandidates(f.auto, []string{"a1", "b1"}, f.now)
	f.o.e.updateCandidates(f.spare, []string{"s1"}, f.now)
	return f
}

func (f *viewFixture) view(t *testing.T) ViewData {
	t.Helper()
	v, err := f.o.View()
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func (f *viewFixture) wait(d time.Duration) { f.now = f.now.Add(d) }

// down plays a failed probe of every outbound of the balancer but its
// choice, then the failure of the choice: the balancer runs out.
func (f *viewFixture) exhaust(s *selector) {
	for _, tag := range s.candidates {
		if tag != s.current() {
			f.o.e.markDown(f.o.e.path(tag), "probe", f.now)
		}
	}
	f.o.e.markDown(f.o.e.path(s.current()), "traffic", f.now)
}

func viewPathOf(t *testing.T, v ViewData, tag string) viewPath {
	t.Helper()
	for _, p := range v.Paths {
		if p.Outbound == tag {
			return p
		}
	}
	t.Fatalf("no path %s in %+v", tag, v.Paths)
	return viewPath{}
}

func currents(v ViewData) []string {
	var out []string
	for _, p := range v.Paths {
		if p.IsCurrent {
			out = append(out, p.Outbound)
		}
	}
	return out
}

// The state follows where new connections of the user balancer go: its
// choice, the choice it knows is down while it probes the others, the
// outbound it holds once it ran out, and its fallbackTag after the hold. A
// lost network comes before all of them.
func TestViewStateFollowsTheCurrentOutbound(t *testing.T) {
	f := newViewFixture(t, &SelectorConfig{})
	// Before the first connection the balancer has not chosen; its paths
	// are known from the config.
	v := f.view(t)
	if v.State != "ready" || v.Outbound.Tag != "" || len(v.Paths) != 3 || currents(v) != nil || v.LastSwitch != nil ||
		viewPathOf(t, v, "a1").State != "unknown" || viewPathOf(t, v, "a1").IsActive {
		t.Fatalf("before the first connection: %+v", v)
	}
	f.o.NetworkLost()
	if v = f.view(t); v.State != "no_network" {
		t.Fatalf("no network before the first connection: %+v", v)
	}
	f.o.NetworkChanged("wifi")
	f.connect()
	v = f.view(t)
	if v.State != "ok" || v.Balancer != "auto" || v.Run != f.o.run || !v.Network.IsUp ||
		!reflect.DeepEqual(v.Outbound, viewOutbound{Tag: "a1", Protocol: "vless", Labels: map[string]string{"country": "nl"}}) {
		t.Fatalf("chosen: %+v", v)
	}
	if got := []string{v.Paths[0].Outbound, v.Paths[1].Outbound, v.Paths[2].Outbound}; !slices.Equal(got, []string{"a1", "b1", "s1"}) {
		t.Fatalf("paths %v", got)
	}
	a1, s1 := viewPathOf(t, v, "a1"), viewPathOf(t, v, "s1")
	if !a1.IsCurrent || !a1.IsActive || a1.Balancer != "auto" || a1.Protocol != "vless" || a1.Cost != 1 || a1.Labels["country"] != "nl" ||
		s1.IsCurrent || !s1.IsActive || s1.Balancer != "spare" || s1.State != "unknown" {
		t.Fatalf("a1 %+v, s1 %+v", a1, s1)
	}
	if v.LastSwitch == nil || *v.LastSwitch != (viewSwitch{To: "a1", Reason: "start"}) {
		t.Fatalf("last switch %+v", v.LastSwitch)
	}

	// The choice failed; the balancer stays on it and probes b1.
	f.wait(time.Second)
	f.o.e.markDown(f.o.e.path("a1"), "traffic", f.now)
	if v = f.view(t); v.State != "searching" || v.Outbound.Tag != "a1" || v.Outbound.IsHeld || viewPathOf(t, v, "a1").State != "down" {
		t.Fatalf("choice down: %+v", v)
	}
	jobs, _ := f.o.e.tick(f.now)
	if len(jobs) != 1 || jobs[0].tag != "b1" {
		t.Fatalf("jobs %v", jobs)
	}
	f.wait(3 * time.Second)
	v = f.view(t)
	if b1 := viewPathOf(t, v, "b1"); b1.Probing == nil || *b1.Probing != (viewProbing{Reason: "failover", ForMs: 3000}) || !slices.Equal(v.Doing, []string{"probing"}) {
		t.Fatalf("probing %+v, doing %v", b1.Probing, v.Doing)
	}
	if viewPathOf(t, v, "a1").Probing != nil {
		t.Fatal("a1 is not probed")
	}

	// The probe failed: nothing is left, the last outbound is held.
	f.o.e.onProbeResult(jobs[0], probeResult{}, f.now)
	v = f.view(t)
	if v.State != "searching" || v.Outbound.Tag != "a1" || !v.Outbound.IsHeld || viewPathOf(t, v, "b1").Probing != nil || len(v.Doing) != 0 {
		t.Fatalf("held: %+v", v)
	}
	if a1 = viewPathOf(t, v, "a1"); !a1.IsCurrent || a1.IsActive || a1.QuarantineMs <= 0 {
		t.Fatalf("held a1 %+v", a1)
	}

	f.wait(exhaustedHold)
	if v = f.view(t); v.State != "blocked" || !reflect.DeepEqual(v.Outbound, viewOutbound{Tag: "block"}) || currents(v) != nil {
		t.Fatalf("after the hold: %+v", v)
	}

	f.o.NetworkLost()
	if v = f.view(t); v.State != "no_network" || v.Network.IsUp {
		t.Fatalf("network lost: %+v", v)
	}
	f.o.NetworkChanged("wifi")
	if v = f.view(t); v.State != "blocked" || !v.Network.IsUp {
		t.Fatalf("network back: %+v", v)
	}

	// The user keeps trying and the recheck of a resting outbound passes.
	seq := v.Seq
	for range 120 {
		if f.auto.current() != "" {
			break
		}
		f.wait(time.Second)
		f.o.e.noteDemand(f.now)
		jobs, _ = f.o.e.tick(f.now)
		for _, j := range jobs {
			f.o.e.onProbeResult(j, probeResult{ok: true}, f.now)
		}
	}
	v = f.view(t)
	if v.State != "ok" || v.Outbound.Tag != f.auto.current() || v.Outbound.IsHeld || v.Seq <= seq ||
		v.LastSwitch == nil || v.LastSwitch.From != "" || v.LastSwitch.To != v.Outbound.Tag {
		t.Fatalf("recovered: %+v, last switch %+v", v, v.LastSwitch)
	}
}

// A balancer that ran out and has a fallbackBalancer is ok while that one
// has a choice, searching while it holds its last outbound, blocked after.
func TestViewStateThroughFallbackBalancer(t *testing.T) {
	f := newViewFixture(t, &SelectorConfig{FallbackBalancer: "spare"}).connect()
	f.exhaust(f.auto)
	if v := f.view(t); v.State != "searching" || !v.Outbound.IsHeld {
		t.Fatalf("held: %+v", v)
	}
	f.wait(exhaustedHold)
	v := f.view(t)
	if v.State != "ok" || !reflect.DeepEqual(v.Outbound, viewOutbound{Tag: "s1", Protocol: "trojan"}) || !slices.Equal(currents(v), []string{"s1"}) || v.Balancer != "auto" {
		t.Fatalf("fallback balancer: %+v", v)
	}
	f.exhaust(f.spare)
	if v = f.view(t); v.State != "searching" || v.Outbound.Tag != "s1" || !v.Outbound.IsHeld {
		t.Fatalf("fallback balancer holds: %+v", v)
	}
	f.wait(exhaustedHold)
	if v = f.view(t); v.State != "blocked" || v.Outbound.Tag != "direct" {
		t.Fatalf("fallback tag of the fallback balancer: %+v", v)
	}
}

// A pinned balancer does not leave a dead outbound, so nothing is searched.
func TestViewPinnedDownIsBlocked(t *testing.T) {
	f := newViewFixture(t, &SelectorConfig{}).connect()
	if !f.o.Pin("auto", "a1", "") {
		t.Fatal("pin refused")
	}
	f.o.e.markDown(f.o.e.path("a1"), "traffic", f.now)
	if v := f.view(t); v.State != "blocked" || v.Outbound.Tag != "a1" || v.Outbound.IsHeld {
		t.Fatalf("pinned down: %+v", v)
	}
}

// needs and doing name what the engine already knows, in a fixed order.
func TestViewNeedsAndDoing(t *testing.T) {
	f := newViewFixture(t, &SelectorConfig{}).connect()
	p := newProvider(&providerSettings{tag: "fresh"})
	p.setAt = f.now.Add(-time.Minute)
	f.o.e.providers = []*provider{p}
	provider := func() viewProvider { return f.view(t).Providers[0] }
	v := f.view(t)
	if len(v.Needs) != 0 || len(v.Doing) != 0 || v.Needs == nil || v.Doing == nil || provider() != (viewProvider{Provider: "fresh", State: "ok", SetAgeMs: 60000}) {
		t.Fatalf("quiet: needs %v doing %v providers %+v", v.Needs, v.Doing, v.Providers)
	}

	p.fetching = true
	f.auto.returnToCheaperTarget = "a1"
	f.o.e.running[1] = &probeJob{id: 1, tag: "direct", reason: reasonConnectivity}
	if v = f.view(t); v.Providers[0].State != "fetching" || !slices.Equal(v.Doing, []string{"refreshing", "returning", "checking_network"}) {
		t.Fatalf("busy: doing %v providers %+v", v.Doing, v.Providers)
	}
	p.fetching = false
	p.failures, p.backoffUntil = 1, f.now.Add(time.Minute)
	if got := provider().State; got != "waiting" {
		t.Fatalf("backoff: %s", got)
	}
	p.failures, p.retryAfterUntil = 0, f.now.Add(time.Minute)
	if got := provider().State; got != "waiting" {
		t.Fatalf("Retry-After: %s", got)
	}

	p.authFailed, p.baseReported = true, "v2"
	f.o.failed.Store(true)
	if v = f.view(t); !slices.Equal(v.Needs, []string{"restart", "session", "config"}) || v.Providers[0].State != "refused" || v.State != "ok" {
		t.Fatalf("needs %v providers %+v state %s", v.Needs, v.Providers, v.State)
	}
}

// seq stays while only durations change and grows by one with the choice.
func TestViewSeqCountsChanges(t *testing.T) {
	f := newViewFixture(t, &SelectorConfig{}).connect()
	first := f.view(t)
	f.wait(5 * time.Second)
	second := f.view(t)
	if first.Seq != 1 || second.Seq != 1 || first.SinceMs != 0 || second.SinceMs != 5000 {
		t.Fatalf("seq %d then %d, since %d then %d", first.Seq, second.Seq, first.SinceMs, second.SinceMs)
	}
	f.o.e.switchTo(f.auto, "b1", "failover", f.now)
	f.wait(time.Second)
	third := f.view(t)
	if third.Seq != 2 || third.SinceMs != 6000 || third.Outbound.Tag != "b1" || *third.LastSwitch != (viewSwitch{From: "a1", To: "b1", Reason: "failover", AgoMs: 1000}) {
		t.Fatalf("after the switch: seq %d since %d %+v", third.Seq, third.SinceMs, third.LastSwitch)
	}
	if again := f.view(t); again.Seq != 2 {
		t.Fatalf("seq %d without a change", again.Seq)
	}
	f.exhaust(f.auto)
	f.wait(2 * time.Second)
	if held := f.view(t); held.Seq != 3 || held.State != "searching" || held.SinceMs != 0 {
		t.Fatalf("held: seq %d since %d", held.Seq, held.SinceMs)
	}
}

// Every field named *_ms, and none of the others, is left out of what seq
// compares.
func TestViewSeqIgnoresEveryDuration(t *testing.T) {
	var fill func(v reflect.Value)
	fill = func(v reflect.Value) {
		switch v.Kind() {
		case reflect.Pointer:
			v.Set(reflect.New(v.Type().Elem()))
			fill(v.Elem())
		case reflect.Struct:
			for i := range v.NumField() {
				fill(v.Field(i))
			}
		case reflect.Slice:
			v.Set(reflect.MakeSlice(v.Type(), 1, 1))
			fill(v.Index(0))
		case reflect.Map:
			v.Set(reflect.ValueOf(map[string]string{"k": "v"}))
		case reflect.String:
			v.SetString("x")
		case reflect.Bool:
			v.SetBool(true)
		case reflect.Int64:
			v.SetInt(7)
		case reflect.Uint64:
			v.SetUint(7)
		case reflect.Float32:
			v.SetFloat(7)
		default:
			t.Fatalf("no value for %s", v.Type())
		}
	}
	var full ViewData
	fill(reflect.ValueOf(&full).Elem())
	generic := func(v ViewData) (out map[string]any) {
		raw, _ := json.Marshal(v)
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	var compare func(where string, was, now any)
	compare = func(where string, was, now any) {
		switch was := was.(type) {
		case map[string]any:
			for key, value := range was {
				left, _ := now.(map[string]any)
				compare(where+"."+key, value, left[key])
			}
		case []any:
			compare(where+"[]", was[0], now.([]any)[0])
		default:
			timed := strings.HasSuffix(where, "_ms") || where == ".seq"
			if zero := now == nil || now == float64(0); timed != zero || !timed && was != now {
				t.Errorf("%s: %v became %v", where, was, now)
			}
		}
	}
	compare("", generic(full), generic(full.timeless()))
}

// View is no demand of the user: it wakes nothing and learns no outbound.
func TestViewChangesNothing(t *testing.T) {
	f := newViewFixture(t, &SelectorConfig{}).connect()
	f.o.e.lastDemand.Store(1)
	known := len(f.o.e.paths)
	f.view(t)
	if f.o.e.lastDemand.Load() != 1 || len(f.o.kick) != 0 || len(f.o.e.paths) != known || len(f.o.e.queue) != 0 {
		t.Fatalf("demand %d, wakes %d, paths %d of %d, queue %d", f.o.e.lastDemand.Load(), len(f.o.kick), len(f.o.e.paths), known, len(f.o.e.queue))
	}
}

func TestViewWithoutItsBlockIsDisabled(t *testing.T) {
	o := newObserver(context.Background(), &Config{})
	t.Cleanup(func() { o.Close() })
	_, err := o.View()
	var refusal *CommandError
	if !errors.As(err, &refusal) || refusal.Code != "feature_disabled" {
		t.Fatalf("error %v", err)
	}
	if o.FeatureEnabled("view") {
		t.Fatal("view enabled without its block")
	}
}
