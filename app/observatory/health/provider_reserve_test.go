package health

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"
)

func reserveProof(e *engine, tag string, at time.Time) {
	p := e.pathAt(tag, at)
	p.candidateVerifiedAt, p.candidateEpoch = at, e.candidateEpoch
	p.candidateChecked = true
	p.lastProbeResult = probeResult{ok: true, reason: "candidate"}
	p.status = statusUp
}

func TestProbeOnlyRoadsOrderBoundAndUserIsolation(t *testing.T) {
	paths := []*PathDefinition{{Tag: "p-user", Cost: 1}}
	for _, tag := range []string{"p-a", "p-b", "p-c", "p-d"} {
		paths = append(paths, &PathDefinition{Tag: tag, Cost: 2, IsProbeOnly: true})
	}
	te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: paths}, "p-user", "p-a", "p-b", "p-c", "p-d")
	cfg := te.prov().cfg
	for i, tag := range []string{"p-a", "p-b", "p-c", "p-d"} {
		reserveProof(te.engine, tag, at(time.Duration(i+1)*time.Second))
	}
	te.sel.setActive("")
	te.sel.exhausted = true
	if _, skip := te.resolveProviderVia(cfg, "auto", at(5*time.Second)); skip != "balancer exhausted" {
		t.Fatal(skip)
	}
	cfg.probeOnlyRoads = true
	roads, skip := te.resolveProviderVia(cfg, "auto", at(5*time.Second))
	if skip != "" || !slices.Equal(roads, []string{"p-d", "p-c"}) {
		t.Fatal(roads, skip)
	}
	te.paths["p-d"].candidateEpoch++
	te.retiring["p-c"] = true
	roads, skip = te.resolveProviderVia(cfg, "auto", at(5*time.Second))
	if skip != "" || !slices.Equal(roads, []string{"p-b", "p-a"}) {
		t.Fatal(roads, skip)
	}
	te.sel.setActive("p-user")
	roads, skip = te.resolveProviderVia(cfg, "auto", at(5*time.Second))
	if skip != "" || !slices.Equal(roads, []string{"p-user"}) {
		t.Fatal(roads, skip)
	}
	if _, skip = te.resolveProviderVia(cfg, "p-b", at(5*time.Second)); skip != "probe-only outbound" {
		t.Fatal(skip)
	}
	te.paths["p-user"].status = statusDown
	te.paths["p-user"].quarantineUntil = at(time.Hour)
	te.sel.setActive("")
	te.updateCandidates(te.sel, []string{"p-user", "p-a", "p-b", "p-c", "p-d"}, at(5*time.Second))
	if got := te.sel.current(); got != "" {
		t.Fatalf("user selected probe-only %s", got)
	}
}

func reserveAnswer(t *testing.T, source string, by string) *providerAnswer {
	t.Helper()
	raw := fmt.Sprintf(`{"outbounds":[{"tag":"slot","protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":1,"provenBy":%s}}%s]}`, by, source)
	a, err := parseAnswer([]byte(raw))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

const reserveProbe = `,{"tag":"probe","protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":2,"isProbeOnly":true}}`

func TestProvenByApplyFreshOldNetworkAndFailure(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		age                        time.Duration
		changeNetwork, resetSource bool
		want                       bool
	}{
		{name: "fresh", age: time.Second, want: true}, {name: "boundary", age: 60 * time.Second, want: true},
		{name: "old", age: 60*time.Second + time.Nanosecond}, {name: "other network", age: time.Second, changeNetwork: true},
		{name: "replaced proof", age: time.Second, resetSource: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newApplyFixture(t, time.Minute)
			f.o.clock = func() time.Time { return at(2 * time.Minute) }
			initial, _ := parseAnswer([]byte(`{"outbounds":[{"tag":"probe","protocol":"vless","v":0,"selection":{"balancer":"auto","probe":"check","cost":2,"isProbeOnly":true}}]}`))
			if _, err := f.apply(t, initial); err != nil {
				t.Fatal(err)
			}
			reserveProof(f.o.e, "p-probe", f.o.now().Add(-tc.age))
			if tc.changeNetwork {
				f.o.e.onNetworkChange("other", f.o.now())
			}
			source := reserveProbe
			if tc.resetSource {
				source = `,{"tag":"probe","protocol":"vless","v":1,"selection":{"balancer":"auto","probe":"check","cost":2,"isProbeOnly":true}}`
			}
			if _, err := f.apply(t, reserveAnswer(t, source, `"probe"`)); err != nil {
				t.Fatal(err)
			}
			p := f.o.e.paths["p-slot"]
			if (p.status == statusUp) != tc.want {
				t.Fatalf("state %v want up=%v", p.status, tc.want)
			}
			if tc.want {
				if p.reason != "proven_by" || !p.lastProbe.IsZero() {
					t.Fatal("new slot probed or wrong reason")
				}
				sel := f.o.e.selectors["auto"]
				f.o.e.updateCandidates(sel, []string{"p-slot", "p-probe"}, f.o.now())
				if sel.current() != "p-slot" {
					t.Fatalf("not selected immediately: %s", sel.current())
				}
				f.o.e.onFailure("p-slot", failDial, "t.test", 1, f.o.now().Add(time.Second))
				if len(p.failures) == 0 && p.status == statusUp {
					t.Fatal("user failure ignored")
				}
			}
		})
	}
}

func TestProvenByRejectsBadReferencesBeforeMutation(t *testing.T) {
	for _, by := range []string{`"missing"`, `"real"`, `"p-probe"`, `""`, `null`, `12`, `{}`} {
		t.Run(by, func(t *testing.T) {
			f := newApplyFixture(t, time.Minute)
			initial, _ := parseAnswer([]byte(`{"outbounds":[{"tag":"real","protocol":"vless","selection":{"balancer":"auto","probe":"check","cost":1}}` + reserveProbe + `]}`))
			if _, err := f.apply(t, initial); err != nil {
				t.Fatal(err)
			}
			before := f.m.GetHandler("p-real")
			if _, err := f.apply(t, reserveAnswer(t, reserveProbe, by)); err == nil {
				t.Fatal("bad reference accepted")
			}
			if f.m.GetHandler("p-real") != before || f.m.GetHandler("p-slot") != nil {
				t.Fatal("rejected response mutated handlers")
			}
		})
	}
	// Metadata is provider-only; it is removed before loading an outbound.
	raw := json.RawMessage(`[{"tag":"slot","protocol":"freedom","selection":{"provenBy":"probe"}}]`)
	if _, err := loadOutbounds(raw); err != nil {
		t.Fatal(err)
	}
}
