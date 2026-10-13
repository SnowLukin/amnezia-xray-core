package router

import (
	"strings"
	"testing"

	"github.com/xtls/xray-core/features/outbound"
)

type fallbackTestManager struct{ outbound.Manager }

func (*fallbackTestManager) Select(tags []string) []string { return tags }

type fallbackTestStrategy struct {
	selected, target string
	starts           int
}

func (s *fallbackTestStrategy) PickOutbound([]string) string { return s.selected }
func (s *fallbackTestStrategy) FallbackBalancer() string     { return s.target }
func (s *fallbackTestStrategy) FallbackSelected(string)      { s.starts++ }

func fallbackTestBalancer(selected, target, last string) (*Balancer, *fallbackTestStrategy) {
	s := &fallbackTestStrategy{selected: selected, target: target}
	return &Balancer{strategy: s, fallbackBalancer: target, fallbackTag: last, ohm: &fallbackTestManager{}}, s
}

func TestBalancerFallbackUsesTargetStrategyAndOnlyAfterExhaustion(t *testing.T) {
	source, sourceStrategy := fallbackTestBalancer("own", "main", "source-block")
	target := &Balancer{selectors: []string{"main-one", "main-two"}, strategy: &RoundRobinStrategy{}, ohm: &fallbackTestManager{}}
	if err := bindBalancerFallbacks(map[string]*Balancer{"ai": source, "main": target}); err != nil {
		t.Fatal(err)
	}
	if tag, err := source.PickOutbound(); err != nil || tag != "own" || sourceStrategy.starts != 0 {
		t.Fatalf("own choice %s %v", tag, err)
	}
	sourceStrategy.selected = ""
	for _, want := range []string{"main-one", "main-two", "main-one"} {
		if tag, err := source.PickOutbound(); err != nil || tag != want {
			t.Fatalf("target strategy %s %v want=%s", tag, err, want)
		}
	}
	if sourceStrategy.starts != 3 {
		t.Fatal("target selection was not confirmed", sourceStrategy.starts)
	}
	sourceStrategy.selected = "recovered"
	if tag, err := source.PickOutbound(); err != nil || tag != "recovered" {
		t.Fatalf("recovery %s %v", tag, err)
	}
	source.override.Put("pinned")
	sourceStrategy.selected = ""
	if tag, err := source.PickOutbound(); err != nil || tag != "pinned" || sourceStrategy.starts != 3 {
		t.Fatalf("pin delegated %s %v", tag, err)
	}
}

func TestBalancerFallbackUsesTerminalFallbackTag(t *testing.T) {
	ai, a := fallbackTestBalancer("", "main", "wrong-ai-block")
	main, m := fallbackTestBalancer("", "last", "wrong-main-block")
	last, _ := fallbackTestBalancer("", "", "terminal-block")
	if err := bindBalancerFallbacks(map[string]*Balancer{"ai": ai, "main": main, "last": last}); err != nil {
		t.Fatal(err)
	}
	if tag, err := ai.PickOutbound(); err != nil || tag != "terminal-block" || a.starts != 1 || m.starts != 1 {
		t.Fatalf("chain %s %v starts=%d/%d", tag, err, a.starts, m.starts)
	}
}

func TestBalancerFallbackMapStaysImmutableAcrossGenerations(t *testing.T) {
	source, _ := fallbackTestBalancer("", "main", "block")
	oldTarget, _ := fallbackTestBalancer("old", "", "block")
	generation := map[string]*Balancer{"ai": source, "main": oldTarget}
	if err := bindBalancerFallbacks(generation); err != nil {
		t.Fatal(err)
	}
	newTarget, _ := fallbackTestBalancer("new", "", "block")
	generation["main"] = newTarget
	if tag, err := source.PickOutbound(); err != nil || tag != "old" {
		t.Fatalf("generation changed %s %v", tag, err)
	}
}

func TestBalancerFallbackRejectsInvalidRuntimeGraphs(t *testing.T) {
	for _, tc := range []struct {
		links []string
		want  string
	}{
		{[]string{"missing"}, "unknown"},
		{[]string{"a"}, "cycle"},
		{[]string{"b", "a"}, "cycle"},
		{[]string{"b", "c", "d", "e", ""}, "exceeds 4"},
	} {
		balancers := map[string]*Balancer{}
		for i, target := range tc.links {
			balancers[string(rune('a'+i))], _ = fallbackTestBalancer("", target, "block")
		}
		if err := bindBalancerFallbacks(balancers); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("links=%v error=%v want=%s", tc.links, err, tc.want)
		}
	}
	balancers := map[string]*Balancer{}
	for i, target := range []string{"b", "c", "d", ""} {
		balancers[string(rune('a'+i))], _ = fallbackTestBalancer("", target, "block")
	}
	if err := bindBalancerFallbacks(balancers); err != nil {
		t.Fatal(err)
	}
}
