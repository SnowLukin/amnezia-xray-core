package conf_test

import (
	"encoding/json"
	"sort"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"google.golang.org/protobuf/proto"
)

func fallbackBalancerConfig(edges map[string]string, stock string) string {
	tags := make([]string, 0, len(edges))
	for tag := range edges {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	outbounds := []any{map[string]any{"tag": "block", "protocol": "blackhole"}}
	balancers := []any{}
	for _, tag := range tags {
		outbounds = append(outbounds, map[string]any{"tag": "path-" + tag, "protocol": "freedom", "selection": map[string]any{"balancer": tag, "cost": 1, "probe": "check"}})
		settings := map[string]any{}
		if edges[tag] != "" {
			settings["fallbackBalancer"] = edges[tag]
		}
		balancers = append(balancers, map[string]any{"tag": tag, "fallbackTag": "block", "strategy": map[string]any{"type": "leastCost", "settings": settings}})
	}
	if stock != "" {
		outbounds = append(outbounds, map[string]any{"tag": "stock-path", "protocol": "freedom"})
		balancers = append(balancers, map[string]any{"tag": stock, "selector": []string{"stock-path"}, "strategy": map[string]any{"type": "random"}})
	}
	body, _ := json.Marshal(map[string]any{"outbounds": outbounds, "routing": map[string]any{"balancers": balancers}, "healthObservatory": map[string]any{"probes": map[string]any{"profiles": []any{map[string]any{"name": "check", "steps": []any{map[string]any{"name": "check", "destinations": []string{"https://example.test/204"}}}}}}}})
	return string(body)
}

func TestFallbackBalancerConfigValidation(t *testing.T) {
	for _, tc := range []struct {
		name, stock, want string
		edges             map[string]string
	}{
		{name: "absent compatibility", edges: map[string]string{"ai": ""}},
		{name: "leastCost target", edges: map[string]string{"ai": "main", "main": ""}},
		{name: "stock target", stock: "stock", edges: map[string]string{"ai": "stock"}},
		{name: "four balancers", edges: map[string]string{"a": "b", "b": "c", "c": "d", "d": ""}},
		{name: "unknown target", edges: map[string]string{"ai": "missing"}, want: "unknown balancer missing; chain ai -> missing"},
		{name: "self target", edges: map[string]string{"ai": "ai"}, want: "cannot fall back to itself; chain ai -> ai"},
		{name: "cycle", edges: map[string]string{"ai": "main", "main": "ai"}, want: "cycle"},
		{name: "five balancers", edges: map[string]string{"a": "b", "b": "c", "c": "d", "d": "e", "e": ""}, want: "at most 4 balancers: a -> b -> c -> d -> e"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := loadConfig(fallbackBalancerConfig(tc.edges, tc.stock))
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), "routing.balancers[") || !strings.Contains(err.Error(), "].strategy.settings.fallbackBalancer") {
				t.Fatalf("error %v must contain %q and the full config path", err, tc.want)
			}
			if tc.name == "cycle" && !(strings.Contains(err.Error(), "ai -> main -> ai") || strings.Contains(err.Error(), "main -> ai -> main")) {
				t.Fatalf("cycle chain omitted: %v", err)
			}
		})
	}
}

func TestFallbackBalancerStrictShape(t *testing.T) {
	base := fallbackBalancerConfig(map[string]string{"ai": "main", "main": ""}, "")
	for _, value := range []string{`""`, `null`, `true`, `1`, `{}`, `[]`} {
		if err := loadConfig(strings.Replace(base, `"fallbackBalancer":"main"`, `"fallbackBalancer":`+value, 1)); err == nil || !strings.Contains(err.Error(), "fallbackBalancer") {
			t.Fatalf("invalid fallback tag accepted: %s: %v", value, err)
		}
	}
	if err := loadConfig(strings.Replace(base, `"fallbackTag":"block",`, "", 1)); err == nil || !strings.Contains(err.Error(), "explicit fallbackTag") {
		t.Fatal("fallbackBalancer bypassed required final outbound", err)
	}
}

func TestFallbackBalancerProtobufContract(t *testing.T) {
	field := (&health.SelectorConfig{}).ProtoReflect().Descriptor().Fields().ByName("fallback_balancer")
	if field == nil || field.Number() != 15 || field.JSONName() != "fallbackBalancer" {
		t.Fatalf("unexpected wire contract: %v", field)
	}
	raw, err := proto.Marshal(&health.SelectorConfig{FallbackBalancer: "main"})
	if err != nil {
		t.Fatal(err)
	}
	var restored health.SelectorConfig
	if err := proto.Unmarshal(raw, &restored); err != nil || restored.GetFallbackBalancer() != "main" {
		t.Fatalf("lost fallback target: %v, %v", restored, err)
	}
}
