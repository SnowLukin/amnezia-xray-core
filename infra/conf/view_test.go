package conf_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	. "github.com/xtls/xray-core/infra/conf"
)

// viewConfig has the leastCost balancers auto and, with second, udp, the
// balancer plain of another strategy, and host.view as given.
func viewConfig(view string, second bool) string {
	udp, udpPath := "", ""
	if second {
		udp = `,{"tag":"udp","fallbackTag":"block","strategy":{"type":"leastCost"}}`
		udpPath = `,{"tag":"u1","protocol":"freedom","selection":{"balancer":"udp","cost":1,"probe":"check"}}`
	}
	return `{"outbounds":[{"tag":"block","protocol":"blackhole"},{"tag":"a1","protocol":"Freedom","selection":{"balancer":"auto","cost":1,"probe":"check"}}` + udpPath + `],
		"routing":{"balancers":[{"tag":"auto","fallbackTag":"block","strategy":{"type":"leastCost"}},{"tag":"plain","selector":["a"],"strategy":{"type":"random"}}` + udp + `]},
		"healthObservatory":{"probes":{"profiles":[{"name":"check","steps":[{"name":"transport","destinations":["https://probe.test/204"]}]}]},"host":{"view":` + view + `}}}`
}

func builtHealth(t *testing.T, raw string) *health.Config {
	t.Helper()
	var c Config
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatal(err)
	}
	built, err := c.Build()
	if err != nil {
		t.Fatal(err)
	}
	for _, app := range built.App {
		if msg, _ := app.GetInstance(); msg != nil {
			if h, ok := msg.(*health.Config); ok {
				return h
			}
		}
	}
	t.Fatal("no health config")
	return nil
}

// host.view names the user balancer; the only leastCost balancer needs no
// name. Each path carries the protocol of its outbound for the view.
func TestViewBlockNamesTheUserBalancer(t *testing.T) {
	for _, tc := range []struct {
		name, view string
		second     bool
		want       string
	}{
		{"the only balancer", `{}`, false, "auto"},
		{"named", `{"balancer":"udp"}`, true, "udp"},
	} {
		h := builtHealth(t, viewConfig(tc.view, tc.second))
		if h.GetView().GetBalancer() != tc.want {
			t.Errorf("%s: view %v", tc.name, h.GetView())
		}
		if h.Paths[0].Tag != "a1" || h.Paths[0].Protocol != "freedom" {
			t.Errorf("%s: path %v", tc.name, h.Paths[0])
		}
	}
	if h := builtHealth(t, strings.Replace(viewConfig(`{}`, false), `,"host":{"view":{}}`, "", 1)); h.View != nil {
		t.Fatalf("view without its block: %v", h.View)
	}
}

func TestViewBlockIsChecked(t *testing.T) {
	for _, tc := range []struct {
		name, view string
		second     bool
		want       string
	}{
		{"unknown balancer", `{"balancer":"other"}`, false, "healthObservatory.host.view.balancer other must name a leastCost balancer"},
		{"not leastCost", `{"balancer":"plain"}`, false, "healthObservatory.host.view.balancer plain must name a leastCost balancer"},
		{"two balancers and no name", `{}`, true, "healthObservatory.host.view.balancer is required unless the config has exactly one leastCost balancer"},
		{"unknown key", `{"balancers":["auto"]}`, false, "healthObservatory.host.view.balancers: unknown key"},
		{"flag", `{"isEnabled":true}`, false, "healthObservatory.host.view.isEnabled is not supported"},
		{"null", `null`, false, "healthObservatory.host.view must not be null"},
	} {
		var c Config
		err := json.Unmarshal([]byte(viewConfig(tc.view, tc.second)), &c)
		if err == nil {
			_, err = c.Build()
		}
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want %s", tc.name, err, tc.want)
		}
	}
}
