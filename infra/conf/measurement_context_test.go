package conf_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	. "github.com/xtls/xray-core/infra/conf"
)

func TestMeasurementConfigLabels(t *testing.T) {
	raw := `{"log":{"loglevel":"none"},"outbounds":[{"tag":"a1","protocol":"freedom","path":{"balancer":"auto","group":"g","labels":{"node":"n1","entry":"own"}}}],"routing":{"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{"groups":[{"name":"g","cost":1,"labels":{"location":"NL","entry":"shared"}}]}}}]},"healthObservatory":{"pathLabels":{"isEnabled":true},"network":{"labelRequest":{"isEnabled":true,"outbound":"a1","destination":"http://127.0.0.1/whoami","labels":{"asn":"X-Network-Asn"}}},"report":{"labelContext":{"isEnabled":true},"labels":{"cohort":"a"}}}}`
	var cfg Config
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		t.Fatal(err)
	}
	built, err := cfg.Build()
	if err != nil {
		t.Fatal(err)
	}
	var h *health.Config
	for _, app := range built.App {
		msg, err := app.GetInstance()
		if err == nil {
			if v, ok := msg.(*health.Config); ok {
				h = v
			}
		}
	}
	if h == nil || h.Paths[0].Labels["location"] != "NL" || h.Paths[0].Labels["entry"] != "own" || h.Report.Labels["cohort"] != "a" || h.Network.LabelRequest.Labels["asn"] != "X-Network-Asn" {
		t.Fatalf("lost labels: %+v", h)
	}
	for _, tc := range []struct{ old, new string }{
		{`"node":"n1"`, `"node":"bad value"`}, {`"cohort":"a"`, `"COHORT":"a"`},
		{`"X-Network-Asn"`, `"bad header"`}, {`"http://127.0.0.1/whoami"`, `"ftp://host/a"`},
		{`"destination":"http://127.0.0.1/whoami"`, `"destination":"http://127.0.0.1/whoami","timeout":"0s"`},
		{`"destination":"http://127.0.0.1/whoami"`, `"destination":"http://127.0.0.1/whoami","requestsPerHour":0`},
		{`"labels":{"cohort":"a"}`, `"labels":{"k0":"v","k1":"v","k2":"v","k3":"v","k4":"v","k5":"v","k6":"v","k7":"v","k8":"v"}`},
	} {
		t.Run(fmt.Sprint(tc.new), func(t *testing.T) {
			var c Config
			err := json.Unmarshal([]byte(strings.ReplaceAll(raw, tc.old, tc.new)), &c)
			if err == nil {
				_, err = c.Build()
			}
			if err == nil {
				t.Fatal("invalid label config accepted")
			}
		})
	}
}
