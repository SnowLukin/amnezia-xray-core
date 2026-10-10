package conf

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
)

func TestLeastCostGroupThreshold(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int32
	}{
		{`{}`, 3}, {`{"failedInRowToLeave":1}`, 1}, {`{"failedInRowToLeave":16}`, 16},
		{`{"failedInRowToLeave":0}`, 0}, {`{"failedInRowToLeave":-1}`, 0}, {`{"failedInRowToLeave":17}`, 0},
	} {
		t.Run(tc.value, func(t *testing.T) {
			var cfg strategyLeastCostConfig
			body := fmt.Sprintf(`{"groups":[{"name":"relay","cost":1,%s}]}`, strings.Trim(tc.value, "{}"))
			body = strings.Replace(body, `1,}`, `1}`, 1)
			if err := json.Unmarshal([]byte(body), &cfg); err != nil {
				t.Fatal(err)
			}
			built, err := cfg.Build()
			if tc.want == 0 {
				if err == nil || !strings.Contains(err.Error(), "failedInRowToLeave") {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := built.(*health.SelectorConfig).Groups[0].FailedInRowToLeave; got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

func TestLeastCostRejectsDuplicateGroupNames(t *testing.T) {
	var c Config
	raw := `{"outbounds":[{"tag":"relay-a1","protocol":"freedom","path":{"balancer":"auto","group":"relay"}}],"routing":{"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{"groups":[{"name":"relay","cost":1},{"name":"relay","cost":2}]}}}]},"healthObservatory":{}}`
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Build(); err == nil || !strings.Contains(err.Error(), "duplicate group") {
		t.Fatal(err)
	}
}
