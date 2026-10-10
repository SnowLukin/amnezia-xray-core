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
	}{{"{}", 3}, {`{"failedInRowToLeave":1}`, 1}, {`{"failedInRowToLeave":16}`, 16}, {`{"failedInRowToLeave":0}`, 0}, {`{"failedInRowToLeave":-1}`, 0}, {`{"failedInRowToLeave":17}`, 0}} {
		t.Run(tc.value, func(t *testing.T) {
			var cfg strategyLeastCostConfig
			if err := json.Unmarshal([]byte(fmt.Sprintf(`{"costs":[{"match":"a","cost":1,"group":%s}]}`, tc.value)), &cfg); err != nil {
				t.Fatal(err)
			}
			c, err := cfg.Build()
			if tc.want == 0 {
				if err == nil || !strings.Contains(err.Error(), "failedInRowToLeave") {
					t.Fatal(err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := c.(*health.SelectorConfig).Costs[0].Group.FailedInRowToLeave; got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

func TestLeastCostRejectsAmbiguousGroupNames(t *testing.T) {
	for _, body := range []string{
		`{"costs":[{"match":"a.","cost":1,"group":{}},{"match":"a.","isRegexp":true,"cost":2,"group":{}}]}`,
		`{"costs":[{"match":"a","cost":1},{"match":"a","cost":2,"group":{}}]}`,
	} {
		var cfg strategyLeastCostConfig
		if err := json.Unmarshal([]byte(body), &cfg); err != nil {
			t.Fatal(err)
		}
		if _, err := cfg.Build(); err == nil || !strings.Contains(err.Error(), "costs.group match must be unique") {
			t.Fatal(err)
		}
	}
}
