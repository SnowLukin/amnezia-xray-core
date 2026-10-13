package conf

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestLeastCostReturnIntervalPresence(t *testing.T) {
	for _, tc := range []struct {
		value string
		want  int64
		flag  *bool
	}{{``, 0, nil}, {`,"returnToCheaper":{"isEnabled":false}`, 0, boolPtr(false)}, {`,"returnToCheaper":{"interval":"90s"}`, int64(90 * time.Second), boolPtr(true)}} {
		var c Config
		raw := pathConfig(`,"selection":{"balancer":"auto","cost":1`+tc.value+`}`, `{"returnToCheaper":{}}`, `{}`)
		if err := json.Unmarshal([]byte(raw), &c); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Build(); err != nil {
			t.Fatal(err)
		}
		p := c.HealthObservatory.Paths[0]
		if p.ReturnInterval != tc.want || (p.ReturnIsEnabled == nil) != (tc.flag == nil) || (tc.flag != nil && *p.ReturnIsEnabled != *tc.flag) {
			t.Fatalf("presence %v", p)
		}
	}
}
func boolPtr(v bool) *bool { return &v }
func TestLeastCostRejectsNonPositiveReturnInterval(t *testing.T) {
	for _, value := range []string{"-1s", "0s"} {
		raw := pathConfig(`,"selection":{"balancer":"auto","cost":1,"returnToCheaper":{"isEnabled":false,"interval":"`+value+`"}}`, `{}`, `{}`)
		if err := pathConfigError(raw); err == nil || !strings.Contains(err.Error(), "returnToCheaper.interval") {
			t.Fatal(err)
		}
	}
}
