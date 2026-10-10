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
	}{
		{``, 0}, {`,"returnInterval":"0s"`, -1}, {`,"returnInterval":"90s"`, int64(90 * time.Second)},
	} {
		var c Config
		raw := `{"outbounds":[{"tag":"a","protocol":"freedom","path":{"balancer":"auto","cost":1` + tc.value + `}}],"routing":{"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{}}}]},"healthObservatory":{}}`
		if err := json.Unmarshal([]byte(raw), &c); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Build(); err != nil {
			t.Fatal(err)
		}
		if got := c.HealthObservatory.Paths[0].ReturnInterval; got != tc.want {
			t.Errorf("interval %d, want %d", got, tc.want)
		}
	}
}

func TestLeastCostRejectsNegativeReturnInterval(t *testing.T) {
	var c Config
	raw := `{"outbounds":[{"tag":"a","protocol":"freedom","path":{"balancer":"auto","cost":1,"returnInterval":"-1s"}}],"routing":{"balancers":[{"tag":"auto","strategy":{"type":"leastCost","settings":{}}}]},"healthObservatory":{}}`
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Build(); err == nil || !strings.Contains(err.Error(), "returnInterval") {
		t.Fatal(err)
	}
}
