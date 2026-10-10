package conf

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
)

func TestLeastCostReturnIntervalPresence(t *testing.T) {
	var cfg strategyLeastCostConfig
	if err := json.Unmarshal([]byte(`{"costs":[{"match":"a","cost":1},{"match":"b","cost":3,"returnInterval":"0s"},{"match":"c","cost":5,"returnInterval":"90s"}]}`), &cfg); err != nil {
		t.Fatal(err)
	}
	message, err := cfg.Build()
	if err != nil {
		t.Fatal(err)
	}
	entries := message.(*health.SelectorConfig).GetCosts()
	for i, want := range []int64{0, -1, int64(90 * time.Second)} {
		if got := entries[i].GetReturnInterval(); got != want {
			t.Errorf("entry %d return interval = %d, want %d", i, got, want)
		}
	}
}

func TestLeastCostRejectsNegativeReturnInterval(t *testing.T) {
	var cfg strategyLeastCostConfig
	if err := json.Unmarshal([]byte(`{"costs":[{"match":"a","cost":1,"returnInterval":"-1s"}]}`), &cfg); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.Build(); err == nil || !strings.Contains(err.Error(), "returnInterval") {
		t.Fatalf("negative returnInterval error = %v", err)
	}
}
