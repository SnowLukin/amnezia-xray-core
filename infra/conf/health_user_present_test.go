package conf_test

import (
	"encoding/json"

	"github.com/xtls/xray-core/infra/conf"
	"strings"
	"testing"
	"time"
)

func TestHealthWakeJSONPresence(t *testing.T) {
	for _, tc := range []struct {
		raw                 string
		freshness, interval int64
	}{
		{`{}`, 0, 0},
		{`{"wake":{}}`, 0, 0},
		{`{"wake":{"freshness":"0s","minInterval":"0s"}}`, -1, -1},
		{`{"wake":{"freshness":"2m","minInterval":"7m"}}`, int64(2 * time.Minute), int64(7 * time.Minute)},
		{`{"wake":{"freshness":"0s"}}`, -1, 0},
		{`{"wake":{"minInterval":"0s"}}`, 0, -1},
	} {
		cfg, err := buildHealth(t, tc.raw)
		if err != nil || cfg.GetWake().GetFreshness() != tc.freshness || cfg.GetWake().GetMinInterval() != tc.interval {
			t.Fatalf("%s: cfg %+v error %v", tc.raw, cfg, err)
		}
	}
	for _, key := range []string{"freshness", "minInterval"} {
		for _, value := range []string{`"-1s"`, `12`, `"bad"`} {
			raw := `{"wake":{"` + key + `":` + value + `}}`
			var cfg conf.HealthObservatoryConfig
			err := json.Unmarshal([]byte(raw), &cfg)
			if err == nil {
				_, err = cfg.Build()
			}
			if err == nil {
				t.Fatalf("invalid wake duration accepted: %s", raw)
			}
			if value == `"-1s"` && !strings.Contains(err.Error(), "wake."+key) {
				t.Fatalf("error does not name field: %v", err)
			}
		}
	}
}
