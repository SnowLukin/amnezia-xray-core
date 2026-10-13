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
		{`{"host":{"userPresent":{}}}`, 0, 0},
		{`{"host":{"userPresent":{"isEnabled":false}}}`, 0, 0},
		{`{"host":{"userPresent":{"isEnabled":true,"freshness":"2m","minInterval":"7m"}}}`, int64(2 * time.Minute), int64(7 * time.Minute)},
		{`{"host":{"userPresent":{"freshness":"60s"}}}`, int64(time.Minute), 0},
		{`{"host":{"userPresent":{"minInterval":"3h"}}}`, 0, int64(3 * time.Hour)},
	} {
		cfg, err := buildHealth(t, tc.raw)
		if err != nil || cfg.GetWake().GetFreshness() != tc.freshness || cfg.GetWake().GetMinInterval() != tc.interval {
			t.Fatalf("%s: cfg %+v error %v", tc.raw, cfg, err)
		}
	}
	for _, key := range []string{"freshness", "minInterval"} {
		for _, value := range []string{`"-1s"`, `"0s"`, `12`, `"bad"`} {
			raw := `{"host":{"userPresent":{"` + key + `":` + value + `}}}`
			var cfg conf.HealthObservatoryConfig
			err := json.Unmarshal([]byte(raw), &cfg)
			if err == nil {
				_, err = cfg.Build()
			}
			if err == nil {
				t.Fatalf("invalid wake duration accepted: %s", raw)
			}
			if value == `"-1s"` && !strings.Contains(err.Error(), "host.userPresent."+key) {
				t.Fatalf("error does not name field: %v", err)
			}
		}
	}
}
