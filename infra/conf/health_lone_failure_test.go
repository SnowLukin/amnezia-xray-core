package conf_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/infra/conf"
)

func TestHealthLoneFailureJSONPresence(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want int64
	}{
		{`{}`, 0},
		{`{"passive":{}}`, 0},
		{`{"passive":{"loneFailureAfter":"0s"}}`, -1},
		{`{"passive":{"loneFailureAfter":"4m"}}`, int64(4 * time.Minute)},
	} {
		cfg, err := buildHealth(t, tc.raw)
		if err != nil || cfg.GetPassive().GetLoneFailureAfter() != tc.want {
			t.Fatalf("%s: cfg %+v error %v", tc.raw, cfg, err)
		}
	}
	for _, raw := range []string{`{"passive":{"loneFailureAfter":"-1s"}}`, `{"passive":{"loneFailureAfter":12}}`, `{"passive":{"loneFailureAfter":"bad"}}`} {
		var cfg conf.HealthObservatoryConfig
		err := json.Unmarshal([]byte(raw), &cfg)
		if err == nil {
			_, err = cfg.Build()
		}
		if err == nil {
			t.Fatalf("invalid loneFailureAfter accepted: %s", raw)
		}
		if strings.Contains(raw, "-1s") && !strings.Contains(err.Error(), "passive.loneFailureAfter") {
			t.Fatalf("error does not name field: %v", err)
		}
	}
}
