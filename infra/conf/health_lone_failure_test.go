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
		{`{"evidence":{"loneFailureProbe":{}}}`, 0},
		{`{"evidence":{}}`, 0},
		{`{"evidence":{"loneFailureProbe":{"after":"4m"}}}`, int64(4 * time.Minute)},
	} {
		cfg, err := buildHealth(t, tc.raw)
		if err != nil || cfg.GetPassive().GetLoneFailureAfter() != tc.want {
			t.Fatalf("%s: cfg %+v error %v", tc.raw, cfg, err)
		}
	}
	for _, raw := range []string{`{"evidence":{"loneFailureProbe":{"after":"-1s"}}}`, `{"evidence":{"loneFailureProbe":{"after":12}}}`, `{"evidence":{"loneFailureProbe":{"after":"bad"}}}`} {
		var cfg conf.HealthObservatoryConfig
		err := json.Unmarshal([]byte(raw), &cfg)
		if err == nil {
			_, err = cfg.Build()
		}
		if err == nil {
			t.Fatalf("invalid loneFailureAfter accepted: %s", raw)
		}
		if strings.Contains(raw, "-1s") && !strings.Contains(err.Error(), "evidence.loneFailureProbe.after") {
			t.Fatalf("error does not name field: %v", err)
		}
	}
}
