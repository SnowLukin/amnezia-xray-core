package conf_test

import (
	"math"
	"strings"
	"testing"

	. "github.com/xtls/xray-core/infra/conf"
)

func TestHealthQuietShareJSON(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want float64
	}{
		{`{}`, 0},
		{`{"report":{}}`, 0},
		{`{"report":{"quietShare":0}}`, 0},
		{`{"report":{"quietShare":0.05}}`, 0.05},
		{`{"report":{"quietShare":1}}`, 1},
	} {
		cfg, err := buildHealth(t, tc.raw)
		if err != nil || cfg.GetReport().GetQuietShare() != tc.want {
			t.Fatalf("%s: cfg %+v error %v", tc.raw, cfg, err)
		}
	}
	for _, raw := range []string{`{"report":{"quietShare":-0.1}}`, `{"report":{"quietShare":1.1}}`} {
		_, err := buildHealth(t, raw)
		if err == nil || !strings.Contains(err.Error(), "report.quietShare") {
			t.Fatalf("%s: error %v", raw, err)
		}
	}
}

func TestHealthQuietShareBuildRejectsNonFinite(t *testing.T) {
	for _, share := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		cfg := HealthObservatoryConfig{Report: &HealthReportConfig{QuietShare: share}}
		if _, err := cfg.Build(); err == nil || !strings.Contains(err.Error(), "report.quietShare") {
			t.Fatalf("quietShare %v: error %v", share, err)
		}
	}
}
