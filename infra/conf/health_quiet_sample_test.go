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
		{`{"telemetry":{"report":{}}}`, 0},
		{`{"telemetry":{"report":{"quietSampleFraction":0}}}`, 0},
		{`{"telemetry":{"report":{"quietSampleFraction":0.05}}}`, 0.05},
		{`{"telemetry":{"report":{"quietSampleFraction":1}}}`, 1},
	} {
		cfg, err := buildHealth(t, tc.raw)
		if err != nil || cfg.GetReport().GetQuietShare() != tc.want {
			t.Fatalf("%s: cfg %+v error %v", tc.raw, cfg, err)
		}
	}
	for _, raw := range []string{`{"telemetry":{"report":{"quietSampleFraction":-0.1}}}`, `{"telemetry":{"report":{"quietSampleFraction":1.1}}}`} {
		_, err := buildHealth(t, raw)
		if err == nil || !strings.Contains(err.Error(), "report.quietSampleFraction") {
			t.Fatalf("%s: error %v", raw, err)
		}
	}
}

func TestHealthQuietShareBuildRejectsNonFinite(t *testing.T) {
	for _, share := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		cfg := HealthObservatoryConfig{Telemetry: HealthTelemetryConfig{Report: HealthReportConfig{QuietSampleFraction: share}}}
		if _, err := cfg.Build(); err == nil || !strings.Contains(err.Error(), "report.quietSampleFraction") {
			t.Fatalf("quietShare %v: error %v", share, err)
		}
	}
}
