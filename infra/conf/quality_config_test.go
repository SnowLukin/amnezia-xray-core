package conf_test

import (
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"google.golang.org/protobuf/proto"
)

func TestHealthQualityFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want bool
	}{
		{"default", `{}`, false},
		{"empty_quality", `{"telemetry":{"quality":{}}}`, true},

		{"enabled", `{"telemetry":{"quality":{}}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := buildHealth(t, tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.GetFeatures().GetQuality(); got != tc.want {
				t.Fatalf("quality.isEnabled %v, want %v", got, tc.want)
			}
			wire, err := proto.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			var restored health.Config
			if err := proto.Unmarshal(wire, &restored); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(cfg, &restored) || restored.GetFeatures().GetQuality() != tc.want {
				t.Fatal("protobuf round trip changed the quality flag")
			}
		})
	}
}
