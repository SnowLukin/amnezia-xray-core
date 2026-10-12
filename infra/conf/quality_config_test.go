package conf_test

import (
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"google.golang.org/protobuf/proto"
)

func TestHealthQualitySkipFlag(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  string
		want bool
	}{
		{"default", `{}`, false},
		{"empty_passive", `{"passive":{}}`, false},
		{"explicit_false", `{"passive":{"shouldSkipQuality":false}}`, false},
		{"enabled", `{"passive":{"shouldSkipQuality":true}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := buildHealth(t, tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			if got := cfg.GetPassive().GetShouldSkipQuality(); got != tc.want {
				t.Fatalf("shouldSkipQuality %v, want %v", got, tc.want)
			}
			wire, err := proto.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			var restored health.Config
			if err := proto.Unmarshal(wire, &restored); err != nil {
				t.Fatal(err)
			}
			if !proto.Equal(cfg, &restored) || restored.GetPassive().GetShouldSkipQuality() != tc.want {
				t.Fatal("protobuf round trip changed the quality flag")
			}
		})
	}
}

func TestHealthQualitySkipFlagWireField(t *testing.T) {
	field := (&health.PassiveConfig{}).ProtoReflect().Descriptor().Fields().ByName("should_skip_quality")
	if field == nil || field.Number() != 32 || field.JSONName() != "shouldSkipQuality" {
		t.Fatalf("quality flag protobuf contract changed: %v", field)
	}
}
