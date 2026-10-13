package health_test

import (
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/metrics"
	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
)

func TestV2MetricsActualConstructorRequiresRegisteredExporter(t *testing.T) {
	config := func(t *testing.T) *core.Config {
		t.Helper()
		cfg, err := core.LoadConfig("json", strings.NewReader(`{"metrics":{"tag":"metrics"},"healthObservatory":{"telemetry":{"metrics":{}}}}`))
		if err != nil {
			t.Fatal(err)
		}
		return cfg
	}
	t.Run("present", func(t *testing.T) {
		inst, err := core.New(config(t))
		if err != nil {
			t.Fatal(err)
		}
		defer inst.Close()
		o := health.FromInstance(inst)
		if o == nil || !strings.Contains(string(o.Status()), `"metrics":true`) {
			t.Fatal("metrics-enabled observer missing")
		}
	})
	for _, late := range []bool{false, true} {
		name := "missing"
		if late {
			name = "direct protobuf late app"
		}
		t.Run(name, func(t *testing.T) {
			cfg := config(t)
			kept := cfg.App[:0]
			var exporter *serial.TypedMessage
			for _, app := range cfg.App {
				value, err := app.GetInstance()
				if err != nil {
					t.Fatal(err)
				}
				if _, ok := value.(*metrics.Config); ok {
					exporter = app
					continue
				}
				kept = append(kept, app)
			}
			cfg.App = kept
			if late {
				cfg.App = append(cfg.App, exporter)
			}
			inst, err := core.New(cfg)
			if inst != nil {
				inst.Close()
			}
			if err == nil || !strings.Contains(err.Error(), "healthObservatory.telemetry.metrics requires metrics exporter") {
				t.Fatalf("missing/late exporter error: %v", err)
			}
		})
	}
}
