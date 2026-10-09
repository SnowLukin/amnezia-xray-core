package health_test

import (
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
)

// Every handler of the outbound manager has Sleep; the proxy behind it tells
// which outbounds the observer puts to sleep.
func TestWireGuardOutboundsCanSleep(t *testing.T) {
	const key = "0000000000000000000000000000000000000000000000000000000000000001"
	cfg, err := core.LoadConfig("json", strings.NewReader(`{"log": {"loglevel": "none"},
		"outbounds": [{"tag": "a1", "protocol": "freedom"},
		  {"tag": "w1", "protocol": "wireguard", "settings": {"secretKey": "`+key+`", "address": ["10.0.0.2/32"],
		    "noKernelTun": true, "peers": [{"publicKey": "`+key+`", "endpoint": "127.0.0.1:9"}]}}],
		"healthObservatory": {"subjectSelector": ["a", "w"]}}`))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := core.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	defer inst.Close()
	ohm := inst.GetFeature(outbound.ManagerType()).(outbound.Manager)
	for tag, want := range map[string]bool{"a1": false, "w1": true} {
		if got := health.CanSleep(ohm.GetHandler(tag)); got != want {
			t.Errorf("%s can sleep: %v, want %v", tag, got, want)
		}
	}
	// Never used, the WireGuard outbound holds nothing to keep it awake.
	if !ohm.GetHandler("w1").(outbound.Sleeper).Sleep() {
		t.Fatal("an unused WireGuard outbound refused to sleep")
	}
}
