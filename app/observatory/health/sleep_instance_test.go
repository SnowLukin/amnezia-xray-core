package health_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

// wireGuardCore runs a core with a WireGuard outbound w1 whose peer is not
// there, which a connection never reaches.
func wireGuardCore(t *testing.T) *core.Instance {
	t.Helper()
	const key = "0000000000000000000000000000000000000000000000000000000000000001"
	cfg, err := core.LoadConfig("json", strings.NewReader(`{"log": {"loglevel": "none"},
		"outbounds": [{"tag": "a1", "protocol": "freedom"},
		  {"tag": "w1", "protocol": "wireguard", "settings": {"secretKey": "`+key+`", "address": ["10.0.0.2/32"],
		    "noKernelTun": true, "peers": [{"publicKey": "`+key+`", "endpoint": "127.0.0.1:9"}]}}],
		"healthObservatory": {}}`))
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
	t.Cleanup(func() { inst.Close() })
	return inst
}

// Every handler of the outbound manager has Sleep; the proxy behind it tells
// which outbounds the observer puts to sleep.
func TestWireGuardOutboundsCanSleep(t *testing.T) {
	inst := wireGuardCore(t)
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

// A direct connection does not make an outbound without path observable.
func TestWireGuardWithoutPathStaysOutsideTheObserver(t *testing.T) {
	inst := wireGuardCore(t)
	obs := health.FromInstance(inst)
	seen := func() bool {
		var st struct {
			Paths []struct {
				Outbound string `json:"outbound"`
			} `json:"paths"`
		}
		if err := json.Unmarshal(obs.Status(), &st); err != nil {
			t.Fatal(err)
		}
		for _, p := range st.Paths {
			if p.Outbound == "w1" {
				return true
			}
		}
		return false
	}
	if seen() {
		t.Fatal("setup: w1 known before any connection")
	}
	// The proxy refuses a connection without a target before it builds
	// anything, which is enough for the handler to tell of it.
	r, w := pipe.New()
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{}})
	inst.GetFeature(outbound.ManagerType()).(outbound.Manager).GetHandler("w1").Dispatch(ctx, &transport.Link{Reader: r, Writer: w})
	if seen() {
		t.Fatal("a connection through w1 made an outbound without path observable")
	}
}
