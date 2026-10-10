package health_test

import (
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/core"
)

func TestCurrentIsTheRunningObservatory(t *testing.T) {
	cfg, err := core.LoadConfig("json", strings.NewReader(`{"log": {"loglevel": "none"},
		"outbounds": [{"tag": "a1", "protocol": "freedom", "path":{"balancer":"auto","cost":1}}], "routing":{"balancers":[{"tag":"auto","strategy":{"type":"leastCost"}}]}, "healthObservatory": {}}`))
	if err != nil {
		t.Fatal(err)
	}
	inst, err := core.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if health.Current() != nil {
		t.Fatal("an observatory that has not started is current")
	}
	if err := inst.Start(); err != nil {
		t.Fatal(err)
	}
	if got := health.Current(); got == nil || got != health.FromInstance(inst) {
		t.Fatal("the running observatory is not current")
	}
	inst.Close()
	if health.Current() != nil {
		t.Fatal("a closed observatory is still current")
	}
}
