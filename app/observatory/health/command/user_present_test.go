package command_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/app/observatory/health/command"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/routing"
	routing_session "github.com/xtls/xray-core/features/routing/session"
)

func TestUserPresentReturnsBeforeProbeFinishes(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var entered, released sync.Once
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered.Do(func() { close(started) })
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() {
		released.Do(func() { close(release) })
		ts.Close()
	})
	s, inst := service(t, `{
	  "log": {"loglevel": "none"},
	  "outbounds": [{"tag": "drop", "protocol": "blackhole"},
	    {"tag": "a1", "protocol": "freedom", "settings": {"finalRules": [{"action": "allow"}]},
	      "path": {"balancer": "auto", "cost": 1, "probe": "check"}}],
	  "routing": {"rules": [{"domain": ["full:target.test"], "balancerTag": "auto"}],
	    "balancers": [{"tag": "auto", "strategy": {"type": "leastCost"}, "fallbackTag": "drop"}]},
	  "healthObservatory": {"report": {"quietShare": 1},
	    "probes": [{"name": "check", "destination": "`+ts.URL+`/204", "timeout": "10s"}]}
	}`)
	router := inst.GetFeature(routing.RouterType()).(routing.Router)
	if _, err := router.PickRoute(routing_session.AsRoutingContext(session.ContextWithOutbounds(context.Background(),
		[]*session.Outbound{{Target: net.TCPDestination(net.DomainAddress("target.test"), 80)}}))); err != nil {
		t.Fatal(err)
	}
	returned := make(chan error, 1)
	go func() {
		_, err := s.UserPresent(context.Background(), &command.UserPresentRequest{})
		returned <- err
	}()
	select {
	case err := <-returned:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("UserPresent waited for the network result")
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("UserPresent did not start the selected path's probe")
	}
	for range 2 {
		if _, err := s.UserPresent(context.Background(), &command.UserPresentRequest{}); err != nil {
			t.Fatal(err)
		}
	}
	var report struct {
		Started int `json:"wake_probe_count"`
		Skipped int `json:"wake_skipped_count"`
	}
	if err := json.Unmarshal(health.FromInstance(inst).Report(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Started != 1 || report.Skipped != 2 {
		t.Fatalf("repeated presents: %+v", report)
	}
	released.Do(func() { close(release) })
}

func TestUserPresentWithoutObservatory(t *testing.T) {
	s, _ := service(t, `{"log": {"loglevel": "none"}, "outbounds": [{"tag": "direct", "protocol": "freedom"}]}`)
	if _, err := s.UserPresent(context.Background(), &command.UserPresentRequest{}); err == nil {
		t.Fatal("UserPresent accepted without a health observatory")
	}
}
