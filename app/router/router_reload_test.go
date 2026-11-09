package router

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	"google.golang.org/protobuf/proto"
)

type reloadTestObservatory struct {
	build func(tag string) (extension.OutboundSelector, error)
}

func (*reloadTestObservatory) Type() interface{} { return extension.ObservatoryType() }
func (*reloadTestObservatory) Start() error      { return nil }
func (*reloadTestObservatory) Close() error      { return nil }
func (*reloadTestObservatory) GetObservation(context.Context) (proto.Message, error) {
	return nil, nil
}

func (o *reloadTestObservatory) OutboundSelector(tag string, _ *serial.TypedMessage) (extension.OutboundSelector, error) {
	return o.build(tag)
}

type reloadTestSelector struct {
	current string
	selects int
}

func (s *reloadTestSelector) Select([]string) string { s.selects++; return s.current }
func (s *reloadTestSelector) Current() string        { return s.current }

func reloadTestContext(t *testing.T, obs *reloadTestObservatory) context.Context {
	t.Helper()
	inst, err := core.New(&core.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := inst.AddFeature(obs); err != nil {
		t.Fatal(err)
	}
	return context.WithValue(context.Background(), core.XrayKey(1), inst)
}

func leastCostRule(tag, fallback string) *BalancingRule {
	return &BalancingRule{Tag: tag, Strategy: "leastCost", OutboundSelector: []string{"a"}, FallbackTag: fallback}
}

func tcpRule(tag string) *RoutingRule {
	return &RoutingRule{TargetTag: &RoutingRule_Tag{Tag: tag}, Networks: []net.Network{net.Network_TCP}}
}

func pickTCP(r *Router) (string, error) {
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
		Target: net.TCPDestination(net.DomainAddress("example.com"), 80),
	}})
	route, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
	if err != nil {
		return "", err
	}
	return route.GetOutboundTag(), nil
}

func TestPickRouteDoesNotWaitForReload(t *testing.T) {
	building := make(chan struct{})
	release := make(chan struct{})
	obs := &reloadTestObservatory{build: func(string) (extension.OutboundSelector, error) {
		close(building)
		<-release
		return &reloadTestSelector{}, nil
	}}
	r := &Router{ctx: reloadTestContext(t, obs)}
	r.rules.Store(&[]*Rule{})
	r.balancers.Store(&map[string]*Balancer{})
	if err := r.ReloadRules(&Config{Rule: []*RoutingRule{tcpRule("first")}}, false); err != nil {
		t.Fatal(err)
	}

	reloaded := make(chan error, 1)
	go func() {
		reloaded <- r.ReloadRules(&Config{
			BalancingRule: []*BalancingRule{leastCostRule("auto", "block")},
			Rule:          []*RoutingRule{tcpRule("second")},
		}, false)
	}()
	<-building

	picked := make(chan string, 1)
	go func() {
		tag, _ := pickTCP(r)
		picked <- tag
	}()
	select {
	case tag := <-picked:
		if tag != "first" {
			t.Errorf("route during reload = %q, want first", tag)
		}
	case <-time.After(2 * time.Second):
		t.Error("PickRoute waited for ReloadRules")
	}
	close(release)
	if err := <-reloaded; err != nil {
		t.Fatal(err)
	}
	if tag, err := pickTCP(r); err != nil || tag != "second" {
		t.Fatalf("route after reload = %q, %v", tag, err)
	}
}

func TestRejectedReloadDoesNotConfigureSelector(t *testing.T) {
	var built []string
	obs := &reloadTestObservatory{build: func(tag string) (extension.OutboundSelector, error) {
		built = append(built, tag)
		return &reloadTestSelector{}, nil
	}}
	r := &Router{ctx: reloadTestContext(t, obs)}
	r.rules.Store(&[]*Rule{})
	r.balancers.Store(&map[string]*Balancer{})
	broken := tcpRule("x")
	broken.TargetTag = &RoutingRule_BalancingTag{BalancingTag: "missing"}
	err := r.ReloadRules(&Config{
		BalancingRule: []*BalancingRule{leastCostRule("auto", "block")},
		Rule:          []*RoutingRule{broken},
	}, true)
	if err == nil {
		t.Fatal("a rule pointing to a missing balancer was accepted")
	}
	if len(built) != 0 {
		t.Fatalf("rejected reload configured selectors %v", built)
	}
}

func TestAddedLeastCostNeedsFallbackTag(t *testing.T) {
	obs := &reloadTestObservatory{build: func(string) (extension.OutboundSelector, error) {
		return &reloadTestSelector{}, nil
	}}
	r := &Router{ctx: reloadTestContext(t, obs)}
	r.rules.Store(&[]*Rule{})
	r.balancers.Store(&map[string]*Balancer{})
	err := r.AddRule(serial.ToTypedMessage(&Config{BalancingRule: []*BalancingRule{leastCostRule("auto", "")}}), true)
	if err == nil || !strings.Contains(err.Error(), "fallbackTag") {
		t.Fatalf("leastCost without fallbackTag: %v", err)
	}
	if _, found := (*r.balancers.Load())["auto"]; found {
		t.Fatal("rejected balancer was stored")
	}
}

func TestLeastCostPrincipleTargetDoesNotSelect(t *testing.T) {
	selector := &reloadTestSelector{current: "a"}
	r := &Router{}
	r.balancers.Store(&map[string]*Balancer{"auto": {
		selectors: []string{"a"},
		ohm:       &fallbackTestManager{},
		strategy:  &LeastCostStrategy{balancerTag: "auto", selector: selector},
	}})
	got, err := r.GetPrincipleTarget("auto")
	if err != nil || len(got) != 1 || got[0] != "a" {
		t.Fatalf("principle target = %v, %v", got, err)
	}
	if selector.selects != 0 {
		t.Fatalf("GetPrincipleTarget called Select %d times", selector.selects)
	}
}

func TestStartFailsWhenSelectorCannotBeCreated(t *testing.T) {
	obs := &reloadTestObservatory{build: func(string) (extension.OutboundSelector, error) {
		return nil, errors.New("bad selector settings")
	}}
	ctx := reloadTestContext(t, obs)
	r := new(Router)
	err := r.Init(ctx, &Config{BalancingRule: []*BalancingRule{leastCostRule("auto", "block")}}, nil, nil, nil)
	if err == nil {
		err = r.Start()
	}
	if err == nil || !strings.Contains(err.Error(), "bad selector settings") {
		t.Fatalf("start with a broken selector: %v", err)
	}
}

func TestSelectorErrorAtRuntimeKeepsRouting(t *testing.T) {
	fail := false
	obs := &reloadTestObservatory{build: func(string) (extension.OutboundSelector, error) {
		if fail {
			return nil, errors.New("bad selector settings")
		}
		return &reloadTestSelector{}, nil
	}}
	r := new(Router)
	if err := r.Init(reloadTestContext(t, obs), &Config{}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	fail = true
	if err := r.ReloadRules(&Config{BalancingRule: []*BalancingRule{leastCostRule("auto", "block")}}, true); err != nil {
		t.Fatalf("runtime reload: %v", err)
	}
}
