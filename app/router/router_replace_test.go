package router_test

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/geodata"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	routing_session "github.com/xtls/xray-core/features/routing/session"
	"github.com/xtls/xray-core/testing/mocks"
)

func TestReplaceRulesSwapsAllOrNothing(t *testing.T) {
	route := func(tag string) *Config {
		return &Config{Rule: []*RoutingRule{{TargetTag: &RoutingRule_Tag{Tag: tag}, Networks: []net.Network{net.Network_TCP}}}}
	}
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	r := new(Router)
	common.Must(r.Init(context.TODO(), route("first"), mocks.NewDNSClient(mockCtl), &mockOutboundManager{
		Manager:         mocks.NewOutboundManager(mockCtl),
		HandlerSelector: mocks.NewOutboundHandlerSelector(mockCtl),
	}, nil))
	pick := func() string {
		ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
			Target: net.TCPDestination(net.DomainAddress("example.com"), 80),
		}})
		got, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
		common.Must(err)
		return got.GetOutboundTag()
	}

	common.Must(r.ReplaceRules(route("second"), false))
	if tag := pick(); tag != "second" {
		t.Fatalf("after a replace the route goes to %q, want second", tag)
	}
	broken := route("third")
	broken.Rule[0].TargetTag = &RoutingRule_BalancingTag{BalancingTag: "missing"}
	if err := r.ReplaceRules(broken, false); err == nil {
		t.Fatal("a rule pointing to a missing balancer was accepted")
	}
	if tag := pick(); tag != "second" {
		t.Fatalf("a config that does not build changed the route to %q", tag)
	}
}

// Run with -race: routes are picked on every connection while a provider
// replaces the rules and the domain strategy.
func TestReplaceRulesWhileRouting(t *testing.T) {
	route := func(tag string, strategy Config_DomainStrategy) *Config {
		return &Config{DomainStrategy: strategy,
			Rule: []*RoutingRule{{TargetTag: &RoutingRule_Tag{Tag: tag}, Networks: []net.Network{net.Network_TCP}}}}
	}
	mockCtl := gomock.NewController(t)
	defer mockCtl.Finish()
	mockDNS := mocks.NewDNSClient(mockCtl)
	mockDNS.EXPECT().LookupIP(gomock.Any(), gomock.Any()).Return([]net.IP{{192, 168, 0, 1}}, uint32(600), nil).AnyTimes()
	r := new(Router)
	common.Must(r.Init(context.TODO(), route("first", Config_AsIs), mockDNS, &mockOutboundManager{
		Manager:         mocks.NewOutboundManager(mockCtl),
		HandlerSelector: mocks.NewOutboundHandlerSelector(mockCtl),
	}, nil))
	done := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
				Target: net.TCPDestination(net.DomainAddress("example.com"), 80),
			}})
			for {
				select {
				case <-done:
					return
				default:
				}
				if _, err := r.PickRoute(routing_session.AsRoutingContext(ctx)); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	strategies := []Config_DomainStrategy{Config_IpIfNonMatch, Config_IpOnDemand, Config_AsIs}
	for i := 0; i < 200; i++ {
		common.Must(r.ReplaceRules(route("second", strategies[i%len(strategies)]), i%2 == 0))
	}
	close(done)
	wg.Wait()
}

// ipRouter routes 192.168.0.0/16 to "lan" and nothing else; example.com
// resolves into that range.
func ipRouter(t *testing.T, strategy Config_DomainStrategy) (*Router, func() string) {
	t.Helper()
	mockCtl := gomock.NewController(t)
	t.Cleanup(mockCtl.Finish)
	mockDNS := mocks.NewDNSClient(mockCtl)
	mockDNS.EXPECT().LookupIP(gomock.Eq("example.com"), gomock.Any()).Return([]net.IP{{192, 168, 0, 1}}, uint32(600), nil).AnyTimes()
	r := new(Router)
	common.Must(r.Init(context.TODO(), lanRules(strategy), mockDNS, nil, nil))
	pick := func() string {
		ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{{
			Target: net.TCPDestination(net.DomainAddress("example.com"), 80),
		}})
		got, err := r.PickRoute(routing_session.AsRoutingContext(ctx))
		if err != nil {
			return "none"
		}
		return got.GetOutboundTag()
	}
	return r, pick
}

func lanRules(strategy Config_DomainStrategy) *Config {
	return &Config{DomainStrategy: strategy, Rule: []*RoutingRule{{
		TargetTag: &RoutingRule_Tag{Tag: "lan"},
		Ip: []*geodata.IPRule{{Value: &geodata.IPRule_Custom{Custom: &geodata.CIDRRule{
			Cidr: &geodata.CIDR{Ip: []byte{192, 168, 0, 0}, Prefix: 16},
		}}}},
	}}}
}

func TestReplaceRulesTakesTheStrategyWhenAsked(t *testing.T) {
	r, pick := ipRouter(t, Config_AsIs)
	if got := pick(); got != "none" {
		t.Fatalf("with AsIs a domain matched the IP rule: %q", got)
	}
	common.Must(r.ReplaceRules(lanRules(Config_IpIfNonMatch), true))
	if got := pick(); got != "lan" {
		t.Fatalf("after taking IPIfNonMatch the domain goes to %q, want lan", got)
	}
	common.Must(r.ReplaceRules(lanRules(Config_AsIs), true))
	if got := pick(); got != "none" {
		t.Fatalf("after taking AsIs back the domain goes to %q", got)
	}
}

// Rules that come without a strategy leave the one in force.
func TestReplaceRulesKeepsTheStrategyOtherwise(t *testing.T) {
	r, pick := ipRouter(t, Config_IpIfNonMatch)
	common.Must(r.ReplaceRules(lanRules(Config_AsIs), false))
	if got := pick(); got != "lan" {
		t.Fatalf("new rules without a strategy dropped IPIfNonMatch: the domain goes to %q", got)
	}
}
