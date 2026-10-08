package router_test

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/xtls/xray-core/app/router"
	"github.com/xtls/xray-core/common"
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

	common.Must(r.ReplaceRules(route("second")))
	if tag := pick(); tag != "second" {
		t.Fatalf("after a replace the route goes to %q, want second", tag)
	}
	broken := route("third")
	broken.Rule[0].TargetTag = &RoutingRule_BalancingTag{BalancingTag: "missing"}
	if err := r.ReplaceRules(broken); err == nil {
		t.Fatal("a rule pointing to a missing balancer was accepted")
	}
	if tag := pick(); tag != "second" {
		t.Fatalf("a config that does not build changed the route to %q", tag)
	}
}

// Run with -race: routes are picked on every connection while a provider
// replaces the rules.
func TestReplaceRulesWhileRouting(t *testing.T) {
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
	for i := 0; i < 200; i++ {
		common.Must(r.ReplaceRules(route("second")))
	}
	close(done)
	wg.Wait()
}
