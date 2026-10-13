package router

import (
	"context"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
)

// LeastCostStrategy delegates the choice to the observatory, which keeps the
// chosen outbound until it fails and returns to cheaper ones when they are
// stable again. The settings are opaque to the router.
type LeastCostStrategy struct {
	balancerTag string
	settings    *serial.TypedMessage
	ctx         context.Context
	selector    extension.OutboundSelector
}

func (s *LeastCostStrategy) InjectContext(ctx context.Context) {
	s.ctx = ctx
	common.Must(core.RequireFeatures(ctx, func(observatory extension.Observatory) error {
		// Without a selector the balancer returns no tag and traffic goes to
		// its fallbackTag; rules added at runtime must not stop the process.
		provider, ok := observatory.(extension.OutboundSelectorProvider)
		if !ok {
			errors.LogError(s.ctx, "balancer ", s.balancerTag, ": leastCost needs an observatory that selects outbounds")
			return nil
		}
		selector, err := provider.OutboundSelector(s.balancerTag, s.settings)
		if err != nil {
			errors.LogErrorInner(s.ctx, err, "balancer ", s.balancerTag, ": failed to create leastCost selector")
			return nil
		}
		s.selector = selector
		return nil
	}))
}

func (s *LeastCostStrategy) GetPrincipleTarget(candidates []string) []string {
	if tag := s.PickOutbound(candidates); tag != "" {
		return []string{tag}
	}
	return nil
}

func (s *LeastCostStrategy) PickOutbound(candidates []string) string {
	if s.selector == nil {
		errors.LogError(s.ctx, "balancer ", s.balancerTag, ": leastCost selector is not ready")
		return ""
	}
	return s.selector.Select(candidates)
}

func (s *LeastCostStrategy) FallbackBalancer() string {
	if selector, ok := s.selector.(extension.OutboundSelectorFallback); ok {
		return selector.FallbackBalancer()
	}
	return ""
}

func (s *LeastCostStrategy) FallbackSelected(to string) {
	if selector, ok := s.selector.(extension.OutboundSelectorFallback); ok {
		selector.FallbackSelected(to)
	}
}
