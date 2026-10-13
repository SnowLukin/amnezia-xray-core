package router

import (
	"fmt"
	"strings"
)

// Each generation owns an immutable target map. Routes copied before a reload
// can finish selecting without accessing Router.balancers after its lock ends.
func bindBalancerFallbacks(balancers map[string]*Balancer) error {
	hasFallback := false
	for start, balancer := range balancers {
		if balancer.fallbackBalancer == "" {
			continue
		}
		hasFallback = true
		chain := []string{start}
		seen := map[string]bool{start: true}
		current := balancer
		for current.fallbackBalancer != "" {
			target := current.fallbackBalancer
			chain = append(chain, target)
			path := strings.Join(chain, " -> ")
			if seen[target] {
				return fmt.Errorf("routing.balancers[%s].strategy.settings.fallbackBalancer: cycle %s", start, path)
			}
			if len(chain) > 4 {
				return fmt.Errorf("routing.balancers[%s].strategy.settings.fallbackBalancer: chain exceeds 4 balancers: %s", start, path)
			}
			next := balancers[target]
			if next == nil {
				return fmt.Errorf("routing.balancers[%s].strategy.settings.fallbackBalancer: unknown balancer %s in %s", start, target, path)
			}
			seen[target] = true
			current = next
		}
	}
	if !hasFallback {
		return nil
	}
	targets := make(map[string]*Balancer, len(balancers))
	for tag, balancer := range balancers {
		targets[tag] = balancer
	}
	for _, balancer := range balancers {
		// Existing balancers keep the map of their original generation.
		if balancer.fallbackBalancer != "" && balancer.fallbackTargets == nil {
			balancer.fallbackTargets = targets
		}
	}
	return nil
}
