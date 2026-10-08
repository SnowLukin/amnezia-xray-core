package router

// ReplaceRules swaps all rules and balancers for the ones in config. Unlike
// ReloadRules, it changes nothing when config does not build: the new rules
// are built aside and swapped in at once.
func (r *Router) ReplaceRules(config *Config) error {
	next := &Router{
		domainStrategy: r.domainStrategy,
		dns:            r.dns,
		ctx:            r.ctx,
		ohm:            r.ohm,
		dispatcher:     r.dispatcher,
		balancers:      make(map[string]*Balancer, len(config.BalancingRule)),
	}
	if err := next.ReloadRules(config, true); err != nil {
		return err
	}
	r.mu.Lock()
	old := r.rules
	r.rules, r.balancers = next.rules, next.balancers
	r.mu.Unlock()
	for _, rule := range old {
		if rule.Webhook != nil {
			rule.Webhook.Close()
		}
	}
	return nil
}
