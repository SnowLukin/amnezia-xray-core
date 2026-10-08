package router

// ReplaceRules swaps all rules and balancers for the ones in config. Unlike
// ReloadRules, it changes nothing when config does not build: the new rules
// are built aside and swapped in at once. withStrategy takes the domain
// strategy of config along with them; without it the strategy stays, since
// its zero value AsIs cannot tell a strategy left out from one asked for.
func (r *Router) ReplaceRules(config *Config, withStrategy bool) error {
	next := &Router{
		dns:        r.dns,
		ctx:        r.ctx,
		ohm:        r.ohm,
		dispatcher: r.dispatcher,
		balancers:  make(map[string]*Balancer, len(config.BalancingRule)),
	}
	if err := next.ReloadRules(config, true); err != nil {
		return err
	}
	r.mu.Lock()
	old := r.rules
	r.rules, r.balancers = next.rules, next.balancers
	if withStrategy {
		r.domainStrategy = config.DomainStrategy
	}
	r.mu.Unlock()
	for _, rule := range old {
		if rule.Webhook != nil {
			rule.Webhook.Close()
		}
	}
	return nil
}
