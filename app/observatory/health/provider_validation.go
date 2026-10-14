package health

import (
	"fmt"
	"math"
	"net/url"
	"slices"
)

// Direct protobuf entry points must not revive negative sentinel switches that
// the v2 JSON schema no longer accepts. Zero scalars retain their documented
// defaults; none of these values may remove an admission guard.
func validateProviderConfigurations(c *Config) error {
	for _, p := range c.GetProviders() {
		if p == nil || !featureValue(p.IsEnabled, false) {
			continue
		}
		where := fmt.Sprintf("healthObservatory.providers[%s]", p.GetTag())
		urls := configuredAddresses(p.GetUrls(), p.GetUrl())
		if len(urls) < 1 || len(urls) > 4 {
			return fmt.Errorf("%s.urls must contain 1 to 4 addresses", where)
		}
		for i, destination := range urls {
			u, err := url.Parse(destination)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
				return fmt.Errorf("%s.urls[%d] must be an HTTP(S) URL without credentials or fragment", where, i)
			}
		}
		l, a, t := p.GetLimits(), p.GetApply(), p.GetTriggers()
		if t == nil {
			t = &ProviderTriggers{}
		}
		if len(a.GetBalancers()) < 1 || len(a.GetBalancers()) > 4 {
			return fmt.Errorf("%s.apply.balancers requires 1 to 4 tags", where)
		}
		for i, tag := range a.GetBalancers() {
			if tag == "" || slices.Contains(a.GetBalancers()[:i], tag) {
				return fmt.Errorf("%s.apply.balancers requires distinct nonempty tags", where)
			}
		}
		for name := range p.GetHeaders() {
			if err := CheckHeaderName(name); err != nil {
				return fmt.Errorf("%s.headers: %w", where, err)
			}
		}
		for _, name := range a.GetAcceptedHeaders() {
			if err := CheckHeaderName(name); err != nil {
				return fmt.Errorf("%s.apply.acceptedHeaders: %w", where, err)
			}
		}
		for _, r := range []*DurationRange{t.GetExhaustedDelay(), t.GetMinHealthyDelay()} {
			if r != nil && (r.Min < 0 || r.Max < r.Min) {
				return fmt.Errorf("%s: invalid delay range", where)
			}
		}
		for _, r := range []struct {
			low  int64
			high *int64
		}{{t.GetInterval(), t.IntervalMax}, {t.GetNetworkSettleDelay(), t.NetworkSettleMax}, {t.GetWhenCostAbove().GetAfter(), costAfterMax(t.GetWhenCostAbove())}} {
			if r.high != nil && (r.low < 0 || *r.high < r.low) {
				return fmt.Errorf("%s: invalid delay range", where)
			}
		}
		for name, value := range map[string]int64{"timeout": p.GetTimeout(), "limits.minInterval": l.GetMinInterval(), "limits.urgentMinInterval": l.GetExhaustedMinInterval(), "limits.maxRefreshesPerHour": int64(l.GetMaxRefreshesPerHour()), "apply.drainTimeout": a.GetDrainTimeout(), "apply.maxOutbounds": int64(a.GetMaxOutbounds())} {
			if value < 0 {
				return fmt.Errorf("%s.%s must be positive or omitted", where, name)
			}
		}
		f := p.GetFeatures()
		if f == nil {
			f = &ProviderFeatureConfig{}
		}
		if featureValue(f.Periodic, false) && t.GetInterval() < 0 {
			return fmt.Errorf("%s.triggers.periodic.interval must be positive or omitted", where)
		}
		if featureValue(f.MinHealthy, false) && t.GetMinHealthyOutbounds() < 0 {
			return fmt.Errorf("%s.triggers.minHealthy.outbounds must be positive or omitted", where)
		}
		if featureValue(f.MaxAge, false) && t.GetMaxAge() < 0 {
			return fmt.Errorf("%s.triggers.maxAge.age must be positive or omitted", where)
		}
		if featureValue(f.CostAbove, false) {
			cost := t.GetWhenCostAbove().GetCost()
			if cost <= 0 || math.IsNaN(float64(cost)) || math.IsInf(float64(cost), 0) || t.GetWhenCostAbove().GetAfter() < 0 {
				return fmt.Errorf("%s.triggers.whenCostAbove requires positive cost and nonnegative after", where)
			}
		}
		if featureValue(f.NetworkChange, false) && (t.GetNetworkSettleDelay() < 0 || t.GetNetworkChangeMode() > RefreshOnNetwork_REFRESH_NEVER || t.GetNetworkChangeMode() < 0) {
			return fmt.Errorf("%s.triggers.networkChange requires a valid mode and nonnegative settleDelay", where)
		}
		if featureValue(f.LeaseRenewal, true) {
			ex := t.GetExpiry()
			renew, urgent := ex.GetRenewAtFraction(), ex.GetUrgentAtFraction()
			if renew < 0 || urgent < 0 || renew > maxRenewAt || urgent > maxRenewAt || math.IsNaN(renew) || math.IsNaN(urgent) {
				return fmt.Errorf("%s.triggers.leaseRenewal requires fractions between zero and 0.95", where)
			}
		}
	}
	return nil
}

func costAfterMax(c *CostTrigger) *int64 {
	if c == nil {
		return nil
	}
	return c.AfterMax
}
