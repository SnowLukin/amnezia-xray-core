package extension

import (
	"github.com/xtls/xray-core/common/serial"
)

// OutboundSelector keeps one chosen outbound among balancer candidates and
// changes it only on its own events, not on every connection.
type OutboundSelector interface {
	// Select returns the chosen tag among candidates, or "" when none is usable.
	Select(candidates []string) string
}

// OutboundSelectorProvider is implemented by observatories that also make
// selection decisions for balancers.
type OutboundSelectorProvider interface {
	// OutboundSelector returns the selector of a balancer. Calling it again for
	// the same balancer, for example after routing rules were reloaded, updates
	// the settings and keeps the current choice.
	OutboundSelector(balancerTag string, settings *serial.TypedMessage) (OutboundSelector, error)
}
