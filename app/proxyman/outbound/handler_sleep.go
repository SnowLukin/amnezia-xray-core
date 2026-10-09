package outbound

import (
	"github.com/xtls/xray-core/features/outbound"
)

// Sleep lives here, apart from handler.go, so the handler body keeps
// following upstream without conflicts.

var _ outbound.Sleeper = (*Handler)(nil)

// Sleep implements outbound.Sleeper for the proxy of the handler. A proxy
// that is not a Sleeper holds nothing to release, so the handler is asleep.
func (h *Handler) Sleep() bool {
	if s, ok := h.proxy.(outbound.Sleeper); ok {
		return s.Sleep()
	}
	return true
}
