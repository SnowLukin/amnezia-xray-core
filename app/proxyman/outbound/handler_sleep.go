package outbound

import (
	"github.com/xtls/xray-core/features/outbound"
)

// Sleep lives here, apart from handler.go, so the handler body keeps
// following upstream without conflicts.

var _ outbound.Sleeper = (*Handler)(nil)

// Sleep implements outbound.Sleeper for the proxy of the handler. It also
// returns false when the proxy has nothing to release: to tell such proxies
// apart, check whether GetOutbound implements outbound.Sleeper.
func (h *Handler) Sleep() bool {
	if s, ok := h.proxy.(outbound.Sleeper); ok {
		return s.Sleep()
	}
	return false
}
