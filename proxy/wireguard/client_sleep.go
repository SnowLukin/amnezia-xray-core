package wireguard

import (
	"github.com/xtls/xray-core/features/outbound"
)

var _ outbound.Sleeper = (*Handler)(nil)

// Sleep implements outbound.Sleeper. While no connection uses the outbound or
// sets it up, it closes the device and the stack; the next connection creates
// both again. Unlike Close, it leaves the outbound usable.
//
// A connection counts until Process returns, which is right after it closed
// its socket in the tunnel: the closing exchange may still be under way, and
// Sleep cuts it short. Callers sleep outbounds that have been idle a while.
func (h *Handler) Sleep() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.active > 0 || h.initDone != nil {
		return false
	}
	h.closeLocked()
	return true
}
