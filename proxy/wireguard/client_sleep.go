package wireguard

import (
	"github.com/xtls/xray-core/features/outbound"
)

var _ outbound.Sleeper = (*Handler)(nil)

// Sleep implements outbound.Sleeper. While no connection uses the outbound,
// it closes the device and the stack; the next connection creates both again.
// Unlike Close, it leaves the outbound usable.
func (h *Handler) Sleep() bool {
	// A connection holds the lock while it creates the stack and the device,
	// which can take seconds when the endpoint needs DNS. The outbound is in
	// use then, and the caller should not wait for it.
	if !h.mu.TryLock() {
		return false
	}
	defer h.mu.Unlock()
	if h.active > 0 {
		return false
	}
	h.closeLocked()
	return true
}

func (h *Handler) release() {
	h.mu.Lock()
	h.active--
	h.mu.Unlock()
}
