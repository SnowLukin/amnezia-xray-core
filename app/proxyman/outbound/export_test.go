package outbound

import (
	"github.com/xtls/xray-core/features/outbound"
)

func WrapObserver(h outbound.Handler, wrap func(outbound.ConnectionObserver) outbound.ConnectionObserver) {
	hh := h.(*Handler)
	hh.observer = wrap(hh.observer)
}
