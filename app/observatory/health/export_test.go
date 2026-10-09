package health

import (
	"time"

	"github.com/xtls/xray-core/features/outbound"
)

func Probe(o *Observer, tag, dest string, minBytes int64, expect ...int32) (bool, int64, string) {
	r := probeOnce(o, tag, dest, minBytes, expect...)
	return r.ok, r.bytes, r.err
}

// ProbeClass is the class a provider would see for the error of the probe.
func ProbeClass(o *Observer, tag, dest string, minBytes int64, expect ...int32) string {
	return probeOnce(o, tag, dest, minBytes, expect...).class
}

func probeOnce(o *Observer, tag, dest string, minBytes int64, expect ...int32) probeResult {
	return o.probe(tag, &probeSettings{destination: dest, method: "GET", minBytes: minBytes, expectStatus: expect,
		timeout: 4 * time.Second, progressTimeout: 2 * time.Second})
}

// QueueProbe asks for a probe of tag, as after a network change.
func QueueProbe(o *Observer, tag string) {
	o.mu.Lock()
	now := o.now()
	o.e.noteDemand(now)
	o.e.enqueue(tag, reasonNetwork, "", now)
	o.mu.Unlock()
	o.wake()
}

// CanSleep tells whether the observer would put the outbound of a handler
// to sleep.
func CanSleep(h outbound.Handler) bool {
	return sleeperOf(h) != nil
}
