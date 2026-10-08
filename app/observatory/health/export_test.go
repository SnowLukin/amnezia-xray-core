package health

import "time"

func Probe(o *Observer, tag, dest string, minBytes int64, expect ...int32) (bool, int64, string) {
	r := o.probe(tag, &probeSettings{destination: dest, method: "GET", minBytes: minBytes, expectStatus: expect,
		timeout: 4 * time.Second, progressTimeout: 2 * time.Second})
	return r.ok, r.bytes, r.err
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
