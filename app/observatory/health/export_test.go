package health

import "time"

func Probe(o *Observer, tag, dest string, minBytes int64, expect ...int32) (bool, int64, string) {
	r := o.probe(tag, &probeSettings{destination: dest, method: "GET", minBytes: minBytes, expectStatus: expect,
		timeout: 4 * time.Second, progressTimeout: 2 * time.Second})
	return r.ok, r.bytes, r.err
}
