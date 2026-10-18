package health

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func traceRecords(t *testing.T, d TraceData) []map[string]any {
	t.Helper()
	var out []map[string]any
	if err := json.Unmarshal(d.Records, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// The ring stays within its size, drops the oldest lines, and gives the host
// only what it did not take yet.
func TestTraceRingBoundsAndExportsAfter(t *testing.T) {
	r := newTraceRing(&traceSettings{maxBytes: minTraceBytes, connections: true}, t0)
	for i := 0; i < 2000; i++ {
		r.event(at(time.Duration(i)*time.Millisecond), "probe_start", map[string]any{"outbound": "a1", "reason": "candidate"})
	}
	all := r.export(0)
	if r.size > minTraceBytes || all.Dropped == 0 || all.Seq != 2000 {
		t.Fatalf("size %d dropped %d seq %d", r.size, all.Dropped, all.Seq)
	}
	recs := traceRecords(t, all)
	if last := recs[len(recs)-1]; last["seq"] != float64(2000) || last["event"] != "probe_start" {
		t.Fatalf("last %+v", last)
	}
	if tail := traceRecords(t, r.export(1998)); len(tail) != 2 || tail[0]["seq"] != float64(1999) {
		t.Fatalf("tail %+v", tail)
	}
	if none := traceRecords(t, r.export(2000)); len(none) != 0 {
		t.Fatalf("nothing new expected: %+v", none)
	}
}

// A user connection keeps its timings and outcome but never its
// destination; the network the host reported is hashed as well.
func TestTraceKeepsNoDestination(t *testing.T) {
	o := newObserver(context.Background(), &Config{Paths: observed("a1"), Trace: &TraceConfig{}})
	tr := &connTracker{o: o, tag: "a1", target: "secret-site.example:443", started: int64(time.Second)}
	tr.dialed.Store(int64(time.Second + 90*time.Millisecond))
	tr.firstRead.Store(int64(time.Second + 400*time.Millisecond))
	tr.bytesRead.Store(70 << 10)
	tr.bytesWritten.Store(300)
	o.traceEnd(tr, nil)
	failed := &connTracker{o: o, tag: "a1", target: "secret-site.example:443", started: int64(2 * time.Second)}
	failed.failKind.Store(int32(failDialTimeout) + 1)
	o.traceEnd(failed, errors.New("i/o timeout"))
	o.e.trace.event(o.clock(), "network_change", map[string]any{"network": "home-wifi-id", "is_known": false})
	d, err := o.ExportTrace(0)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(d.Records), "secret-site") || strings.Contains(string(d.Records), "home-wifi-id") {
		t.Fatalf("destination or network leaked: %s", d.Records)
	}
	recs := traceRecords(t, d)
	ok, fail, net := recs[0], recs[1], recs[2]["fields"].(map[string]any)
	if ok["outcome"] != "ok" || ok["dial_ms"] != float64(90) || ok["first_byte_ms"] != float64(400) || ok["down"] != "64k-1m" ||
		ok["up"] != "<1k" || ok["port"] != "443" || ok["target"] != fail["target"] || !strings.HasPrefix(ok["target"].(string), "h:") {
		t.Fatalf("ok record %+v / %+v", ok, fail)
	}
	if fail["outcome"] != "dial_timeout" || !strings.HasPrefix(net["network"].(string), "h:") {
		t.Fatalf("failed %+v network %+v", fail, net)
	}
}

// Connection records can be turned off and leave the events; without the
// block there is no trace at all.
func TestTraceConnectionsOffAndDisabled(t *testing.T) {
	off := false
	o := newObserver(context.Background(), &Config{Paths: observed("a1"), Trace: &TraceConfig{ConnectionsEnabled: &off}})
	o.traceEnd(&connTracker{o: o, tag: "a1", target: "x:443"}, nil)
	o.e.trace.event(o.clock(), "switch", map[string]any{"balancer": "auto", "to": "a1"})
	recs := traceRecords(t, o.e.trace.export(0))
	if len(recs) != 1 || recs[0]["type"] != "event" {
		t.Fatalf("records %+v", recs)
	}
	none := newObserver(context.Background(), &Config{Paths: observed("a1")})
	if _, err := none.ExportTrace(0); err == nil || err.(*CommandError).Code != "feature_disabled" {
		t.Fatal(err)
	}
}

// The text of an error may name a server; the trace keeps a class only.
func TestTraceKeepsErrorClassesOnly(t *testing.T) {
	r := newTraceRing(&traceSettings{maxBytes: minTraceBytes}, t0)
	r.event(t0, "provider_fetch", map[string]any{"is_ok": false, "error": "dial tcp 203.0.113.10:443: connect: connection refused"})
	r.event(t0, "provider_fetch", map[string]any{"is_ok": false, "error": "balancer exhausted"})
	d := r.export(0)
	if strings.Contains(string(d.Records), "203.0.113.10") {
		t.Fatalf("address leaked: %s", d.Records)
	}
	recs := traceRecords(t, d)
	if recs[0]["fields"].(map[string]any)["error"] != "other" || recs[1]["fields"].(map[string]any)["error"] != "balancer exhausted" {
		t.Fatalf("records %+v", recs)
	}
}
