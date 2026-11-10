package health

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
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
	all := r.export(0, 0)
	if r.size > minTraceBytes || all.Dropped == 0 || all.Seq != 2000 {
		t.Fatalf("size %d dropped %d seq %d", r.size, all.Dropped, all.Seq)
	}
	recs := traceRecords(t, all)
	if last := recs[len(recs)-1]; last["seq"] != float64(2000) || last["event"] != "probe_start" {
		t.Fatalf("last %+v", last)
	}
	if tail := traceRecords(t, r.export(1998, 0)); len(tail) != 2 || tail[0]["seq"] != float64(1999) {
		t.Fatalf("tail %+v", tail)
	}
	if none := traceRecords(t, r.export(2000, 0)); len(none) != 0 {
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
	o.e.emit(o.clock(), "network_change", map[string]any{"network": o.e.networkHash("home-wifi-id"), "is_known": false})
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
	recs := traceRecords(t, o.e.trace.export(0, 0))
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
	r.event(t0, "provider_fetch", map[string]any{"is_ok": false, "error": "balancer_exhausted"})
	d := r.export(0, 0)
	if strings.Contains(string(d.Records), "203.0.113.10") {
		t.Fatalf("address leaked: %s", d.Records)
	}
	recs := traceRecords(t, d)
	if recs[0]["fields"].(map[string]any)["error"] != "other" || recs[1]["fields"].(map[string]any)["error"] != "balancer_exhausted" {
		t.Fatalf("records %+v", recs)
	}
	// Every field that holds an error, whatever its prefix.
	r.event(t0, "some_event", map[string]any{"last_error": "dial tcp 203.0.113.11:443: i/o timeout", "probe_error": "timeout"})
	d = r.export(0, 0)
	if strings.Contains(string(d.Records), "203.0.113.11") {
		t.Fatalf("address leaked: %s", d.Records)
	}
	f := traceRecords(t, d)[2]["fields"].(map[string]any)
	if f["last_error"] != "other" || f["probe_error"] != "timeout" {
		t.Fatalf("fields %+v", f)
	}
}

// A host takes a full ring in parts of a bounded size: each part stays
// within the bound, says whether more follows, and the parts add up to the
// ring without a gap. Building a part costs about the part, not the ring.
func TestTraceExportsInParts(t *testing.T) {
	r := newTraceRing(&traceSettings{maxBytes: maxTraceBytes}, t0)
	for i := 0; r.dropped == 0; i++ {
		r.event(at(time.Duration(i)*time.Millisecond), "probe_start", map[string]any{"outbound": "a1", "reason": "candidate"})
	}
	const part = 64 << 10
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	d := r.export(0, part)
	runtime.ReadMemStats(&after)
	if grown := after.TotalAlloc - before.TotalAlloc; grown > 2*part {
		t.Fatalf("a part of %d bytes allocated %d", part, grown)
	}
	seen, next := 0, uint64(0)
	for {
		if len(d.Records) > part {
			t.Fatalf("part of %d bytes", len(d.Records))
		}
		recs := traceRecords(t, d)
		for _, rec := range recs {
			if seq := uint64(rec["seq"].(float64)); next != 0 && seq != next+1 {
				t.Fatalf("gap: %d after %d", seq, next)
			}
			next = uint64(rec["seq"].(float64))
		}
		seen += len(recs)
		if len(recs) > 0 && d.Seq != next {
			t.Fatalf("seq %d, last record %d", d.Seq, next)
		}
		if !d.IsMore {
			break
		}
		d = r.export(d.Seq, part)
	}
	if seen != len(r.lines) || next != r.seq {
		t.Fatalf("parts held %d of %d lines, last %d of %d", seen, len(r.lines), next, r.seq)
	}
	// Without a bound the host gets the whole ring at once.
	if whole := r.export(0, 0); whole.IsMore || len(traceRecords(t, whole)) != len(r.lines) {
		t.Fatal("unbounded export cut")
	}
	// A line larger than the bound still goes out alone.
	if one := r.export(0, 1); len(traceRecords(t, one)) != 1 || !one.IsMore {
		t.Fatalf("tiny bound %s", one.Records)
	}
}

// After a restart the host may ask with the number it had from the earlier
// run, past every line of this one: it gets this trace from its start.
func TestTraceAfterFromEarlierRunStartsOver(t *testing.T) {
	r := newTraceRing(&traceSettings{maxBytes: minTraceBytes}, t0)
	r.event(t0, "switch", map[string]any{"balancer": "auto", "to": "a1"})
	r.event(t0, "switch", map[string]any{"balancer": "auto", "to": "b1"})
	if recs := traceRecords(t, r.export(5000, 0)); len(recs) != 2 || recs[0]["seq"] != float64(1) {
		t.Fatalf("records %+v", recs)
	}
}

// The ring of a trace without maxBytes fits the memory of a phone tunnel.
func TestTraceDefaultSize(t *testing.T) {
	if s := newSettings(&Config{Trace: &TraceConfig{}}); s.trace.maxBytes != 256<<10 {
		t.Fatalf("default %d", s.trace.maxBytes)
	}
}
