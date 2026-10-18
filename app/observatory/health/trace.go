package health

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	goerrors "errors"
	"maps"
	"strings"
	"sync"
	"time"
)

// A trace keeps raw evidence for a later look at one session: every user
// connection, how it went, and every event of the core, so that a session can
// be replayed with other settings. It never holds where the user went: a
// destination becomes a hash with a salt of this trace that is kept nowhere,
// so a trace tells the same site from another but not which one, and two
// traces cannot be matched.
const (
	defaultTraceBytes = 2 << 20
	minTraceBytes     = 64 << 10
	maxTraceBytes     = 2 << 20
)

type traceSettings struct {
	maxBytes    int
	connections bool
}

type traceRing struct {
	mu          sync.Mutex
	max         int
	connections bool
	salt        [16]byte
	start       time.Time
	wall        time.Time
	lines       [][]byte
	size        int
	// The number of the last line, and of the lines dropped to stay within
	// max.
	seq     uint64
	dropped uint64
}

func newTraceRing(s *traceSettings, now time.Time) *traceRing {
	r := &traceRing{max: s.maxBytes, connections: s.connections, start: now, wall: now.Round(0)}
	_, _ = rand.Read(r.salt[:])
	return r
}

// hash names a value within this trace only.
func (r *traceRing) hash(v string) string {
	h := sha256.New()
	h.Write(r.salt[:])
	h.Write([]byte(v))
	return "h:" + hex.EncodeToString(h.Sum(nil)[:6])
}

func (r *traceRing) add(now time.Time, record map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	record["seq"] = r.seq
	record["t"] = max(now.Sub(r.start).Milliseconds(), 0)
	line, err := json.Marshal(record)
	if err != nil {
		return
	}
	r.lines = append(r.lines, line)
	r.size += len(line) + 1
	for r.size > r.max && len(r.lines) > 1 {
		r.size -= len(r.lines[0]) + 1
		r.lines[0] = nil
		r.lines = r.lines[1:]
		r.dropped++
	}
}

// event keeps an event of the core. Its fields name no destination already;
// the network the host reported is hashed like a destination.
func (r *traceRing) event(now time.Time, typ string, fields map[string]any) {
	f := fields
	if id, ok := fields["network"].(string); ok && id != "" {
		f = maps.Clone(fields)
		f["network"] = r.hash("network:" + id)
	}
	// Some events keep the text of an error, which may name an address of
	// a server: the trace keeps only a class.
	if text, ok := f["error"].(string); ok && !isErrorClass(text) {
		f = maps.Clone(f)
		f["error"] = "other"
	}
	r.add(now, map[string]any{"type": "event", "event": typ, "fields": f})
}

// traceConn is one user connection as the trace keeps it.
type traceConn struct {
	path, network, target, outcome string
	dialMs, firstByteMs, ms        int64
	up, down                       int64
}

func sizeBucket(n int64) string {
	switch {
	case n <= 0:
		return "0"
	case n < 1<<10:
		return "<1k"
	case n < 64<<10:
		return "1-64k"
	case n < 1<<20:
		return "64k-1m"
	default:
		return ">1m"
	}
}

func portClass(target string) string {
	i := strings.LastIndexByte(target, ':')
	if i < 0 {
		return "other"
	}
	switch port := target[i+1:]; port {
	case "443", "80":
		return port
	default:
		return "other"
	}
}

func (r *traceRing) conn(now time.Time, c traceConn) {
	if !r.connections {
		return
	}
	rec := map[string]any{"type": "conn", "path": c.path, "net": c.network, "outcome": c.outcome,
		"up": sizeBucket(c.up), "down": sizeBucket(c.down), "port": portClass(c.target), "ms": c.ms}
	if c.target != "" {
		host := c.target
		if i := strings.LastIndexByte(host, ':'); i > 0 {
			host = host[:i]
		}
		rec["target"] = r.hash("target:" + host)
	}
	if c.dialMs >= 0 {
		rec["dial_ms"] = c.dialMs
	}
	if c.firstByteMs >= 0 {
		rec["first_byte_ms"] = c.firstByteMs
	}
	r.add(now, rec)
}

// TraceData is what ExportTrace returns: the lines after the number asked
// for, as JSON lines, the number of the last one, how many lines the ring
// dropped since it started, and when the trace started by the wall clock.
type TraceData struct {
	Seq     uint64          `json:"seq"`
	Dropped uint64          `json:"dropped"`
	Started time.Time       `json:"started"`
	Records json.RawMessage `json:"records"`
}

func (r *traceRing) export(after uint64) TraceData {
	r.mu.Lock()
	defer r.mu.Unlock()
	first := r.seq - uint64(len(r.lines)) + 1
	var b bytes.Buffer
	b.WriteByte('[')
	n := 0
	for i, line := range r.lines {
		if first+uint64(i) <= after {
			continue
		}
		if n > 0 {
			b.WriteByte(',')
		}
		b.Write(line)
		n++
	}
	b.WriteByte(']')
	return TraceData{Seq: r.seq, Dropped: r.dropped, Started: r.wall, Records: b.Bytes()}
}

// ExportTrace returns the records of the trace after the number the host has,
// to append to its own trace file. The core keeps the ring as it is: the
// host asks again with the number it got.
func (o *Observer) ExportTrace(after uint64) (TraceData, error) {
	r := o.e.trace
	if r == nil {
		return TraceData{}, NewCommandError("feature_disabled", "trace is disabled")
	}
	return r.export(after), nil
}

// traceEnd writes how a user connection ended, when the trace keeps them.
func (o *Observer) traceEnd(t *connTracker, err error) {
	r := o.e.trace
	if r == nil || !r.connections {
		return
	}
	now := o.nowNano()
	outcome := "empty"
	switch {
	case t.failKind.Load() > 0:
		outcome = failureKind(t.failKind.Load() - 1).String()
	case err != nil && (goerrors.Is(err, context.Canceled) || t.has(reportedAborted)):
		outcome = "user_closed"
	case t.bytesRead.Load() > 0:
		outcome = "ok"
	case err != nil:
		outcome = "closed"
	}
	ms := func(at int64) int64 {
		if at <= 0 {
			return -1
		}
		return max(at-t.started, 0) / int64(time.Millisecond)
	}
	network := "tcp"
	if t.udp {
		network = "udp"
	}
	r.conn(o.clock(), traceConn{path: t.tag, network: network, target: t.target, outcome: outcome,
		dialMs: ms(t.dialed.Load()), firstByteMs: ms(t.firstRead.Load()), ms: ms(now),
		up: t.bytesWritten.Load(), down: t.bytesRead.Load()})
}

// isErrorClass tells a short class such as timeout or balancer exhausted
// from the text of an error.
func isErrorClass(s string) bool {
	if len(s) > 32 {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && c != '_' && c != ' ' {
			return false
		}
	}
	return true
}
