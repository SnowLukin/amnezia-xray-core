package health

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"
)

func qualityMemoryModel(paths, segments int, longKeys, witness bool) *qualitySummary {
	now := t0
	q := newQualitySummary(now)
	initial := qualityInitialSample{hasTTFB: true, ttfb: 50 * time.Millisecond, hasDial: true, dial: 20 * time.Millisecond,
		tcp: tcpQualitySnapshot{rtt: 30 * time.Millisecond, minRTT: 10 * time.Millisecond, present: qualityHasRTT | qualityHasMinRTT}}
	end := qualityEndSample{hasBytes: true, bytesDown: 4 << 20, hasBulk: true, bulkKbps: 2000,
		s1:      tcpQualitySnapshot{rxTotal: 1, present: qualityHasRX},
		s2:      tcpQualitySnapshot{loadRTT: 100 * time.Millisecond, rxOutOfOrder: 1, rxTotal: 1001, present: qualityHasRX | qualityHasLoadRTT},
		lastTCP: tcpQualitySnapshot{txTotal: 1000, present: qualityHasTX}}
	for segment := 0; segment < segments; segment++ {
		count, keyLength := paths, qualityCollectionTagBytes/qualityCollectionPaths
		if longKeys && segment+1 < segments {
			// Each closed segment can retain its full key budget in only
			// four winners. The current/open scopes then admit all 64 tags.
			count, keyLength = qualitySegmentPaths, qualityCollectionTagBytes/qualitySegmentPaths
		}
		for path := 0; path < count; path++ {
			tag := fmt.Sprintf("s%d-p%02d", segment, path)
			if longKeys {
				tag += strings.Repeat("x", keyLength-len(tag))
			}
			q.addInitial(q.epoch, tag, false, initial)
			q.addEnd(q.epoch, tag, false, end)
		}
		if witness {
			q.addInitial(q.epoch, "", true, initial)
			q.addEnd(q.epoch, "", true, end)
		}
		now = now.Add(time.Minute)
		if segment+1 < segments {
			q.networkChanged("cell:25001:private-memory-fixture", false, now)
		}
	}
	return q
}

// Run this diagnostic alone in a fresh non-race test process. GC/live heap
// differences over a fixed 128 retained models report actual allocation cost;
// they are not a noisy whole-core heap threshold or an estimate from sizeof.
func TestQualitySummaryRetainedMemory(t *testing.T) {
	const copies = 128
	for _, model := range []struct {
		name              string
		paths, segments   int
		longKeys, witness bool
	}{
		{"typical_one_path", 1, 1, false, false},
		{"current64_and_three_closed_full_key_budgets_with_witness", 64, 4, true, true},
	} {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		held := make([]*qualitySummary, copies)
		for i := range held {
			held[i] = qualityMemoryModel(model.paths, model.segments, model.longKeys, model.witness)
		}
		q := held[0]
		if len(q.current.paths) != model.paths || len(q.open.paths) != model.paths || len(q.closed) != model.segments-1 {
			t.Fatal("retained memory fixture omitted live histories or scalar closed segments")
		}
		if model.longKeys {
			keys := q.current.tagBytes + q.open.tagBytes
			for _, segment := range q.closed {
				for tag := range segment.Paths {
					keys += len(tag)
				}
			}
			if keys != 5*qualityCollectionTagBytes {
				t.Fatalf("retained key budget fixture incomplete: %d bytes", keys)
			}
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		delta := int64(after.HeapAlloc) - int64(before.HeapAlloc)
		t.Logf("%s: retained models=%d heap_delta=%d bytes mean=%.1f bytes/model; maps/keys/scalars included, process/samplers excluded", model.name, copies, delta, float64(delta)/copies)
		runtime.KeepAlive(held)
	}
	t.Logf("sizeof: summary=%d accumulator=%d connectionQuality=%d bytes (layout only; not heap)", unsafe.Sizeof(qualitySummary{}), unsafe.Sizeof(qualityAccumulator{}), unsafe.Sizeof(connectionQuality{}))
}

func BenchmarkQualitySummaryBuildTypical(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		q := qualityMemoryModel(1, 1, false, false)
		runtime.KeepAlive(q)
	}
}

func BenchmarkQualitySummaryBuildBounded(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		q := qualityMemoryModel(64, 4, true, true)
		runtime.KeepAlive(q)
	}
}
