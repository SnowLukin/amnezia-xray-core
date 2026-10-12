package health

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"
)

// JSON escapes an ASCII control byte as six bytes. The normal short-tag size
// test therefore cannot stand for the worst wire size of a 16 KiB tag budget.
// Fix this separate bound before execution: four segments at six times the
// key budget plus their scalar measurements must fit within 512 KiB.
func TestQualitySummaryEscapedTagWireSize(t *testing.T) {
	now := t0
	q := newQualitySummary(now)
	for segment := range qualityReportSegments {
		for path := range qualitySegmentPaths {
			prefix := fmt.Sprintf("%d-%d-", segment, path)
			tag := prefix + strings.Repeat("\x00", qualityCollectionTagBytes/qualitySegmentPaths-len(prefix))
			q.addInitial(q.epoch, tag, false, qualityInitialSample{hasTTFB: true, ttfb: time.Millisecond, hasDial: true, dial: time.Millisecond})
			q.addEnd(q.epoch, tag, false, qualityEndSample{hasBytes: true, bytesDown: int64(path + 1), hasBulk: true, bulkKbps: 2000})
		}
		now = now.Add(30 * time.Second)
		if segment+1 < qualityReportSegments {
			q.networkChanged("wifi:private-wire-size-fixture", false, now)
		}
	}
	views, dropped := q.report(now, false)
	if len(views) != qualityReportSegments || dropped != 0 {
		t.Fatal("wire-size fixture omitted its four populated segments")
	}
	for _, v := range views {
		if len(v.Paths) != qualitySegmentPaths || v.PathsDropped != 0 || v.SamplesDropped != 0 {
			t.Fatal("wire-size fixture omitted admitted full-budget tags")
		}
	}
	raw, err := json.Marshal(views)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("full 16KiB control-byte tag budget in each of four segments: JSON=%d bytes", len(raw))
	if len(raw) < 300*1024 || len(raw) > 512*1024 {
		t.Fatalf("escaped-tag fixture/bound changed: %d bytes", len(raw))
	}
	if strings.Contains(string(raw), "private-wire-size-fixture") {
		t.Fatal("quality wire payload contains the private network suffix")
	}
}
