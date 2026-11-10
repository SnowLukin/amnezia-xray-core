package health

import (
	"bytes"
	"context"
	"errors"
	"google.golang.org/protobuf/proto"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
)

type qualityDecisionTrace struct {
	events     []byte
	choice     string
	downs      []downMark
	jobs       []probeJob
	quarantine time.Time
}

// Exercise the actual connection hooks with the same clock and controller RNG
// on each side. Very slow bulk data, large loaded RTT and many socket repeats
// must not become controller evidence. A later real dial failure still does.
func qualityDecisionRun(t *testing.T, skip, skipProbe bool) qualityDecisionTrace {
	t.Helper()
	cfg := &Config{Features: &FeatureConfig{Quality: proto.Bool(!skip), Report: proto.Bool(true), Events: proto.Bool(true)},
		Schedule:     tune(&ScheduleConfig{}, scheduleKnobs{idleAfter: time.Hour}),
		Connectivity: &ConnectivityConfig{Outbound: "net", Destinations: []string{"https://x.test/204"}, Witnesses: []string{"direct"}},
	}
	te := newTestEngine(t, cfg, &SelectorConfig{Paths: pathSpecs("a1", 1, "b1", 3), Features: probeFirst(!skipProbe)}, "a1", "b1")
	o, clk := newTestObserver()
	o.s, o.e = te.s, te.engine
	o.e.report.begin(clk.now())
	if !skip {
		o.e.quality = newQualitySummary(clk.now())
	}
	o.qualityInfo = func(syscall.RawConn) tcpQualitySnapshot {
		return tcpQualitySnapshot{rtt: 2 * time.Second, minRTT: 10 * time.Millisecond, loadRTT: 7 * time.Second,
			rxOutOfOrder: 90, rxTotal: 100, txRetransmit: 900, txTotal: 1000,
			present: qualityHasRTT | qualityHasMinRTT | qualityHasLoadRTT | qualityHasRX | qualityHasTX}
	}
	tr, read, write := qualityOpenPath(o, clk, qualityTestConn{raw: &qualityTestRawConn{}}, 300*time.Millisecond)
	write.Add(100)
	clk.advance(500 * time.Millisecond)
	read.Add(1024)
	clk.advance(30 * time.Second)
	read.Add((1 << 20) - 1024)
	clk.advance(30 * time.Second)
	read.Add(3 << 20)
	tr.Finished(nil)
	w := o.StartDial(context.Background(), "direct", net.Destination{}, nil)
	wr, ww := w.Dialed(qualityTestConn{raw: &qualityTestRawConn{}}, nil, nil, nil)
	if ww != nil {
		ww.Add(100)
	}
	wr.Add(1024)
	clk.advance(time.Second)
	wr.Add(2 << 20)
	w.Finished(nil)
	if te.path("a1").status != statusUp || te.sel.current() != "a1" {
		t.Fatal("quality-looking traffic changed the healthy path")
	}
	if !skip {
		current := o.e.quality.currentView(clk.now())
		v := qualitySummaryPath(t, current, "a1")
		if v.BulkCount != 1 || v.TCPCount != 1 || v.BulkKbpsMax == nil || *v.BulkKbpsMax >= 1000 ||
			v.BulkRTTP50Ms == nil || *v.BulkRTTP50Ms != 8000 || v.TXRetransmitFraction == nil || *v.TXRetransmitFraction != 0.9 {
			t.Fatal("enabled comparison did not actually collect the adverse measurements")
		}
		if current.Witness == nil || current.Witness.TTFBCount != 1 || current.Witness.DialCount != 1 ||
			current.Witness.TCPCount != 1 || current.Witness.BulkCount != 1 {
			t.Fatal("enabled comparison did not collect its configured witness")
		}
	} else if o.e.quality != nil {
		t.Fatal("disabled comparison retained a summary")
	}
	for range 3 {
		clk.advance(time.Second)
		failed := o.StartDial(context.Background(), "a1", net.TCPDestination(net.DomainAddress("target.test"), 443), nil)
		failed.Dialed(nil, errors.New("connection refused"), nil, nil)
		failed.Finished(nil)
	}
	if _, pending := te.pendingSuspicion["a1"]; te.path("a1").status != statusSuspect || !pending {
		t.Fatal("real dial failures did not request the normal connectivity proof")
	}
	jobs := te.run(t, clk.now(), map[string]bool{"a1": false, "b1": true})
	if te.path("a1").status != statusDown {
		t.Fatal("real dial failures with successful connectivity proof did not mark the path down")
	}
	if te.sel.current() != "b1" {
		t.Fatalf("real failover selected %q, want b1", te.sel.current())
	}
	var copied []probeJob
	for _, j := range jobs {
		copied = append(copied, *j)
	}
	events, _ := te.events.drain()
	return qualityDecisionTrace{events: events, choice: te.sel.current(), downs: te.takeDowns(), jobs: copied, quarantine: te.path("a1").quarantineUntil}
}

func TestQualitySignalsPreserveDecisionsAndFailoverTime(t *testing.T) {
	for _, skipProbe := range []bool{false, true} {
		t.Run(map[bool]string{false: "probe_before_switch", true: "switch_without_probe"}[skipProbe], func(t *testing.T) {
			off, on := qualityDecisionRun(t, true, skipProbe), qualityDecisionRun(t, false, skipProbe)
			if !bytes.Equal(off.events, on.events) || off.choice != on.choice || !reflect.DeepEqual(off.downs, on.downs) ||
				!reflect.DeepEqual(off.jobs, on.jobs) || !off.quarantine.Equal(on.quarantine) {
				t.Fatalf("quality changed controller events/timestamps, choices, downs, probes or quarantine:\noff=%+v\non=%+v", off, on)
			}
		})
	}
}
