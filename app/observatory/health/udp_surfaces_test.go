package health

import (
	"encoding/json"
	"testing"
	"time"
)

func TestUDPStatusProviderAndEveryNetworkChangeResets(t *testing.T) {
	for _, mode := range []NetworkChangeMode{NetworkChangeMode_RECHECK, NetworkChangeMode_FORGET, NetworkChangeMode_IGNORE} {
		te := newTestEngine(t, nil, &SelectorConfig{NetworkChangeAction: mode}, "a1")
		p := te.paths["a1"]
		now := t0.Add(40 * time.Second)
		if got := te.status(now).Paths[0].UDP; got.State != "unknown" || got.Answered != 0 || got.AgeMs != 0 {
			t.Fatalf("unknown UDP status %+v", got)
		}
		if got := describeOutbound(p, "a1", 0, now); got.UDP != nil {
			t.Fatal("provider received UDP without a probe")
		}
		p.udp = udpSnapshot{state: "dead", sent: 5, at: t0, reason: reasonUDPSuspect}
		status := te.status(now).Paths[0].UDP
		if status.State != "dead" || status.Sent != 5 || status.Reason != "udp_suspect" || status.AgeMs != 40000 {
			t.Fatalf("UDP status %+v", status)
		}
		encoded, err := json.Marshal(describeOutbound(p, "a1", 0, now))
		if err != nil {
			t.Fatal(err)
		}
		var outbound map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &outbound); err != nil {
			t.Fatal(err)
		}
		if string(outbound["udp"]) != `{"state":"dead","rttMs":0,"agoMs":40000}` {
			t.Fatalf("provider UDP encoding %s", outbound["udp"])
		}
		p.udpEvidence = []udpEvidence{{at: t0, target: "private.example:53"}}
		generation := te.generation
		te.onNetworkChange("", now)
		if p.statusUDP(now).State != "unknown" || describeOutbound(p, "a1", 0, now).UDP != nil || len(p.udpEvidence) != 0 {
			t.Fatalf("network action %v retained UDP observation", mode)
		}
		if mode == NetworkChangeMode_IGNORE && te.generation != generation {
			t.Fatal("UDP reset changed the legacy IGNORE generation")
		}
		p.udp = udpSnapshot{state: "ok", answered: 5, sent: 5, at: now, rtt: 84 * time.Millisecond, jitter: 6 * time.Millisecond, reason: reasonUDPSwitch}
		te.onNetworkChange("", now.Add(time.Second))
		if p.statusUDP(now).State != "unknown" {
			t.Fatal("same empty network id retained UDP observation")
		}
	}
}

func TestUDPReportCountsSurviveDrainAndOverflowWithoutDecision(t *testing.T) {
	te := newTestEngine(t, &Config{Events: &EventsConfig{MaxEvents: 1}}, nil, "a1")
	te.events.drain()
	before := te.report.decisions
	for _, item := range []struct {
		state  string
		reason string
		index  int
	}{
		{"ok", "udp_switch", 0}, {"lossy", "host", 1}, {"dead", "udp_suspect", 1}, {"unsupported", "host", 0},
	} {
		te.emit(t0, "udp_probe", map[string]any{"outbound": "a1", "state": item.state, "reason": item.reason, "destination_index": item.index})
	}
	te.emit(t0, "udp_suspect", map[string]any{"outbound": "a1", "flows": 3, "targets": 2, "window_ms": 60000})
	te.events.drain()
	c := te.report.counts
	if c.UDPProbeCount != 4 || c.UDPProbeOKCount != 1 || c.UDPProbeLossyCount != 1 || c.UDPProbeDeadCount != 1 || c.UDPProbeSecondDestinationCount != 2 || c.UDPSuspectCount != 1 || c.UDPSuspectConfirmedCount != 1 {
		t.Fatalf("UDP report counts %+v", c)
	}
	if c.Probes != 0 || c.HostProbes != 0 || te.report.decisions != before || te.report.decided {
		t.Fatal("UDP observations counted as TCP probes or decisions")
	}
	if te.events.droppedTotal == 0 {
		t.Fatal("fixture did not overflow")
	}
	if te.takeReport(false, at(time.Hour)) != nil {
		t.Fatal("UDP observations made an interval report due")
	}
}

func TestUDPReportProbeCountersAndMediansBelongToWindow(t *testing.T) {
	te := newTestEngine(t, &Config{Report: &ReportConfig{Interval: int64(time.Second)}}, nil, "a1")
	p := te.paths["a1"]
	p.stats.udpProbes = 2
	p.stats.udpProbesDead = 1
	p.stats.udpRTT.add(80 * time.Millisecond)
	p.stats.udpJitter.add(6 * time.Millisecond)
	p.udp = udpSnapshot{state: "dead", at: t0, reason: reasonHost}
	te.report.decide()
	snapshot := te.snapshotReport(at(2 * time.Second))
	if snapshot.Paths["a1"].UDPProbes != 2 || p.stats.udpRTT.total != 1 {
		t.Fatal("snapshot consumed UDP probe samples")
	}
	first := te.takeReport(false, at(2*time.Second))
	if first == nil {
		t.Fatal("missing decision report")
	}
	got := first.Paths["a1"]
	if got.UDPProbes != 2 || got.UDPProbesDead != 1 || got.UDPRTTP50Ms == 0 || got.UDPJitterP50Ms == 0 || got.UDPState != "dead" {
		t.Fatalf("first UDP report %+v", got)
	}
	if p.stats.udpRTT.total != 0 || p.stats.udpJitter.total != 0 {
		t.Fatal("taken report retained UDP histogram samples")
	}
	if p.statusUDP(at(3*time.Second)).State != "dead" || describeOutbound(p, "a1", 0, at(3*time.Second)).UDP == nil {
		t.Fatal("report reset the latest UDP snapshot")
	}
	p.stats.udpProbes++
	p.stats.udpRTT.add(50 * time.Millisecond)
	p.stats.udpJitter.add(time.Millisecond)
	p.udp = udpSnapshot{state: "ok", at: at(3 * time.Second), reason: reasonHost}
	te.report.decide()
	second := te.takeReport(false, at(4*time.Second))
	if second.Paths["a1"].UDPProbes != 1 || second.Paths["a1"].UDPProbesDead != 0 || second.Paths["a1"].UDPState != "ok" {
		t.Fatalf("second UDP report %+v", second.Paths["a1"])
	}
}
