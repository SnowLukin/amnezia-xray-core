package health

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func udpHostConfig() *Config {
	c := hostConfig(nil)
	c.UdpProbe = &UDPProbeConfig{Destinations: []string{"stun.test:3478"}}
	return c
}

func TestUDPHostRequestKindsDoNotJoinOrAnswerEachOther(t *testing.T) {
	te := newTestEngine(t, udpHostConfig(), &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}, "a1", "b1")
	tcp, _, err := te.requestProbe("auto", []string{"a1"}, at(0))
	if err != nil {
		t.Fatal(err)
	}
	udp, _, err := te.requestProbeKind("auto", []string{"a1"}, true, at(0))
	if err != nil {
		t.Fatal(err)
	}
	if tcp == udp {
		t.Fatal("UDP joined TCP ticket")
	}
	joined, _, err := te.requestProbeKind("auto", []string{"a1"}, true, at(time.Second))
	if err != nil || joined != udp {
		t.Fatalf("%d %v", joined, err)
	}
	if len(te.queue) != 2 {
		t.Fatalf("queue=%+v", te.queue)
	}
	var udpJob, tcpJob *probeJob
	for _, j := range te.queue {
		if j.isUDP {
			udpJob = j
		} else {
			tcpJob = j
		}
	}
	te.answerRequests(tcpJob, probeResult{ok: true}, at(time.Second))
	ur := te.request(t, udp)
	if !ur.open() || ur.items[0].result != probePending {
		t.Fatalf("TCP answered UDP: %+v", ur)
	}
	te.answerUDPRequests(udpJob, udpProbeResult{state: "lossy", answered: 2, sent: 5, rtt: time.Millisecond}, at(2*time.Second))
	if ur.open() || ur.items[0].udp == nil || ur.items[0].udp.State != "lossy" {
		t.Fatalf("%+v", ur)
	}
	raw, _ := json.Marshal(ur.view(at(2 * time.Second)))
	if !strings.Contains(string(raw), `"is_udp":true`) || !strings.Contains(string(raw), `"udp":{"state":"lossy"`) {
		t.Fatal(string(raw))
	}
}

func TestUDPHostOnlyReusesUDPResult(t *testing.T) {
	te := newTestEngine(t, udpHostConfig(), &SelectorConfig{Paths: pathSpecs("a", 1)}, "a1")
	te.onSuccess("a1", at(0))
	id, _, err := te.requestProbeKind("auto", []string{"a1"}, true, at(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if r := te.request(t, id); !r.open() || r.items[0].source == "recent" {
		t.Fatalf("TCP success reused: %+v", r)
	}
	te.expireUDPRequests("a1", "replacement", at(2*time.Second))
	te.dropUnclaimedHost()
	p := te.path("a1")
	p.udp = udpSnapshot{state: "dead", sent: 5, at: at(3 * time.Second), reason: reasonHost}
	id, _, err = te.requestProbeKind("auto", []string{"a1"}, true, at(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	r := te.request(t, id)
	if r.open() || r.items[0].result != "dead" || r.items[0].udp == nil || r.items[0].source != "recent" {
		t.Fatalf("%+v", r)
	}
}

func TestUDPConfigDefaultsAndDisabledEvidence(t *testing.T) {
	s := newSettings(&Config{})
	if s.udpProbe != nil || s.passive.udpReplyTimeout != 0 {
		t.Fatalf("%+v", s)
	}
	c := &Config{Features: &FeatureConfig{UdpProbes: v2Bool(true), UdpReplies: v2Bool(false)}, UdpProbe: &UDPProbeConfig{Destinations: []string{"stun.test:3478"}}, Passive: &PassiveConfig{UdpReplyTimeout: -1}}
	if err := c.ValidateUDP(); err != nil {
		t.Fatal(err)
	}
	s = newSettings(c)
	u := s.udpProbe
	if u.requests != 5 || u.interval != 200*time.Millisecond || u.timeout != 2*time.Second || u.perHour != 6 || s.passive.udpReplyTimeout != 0 || !u.destinations[0].Address.Family().IsDomain() {
		t.Fatalf("%+v", s)
	}
	c.UdpProbe.Requests = -1
	if c.ValidateUDP() == nil {
		t.Fatal("negative requests accepted")
	}
}

func TestUDPHostRejectsProbeOnlyWithoutTicket(t *testing.T) {
	sc := &SelectorConfig{Paths: pathSpecs("a", 1, "b", 3)}
	sc.Paths[1].IsProbeOnly = true
	te := newTestEngine(t, udpHostConfig(), sc, "a1", "b1")
	if _, _, err := te.requestProbeKind("auto", []string{"b1"}, true, at(0)); err == nil {
		t.Fatal("accepted probe-only UDP")
	}
	if len(te.requests) > 0 {
		t.Fatal("rejected request allocated ticket")
	}
	var r ProbeRequest
	if err := json.Unmarshal([]byte(`{"balancer":"auto","isUdp":true}`), &r); err != nil || !r.IsUDP {
		t.Fatalf("%+v %v", r, err)
	}
}
