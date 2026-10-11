package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
)

const messagesPerPeer = 500

type messageRecord struct {
	Sequence         uint32 `json:"sequence"`
	SentUnixNano     int64  `json:"sent_unix_ns"`
	ReceivedUnixNano int64  `json:"received_unix_ns"`
}

type messages struct {
	mu         sync.Mutex
	byPeer     map[string][]messageRecord
	identities map[string][]byte
	invalid    map[string]int
}

func (m *messages) add(peer string, payload []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(payload) != 20 || !bytes.Equal(payload[:8], m.identities[peer]) ||
		binary.BigEndian.Uint32(payload[8:12]) >= messagesPerPeer {
		m.invalid[peer]++
		return
	}
	record := messageRecord{
		Sequence:         binary.BigEndian.Uint32(payload[8:12]),
		SentUnixNano:     int64(binary.BigEndian.Uint64(payload[12:])),
		ReceivedUnixNano: time.Now().UnixNano(),
	}
	m.byPeer[peer] = append(m.byPeer[peer], record)
}

func (m *messages) result(peer string) map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	records := append([]messageRecord(nil), m.byPeer[peer]...)
	seen := map[uint32]bool{}
	reordered := 0
	var largest uint32
	jitter := 0.0
	for index, record := range records {
		if seen[record.Sequence] {
			continue
		}
		seen[record.Sequence] = true
		if index > 0 && record.Sequence < largest {
			reordered++
		}
		if record.Sequence > largest {
			largest = record.Sequence
		}
		if index > 0 {
			interval := float64(record.ReceivedUnixNano-records[index-1].ReceivedUnixNano) / 1e6
			if interval > 20 {
				jitter += interval - 20
			} else {
				jitter += 20 - interval
			}
		}
	}
	if len(records) > 1 {
		jitter /= float64(len(records) - 1)
	}
	return map[string]any{"sent": messagesPerPeer, "received_unique": len(seen),
		"lost": messagesPerPeer - len(seen), "reordered": reordered,
		"duplicates": len(records) - len(seen), "invalid": m.invalid[peer],
		"identity_hex":                    hex.EncodeToString(m.identities[peer]),
		"ok":                              len(seen) == messagesPerPeer && m.invalid[peer] == 0,
		"interarrival_jitter_ms_mean_abs": jitter, "records": records}
}

func selectedPair(pc *webrtc.PeerConnection) (map[string]any, error) {
	if pc.SCTP() == nil || pc.SCTP().Transport() == nil || pc.SCTP().Transport().ICETransport() == nil {
		return nil, errors.New("Pion ICE transport missing")
	}
	pair, err := pc.SCTP().Transport().ICETransport().GetSelectedCandidatePair()
	if err != nil {
		return nil, err
	}
	if pair == nil || pair.Local == nil || pair.Remote == nil {
		return nil, errors.New("no selected ICE candidate pair")
	}
	return map[string]any{
		"local_type": pair.Local.Typ.String(), "remote_type": pair.Remote.Typ.String(),
		"local_address": pair.Local.Address, "local_port": pair.Local.Port,
		"remote_address": pair.Remote.Address, "remote_port": pair.Remote.Port,
		"pair": pair.String(),
	}, nil
}

func connected(pc *webrtc.PeerConnection, name string) (<-chan struct{}, <-chan error) {
	ready, failed := make(chan struct{}, 1), make(chan error, 1)
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			select {
			case ready <- struct{}{}:
			default:
			}
		case webrtc.PeerConnectionStateFailed:
			select {
			case failed <- fmt.Errorf("%s peer failed", name):
			default:
			}
		}
	})
	return ready, failed
}

func waitPeer(ready <-chan struct{}, failed <-chan error) error {
	select {
	case <-ready:
		return nil
	case err := <-failed:
		return err
	case <-time.After(60 * time.Second):
		return errors.New("Pion connection timeout")
	}
}

func sendMessages(channel *webrtc.DataChannel, identity []byte) error {
	for sequence := uint32(0); sequence < messagesPerPeer; sequence++ {
		packet := make([]byte, 20)
		copy(packet[:8], identity)
		binary.BigEndian.PutUint32(packet[8:12], sequence)
		binary.BigEndian.PutUint64(packet[12:], uint64(time.Now().UnixNano()))
		if err := channel.Send(packet); err != nil {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

func run(mode, socks, target string, hubA, hubB, turnPort int) (map[string]any, error) {
	if mode != "host" && mode != "relay" {
		return nil, errors.New("mode must be host or relay")
	}
	started := time.Now()
	ledgerA, ledgerB := &routeLedger{}, &routeLedger{}
	networkA, err := newRoutedNet(socks, hubA, mode, ledgerA)
	if err != nil {
		return nil, err
	}
	networkB, err := newRoutedNet(socks, hubB, mode, ledgerB)
	if err != nil {
		return nil, err
	}
	var hub *hostHub
	var turn *turnServer
	if mode == "host" {
		hub, err = newHostHub(hubA, hubB)
		if err != nil {
			return nil, err
		}
		defer hub.Close()
	} else {
		turn, err = newTURNServer(turnPort)
		if err != nil {
			return nil, err
		}
		defer turn.Close()
	}
	var configuration webrtc.Configuration
	if mode == "relay" {
		configuration = webrtc.Configuration{
			ICETransportPolicy: webrtc.ICETransportPolicyRelay,
			ICEServers: []webrtc.ICEServer{{URLs: []string{fmt.Sprintf("turn:%s:%d?transport=udp", target, turnPort)},
				Username: "stage1", Credential: "stage1-secret"}},
		}
	}
	engineA, engineB := webrtc.SettingEngine{}, webrtc.SettingEngine{}
	engineA.SetNet(networkA)
	engineB.SetNet(networkB)
	engineA.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	engineB.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	peerA, err := webrtc.NewAPI(webrtc.WithSettingEngine(engineA)).NewPeerConnection(configuration)
	if err != nil {
		return nil, err
	}
	defer peerA.Close()
	peerB, err := webrtc.NewAPI(webrtc.WithSettingEngine(engineB)).NewPeerConnection(configuration)
	if err != nil {
		return nil, err
	}
	defer peerB.Close()
	readyA, failedA := connected(peerA, "A")
	readyB, failedB := connected(peerB, "B")
	records := &messages{byPeer: map[string][]messageRecord{}, identities: map[string][]byte{}, invalid: map[string]int{}}
	for _, direction := range []string{"a_to_b", "b_to_a"} {
		identity := make([]byte, 8)
		if _, err = rand.Read(identity); err != nil {
			return nil, err
		}
		records.identities[direction] = identity
	}
	peerA.OnDataChannel(func(channel *webrtc.DataChannel) {
		channel.OnMessage(func(message webrtc.DataChannelMessage) { records.add("b_to_a", message.Data) })
	})
	peerB.OnDataChannel(func(channel *webrtc.DataChannel) {
		channel.OnMessage(func(message webrtc.DataChannelMessage) { records.add("a_to_b", message.Data) })
	})
	aToB, err := peerA.CreateDataChannel("a-to-b", nil)
	if err != nil {
		return nil, err
	}
	bToA, err := peerB.CreateDataChannel("b-to-a", nil)
	if err != nil {
		return nil, err
	}
	openA, openB := make(chan struct{}, 1), make(chan struct{}, 1)
	aToB.OnOpen(func() { openA <- struct{}{} })
	bToA.OnOpen(func() { openB <- struct{}{} })
	offer, err := peerA.CreateOffer(nil)
	if err != nil {
		return nil, err
	}
	completeA := webrtc.GatheringCompletePromise(peerA)
	if err = peerA.SetLocalDescription(offer); err != nil {
		return nil, err
	}
	select {
	case <-completeA:
	case <-time.After(45 * time.Second):
		return nil, errors.New("peer A ICE gathering timeout")
	}
	if err = peerB.SetRemoteDescription(*peerA.LocalDescription()); err != nil {
		return nil, err
	}
	answer, err := peerB.CreateAnswer(nil)
	if err != nil {
		return nil, err
	}
	completeB := webrtc.GatheringCompletePromise(peerB)
	if err = peerB.SetLocalDescription(answer); err != nil {
		return nil, err
	}
	select {
	case <-completeB:
	case <-time.After(45 * time.Second):
		return nil, errors.New("peer B ICE gathering timeout")
	}
	if err = peerA.SetRemoteDescription(*peerB.LocalDescription()); err != nil {
		return nil, err
	}
	if err = waitPeer(readyA, failedA); err != nil {
		return nil, err
	}
	if err = waitPeer(readyB, failedB); err != nil {
		return nil, err
	}
	select {
	case <-openA:
	case <-time.After(15 * time.Second):
		return nil, errors.New("A data channel closed")
	}
	select {
	case <-openB:
	case <-time.After(15 * time.Second):
		return nil, errors.New("B data channel closed")
	}
	established := time.Since(started)
	pairA, err := selectedPair(peerA)
	if err != nil {
		return nil, err
	}
	pairB, err := selectedPair(peerB)
	if err != nil {
		return nil, err
	}
	expectedType := mode
	if pairA["local_type"] != expectedType || pairA["remote_type"] != expectedType ||
		pairB["local_type"] != expectedType || pairB["remote_type"] != expectedType {
		return nil, fmt.Errorf("selected candidate pair type differs from %s: A=%v B=%v", mode, pairA, pairB)
	}
	transferStarted := time.Now().UnixNano()
	samples := []map[string]any{}
	snapshot := func() error {
		a, pairErr := selectedPair(peerA)
		if pairErr != nil {
			return pairErr
		}
		b, pairErr := selectedPair(peerB)
		if pairErr != nil {
			return pairErr
		}
		if a["pair"] != pairA["pair"] || b["pair"] != pairB["pair"] ||
			a["local_type"] != mode || a["remote_type"] != mode ||
			b["local_type"] != mode || b["remote_type"] != mode {
			return errors.New("selected pair changed during application transfer")
		}
		samples = append(samples, map[string]any{"at_unix_ns": time.Now().UnixNano(), "a": a, "b": b})
		return nil
	}
	if err = snapshot(); err != nil {
		return nil, err
	}
	sent := make(chan error, 2)
	go func() {
		err := sendMessages(aToB, records.identities["a_to_b"])
		sent <- err
		if err == nil {
			sent <- sendMessages(bToA, records.identities["b_to_a"])
		}
	}()
	timer := time.NewTimer(45 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	sentPeers := 0
	for {
		select {
		case sendErr := <-sent:
			if sendErr != nil {
				return nil, sendErr
			}
			sentPeers++
		case err = <-failedA:
			return nil, err
		case err = <-failedB:
			return nil, err
		case <-timer.C:
			return nil, fmt.Errorf("DataChannel delivery timeout: a_to_b=%v b_to_a=%v",
				records.result("a_to_b")["received_unique"], records.result("b_to_a")["received_unique"])
		case <-ticker.C:
			if err = snapshot(); err != nil {
				return nil, err
			}
		}
		if sentPeers == 2 && records.result("a_to_b")["ok"].(bool) && records.result("b_to_a")["ok"].(bool) {
			break
		}
	}
	if err = snapshot(); err != nil {
		return nil, err
	}
	transferEnded := time.Now().UnixNano()
	routesA, routesB := ledgerA.result(), ledgerB.result()
	transferRoutesA := applicationRoutes(routesA, transferStarted, transferEnded)
	transferRoutesB := applicationRoutes(routesB, transferStarted, transferEnded)
	expectedDestination := func(pair map[string]any) string {
		if mode == "relay" {
			return net.JoinHostPort(target, fmt.Sprint(turnPort))
		}
		return net.JoinHostPort(fmt.Sprint(pair["remote_address"]), fmt.Sprint(pair["remote_port"]))
	}
	routeAOK := applicationRouteProved(transferRoutesA, expectedDestination(pairA))
	routeBOK := applicationRouteProved(transferRoutesB, expectedDestination(pairB))
	if !routeAOK || !routeBOK {
		return nil, errors.New("both peers' application DTLS traffic through selected routes not proved")
	}
	result := map[string]any{
		"mode": mode, "setup_ms": established.Milliseconds(), "selected_pair": pairA,
		"selected_pair_b": pairB, "peer_a": routesA, "peer_b": routesB,
		"a_to_b": records.result("a_to_b"), "b_to_a": records.result("b_to_a"),
		"transfer_started_unix_ns": transferStarted, "transfer_ended_unix_ns": transferEnded,
		"selected_pairs_during_transfer": samples,
		"application_routes_a":           transferRoutesA, "application_routes_b": transferRoutesB,
		"application_route_proved": routeAOK && routeBOK,
		"route_topology":           "Pion transport.Net -> SOCKS UDP association -> selected Xray outbound -> controlled target",
	}
	if mode == "host" {
		result["host_forwarder"] = hub.result()
		result["host_scope"] = "host candidates over controlled two-port forwarder; no NAT or Internet claim"
	} else {
		allocations := turn.server.AllocationCount()
		result["turn_allocations"] = allocations
		result["turn_destination"] = net.JoinHostPort(target, fmt.Sprint(turnPort))
		result["turn_route_proved"] = allocations >= 2 &&
			hasDestination(routesA, turnPort) && hasDestination(routesB, turnPort)
		if !result["turn_route_proved"].(bool) {
			return nil, errors.New("TURN allocations or both peer routes missing")
		}
	}
	return result, nil
}

func applicationRoutes(routes map[string]any, started, ended int64) []routeEvent {
	result := []routeEvent{}
	for _, event := range routes["events"].([]routeEvent) {
		if event.AtUnixNano >= started && event.AtUnixNano <= ended && event.ApplicationData {
			result = append(result, event)
		}
	}
	return result
}

func applicationRouteProved(events []routeEvent, destination string) bool {
	sent, received := false, false
	for _, event := range events {
		if event.Destination != destination || !event.ApplicationData ||
			(event.Direction != "send" && event.Direction != "recv") {
			return false
		}
		sent = sent || event.Direction == "send"
		received = received || event.Direction == "recv"
	}
	return sent && received
}

func hasDestination(routes map[string]any, port int) bool {
	for _, event := range routes["events"].([]routeEvent) {
		if event.Direction == "send" && strings.HasSuffix(event.Destination, fmt.Sprintf(":%d", port)) {
			return true
		}
	}
	return false
}

func main() {
	mode := flag.String("mode", "", "host or relay")
	socks := flag.String("socks", "", "Xray SOCKS address")
	target := flag.String("target", "", "target address visible through selected Xray path")
	hubA := flag.Int("hub-a", 0, "host candidate port A")
	hubB := flag.Int("hub-b", 0, "host candidate port B")
	turnPort := flag.Int("turn", 0, "TURN port")
	output := flag.String("output", "", "result JSON outside Git")
	flag.Parse()
	if *socks == "" || *target == "" || *output == "" || *hubA == 0 || *hubB == 0 || *turnPort == 0 {
		fmt.Fprintln(os.Stderr, "all route, port and output arguments are required")
		os.Exit(2)
	}
	result, err := run(*mode, *socks, *target, *hubA, *hubB, *turnPort)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	bytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err = os.WriteFile(*output, bytes, 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
