package health

// Integrated fixture reconciled with accepted Stage 1 cf3b4a8d and the
// 11 October quality spec. Root owns compilation and runtime checks.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Match the serialized string width, including encoding/json escaping of
// control bytes, HTML-sensitive characters and U+2028. Quotes and field syntax
// remain part of the fixed metadata. This normalizes fixture accounting only;
// no request value is truncated or rewritten.
func qualityProviderRichStringBytes(t *testing.T, value string) int {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return len(encoded) - 2
}

// The fixture's caller-chosen variable text appears in these specific fields.
// A selected tag also appears in balancer.current and must be counted twice.
// A degraded reason repeats the first failed caller-chosen step name.
// Provider/balancer names and baseVersion are fixed/empty in this fixture.
func qualityProviderRichFixedBytes(t *testing.T, body []byte, r providerRequest) int {
	t.Helper()
	chosen := qualityProviderRichStringBytes(t, r.Version)
	chosen += qualityProviderRichStringBytes(t, r.Balancer.Current)
	for _, outbound := range r.Outbounds {
		chosen += qualityProviderRichStringBytes(t, outbound.Tag)
		chosen += qualityProviderRichStringBytes(t, outbound.Reason)
		if outbound.Probe == nil {
			continue
		}
		if outbound.Probe.Profile != nil {
			chosen += qualityProviderRichStringBytes(t, *outbound.Probe.Profile)
		}
		if outbound.Probe.Steps != nil {
			for _, step := range *outbound.Probe.Steps {
				chosen += qualityProviderRichStringBytes(t, step.Name)
			}
		}
	}
	if chosen > len(body) {
		t.Fatalf("normalized text exceeds serialized body: %d > %d", chosen, len(body))
	}
	return len(body) - chosen
}

func TestQualityProviderRichCombinedBodyBudget(t *testing.T) {
	// Preserve TestRequestBodyBounded's 64 KiB and the existing quality-only
	// 128 KiB fixture. Four raw steps add separately reviewed fixed schema.
	const qualityBudget = 128 << 10
	const historicalEnvelopeBytes = 44
	const envelopeBytes = 70
	const stepBytes = 160
	const stepsPerSnapshot = 4
	const snapshotDelta = 128 * (envelopeBytes + stepsPerSnapshot*(stepBytes+1))
	// Keep the original 214 KiB ceiling. Stage 1 now counts its bounded
	// scheduler reason as fixed metadata; caller text normalization never
	// removes these bytes. The final combined fixture must fit this ceiling.
	const combinedFixedBudget = qualityBudget + 128*(historicalEnvelopeBytes+stepsPerSnapshot*(stepBytes+1))
	if snapshotDelta != 91392 || combinedFixedBudget != 219136 || maxProviderOutbounds != 128 || qualityCollectionPaths != 64 {
		t.Fatal("review rich combined metadata budget when collection limits change")
	}
	destination := 3
	metadata := requestProbeStep{IsPassed: false, ConnectMs: math.MinInt64, Ms: math.MinInt64, Bytes: math.MinInt64,
		Destination: &destination, Error: "server_unverified"}
	encoded, err := json.Marshal(metadata)
	if err != nil || len(encoded) != stepBytes {
		t.Fatalf("raw step metadata: %d bytes, want %d: %s (%v)", len(encoded), stepBytes, encoded, err)
	}
	emptyProfile := ""
	emptySteps := []requestProbeStep{}
	envelope := struct {
		Profile *string             `json:"profile"`
		Reason  string              `json:"reason"`
		State   string              `json:"state"`
		Steps   *[]requestProbeStep `json:"steps"`
	}{&emptyProfile, reasonNetwork.String(), "degraded", &emptySteps}
	encoded, err = json.Marshal(envelope)
	if err != nil || len(encoded) != envelopeBytes {
		t.Fatalf("raw snapshot envelope: %d bytes, want %d (%v)", len(encoded), envelopeBytes, err)
	}

	escaped := "長é\"\\\n<>&\u2028"
	shortFixed := -1
	for _, tc := range []struct {
		name, tagText, snapshotText string
	}{
		{"short", "", ""},
		// Raw tag bytes remain within the 16 KiB admission budget for all
		// 64 measured tags. Profile/step names have no such width bound.
		{"escaped", strings.Repeat(escaped, 8), strings.Repeat(escaped, 1024)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tags := make([]string, 0, maxProviderOutbounds)
			ownTags := make(map[string]int, maxProviderOutbounds)
			for i := 0; i < maxProviderOutbounds; i++ {
				short := fmt.Sprintf("node-%03d-%s", i, tc.tagText)
				tags = append(tags, "p-"+short)
				ownTags[short] = i
			}
			te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: pathSpecs("p-", 1)}, tags...)
			te.applyProviderSet(te.prov(), providerChange{tags: tags}, t0)
			const networkID = "cell:25001:QUALITY_PRIVATE_FINGERPRINT"
			te.networkID = networkID
			te.networks[networkID] = map[string]string{}
			te.quality = newQualitySummary(t0)
			te.qualityEpoch = te.quality.networkChanged(networkID, true, t0)
			results := []probeStepResult{
				{name: tc.snapshotText + "transport", ok: true, connect: 820 * time.Millisecond, elapsed: 1400 * time.Millisecond, bytes: 32768, destination: 0},
				{name: tc.snapshotText + "optional", connect: 321 * time.Millisecond, elapsed: 654 * time.Millisecond, bytes: 12345, destination: -1, class: "server_unverified"},
				{name: tc.snapshotText + "download", ok: true, connect: time.Duration(math.MaxInt64), elapsed: time.Duration(math.MaxInt64), bytes: math.MaxInt64, destination: 3},
				{name: tc.snapshotText + "optional-body", connect: 7 * time.Millisecond, elapsed: 999 * time.Millisecond, bytes: math.MinInt64, destination: -1, class: "short_body"},
			}
			zero, three := 0, 3
			wantSteps := []requestProbeStep{
				{Name: results[0].name, IsPassed: true, ConnectMs: 820, Ms: 1400, Bytes: 32768, Destination: &zero},
				{Name: results[1].name, ConnectMs: 321, Ms: 654, Bytes: 12345, Error: "server_unverified"},
				{Name: results[2].name, IsPassed: true, ConnectMs: math.MaxInt64 / int64(time.Millisecond), Ms: math.MaxInt64 / int64(time.Millisecond), Bytes: math.MaxInt64, Destination: &three},
				{Name: results[3].name, ConnectMs: 7, Ms: 999, Bytes: math.MinInt64, Error: "short_body"},
			}
			profile := tc.snapshotText + "full"
			const privateError = "https://QUALITY_PRIVATE_DESTINATION.invalid/path"
			for i, tag := range tags {
				p := te.path(tag)
				p.status, p.reason, p.statusSince = statusDegraded, results[1].name, t0
				p.quarantineLevel, p.quarantineUntil = 9, at(time.Hour)
				for kind := range p.stats.failuresByKind {
					p.stats.failuresByKind[kind] = 1_000_000
				}
				p.stats.probes, p.lastProbeEnd, p.lastRTT = 1, t0, 180*time.Millisecond
				p.lastError, p.lastClass = privateError, "status_503"
				p.lastProbeResult = probeResult{ok: true, profile: profile, reason: reasonNetwork.String(), state: statusDegraded, steps: results}
				p.stats.connsOK, p.stats.connsFailed, p.stats.bytesUp, p.stats.bytesDown = 1_000_000, 1_000_000, 1<<40, 1<<40
				p.stats.activeTime = 24 * time.Hour
				p.stats.ttfb.add(10 * time.Second)
				if i < qualityCollectionPaths {
					te.quality.addInitial(te.qualityEpoch, tag, false, qualityInitialSample{
						hasTTFB: true, hasDial: true, ttfb: 10 * time.Second, dial: 10 * time.Second,
						tcp: tcpQualitySnapshot{rtt: 10 * time.Second, present: qualityHasRTT | qualityHasMinRTT},
					})
					s2 := tcpQualitySnapshot{loadRTT: 10 * time.Second, rxTotal: 1000, txTotal: 10000,
						present: qualityHasLoadRTT | qualityHasRX | qualityHasTX}
					te.quality.addEnd(te.qualityEpoch, tag, false, qualityEndSample{
						bytesDown: 1 << 40, hasBulk: true, bulkKbps: 3_000_000,
						s1: tcpQualitySnapshot{present: qualityHasRX}, s2: s2, lastTCP: s2,
					})
				}
			}
			qualityProjectionSeed(te.engine, "direct", true)
			te.path("QUALITY_FOREIGN_PROVIDER_PATH")
			te.prov().lastFailedAt, te.prov().lastClass = t0, strings.Repeat("r", 1<<20)
			te.prov().version = tc.snapshotText + "version"
			var body []byte
			if tc.name == "escaped" {
				te.requestRefresh("fresh")
				jobs, _ := te.refreshAt(at(time.Minute))
				if len(jobs) != 1 || jobs[0].request == nil {
					t.Fatal("escaped fixture did not create its refresh snapshot")
				}
				observer := &Observer{run: "4f1c0de2a9b87765"}
				sent, err := observer.requestBody(&providerRuntime{cfg: te.prov().cfg}, jobs[0])
				if err != errProviderRequestTooLarge || sent != nil {
					t.Fatalf("escaped essential body: bytes %d error %v, want bounded refusal", len(sent), err)
				}
				// Preserve the full raw formatter oracle offline. The actual
				// transport refused this oversized caller metadata above.
				body, err = json.Marshal(jobs[0].request)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				_, body, _ = requestOf(t, te, at(time.Minute))
			}
			var decoded providerRequest
			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatal(err)
			}
			if len(decoded.Outbounds) != maxProviderOutbounds || len(te.quality.current.paths) != qualityCollectionPaths ||
				te.quality.current.tagBytes > qualityCollectionTagBytes || te.quality.current.samplesDropped != 0 {
				t.Fatal("fixture lost required 128 raw snapshots or 64 admitted quality histories")
			}
			if decoded.Version != te.prov().version || !decoded.Balancer.IsOwn {
				t.Fatal("fixture lost caller-chosen version or own active tag")
			}
			if _, own := ownTags[decoded.Balancer.Current]; !own {
				t.Fatal("fixture active tag is not the provider's full displayed tag")
			}
			if decoded.Network.ID != providerHash(te.prov().cfg.url, networkID) || !decoded.Network.IsKnown || !decoded.Network.IsUp ||
				decoded.WitnessQuality == nil || decoded.WitnessQuality.BytesDown == nil || *decoded.WitnessQuality.BytesDown != 0 {
				t.Fatal("combined rich request lost opaque network hash, network state, or measured witness zero")
			}
			for _, absent := range []string{"networkType", "networkOperator", "network_type", "network_operator"} {
				if bytes.Contains(body, []byte(`"`+absent+`":`)) {
					t.Fatalf("combined rich request interpreted opaque network id as %s", absent)
				}
			}
			current := te.quality.currentView(at(time.Minute))
			if !current.IsNetworkKnown || !reflect.DeepEqual(decoded.WitnessQuality, requestQualityOf(*current.Witness)) {
				t.Fatal("combined rich request changed witness quality")
			}
			measured := 0
			for i, outbound := range decoded.Outbounds {
				if outbound.Tag != strings.TrimPrefix(tags[i], "p-") {
					t.Fatal("combined rich request changed exact provider tag or order")
				}
				if outbound.State != "degraded" || outbound.Reason != results[1].name {
					t.Fatal("combined rich request lost raw degraded reason from the first failed step")
				}
				p := outbound.Probe
				if p == nil || p.Profile == nil || *p.Profile != profile || p.Reason != reasonNetwork.String() || p.State != "degraded" || !p.IsPassed ||
					p.Error != "" || p.RTTMs != 180 || p.AgoMs != 60000 || p.Steps == nil || !reflect.DeepEqual(*p.Steps, wantSteps) {
					t.Fatalf("outbound %q lost any of its four raw steps or profile/reason/state: %+v", outbound.Tag, p)
				}
				if i >= qualityCollectionPaths {
					if outbound.Quality != nil {
						t.Fatal("unmeasured provider row acquired quality")
					}
					continue
				}
				measured++
				q := outbound.Quality
				if !reflect.DeepEqual(q, requestQualityOf(current.Paths[tags[i]])) {
					t.Fatalf("outbound %q changed quality alongside its raw probe", outbound.Tag)
				}
				if q == nil || q.TCPRTTMinMs == nil || *q.TCPRTTMinMs != 0 || q.BulkSplicedCount == nil || *q.BulkSplicedCount != 0 ||
					q.RXOutOfOrderFraction == nil || *q.RXOutOfOrderFraction != 0 || q.TXRetransmitFraction == nil || *q.TXRetransmitFraction != 0 {
					t.Fatal("combined rich request omitted a measured quality zero")
				}
			}
			if measured != qualityCollectionPaths {
				t.Fatal("fixture did not serialize all 64 measured path records")
			}
			for _, secret := range []string{"QUALITY_PRIVATE_FINGERPRINT", privateError, "QUALITY_FOREIGN_PROVIDER_PATH", "p-node-", "4f1c0de2a9b87765"} {
				if bytes.Contains(body, []byte(secret)) {
					t.Fatalf("combined rich request disclosed %q", secret)
				}
			}
			fixed := qualityProviderRichFixedBytes(t, body, decoded)
			t.Logf("rich combined provider request: %d bytes total, %d fixed metadata bytes, ceiling %d, raw schema delta %d; 128 outbounds x four raw steps, 64 qualities and witness", len(body), fixed, combinedFixedBudget, snapshotDelta)
			if fixed > combinedFixedBudget {
				t.Fatalf("rich combined request takes %d fixed metadata bytes, ceiling %d", fixed, combinedFixedBudget)
			}
			if tc.name == "short" {
				shortFixed = fixed
			} else if shortFixed >= 0 && fixed != shortFixed {
				t.Fatalf("escaped caller text changed normalized fixed metadata: %d, want %d", fixed, shortFixed)
			}
		})
	}
}

func TestQualityProviderRichCapturedSnapshotOwnsScalars(t *testing.T) {
	te := setEngine(t)
	te.quality = newQualitySummary(t0)
	qualityProjectionSeed(te.engine, "p-x1", false)
	qualityProjectionSeed(te.engine, "direct", true)
	p := te.path("p-x1")
	p.lastProbeEnd = t0
	p.lastProbeResult = probeResult{ok: true, state: statusDegraded, profile: "full", reason: reasonHost.String(), steps: []probeStepResult{
		{name: "transport", ok: true, destination: 0},
		{name: "optional", destination: -1, class: "server_unverified"},
		{name: "download", ok: true, destination: 3},
		{name: "optional-body", destination: -1, class: "short_body"},
	}}
	now := at(time.Minute)
	captured := te.providerRequest(te.prov(), &refreshJob{refresh: 1, trigger: triggerHost}, now)
	var row *requestOutbound
	for i := range captured.Outbounds {
		if captured.Outbounds[i].Tag == "x1" {
			row = &captured.Outbounds[i]
		}
	}
	if row == nil || row.Probe == nil || row.Probe.Profile == nil || row.Probe.Reason != reasonHost.String() || row.Probe.Steps == nil || len(*row.Probe.Steps) != 4 ||
		(*row.Probe.Steps)[0].Destination == nil || row.Quality == nil || row.Quality.BytesDown == nil ||
		row.Quality.TCPRTTMinMs == nil || row.Quality.RXOutOfOrderFraction == nil || captured.WitnessQuality == nil || captured.WitnessQuality.BytesDown == nil {
		t.Fatal("copy fixture lost raw step pointers, path quality or witness")
	}
	before, err := json.Marshal(captured)
	if err != nil {
		t.Fatal(err)
	}
	p.lastProbeResult.profile = "changed-source-profile"
	p.lastProbeResult.reason = reasonRecheck.String()
	p.reason = "changed-current-policy-reason"
	p.lastProbeResult.steps[0].name = "changed-source-step"
	p.lastProbeResult.steps[0].destination = 3
	te.quality.addEnd(te.qualityEpoch, "p-x1", false, qualityEndSample{hasBytes: true, bytesDown: 1})
	te.quality.addEnd(te.qualityEpoch, "direct", true, qualityEndSample{hasBytes: true, bytesDown: 2})
	after, err := json.Marshal(captured)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("changing source probe/quality state mutated a captured combined request")
	}
	qualityBefore := te.quality.currentView(now)
	*row.Probe.Profile = "changed-request-profile"
	(*row.Probe.Steps)[0].Name = "changed-request-step"
	*(*row.Probe.Steps)[0].Destination = 7
	*row.Quality.BytesDown = 999
	*row.Quality.TCPRTTMinMs = 999
	*row.Quality.RXOutOfOrderFraction = .5
	*captured.WitnessQuality.BytesDown = 999
	row.Probe.Reason = reasonAudit.String()
	if p.lastProbeResult.profile != "changed-source-profile" || p.lastProbeResult.reason != reasonRecheck.String() || p.reason != "changed-current-policy-reason" || p.lastProbeResult.steps[0].name != "changed-source-step" ||
		p.lastProbeResult.steps[0].destination != 3 || !reflect.DeepEqual(qualityBefore, te.quality.currentView(now)) {
		t.Fatal("changing captured raw/quality pointers mutated the engine's sources")
	}
	next := te.providerRequest(te.prov(), &refreshJob{refresh: 2, trigger: triggerHost}, now)
	found := false
	for _, outbound := range next.Outbounds {
		if outbound.Tag != "x1" {
			continue
		}
		found = true
		if outbound.Probe == nil || outbound.Probe.Profile == nil || *outbound.Probe.Profile != "changed-source-profile" || outbound.Probe.Reason != reasonRecheck.String() || outbound.Reason != "changed-current-policy-reason" ||
			outbound.Probe.Steps == nil || len(*outbound.Probe.Steps) != 4 || (*outbound.Probe.Steps)[0].Destination == nil || *(*outbound.Probe.Steps)[0].Destination != 3 ||
			outbound.Quality == nil || outbound.Quality.BytesDown == nil || *outbound.Quality.BytesDown != 1 {
			t.Fatal("new request retained a prior request's optional pointers")
		}
	}
	if !found || next.WitnessQuality == nil || next.WitnessQuality.BytesDown == nil || *next.WitnessQuality.BytesDown != 2 {
		t.Fatal("new request lost own path or retained the prior witness pointer")
	}
}
