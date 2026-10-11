package health

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func providerProbeJSON(t *testing.T, p *path, now time.Time) map[string]any {
	t.Helper()
	body, err := json.Marshal(describeOutbound(p, "x", 0, now))
	if err != nil {
		t.Fatal(err)
	}
	var outbound map[string]any
	if err := json.Unmarshal(body, &outbound); err != nil {
		t.Fatal(err)
	}
	return outbound["probe"].(map[string]any)
}

func TestProviderProbeLegacyHasNoSnapshot(t *testing.T) {
	for _, failed := range []bool{false, true} {
		t.Run(fmt.Sprint(failed), func(t *testing.T) {
			te := setEngine(t)
			p := te.path("p-x2")
			p.lastProbeEnd, p.lastRTT = t0, 180*time.Millisecond
			want := map[string]any{"isPassed": true, "rttMs": float64(180), "agoMs": float64(1000)}
			if failed {
				p.lastError, p.lastClass = "private error text", "eof"
				want = map[string]any{"isPassed": false, "error": "eof", "agoMs": float64(1000)}
			}
			if got := providerProbeJSON(t, p, at(time.Second)); !reflect.DeepEqual(got, want) {
				t.Fatalf("legacy probe: %v, want %v", got, want)
			}
		})
	}
}

func TestProviderProbeSnapshotFields(t *testing.T) {
	for _, tc := range []struct {
		name    string
		result  probeResult
		lastErr string
		class   string
		want    map[string]any
	}{
		{"builtin", probeResult{ok: true, state: statusUp, steps: []probeStepResult{
			{name: "default", ok: true, connect: 12 * time.Millisecond, elapsed: 34 * time.Millisecond, bytes: 56, destination: 0},
		}}, "", "", map[string]any{
			"profile": "", "state": "up", "isPassed": true, "rttMs": float64(180), "agoMs": float64(1000),
			"steps": []any{map[string]any{"name": "default", "isPassed": true, "connectMs": float64(12), "ms": float64(34), "bytes": float64(56), "destination": float64(0)}},
		}},
		{"state_without_steps", probeResult{state: statusDown}, "private error text", "timeout", map[string]any{
			"profile": "", "state": "down", "steps": []any{}, "isPassed": false, "error": "timeout", "agoMs": float64(1000),
		}},
		{"profile_without_steps", probeResult{ok: true, profile: "named"}, "", "", map[string]any{
			"profile": "named", "state": "up", "steps": []any{}, "isPassed": true, "rttMs": float64(180), "agoMs": float64(1000),
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			te := setEngine(t)
			p := te.path("p-x2")
			p.lastProbeEnd, p.lastRTT = t0, 180*time.Millisecond
			p.lastProbeResult, p.lastError, p.lastClass = tc.result, tc.lastErr, tc.class
			if got := providerProbeJSON(t, p, at(time.Second)); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("snapshot probe: %v, want %v", got, tc.want)
			}
		})
	}
}

func TestProviderProbeFourStepsState(t *testing.T) {
	for _, state := range []pathStatus{statusUp, statusDegraded, statusDown} {
		t.Run(state.String(), func(t *testing.T) {
			te := setEngine(t)
			result := probeResult{ok: state != statusDown, state: state, profile: "full", rtt: 180 * time.Millisecond}
			var wantSteps []any
			for i := 0; i < 4; i++ {
				passed := state == statusUp || i < 3
				step := probeStepResult{name: fmt.Sprintf("step-%d", i), ok: passed,
					connect: time.Duration(11+i) * time.Millisecond, elapsed: time.Duration(21+i) * time.Millisecond,
					bytes: int64(31 + i), destination: i}
				want := map[string]any{"name": step.name, "isPassed": passed, "connectMs": float64(11 + i), "ms": float64(21 + i), "bytes": float64(31 + i)}
				if passed {
					want["destination"] = float64(i)
				} else {
					step.class, step.destination = "short_body", -1
					want["error"] = "short_body"
				}
				result.steps = append(result.steps, step)
				wantSteps = append(wantSteps, want)
			}
			if state == statusDown {
				result.err, result.class = "private destination and error text", "short_body"
			}
			j := te.enqueue("p-x2", reasonConfirm, "auto", t0)
			te.begin(j, t0)
			te.onProbeResult(j, result, at(time.Second))
			want := map[string]any{"profile": "full", "state": state.String(), "steps": wantSteps,
				"isPassed": state != statusDown, "agoMs": float64(1000)}
			if state == statusDown {
				want["error"] = "short_body"
			} else {
				want["rttMs"] = float64(180)
			}
			if got := providerProbeJSON(t, te.path("p-x2"), at(2*time.Second)); !reflect.DeepEqual(got, want) {
				t.Fatalf("four-step snapshot: %v, want %v", got, want)
			}
		})
	}
}

func TestProviderProbeSnapshotNetworkContract(t *testing.T) {
	for _, ignore := range []bool{false, true} {
		t.Run(fmt.Sprint(ignore), func(t *testing.T) {
			te := setEngine(t)
			if ignore {
				te.sel.cfg.onNetworkChange = NetworkChangeMode_IGNORE
			}
			p := te.path("p-x2")
			p.lastProbeEnd, p.lastRTT = t0, 180*time.Millisecond
			p.lastProbeResult = probeResult{ok: true, state: statusUp, profile: "home", steps: []probeStepResult{
				{name: "transport", ok: true, connect: time.Millisecond, elapsed: 2 * time.Millisecond, bytes: 3, destination: 0},
			}}
			before := providerProbeJSON(t, p, at(time.Second))
			te.onNetworkChange("other", at(time.Second))
			if !ignore {
				if got := describeOutbound(p, "x2", 0, at(time.Second)).Probe; got != nil {
					t.Fatalf("previous network probe survived: %+v", got)
				}
				return
			}
			if after := providerProbeJSON(t, p, at(time.Second)); !reflect.DeepEqual(before, after) {
				t.Fatalf("ignored network change changed snapshot: %v -> %v", before, after)
			}
		})
	}
}

func TestProviderProbeRichBodyBudget(t *testing.T) {
	// Keep raw snapshots; only caller-chosen escaped text is outside the fixed metadata ceiling.
	const legacyBudget = 64 << 10
	const envelopeBytes = 44
	const stepBytes = 160
	const stepsPerSnapshot = 4
	const snapshotBudget = legacyBudget + 128*(envelopeBytes+stepsPerSnapshot*(stepBytes+1))
	if snapshotBudget != 150<<10 || maxProviderOutbounds != 128 {
		t.Fatal("review the metadata budget when the provider limit changes")
	}
	destination := 3
	metadata := requestProbeStep{IsPassed: false, ConnectMs: math.MinInt64, Ms: math.MinInt64, Bytes: math.MinInt64,
		Destination: &destination, Error: "server_unverified"}
	encoded, err := json.Marshal(metadata)
	if err != nil || len(encoded) != stepBytes {
		t.Fatalf("step metadata: %d bytes, want %d: %s (%v)", len(encoded), stepBytes, encoded, err)
	}
	emptyProfile := ""
	emptySteps := []requestProbeStep{}
	envelope := struct {
		Profile *string             `json:"profile"`
		State   string              `json:"state"`
		Steps   *[]requestProbeStep `json:"steps"`
	}{&emptyProfile, "degraded", &emptySteps}
	encoded, err = json.Marshal(envelope)
	if err != nil || len(encoded) != envelopeBytes {
		t.Fatalf("snapshot envelope: %d bytes, want %d (%v)", len(encoded), envelopeBytes, err)
	}
	for _, text := range []string{"", strings.Repeat("長é\"\\\n<>&\u2028", 1024)} {
		t.Run(fmt.Sprintf("name_bytes_%d", len(text)), func(t *testing.T) {
			var tags []string
			chosen := 0
			for i := 0; i < maxProviderOutbounds; i++ {
				tag := fmt.Sprintf("de-fra-reality-%03d-%s", i, strings.Repeat("t", 45))
				tags = append(tags, "p-"+tag)
				chosen += len(tag)
			}
			te := newProviderEngine(t, quietProvider(nil), &SelectorConfig{Paths: pathSpecs("p-", 1)}, tags...)
			te.applyProviderSet(te.prov(), providerChange{tags: tags}, t0)
			results := []probeStepResult{
				{name: text + "transport", ok: true, connect: 820 * time.Millisecond, elapsed: 1400 * time.Millisecond, bytes: 32768, destination: 0},
				{name: text + "optional", connect: 321 * time.Millisecond, elapsed: 654 * time.Millisecond, bytes: 12345, destination: -1, class: "server_unverified"},
				{name: text + "download", ok: true, connect: time.Duration(math.MaxInt64), elapsed: time.Duration(math.MaxInt64), bytes: math.MaxInt64, destination: 3},
				{name: text + "optional-body", connect: 7 * time.Millisecond, elapsed: 999 * time.Millisecond, bytes: math.MinInt64, destination: -1, class: "short_body"},
			}
			zero, three := 0, 3
			wantSteps := []requestProbeStep{
				{Name: text + "transport", IsPassed: true, ConnectMs: 820, Ms: 1400, Bytes: 32768, Destination: &zero},
				{Name: text + "optional", ConnectMs: 321, Ms: 654, Bytes: 12345, Error: "server_unverified"},
				{Name: text + "download", IsPassed: true, ConnectMs: math.MaxInt64 / int64(time.Millisecond), Ms: math.MaxInt64 / int64(time.Millisecond), Bytes: math.MaxInt64, Destination: &three},
				{Name: text + "optional-body", ConnectMs: 7, Ms: 999, Bytes: math.MinInt64, Error: "short_body"},
			}
			profile := text + "full"
			for _, name := range append([]string{profile}, results[0].name, results[1].name, results[2].name, results[3].name) {
				encoded, err := json.Marshal(name)
				if err != nil {
					t.Fatal(err)
				}
				chosen += len(tags) * (len(encoded) - 2)
			}
			for _, tag := range tags {
				p := te.path(tag)
				p.status, p.reason, p.statusSince = statusDegraded, "network_change", t0
				p.quarantineLevel, p.quarantineUntil = 9, at(time.Hour)
				for k := range p.stats.failuresByKind {
					p.stats.failuresByKind[k] = 1_000_000
				}
				p.stats.probes, p.lastProbeEnd, p.lastRTT = 1, t0, 180*time.Millisecond
				p.lastProbeResult = probeResult{ok: true, profile: profile, state: statusDegraded, steps: results}
				p.stats.connsOK, p.stats.connsFailed, p.stats.bytesUp, p.stats.bytesDown = 1_000_000, 1_000_000, 1<<40, 1<<40
				p.stats.activeTime = 24 * time.Hour
				p.stats.ttfb.add(10 * time.Second)
			}
			te.prov().lastFailedAt, te.prov().lastClass = t0, strings.Repeat("r", 1<<20)
			te.prov().version = strings.Repeat("v", 256)
			chosen += len(te.prov().version)
			_, body, _ := requestOf(t, te, at(time.Minute))
			var decoded providerRequest
			if err := json.Unmarshal(body, &decoded); err != nil {
				t.Fatal(err)
			}
			if len(decoded.Outbounds) != len(tags) {
				t.Fatalf("body has %d outbounds, want %d", len(decoded.Outbounds), len(tags))
			}
			for _, outbound := range decoded.Outbounds {
				p := outbound.Probe
				if p == nil || p.Profile == nil || *p.Profile != profile || p.State != "degraded" || !p.IsPassed || p.Error != "" || p.RTTMs != 180 || p.AgoMs != 60000 ||
					p.Steps == nil || !reflect.DeepEqual(*p.Steps, wantSteps) {
					t.Fatalf("outbound %q lost raw snapshot: %+v", outbound.Tag, p)
				}
			}
			core := len(body) - chosen
			t.Logf("%d outbounds, four steps each: %d total bytes, %d fixed bytes, ceiling %d", len(tags), len(body), core, snapshotBudget)
			if core > snapshotBudget {
				t.Fatalf("rich body takes %d fixed bytes, ceiling %d", core, snapshotBudget)
			}
		})
	}
}
