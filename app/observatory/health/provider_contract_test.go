package health

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"
)

func (f *applyFixture) status(t *testing.T) statusView {
	t.Helper()
	var view statusView
	if err := json.Unmarshal(f.o.Status(), &view); err != nil {
		t.Fatal(err)
	}
	return view
}

// A backend that rotates the credentials of a slot or its labels and keeps
// the version gets them applied: the set is told by its content.
func TestSameVersionWithNewContentApplies(t *testing.T) {
	f := newApplyFixture(t, time.Minute, func(p *ProviderConfig) {})
	f.o.s.features.PathLabels = true
	first, err := parseAnswer([]byte(`{"version":"slots-7","outbounds":[{"tag":"x1","protocol":"vless","v":1,
		"selection":{"balancer":"auto","probe":"check","cost":1,"labels":{"node":"a"}}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, first); !changed || err != nil {
		t.Fatalf("first set: %v, %v", changed, err)
	}
	old := f.m.GetHandler("p-x1")
	f.open("p-x1")
	rotated, err := parseAnswer([]byte(`{"version":"slots-7","outbounds":[{"tag":"x1","protocol":"vless","v":2,
		"selection":{"balancer":"auto","probe":"check","cost":1,"labels":{"node":"b"}}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, rotated); !changed || err != nil {
		t.Fatalf("new content under the same version: changed %v, %v", changed, err)
	}
	if ev := f.lastApply(t); ev["is_changed"] != true || ev["replaced"] != 1.0 || ev["version"] != "slots-7" {
		t.Fatalf("apply %v", ev)
	}
	if f.m.GetHandler("p-x1") == old {
		t.Fatal("the outbound with new credentials was not replaced")
	}
	var labels map[string]string
	for _, p := range f.status(t).Paths {
		if p.Outbound == "p-x1" {
			labels = p.Labels
		}
	}
	if labels["node"] != "b" {
		t.Fatalf("Status labels %v, want the new ones", labels)
	}
	// The same content again, formatted another way, is the applied set.
	again, err := parseAnswer([]byte(`{"version":"slots-7","outbounds":[ {"tag":"x1","protocol":"vless","v":2,
		"selection":{"balancer":"auto","probe":"check","cost":1,"labels":{"node":"b"}}} ]}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, again); changed || err != nil {
		t.Fatalf("the applied set again: changed %v, %v", changed, err)
	}
}

// A new version over the applied content renames the set without building:
// the next request and the kept set name the new version.
func TestNewVersionOverSameContentOnlyRenames(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	body := `{"version":"%s","outbounds":[{"tag":"x1","protocol":"vless","v":1,"selection":{"balancer":"auto","probe":"check","cost":1}}]}`
	first, _ := parseAnswer([]byte(fmt.Sprintf(body, "a")))
	if _, err := f.apply(t, first); err != nil {
		t.Fatal(err)
	}
	_, seq := f.providerSet(t, 0)
	f.created = nil
	renamed, _ := parseAnswer([]byte(fmt.Sprintf(body, "b")))
	if changed, err := f.apply(t, renamed); changed || err != nil {
		t.Fatalf("renamed set: changed %v, %v", changed, err)
	}
	if len(f.created) != 0 || f.rt.p.version != "b" {
		t.Fatalf("built %d handlers, version %q", len(f.created), f.rt.p.version)
	}
	if _, after := f.providerSet(t, 0); after == seq {
		t.Fatal("the kept set still names the old version")
	}
}

// A newer backend adds keys an older core does not know: the answer applies,
// and provider_apply names what was skipped.
func TestUnknownAnswerKeysAreIgnoredAndNamed(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	a, err := parseAnswer([]byte(`{"protocol":1,"ttl":86400,"outbounds":[
		{"tag":"x1","protocol":"vless","v":1,"selection":{"balancer":"auto","probe":"check","cost":1,"weight":3,"returnToCheaper":{"interval":"5m","mode":"soft"}}},
		{"tag":"x2","protocol":"vless","v":1,"selection":{"balancer":"auto","group":"g","weight":1}}],
		"groups":[{"name":"g","probe":"check","cost":2,"region":"eu","returnToCheaper":{"isEnabled":false,"x":1}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := f.apply(t, a); !changed || err != nil {
		t.Fatalf("apply %v, %v", changed, err)
	}
	ev := f.lastApply(t)
	var got []string
	for _, k := range ev["ignored_keys"].([]any) {
		got = append(got, k.(string))
	}
	want := []string{"ttl", "groups[].region", "groups[].returnToCheaper.x", "selection.weight", "selection.returnToCheaper.mode"}
	if !slices.Equal(got, want) {
		t.Fatalf("ignored_keys %v, want %v", got, want)
	}
	// The guards stay: an answer may not bring routing or the observatory.
	for _, key := range []string{"routing", "healthObservatory"} {
		if _, err := parseAnswer([]byte(`{"outbounds":[{"tag":"x"}],"` + key + `":{}}`)); err == nil || !isAnswerError(err) {
			t.Fatalf("%s: %v, want a refusal", key, err)
		}
	}
	// A known key with a wrong value still refuses the set.
	bad, _ := parseAnswer([]byte(`{"outbounds":[{"tag":"x1","protocol":"vless","v":1,"selection":{"balancer":"auto","probe":"check","cost":"1"}}]}`))
	if _, err := f.apply(t, bad); err == nil {
		t.Fatal("a cost of the wrong type applied")
	}
}

// The ignored keys an event names are bounded.
func TestIgnoredKeysAreBounded(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"outbounds":[{"tag":"x"}]`)
	for i := range 40 {
		fmt.Fprintf(&b, `,"k%02d":1`, i)
	}
	b.WriteString(`,"` + strings.Repeat("y", 500) + `":1}`)
	a, err := parseAnswer([]byte(b.String()))
	if err != nil {
		t.Fatal(err)
	}
	if len(a.ignored) != maxIgnoredKeys {
		t.Fatalf("%d ignored keys named", len(a.ignored))
	}
	for _, k := range a.ignored {
		if len(k) > 70 {
			t.Fatalf("an ignored key of %d bytes", len(k))
		}
	}
}

// Serializers write null for a key without a value: it is no key, wherever
// it stands.
func TestNullIsNoKey(t *testing.T) {
	f := newApplyFixture(t, time.Minute)
	a, err := parseAnswer([]byte(`{"protocol":null,"version":null,"nextRefreshIn":null,"expiresAt":{"x1":null},"serverTime":null,
		"headers":null,"baseVersion":null,"groups":null,"outbounds":[
		{"tag":"x1","protocol":"vless","v":1,"selection":{"balancer":"auto","probe":"check","cost":1,"group":null,"labels":null,"returnToCheaper":null,"probePolicy":null,"isProbeOnly":null}}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if a.hasNextRefreshIn || len(a.expiresAt) != 0 {
		t.Fatalf("null read as a value: %+v", a)
	}
	plain, _ := parseAnswer([]byte(`{"outbounds":[
		{"tag":"x1","protocol":"vless","v":1,"selection":{"balancer":"auto","probe":"check","cost":1,"group":null,"labels":null,"returnToCheaper":null,"probePolicy":null,"isProbeOnly":null}}]}`))
	if a.hash != plain.hash {
		t.Fatal("groups: null names another set than no groups")
	}
	if changed, err := f.apply(t, a); !changed || err != nil {
		t.Fatalf("apply %v, %v", changed, err)
	}
	if ev := f.lastApply(t); ev["ignored_keys"] != nil {
		t.Fatalf("null keys named as ignored: %v", ev["ignored_keys"])
	}
	// A member of a group with the keys of the group set to null.
	member, _ := parseAnswer([]byte(`{"outbounds":[{"tag":"x2","protocol":"vless","v":1,"selection":{"balancer":"auto","group":"g","cost":null,"probe":null}}],
		"groups":[{"name":"g","probe":"check","cost":2,"labels":null,"failedInRowToLeave":null}]}`))
	if _, err := f.apply(t, member); err != nil {
		t.Fatalf("group member with null keys: %v", err)
	}
	// A required key set to null is a required key missing.
	missing, _ := parseAnswer([]byte(`{"outbounds":[{"tag":"x3","protocol":"vless","v":1,"selection":{"balancer":"auto","probe":"check","cost":null}}]}`))
	if _, err := f.apply(t, missing); err == nil || !strings.Contains(err.Error(), "needs cost above zero") {
		t.Fatalf("null cost: %v", err)
	}
	if _, err := parseAnswer([]byte(`{"outbounds":null}`)); err == nil || !strings.Contains(err.Error(), "no outbounds") {
		t.Fatalf("null outbounds: %v", err)
	}
}

// An empty headers object sets nothing, so it is no reason to refuse an
// answer where answers may set no header.
func TestEmptyHeadersWhereNoneAreAccepted(t *testing.T) {
	f := newApplyFixture(t, time.Minute, func(p *ProviderConfig) {
		p.Apply.AcceptedHeaders = nil
		p.Apply.FixedHeaders = true
	})
	for _, headers := range []string{`{}`, `{"Authorization":null}`} {
		a, err := parseAnswer([]byte(`{"headers":` + headers + `,"outbounds":[{"tag":"x1","protocol":"vless","v":1}]}`))
		if err == nil {
			err = checkRotation(a, f.rt.cfg)
		}
		if err != nil {
			t.Fatalf("headers %s: %v", headers, err)
		}
	}
	a, _ := parseAnswer([]byte(`{"headers":{"Authorization":"Bearer t"},"outbounds":[{"tag":"x1","protocol":"vless","v":1}]}`))
	if err := checkRotation(a, f.rt.cfg); err == nil {
		t.Fatal("a header applied where none are accepted")
	}
}
