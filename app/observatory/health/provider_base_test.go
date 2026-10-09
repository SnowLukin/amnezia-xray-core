package health

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// baseFixture is an observer whose config names base, none when empty,
// with a clock that stands still unless a test moves it.
func baseFixture(t *testing.T, base string) (*applyFixture, time.Time) {
	t.Helper()
	f := newApplyFixture(t, time.Minute, func(p *ProviderConfig) { p.BaseVersion = base })
	now := time.Now()
	f.o.clock = func() time.Time { return now }
	return f, now
}

// baseAnswer is the set version with the answer naming base, no base when
// empty.
func baseAnswer(t *testing.T, version, base string) *providerAnswer {
	t.Helper()
	more := ""
	if base != "" {
		more = fmt.Sprintf(`, "baseVersion": %q`, base)
	}
	a, err := parseAnswer([]byte(fmt.Sprintf(`{"version": %q, "outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}]%s}`,
		version, more)))
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// applyBase applies the answer and returns the provider_base_changed events
// it brought as from>to, and the base_version of its provider_apply.
func (f *applyFixture) applyBase(t *testing.T, a *providerAnswer) ([]string, any) {
	t.Helper()
	if _, err := f.apply(t, a); err != nil {
		t.Fatal(err)
	}
	return f.baseEvents(t)
}

func (f *applyFixture) baseEvents(t *testing.T) ([]string, any) {
	t.Helper()
	var told []string
	var named any
	for _, e := range f.events(t) {
		switch e.Type {
		case "provider_base_changed":
			told = append(told, fmt.Sprint(e.Fields["from"], ">", e.Fields["to"]))
		case "provider_apply":
			if e.Fields["ok"] != true {
				t.Fatalf("apply %v", e.Fields)
			}
			named = e.Fields["base_version"]
		}
	}
	return told, named
}

func (f *applyFixture) providerStatus() providerStatusView {
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	return f.o.e.status(f.o.now()).Providers[0]
}

// requestBase returns the baseVersion the next request tells the backend.
func (f *applyFixture) requestBase(t *testing.T) any {
	t.Helper()
	f.o.mu.Lock()
	j := &refreshJob{p: f.rt.p, trigger: triggerHost}
	j.request = f.o.e.providerRequest(f.rt.p, j, f.o.now())
	f.o.mu.Unlock()
	var body map[string]any
	if err := json.Unmarshal(f.o.requestBody(f.rt, j), &body); err != nil {
		t.Fatal(err)
	}
	return body["baseVersion"]
}

// Another base is told once per value, whatever path the answer took and
// whether the set changed with it; the set applies all the same.
func TestBaseChangedOncePerValue(t *testing.T) {
	f, _ := baseFixture(t, "b1")
	for i, step := range []struct {
		version, base string
		told          string
		seq           uint64
	}{
		{"v1", "b2", "[b1>b2]", 1},
		// The same set and base again: nothing new to tell or keep.
		{"v1", "b2", "[]", 1},
		{"v2", "b2", "[]", 2},
		// The same set with another base: the host keeps the new one.
		{"v2", "b3", "[b1>b3]", 3},
	} {
		told, named := f.applyBase(t, baseAnswer(t, step.version, step.base))
		if fmt.Sprint(told) != step.told || named != step.base {
			t.Fatalf("answer %d (%s, %s): told %v, provider_apply base_version %v", i+1, step.version, step.base, told, named)
		}
		if _, seq := f.providerSet(t, 0); seq != step.seq {
			t.Fatalf("answer %d: seq %d, want %d", i+1, seq, step.seq)
		}
	}
	// A refused answer names nothing.
	if _, err := f.apply(t, answer(t, fakeOutbound{Tag: "x1", Bad: true})); err == nil {
		t.Fatal("a set that does not build applied")
	}
	st := f.providerStatus()
	if st.BaseVersion != "b1" || st.AnswerBaseVersion != "b3" || !st.BaseChanged || st.Version != "v2" {
		t.Fatalf("status base %q, answer %q, changed %v, version %q", st.BaseVersion, st.AnswerBaseVersion, st.BaseChanged, st.Version)
	}
	if got := f.requestBase(t); got != "b1" {
		t.Fatalf("request baseVersion %v, want the base of the core", got)
	}
}

// An answer that names the base of the core again clears the change, so the
// next change is told again; an answer without a base clears nothing.
func TestBackToTheBaseClearsIt(t *testing.T) {
	f, _ := baseFixture(t, "b1")
	var all []string
	for _, step := range []struct {
		base    string
		changed bool
	}{{"b2", true}, {"", true}, {"b2", true}, {"b1", false}, {"b2", true}} {
		told, _ := f.applyBase(t, baseAnswer(t, "v1", step.base))
		all = append(all, told...)
		if st := f.providerStatus(); st.BaseChanged != step.changed || st.AnswerBaseVersion != step.base {
			t.Fatalf("after %q: base_changed %v, answer_base_version %q", step.base, st.BaseChanged, st.AnswerBaseVersion)
		}
	}
	if fmt.Sprint(all) != "[b1>b2 b1>b2]" {
		t.Fatalf("told %v, want the change twice", all)
	}
}

// Without a base there is nothing to compare: Status shows what the answers
// name, and the request names no base.
func TestNoBaseNoEvent(t *testing.T) {
	f, _ := baseFixture(t, "")
	for _, base := range []string{"b2", "b3"} {
		if told, named := f.applyBase(t, baseAnswer(t, "v1", base)); told != nil || named != base {
			t.Fatalf("answer %s: told %v, provider_apply base_version %v", base, told, named)
		}
	}
	if st := f.providerStatus(); st.BaseVersion != "" || st.AnswerBaseVersion != "b3" || st.BaseChanged {
		t.Fatalf("status base %q, answer %q, changed %v", st.BaseVersion, st.AnswerBaseVersion, st.BaseChanged)
	}
	if got := f.requestBase(t); got != nil {
		t.Fatalf("request baseVersion %v, want none", got)
	}
}

// A set the host kept names the base of the backend when it was kept, not
// the base of this core: it never becomes the base, and a change it carries
// is told at the start, so the host learns of a restart it put off.
func TestSavedInitialDoesNotMoveTheBase(t *testing.T) {
	for _, tc := range []struct{ config, told, base string }{
		{"b1", "[b1>b2]", "b1"},
		{"", "[]", ""},
	} {
		f, now := baseFixture(t, tc.config)
		f.rt.cfg.initial = []byte(fmt.Sprintf(`{"version": "v1", "baseVersion": "b2", "receivedAt": %q,
			"outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}]}`, stamp(now.Add(-time.Hour))))
		f.o.applyInitial()
		told, named := f.baseEvents(t)
		st := f.providerStatus()
		if fmt.Sprint(told) != tc.told || named != "b2" || st.BaseVersion != tc.base || st.AnswerBaseVersion != "b2" {
			t.Fatalf("config base %q: told %v, apply %v, status base %q, answer %q", tc.config, told, named, st.BaseVersion,
				st.AnswerBaseVersion)
		}
		var want any
		if tc.base != "" {
			want = tc.base
		}
		if got := f.requestBase(t); got != want {
			t.Fatalf("config base %q: request baseVersion %v", tc.config, got)
		}
	}
}

// The answer of the connect that brought the config names its base when the
// config does not; the base of the config comes first.
func TestFreshInitialGivesTheBase(t *testing.T) {
	f, _ := baseFixture(t, "")
	f.rt.cfg.initial = []byte(`{"version": "v1", "baseVersion": "b2", "outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}]}`)
	f.o.applyInitial()
	if told, _ := f.baseEvents(t); told != nil {
		t.Fatalf("told %v at the start", told)
	}
	if st := f.providerStatus(); st.BaseVersion != "b2" || st.BaseChanged {
		t.Fatalf("status base %q, changed %v", st.BaseVersion, st.BaseChanged)
	}
	if got := f.requestBase(t); got != "b2" {
		t.Fatalf("request baseVersion %v", got)
	}
	if told, _ := f.applyBase(t, baseAnswer(t, "v1", "b3")); fmt.Sprint(told) != "[b2>b3]" {
		t.Fatalf("told %v", told)
	}

	f, _ = baseFixture(t, "b1")
	f.rt.cfg.initial = []byte(`{"version": "v1", "baseVersion": "b2", "outbounds": [{"tag": "x1", "protocol": "vless", "v": 1}]}`)
	f.o.applyInitial()
	if told, _ := f.baseEvents(t); fmt.Sprint(told) != "[b1>b2]" {
		t.Fatalf("with a base in the config: told %v", told)
	}
	if st := f.providerStatus(); st.BaseVersion != "b1" {
		t.Fatalf("with a base in the config: status base %q", st.BaseVersion)
	}
}

// The answer of the connect names the config it came with even when its
// outbounds do not build: the core runs on that config, and a later change
// of the base is told against it.
func TestFreshInitialThatDoesNotBuildGivesTheBase(t *testing.T) {
	f, _ := baseFixture(t, "")
	f.rt.cfg.initial = []byte(`{"version": "v1", "baseVersion": "b2", "outbounds": [{"tag": "x1", "protocol": "vless", "bad": true}]}`)
	f.o.applyInitial()
	if ev := f.lastApply(t); ev["ok"] != false || ev["trigger"] != "initial" {
		t.Fatalf("apply %v", ev)
	}
	if st := f.providerStatus(); st.BaseVersion != "b2" || st.BaseChanged {
		t.Fatalf("status base %q, changed %v", st.BaseVersion, st.BaseChanged)
	}
	if told, _ := f.applyBase(t, baseAnswer(t, "v1", "b3")); fmt.Sprint(told) != "[b2>b3]" {
		t.Fatalf("told %v", told)
	}
}

// A backend that names a new base with every answer keeps one value in the
// core, not one per answer.
func TestBaseKeepsOneValue(t *testing.T) {
	f, _ := baseFixture(t, "b1")
	for i := 0; i < 100; i++ {
		f.applyBase(t, baseAnswer(t, "v1", fmt.Sprintf("b%d", i+2)))
	}
	f.o.mu.Lock()
	defer f.o.mu.Unlock()
	if p := f.rt.p; p.base != "b1" || p.baseReported != "b101" || p.answerBase != "b101" {
		t.Fatalf("base %q, told %q, answer %q", p.base, p.baseReported, p.answerBase)
	}
}
