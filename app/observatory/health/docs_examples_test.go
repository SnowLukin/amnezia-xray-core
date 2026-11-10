package health_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
	"github.com/xtls/xray-core/core"
)

type docExample struct {
	where, kind, body, reason string
}

// docExamples returns the JSON blocks of docs/adaptive marked with a line
// `<!-- example: kind -->` and, for each, the first code span of an
// "Ошибка" line right after the block.
func docExamples(t *testing.T) []docExample {
	t.Helper()
	files, err := filepath.Glob("../../../docs/adaptive/*.md")
	if err != nil || len(files) == 0 {
		t.Fatalf("no docs/adaptive/*.md: %v", err)
	}
	var out []docExample
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(raw), "\n")
		for i := 0; i < len(lines); i++ {
			kind, ok := strings.CutPrefix(strings.TrimSpace(lines[i]), "<!-- example: ")
			if !ok {
				continue
			}
			ex := docExample{where: filepath.Base(file) + ":" + strconv.Itoa(i+1), kind: strings.TrimSuffix(kind, " -->")}
			if i+1 >= len(lines) || !strings.HasPrefix(lines[i+1], "```") {
				t.Fatalf("%s: the marker is not followed by a code block", ex.where)
			}
			end := i + 2
			for end < len(lines) && !strings.HasPrefix(lines[end], "```") {
				end++
			}
			ex.body = strings.Join(lines[i+2:end], "\n")
			for j := end + 1; j < len(lines); j++ {
				if strings.TrimSpace(lines[j]) == "" {
					continue
				}
				if strings.HasPrefix(lines[j], "Ошибка") {
					if parts := strings.SplitN(lines[j], "`", 3); len(parts) == 3 {
						ex.reason = parts[1]
					}
				}
				break
			}
			out = append(out, ex)
			i = end
		}
	}
	return out
}

// Every provider answer the docs show is taken or refused by the real set
// parsing and building, under the client config the docs give for them.
func TestDocProviderExamples(t *testing.T) {
	var base string
	var answers []docExample
	found := map[string]int{}
	for _, ex := range docExamples(t) {
		found[ex.kind]++
		switch ex.kind {
		case "provider-config":
			base = ex.body
		case "provider-response", "provider-response-rejected":
			answers = append(answers, ex)
		}
	}
	if found["provider-config"] != 1 || found["provider-response"] == 0 || found["provider-response-rejected"] == 0 {
		t.Fatalf("docs need one provider-config and both kinds of provider answers: %v", found)
	}
	for _, ex := range answers {
		t.Run(ex.where, func(t *testing.T) {
			ok, reason := applyInitial(t, base, ex.body)
			want := ex.kind == "provider-response"
			if ok != want {
				t.Fatalf("%s: taken %v (%s), want %v", ex.kind, ok, reason, want)
			}
			if !want {
				if ex.reason == "" {
					t.Fatal("a refused example names no Ошибка after the block")
				}
				if !strings.Contains(reason, ex.reason) {
					t.Fatalf("refused with %q, the doc says %q", reason, ex.reason)
				}
			}
		})
	}
}

// applyInitial starts a core from config with answer as the initial set of
// its first provider and tells whether the set was applied, or why not.
func applyInitial(t *testing.T, config, answer string) (bool, string) {
	t.Helper()
	var c map[string]any
	if err := json.Unmarshal([]byte(config), &c); err != nil {
		t.Fatalf("provider-config: %v", err)
	}
	c["log"] = map[string]any{"loglevel": "none"}
	ho := c["healthObservatory"].(map[string]any)
	ho["telemetry"] = map[string]any{"events": map[string]any{}}
	ho["providers"].([]any)[0].(map[string]any)["initialAnswer"] = json.RawMessage(answer)
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("answer is not JSON: %v", err)
	}
	cfg, err := core.LoadConfig("json", strings.NewReader(string(raw)))
	if err != nil {
		return false, err.Error()
	}
	inst, err := core.New(cfg)
	if err != nil {
		return false, err.Error()
	}
	if err := inst.Start(); err != nil {
		return false, err.Error()
	}
	defer inst.Close()
	evs, _ := health.FromInstance(inst).DrainEvents()
	var events []health.Event
	if err := json.Unmarshal(evs, &events); err != nil {
		t.Fatal(err)
	}
	for _, e := range events {
		if e.Type == "provider_apply" && e.Fields["trigger"] == "initial" {
			msg, _ := e.Fields["error"].(string)
			return e.Fields["is_ok"] == true, msg
		}
	}
	t.Fatalf("no provider_apply of the initial set in %s", evs)
	return false, ""
}
