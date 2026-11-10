package conf_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Every config the docs show loads, and every refused one is refused with
// the error named on the "Ошибка" line after it.
func TestDocConfigExamples(t *testing.T) {
	files, err := filepath.Glob("../../docs/adaptive/*.md")
	if err != nil || len(files) == 0 {
		t.Fatalf("no docs/adaptive/*.md: %v", err)
	}
	found := map[string]int{}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(raw), "\n")
		for i := 0; i < len(lines); i++ {
			kind, ok := strings.CutPrefix(strings.TrimSpace(lines[i]), "<!-- example: ")
			kind = strings.TrimSuffix(kind, " -->")
			if !ok || kind != "config" && kind != "config-rejected" && kind != "provider-config" {
				continue
			}
			where := filepath.Base(file) + ":" + strconv.Itoa(i+1)
			if i+1 >= len(lines) || !strings.HasPrefix(lines[i+1], "```") {
				t.Fatalf("%s: the marker is not followed by a code block", where)
			}
			end := i + 2
			for end < len(lines) && !strings.HasPrefix(lines[end], "```") {
				end++
			}
			body := strings.Join(lines[i+2:end], "\n")
			reason := ""
			for j := end + 1; j < len(lines); j++ {
				if strings.TrimSpace(lines[j]) == "" {
					continue
				}
				if parts := strings.SplitN(lines[j], "`", 3); strings.HasPrefix(lines[j], "Ошибка") && len(parts) == 3 {
					reason = parts[1]
				}
				break
			}
			found[kind]++
			i = end
			err := loadConfig(body)
			switch {
			case kind != "config-rejected" && err != nil:
				t.Errorf("%s: %v", where, err)
			case kind == "config-rejected" && reason == "":
				t.Errorf("%s: a refused example names no Ошибка after the block", where)
			case kind == "config-rejected" && (err == nil || !strings.Contains(err.Error(), reason)):
				t.Errorf("%s: refused with %v, the doc says %q", where, err, reason)
			}
		}
	}
	if found["config"] == 0 || found["config-rejected"] == 0 {
		t.Fatalf("docs lack marked config examples: %v", found)
	}
}
