package health

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The observatory knows tags, costs and rules only; what an outbound means
// to a product belongs in the config that a backend generates.
var productWords = regexp.MustCompile(`(?i)\b(extra|whitelist|white-list|cdn|relay|conquest)\b`)

func TestNoProductNames(t *testing.T) {
	files, _ := filepath.Glob("*")
	more, _ := filepath.Glob("command/*")
	for _, f := range append(files, more...) {
		if strings.HasSuffix(f, "_test.go") || strings.HasSuffix(f, ".pb.go") {
			continue
		}
		if !strings.HasSuffix(f, ".go") && !strings.HasSuffix(f, ".proto") && !strings.HasSuffix(f, ".md") {
			continue
		}
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(body), "\n") {
			if w := productWords.FindString(line); w != "" {
				t.Errorf("%s:%d names %q", f, i+1, w)
			}
		}
	}
}
