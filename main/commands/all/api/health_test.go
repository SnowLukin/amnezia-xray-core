package api

import (
	"strings"
	"testing"
)

func TestProviderHeadersFromStdin(t *testing.T) {
	var got map[string]string
	err := stdinJSON(strings.NewReader(` {"Authorization": "Bearer t3", "X-Key": ""} `), &got, "headers")
	if err != nil || len(got) != 2 || got["Authorization"] != "Bearer t3" || got["X-Key"] != "" {
		t.Fatalf("headers %v, %v", got, err)
	}
	for _, in := range []string{``, `null`, `["Bearer secret"]`, `{"Authorization": 5}`, `{"Authorization": "Bearer secret"`,
		`{"Authorization": "Bearer secret"} {}`, `{"Authorization": "` + strings.Repeat("secret", 50_000) + `"}`,
		"{\"Authorization\": \"Bearer secret\xff\"}"} {
		var headers map[string]string
		err := stdinJSON(strings.NewReader(in), &headers, "headers")
		if err == nil {
			t.Errorf("%.40s: accepted", in)
		} else if strings.Contains(err.Error(), "secret") {
			t.Errorf("%.40s: the error quotes the input: %v", in, err)
		}
	}
}

func TestProviderURLsFromStdin(t *testing.T) {
	var urls []string
	if err := stdinJSON(strings.NewReader(`["https://a.test/set", "https://b.test/set"]`), &urls, "urls"); err != nil || len(urls) != 2 {
		t.Fatalf("urls %v, %v", urls, err)
	}
	for _, in := range []string{``, `null`, `{"url": "https://secret.test"}`, `"https://secret.test"`} {
		var urls []string
		if err := stdinJSON(strings.NewReader(in), &urls, "urls"); err == nil || strings.Contains(err.Error(), "secret") {
			t.Errorf("%s: %v", in, err)
		}
	}
}
