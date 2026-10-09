package api

import (
	"strings"
	"testing"
)

func TestProviderHeadersFromStdin(t *testing.T) {
	got, err := providerHeaders(strings.NewReader(` {"Authorization": "Bearer t3", "X-Key": ""} `))
	if err != nil || len(got) != 2 || got["Authorization"] != "Bearer t3" || got["X-Key"] != "" {
		t.Fatalf("headers %v, %v", got, err)
	}
	for _, in := range []string{``, `null`, `["Bearer secret"]`, `{"Authorization": 5}`, `{"Authorization": "Bearer secret"`,
		`{"Authorization": "Bearer secret"} {}`, `{"Authorization": "` + strings.Repeat("secret", 50_000) + `"}`,
		"{\"Authorization\": \"Bearer secret\xff\"}"} {
		_, err := providerHeaders(strings.NewReader(in))
		if err == nil {
			t.Errorf("%.40s: accepted", in)
		} else if strings.Contains(err.Error(), "secret") {
			t.Errorf("%.40s: the error quotes the input: %v", in, err)
		}
	}
}
