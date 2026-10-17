package health_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xtls/xray-core/app/observatory/health"
)

func commandCode(err error) string {
	var c *health.CommandError
	if errors.As(err, &c) {
		return c.Code
	}
	return ""
}

// The host replaces the mirrors while the core runs: the next refresh goes to
// the new address only, and the status tells how many and which replacement,
// never the address.
func TestSetProviderURLsTakesTheNextRefresh(t *testing.T) {
	old := &replyServer{status: http.StatusBadGateway}
	first := httptest.NewServer(old)
	defer first.Close()
	found := &setServer{body: `{"outbounds":[]}`}
	second := httptest.NewServer(found)
	defer second.Close()
	obs := twoViaCore(t, first.URL)
	if err := obs.SetProviderURLs("fresh", []string{second.URL + "/set"}); err != nil {
		t.Fatal(err)
	}
	if !obs.RequestProviderRefresh("fresh") {
		t.Fatal("manual refresh refused")
	}
	waitRefreshed(t, obs)
	if old.requests() != 0 || found.requests() != 1 {
		t.Fatalf("requests old=%d new=%d", old.requests(), found.requests())
	}
	var st struct {
		Providers []struct {
			Count      int    `json:"urls_count"`
			Generation uint64 `json:"urls_generation"`
		} `json:"providers"`
	}
	raw := obs.Status()
	if err := json.Unmarshal(raw, &st); err != nil {
		t.Fatal(err)
	}
	if st.Providers[0].Count != 1 || st.Providers[0].Generation != 1 {
		t.Fatalf("status %s", raw)
	}
	// The same list again is no replacement.
	if err := obs.SetProviderURLs("fresh", []string{second.URL + "/set"}); err != nil {
		t.Fatal(err)
	}
	_ = json.Unmarshal(obs.Status(), &st)
	if st.Providers[0].Generation != 1 {
		t.Fatalf("same list counted: %+v", st.Providers[0])
	}
}

func TestSetProviderURLsRefusesBadLists(t *testing.T) {
	obs := twoViaCore(t, "http://127.0.0.1:1/set")
	for name, tc := range map[string]struct {
		provider string
		urls     []string
		code     string
	}{
		"empty":       {"fresh", nil, "invalid_argument"},
		"five":        {"fresh", []string{"https://a.test/1", "https://a.test/2", "https://a.test/3", "https://a.test/4", "https://a.test/5"}, "invalid_argument"},
		"repeated":    {"fresh", []string{"https://a.test/set", "https://a.test/set"}, "invalid_argument"},
		"credentials": {"fresh", []string{"https://user:pw@a.test/set"}, "invalid_argument"},
		"fragment":    {"fresh", []string{"https://a.test/set#x"}, "invalid_argument"},
		"scheme":      {"fresh", []string{"ftp://a.test/set"}, "invalid_argument"},
		"unknown":     {"other", []string{"https://a.test/set"}, "unknown_provider"},
	} {
		if err := obs.SetProviderURLs(tc.provider, tc.urls); commandCode(err) != tc.code {
			t.Errorf("%s: %v", name, err)
		}
	}
}
