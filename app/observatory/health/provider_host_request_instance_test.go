package health_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory/health"
)

type requestStatus struct {
	ID    uint64 `json:"id"`
	State string `json:"state"`
	Code  string `json:"code"`
}

// waitRequest waits until the request of the host leaves pending.
func waitRequest(t *testing.T, obs *health.Observer, id uint64) requestStatus {
	t.Helper()
	var st struct {
		Providers []struct {
			Request *requestStatus `json:"request"`
		} `json:"providers"`
	}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if err := json.Unmarshal(obs.Status(), &st); err != nil {
			t.Fatal(err)
		}
		if r := st.Providers[0].Request; r != nil && r.ID == id && r.State != "pending" {
			return *r
		}
	}
	t.Fatalf("request %d still pending", id)
	return requestStatus{}
}

// The host asks for another country with a header and a refresh. The
// backend answers in the protocol that it cannot give one now or does not
// know the value: the request fails with the code of the body, the set
// stays the one applied before.
func TestHostRequestCarriesTheCodeOfTheBackend(t *testing.T) {
	for _, tc := range []struct {
		location, code string
		status         int
	}{
		{"country:XX", "location_unavailable", http.StatusConflict},
		{"nowhere", "location_unknown", http.StatusBadRequest},
	} {
		t.Run(tc.code, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Header.Get("X-Location") == tc.location {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(`{"protocol": 1, "code": "` + tc.code + `", "message": "` + tc.location + `"}`))
					return
				}
				_, _ = w.Write([]byte(`{"protocol": 1, "outbounds": [{"tag": "x1", "selection": {"balancer": "auto", "probe": "check", "cost": 1}, "protocol": "freedom"}]}`))
			}))
			defer ts.Close()
			obs := twoViaCore(t, ts.URL+"/set", `, "limits": {"urgentMinInterval": "10ms"}`)
			first := obs.RequestProviderRefresh("fresh")
			if r := waitRequest(t, obs, first); r.State != "applied" || r.Code != "" {
				t.Fatalf("first request %+v", r)
			}
			before, seq, err := obs.ExportProviderSet("fresh", 0)
			if err != nil {
				t.Fatal(err)
			}
			if err := obs.SetProviderHeaders("fresh", map[string]string{"X-Location": tc.location}); err != nil {
				t.Fatal(err)
			}
			second := obs.RequestProviderRefresh("fresh")
			if r := waitRequest(t, obs, second); second != first+1 || r.State != "failed" || r.Code != tc.code {
				t.Fatalf("request %d: %+v", second, r)
			}
			// The header the host set changed what it keeps; the set did not.
			after, _, err := obs.ExportProviderSet("fresh", 0)
			if err != nil {
				t.Fatal(err)
			}
			var was, now struct {
				Outbounds json.RawMessage `json:"outbounds"`
			}
			if json.Unmarshal(before, &was) != nil || json.Unmarshal(after, &now) != nil || string(was.Outbounds) != string(now.Outbounds) || seq == 0 {
				t.Fatalf("the set changed: %s then %s", before, after)
			}
		})
	}
}
