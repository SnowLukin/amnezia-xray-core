package health

import "testing"

func TestProviderHeadersPreserveHTTPNames(t *testing.T) {
	if err := CheckHeaderName("Upgrade"); err == nil {
		t.Error("the provider may not replace the HTTP Upgrade header")
	}
	if err := CheckHeaderName("ReturnToCheaper"); err != nil {
		t.Errorf("a strategy name is an ordinary custom header: %v", err)
	}
}
