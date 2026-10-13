package wireguard

import (
	"testing"
	"time"
)

func TestDNSCacheHonorsTTLAndSweepsExpiredAnswers(t *testing.T) {
	var c cache
	c.Cache("live.test", []string{"10.0.0.1"}, 60)
	if got := c.LookupHost("live.test"); len(got) != 1 || got[0] != "10.0.0.1" {
		t.Fatalf("cached answer %v", got)
	}
	c.Cache("expired.test", []string{"10.0.0.2"}, 0)
	if got := c.LookupHost("expired.test"); got != nil {
		t.Fatalf("expired answer %v", got)
	}
	c.Cache("sweep.test", []string{"10.0.0.3"}, 0)
	c.lastSweep = time.Time{}
	c.Cache("new.test", []string{"10.0.0.4"}, 60)
	if _, ok := c.m["sweep.test"]; ok {
		t.Fatal("expired answer retained after sweep")
	}
}
