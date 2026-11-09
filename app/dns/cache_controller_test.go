package dns

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/safe"
	feature_dns "github.com/xtls/xray-core/features/dns"
	"golang.org/x/net/dns/dnsmessage"
)

type cachedOnly struct{ cache *CacheController }

func (s cachedOnly) getCacheController() *CacheController { return s.cache }

func (cachedOnly) sendQuery(context.Context, chan<- error, string, feature_dns.IPOption) {}

func TestCacheMigrationPanicKeepsCacheWorking(t *testing.T) {
	safe.Reset()
	t.Cleanup(safe.Reset)
	c := NewCacheController("test", false, false, 0)
	// flush merges into the existing entry and reads the old record, which is nil here
	c.ips = map[string]*record{"broken.": {}}
	c.dirtyips = map[string]*record{"broken.": nil}

	c.runMigration()

	c.RLock()
	dirty := c.dirtyips
	c.RUnlock()
	if dirty != nil {
		t.Fatal("migration state is left after a panic, expired records are never cleaned")
	}
	if safe.Broken("app/dns") {
		t.Fatal("a panic in cache migration broke app/dns")
	}

	c.ips["expired."] = &record{A: &IPRecord{Expire: time.Now().Add(-time.Minute)}}
	c.ips["live."] = &record{A: &IPRecord{IP: []net.IP{net.IP{1, 2, 3, 4}}, Expire: time.Now().Add(time.Hour)}}
	if err := c.CacheCleanup(); err != nil {
		t.Fatal(err)
	}
	if c.findRecords("expired.") != nil {
		t.Fatal("expired record is not cleaned after a migration panic")
	}
	ips, _, err := queryIP(context.Background(), cachedOnly{c}, "live", feature_dns.IPOption{IPv4Enable: true})
	if err != nil || len(ips) != 1 || !ips[0].Equal(net.IP{1, 2, 3, 4}) {
		t.Fatal("cached answer after a migration panic: ", ips, ", error: ", err)
	}
}

func TestUpdateRecordPanicReleasesLock(t *testing.T) {
	c := NewCacheController("test", false, false, 0)
	// writing to a nil map panics while the cache is locked
	c.ips = nil
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("updateRecord did not panic")
			}
		}()
		c.updateRecord(&dnsRequest{reqType: dnsmessage.TypeA, domain: "a.", start: time.Now()}, &IPRecord{})
	}()
	if !c.TryLock() {
		t.Fatal("cache stays locked after a panic in updateRecord")
	}
	c.Unlock()
}
