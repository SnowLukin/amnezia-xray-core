package health

import (
	"context"
	"sync"
	"testing"

	"github.com/xtls/xray-core/common/net"
)

// BenchmarkConnectionLock measures what a connection pays for the observer
// lock, with the loop idle and with the loop stepping without pause.
func BenchmarkConnectionLock(b *testing.B) {
	for _, tc := range []struct {
		name     string
		features *FeatureConfig
		stepping bool
	}{
		{"closure/idle", v2NoEvidence(), false},
		{"closure/stepping", v2NoEvidence(), true},
		{"tracker/idle", &FeatureConfig{Report: v2Bool(true)}, false},
		{"tracker/stepping", &FeatureConfig{Report: v2Bool(true)}, true},
	} {
		b.Run(tc.name, func(b *testing.B) {
			o, s, now := v2Observer(tc.features, nil)
			defer o.Close()
			h := &selectorHandle{o: o, s: s}
			stop := make(chan struct{})
			var wg sync.WaitGroup
			if tc.stepping {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for {
						select {
						case <-stop:
							return
						default:
						}
						func() {
							o.mu.Lock()
							defer o.mu.Unlock()
							o.e.step(*now)
						}()
					}
				}()
			}
			candidates := []string{"a", "b"}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					tag := h.Select(candidates)
					tr := o.StartDial(context.Background(), tag, net.Destination{}, func() {})
					if tr == nil {
						continue
					}
					read, _ := tr.Dialed(nil, nil, nil, nil)
					read.Add(1)
					tr.Finished(nil)
				}
			})
			b.StopTimer()
			close(stop)
			wg.Wait()
		})
	}
}
