package health

import (
	"math"
	"sync"
)

type qualityEndRequest struct {
	observer  *Observer
	tag       string
	witness   bool
	bytesDown int64
}

// Only the first read and claimed milestones use this gate. It is never held
// across a socket syscall or observer callback, and never used between marks.
// Finish closes admission without waiting for sampling work. The last admitted
// publication can then flush exactly one end from the original byte cutoff.
type qualityLifecycle struct {
	mu           sync.Mutex
	inFlight     int
	closed       bool
	endCollected bool
	// The splice marker suppresses a later aggregate counter update, while
	// preserving one first reply already observable by the legacy socket sweep.
	splicedFirstSuppressed bool
	end                    qualityEndRequest
}

func (q *connectionQuality) beginFirstSample(kernelProgress bool) bool {
	q.life.mu.Lock()
	defer q.life.mu.Unlock()
	if q.life.closed {
		return false
	}
	if kernelProgress && q.life.splicedFirstSuppressed {
		q.life.splicedFirstSuppressed = false
	} else if q.firstClaim.Swap(true) {
		return false
	}
	q.life.inFlight++
	return true
}

func (q *connectionQuality) beginMarkSample(total int64) bool {
	q.life.mu.Lock()
	defer q.life.mu.Unlock()
	boundary := q.next.Load()
	if q.life.closed || uint64(total) < boundary {
		return false
	}
	next := uint64(math.MaxUint64)
	for _, boundary := range qualityMilestones {
		if boundary > uint64(total) {
			next = boundary
			break
		}
	}
	q.next.Store(next)
	q.life.inFlight++
	return true
}

// takeReadyEnd requires the lifecycle gate. Points have been published before
// the last sampler releases its count; no new sampler can enter after closed.
func (q *connectionQuality) takeReadyEnd() (qualityEndRequest, bool) {
	if !q.life.closed || q.life.inFlight != 0 || q.life.endCollected {
		return qualityEndRequest{}, false
	}
	q.life.endCollected = true
	request := q.life.end
	q.life.end = qualityEndRequest{}
	return request, true
}

func (q *connectionQuality) requestEnd(request qualityEndRequest, loadBytesDown func() int64) (qualityEndRequest, bool) {
	q.life.mu.Lock()
	defer q.life.mu.Unlock()
	if q.life.closed {
		return qualityEndRequest{}, false
	}
	q.life.closed = true
	q.next.Store(math.MaxUint64)
	// This immediate atomic counter load follows admission close under the
	// same gate. Every admitted mark incremented the counter before entering
	// the gate, so its total cannot exceed this completion byte cutoff. The
	// loader is not retained and performs no socket or observer operation.
	request.bytesDown = loadBytesDown()
	q.life.end = request
	return q.takeReadyEnd()
}

func (q *connectionQuality) publishedSample() {
	q.life.mu.Lock()
	q.life.inFlight--
	request, ready := q.takeReadyEnd()
	q.life.mu.Unlock()
	if ready && request.observer != nil {
		// Release the private gate before taking the observer lock. Finish's
		// existing lock can continue controller/stat/cleanup work immediately.
		o := request.observer

		func() {
			o.mu.Lock()
			defer o.mu.Unlock()

			o.collectQualityReadyEnd(q, request)

		}()
	}
}
