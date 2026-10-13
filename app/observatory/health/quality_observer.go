package health

import (
	"sync/atomic"
	"time"
)

// changeQualityNetwork runs under the observer lock, independently of the
// routing generation. IGNORE can preserve controller evidence while this
// measurement epoch still closes the old network segment. The summary and
// engine epochs start at zero and advance together on every host change.
func (e *engine) changeQualityNetwork(id string, known bool, now time.Time) {
	e.qualityEpoch++
	if e.quality != nil {
		e.quality.networkChanged(id, known, now)
	}
}

// collectQualityInitial and collectQualityEnd require the existing observer
// lock. Socket snapshots have already been published by the tracker; neither
// function reads the clock, performs a syscall, nor calls a controller method.
// A missing tracker quality state covers UDP/probes/shouldSkipQuality. A
// missing summary also keeps pre-integration tracker-only tests independent.
func (o *Observer) collectQualityInitial(q *connectionQuality, tag string, witness bool) {
	if q == nil || o.e.quality == nil {
		return
	}
	if sample, ok := q.takeInitial(); ok {
		o.e.quality.addInitial(q.network, tag, witness, sample)
		if !witness {
			o.e.labelQualityPath(tag, q.network)
		}
	}
}

func (o *Observer) collectQualityEnd(q *connectionQuality, tag string, witness bool, bytesDown *atomic.Int64) {
	if q == nil {
		return
	}
	// Close measurement admission even in tracker-only fixtures. With no
	// summary the ready request has no callback and consumes no initial data.
	request := qualityEndRequest{}
	if o.e.quality != nil {
		request = qualityEndRequest{observer: o, tag: tag, witness: witness}
	}
	if request, ready := q.requestEnd(request, bytesDown.Load); ready && request.observer != nil {
		o.collectQualityReadyEnd(q, request)
	}
}

// Caller holds observer.mu; lifecycle admission is already closed and all
// admitted snapshots published. A deferred end enters the currently available
// report window. Existing snapshots stay immutable; the epoch gate still
// rejects completion if the network changed while a sampler was in flight.
func (o *Observer) collectQualityReadyEnd(q *connectionQuality, request qualityEndRequest) {
	if o.e.quality == nil {
		return
	}
	o.collectQualityInitial(q, request.tag, request.witness)
	o.e.quality.addEnd(q.network, request.tag, request.witness, q.end(request.bytesDown))
	if !request.witness {
		o.e.labelQualityPath(request.tag, q.network)
	}
}

// Only the winner that has finished publishing the witness's first point may
// call this. A concurrent losing read must not consume a dial-only initial
// sample while S1 is still in flight.
func (o *Observer) qualityWitnessAnswered(t *witnessTracker) {
	if t.quality == nil {
		return
	}

	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.collectQualityInitial(t.quality, "", true)

	}()
}

// Finished's tracker CAS supplies idempotence. qualityRead, unlike the legacy
// witness success counter, continues counting after successBytes; it includes
// the complete connection and splice's final counted bytes.
func (o *Observer) qualityWitnessFinished(t *witnessTracker) {
	if t.quality == nil {
		return
	}

	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		o.collectQualityEnd(t.quality, "", true, &t.qualityRead)

	}()
}
