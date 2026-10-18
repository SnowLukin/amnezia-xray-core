package health

import (
	"maps"
	"time"
)

// reportEvidence tells, for one kind of passive evidence, how often it was
// right: the user connections that failed this way, the suspicions it was
// part of, and how those ended. A suspicion that went down was confirmed; one
// that the traffic or a probe proved false before that was refuted; a down a
// later probe or audit proved false was rescued. Precision of a kind is
// (confirmed - rescued) / suspected.
type reportEvidence struct {
	Failures  int64 `json:"failures"`
	Suspected int64 `json:"suspected"`
	Confirmed int64 `json:"confirmed"`
	Refuted   int64 `json:"refuted"`
	Rescued   int64 `json:"rescued"`
}

// recentWindow is how far back the report looks at the user connections
// when it is taken: whether they were failing right before a stop.
const recentWindow = time.Minute

// recentOutcomes keeps at most this many latest user connection outcomes.
const recentOutcomes = 64

type outcome struct {
	at time.Time
	ok bool
}

// evidenceTracker follows suspicions by outbound through the events.
type evidenceTracker struct {
	// The kinds of the open suspicion of an outbound, and of a down that a
	// rescue may still prove false.
	suspected map[string]map[string]int
	downed    map[string]map[string]int
	recent    []outcome
}

func (r *report) evidence(kind string) *reportEvidence {
	if r.counts.Evidence == nil {
		r.counts.Evidence = map[string]*reportEvidence{}
	}
	e := r.counts.Evidence[kind]
	if e == nil {
		e = &reportEvidence{}
		r.counts.Evidence[kind] = e
	}
	return e
}

// countFailure counts a user connection that failed this way.
func (r *report) countFailure(kind failureKind, now time.Time) {
	r.evidence(kind.String()).Failures++
	r.noteOutcome(false, now)
}

func (r *report) noteOutcome(ok bool, now time.Time) {
	t := &r.evidenceState
	if len(t.recent) == recentOutcomes {
		t.recent = append(t.recent[:0], t.recent[1:]...)
	}
	t.recent = append(t.recent, outcome{at: now, ok: ok})
}

// trackEvidence follows path_state events: which kinds a suspicion rested
// on and how it ended.
func (r *report) trackEvidence(f map[string]any) {
	t := &r.evidenceState
	if t.suspected == nil {
		t.suspected, t.downed = map[string]map[string]int{}, map[string]map[string]int{}
	}
	tag, _ := f["outbound"].(string)
	state, _ := f["state"].(string)
	reason, _ := f["reason"].(string)
	switch state {
	case "suspect":
		kinds, _ := f["kinds"].(map[string]int)
		if len(kinds) == 0 {
			return
		}
		t.suspected[tag] = maps.Clone(kinds)
		for kind := range kinds {
			r.evidence(kind).Suspected++
		}
	case "down":
		if kinds := t.suspected[tag]; kinds != nil {
			for kind := range kinds {
				r.evidence(kind).Confirmed++
			}
			t.downed[tag] = kinds
			delete(t.suspected, tag)
		}
	case "up", "degraded":
		if kinds := t.suspected[tag]; kinds != nil {
			for kind := range kinds {
				r.evidence(kind).Refuted++
			}
			delete(t.suspected, tag)
		}
		if kinds := t.downed[tag]; kinds != nil {
			if reason == "rescued" {
				for kind := range kinds {
					r.evidence(kind).Rescued++
				}
			}
			delete(t.downed, tag)
		}
	}
}

// recentView tells how the user connections went in the last minute before
// the report: a stop while they were failing and no outage reported is worth
// a look, though a reconnect alone proves nothing.
type recentView struct {
	OK               int    `json:"ok"`
	Failed           int    `json:"failed"`
	LastSuccessAgoMs *int64 `json:"last_success_ago_ms,omitempty"`
	LastFailureAgoMs *int64 `json:"last_failure_ago_ms,omitempty"`
}

func (r *report) recentView(now time.Time) *recentView {
	t := &r.evidenceState
	if len(t.recent) == 0 {
		return nil
	}
	v := &recentView{}
	for i := len(t.recent) - 1; i >= 0; i-- {
		o := t.recent[i]
		ago := wholeMilliseconds(now.Sub(o.at))
		if o.ok && v.LastSuccessAgoMs == nil {
			v.LastSuccessAgoMs = &ago
		}
		if !o.ok && v.LastFailureAgoMs == nil {
			v.LastFailureAgoMs = &ago
		}
		if now.Sub(o.at) > recentWindow {
			continue
		}
		if o.ok {
			v.OK++
		} else {
			v.Failed++
		}
	}
	return v
}
