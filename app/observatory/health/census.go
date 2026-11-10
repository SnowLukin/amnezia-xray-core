package health

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"maps"
	"math/rand"
	"slices"
	"sort"
	"strconv"
	"time"
)

const maxCensusProbes = 16

// Keep a bounded fingerprint set instead of network IDs. At the bound new
// networks are not sampled again: forgetting IDs would repeat old networks.
const maxCensusNetworks = 4096

// censusView is a finished observation, separate from controller/probe data.
type censusView struct {
	Network      censusNetworkView            `json:"network"`
	StartedMsAgo int64                        `json:"started_ms_ago"`
	DurationMs   int64                        `json:"duration_ms"`
	IsComplete   bool                         `json:"is_complete"`
	Paths        map[string]censusPathView    `json:"paths"`
	Additional   []*censusView                `json:"additional,omitempty"`
	PathLabels   map[string]map[string]string `json:"path_labels,omitempty"`
}

type censusNetworkView struct {
	IsKnown bool              `json:"is_known"`
	Host    map[string]string `json:"host,omitempty"`
	IP      map[string]string `json:"ip,omitempty"`
}

type censusPathView struct {
	Result        string `json:"result"`
	Source        string `json:"source,omitempty"`
	Reason        string `json:"reason,omitempty"`
	Error         string `json:"error,omitempty"`
	Member        string `json:"member,omitempty"`
	ObservedState string `json:"observed_state"`
	IsSelected    bool   `json:"is_selected,omitempty"`
	ConnectMs     int64  `json:"connect_ms,omitempty"`
	Ms            int64  `json:"ms,omitempty"`
}

type censusStatusView struct {
	IsSelected bool `json:"is_selected"`
	IsRunning  bool `json:"is_running"`
	Checked    int  `json:"checked"`
}

type censusItem struct {
	key, originalKey, tag, selector string
	cost                            float64
	order                           int
	member                          bool
	lazy                            bool
}

type censusRun struct {
	started, ended time.Time
	network        censusNetworkView
	paths          map[string]censusPathView
	pathLabels     map[string]map[string]string
	items          []censusItem
	next, probes   int
	checked        int
	complete       bool
}

type censusState struct {
	selected     bool
	rnd          *rand.Rand
	seen         map[[32]byte]bool
	anonymous    uint64
	networkKey   [32]byte
	networkSince time.Time
	active       *censusRun
	pending      []*censusRun
	lastChecked  int
}

func (e *engine) initCensus(seed int64) {
	if e.s.census == nil {
		return
	}
	// Census sampling and spacing never consume routing's random stream.
	h := sha256.Sum256([]byte("xray-path-census\x00" + strconv.FormatInt(seed, 10)))
	rnd := rand.New(rand.NewSource(int64(binary.BigEndian.Uint64(h[:8]))))
	e.census = &censusState{selected: rnd.Float64() < e.s.census.sampleFraction,
		rnd: rnd, seen: make(map[[32]byte]bool), networkKey: sha256.Sum256([]byte("anonymous\x000"))}
}

// The observer initializes this with its real startup time. Test engines can
// instead initialize it on their first tick, without an engine clock read.
func (e *engine) censusNetworkStarted(now time.Time) {
	if e.census != nil && e.census.networkSince.IsZero() {
		e.census.networkSince = now
	}
}

func (e *engine) censusNetworkChanged(id string, now time.Time) {
	if e.census == nil {
		return
	}
	e.interruptCensus("network_change", now)
	key := "id\x00" + id
	if id == "" {
		// Without a host ID no identity comparison is possible. Each explicit
		// notification is a new anonymous epoch; ordinary ticks do not rotate it.
		e.census.anonymous++
		key = "anonymous\x00" + strconv.FormatUint(e.census.anonymous, 10)
	}
	e.census.networkKey = sha256.Sum256([]byte(key))
	e.census.networkSince = now
}

func (e *engine) censusStateFor(tag string) string {
	if p := e.paths[tag]; p != nil {
		return p.status.String()
	}
	return statusUnknown.String()
}

func (e *engine) censusItems(now time.Time) []censusItem {
	var items []censusItem
	order := 0
	for _, s := range e.sortedSelectors() {
		tags := s.cfg.baseOrder
		if len(tags) == 0 && s.cfg.raw != nil {
			for _, p := range s.cfg.raw.Paths {
				tags = append(tags, p.Tag)
			}
		}
		// A provider may add paths without adding them to the base config.
		tags = slices.Clone(tags)
		for _, tag := range s.inventory {
			if !slices.Contains(tags, tag) {
				tags = append(tags, tag)
			}
		}
		groups := map[string][]string{}
		for _, tag := range tags {
			if c := s.cfg.paths[tag]; c != nil && c.groupName != "" {
				groups[c.groupName] = append(groups[c.groupName], tag)
			}
		}
		seenGroups := map[string]bool{}
		for _, tag := range tags {
			c := s.cfg.paths[tag]
			if c == nil || e.retiring[tag] || e.replacing[tag] {
				continue
			}
			item := censusItem{key: tag, tag: tag, selector: s.tag, cost: c.cost, order: order, lazy: c.lazy}
			order++
			if c.groupName != "" {
				if seenGroups[c.groupName] {
					continue
				}
				seenGroups[c.groupName] = true
				members := groups[c.groupName]
				position := 0
				if g := s.groups[c.groupName]; g != nil && len(g.members) > 0 {
					position = max(slices.Index(members, g.members[(g.position+1)%len(g.members)]), 0)
				} else {
					h := sha256.Sum256([]byte(e.run + "\x00" + c.groupName))
					position = (int(binary.BigEndian.Uint64(h[:8])%uint64(len(members))) + 1) % len(members)
				}
				item.key, item.member, item.tag = c.groupName, true, members[position]
				for i := range members {
					member := members[(position+i)%len(members)]
					if e.retiring[member] || e.replacing[member] {
						continue
					}
					if p := e.paths[member]; p == nil || !p.quarantined(now) {
						item.tag = member
						break
					}
				}
			}
			items = append(items, item)
		}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].cost != items[j].cost {
			return items[i].cost < items[j].cost
		}
		return items[i].order < items[j].order
	})
	counts := map[string]int{}
	for _, item := range items {
		counts[item.key]++
	}
	used := map[string]bool{}
	for i := range items {
		item := &items[i]
		item.originalKey = item.key
		if counts[item.key] > 1 {
			item.key = item.selector + "/" + item.key
		}
		base := item.key
		for suffix := 2; used[item.key]; suffix++ {
			item.key = base + ":" + strconv.Itoa(suffix)
		}
		used[item.key] = true
	}
	return items
}

func (e *engine) censusIdleDeadline() time.Time {
	if e.s.schedule.idleAfter <= 0 || e.lastDemand.Load() == 0 {
		return time.Time{}
	}
	return time.Unix(0, e.lastDemand.Load()).Add(e.s.schedule.idleAfter + time.Nanosecond)
}

func (e *engine) censusOnDemandAllowed(it censusItem) bool {
	if e.s.census.onDemand[it.originalKey] || e.s.census.onDemand[it.tag] {
		return true
	}
	if !it.member {
		return false
	}
	if s := e.selectors[it.selector]; s != nil {
		for tag, path := range s.cfg.paths {
			if path.groupName == it.originalKey && e.s.census.onDemand[tag] {
				return true
			}
		}
	}
	return false
}

func (e *engine) scheduleCensus(now time.Time, idle bool) time.Time {
	c := e.census
	if c == nil || !c.selected {
		return time.Time{}
	}
	if e.networkDown {
		e.interruptCensus("network_lost", now)
		c.networkSince = time.Time{}
		return time.Time{}
	}
	e.censusNetworkStarted(now)
	if c.active != nil && (idle || e.memoryLow) {
		reason := "idle"
		if e.memoryLow {
			reason = "memory"
		}
		e.interruptCensus(reason, now)
	}
	if idle || e.memoryLow {
		return time.Time{}
	}
	if c.active == nil {
		if c.seen[c.networkKey] || len(c.seen) >= maxCensusNetworks {
			return time.Time{}
		}
		due := c.networkSince.Add(e.s.census.startAfter)
		// Demand must belong to this continuous network epoch, not a previous
		// one. Unlock notifications alone do not fabricate user traffic.
		if last := e.lastDemand.Load(); last == 0 || last < c.networkSince.UnixNano() {
			return time.Time{}
		}
		if now.Before(due) {
			return due
		}
		c.seen[c.networkKey] = true
		c.active = &censusRun{started: now, network: censusNetworkView{IsKnown: e.networkKnown},
			paths: map[string]censusPathView{}, items: e.censusItems(now), complete: true}
		if labels := e.networkLabels(); labels != nil {
			c.active.network.Host, c.active.network.IP = labels.Host, labels.IP
		}
		for _, it := range c.active.items {
			v := censusPathView{Result: "pending", ObservedState: e.censusStateFor(it.tag)}
			if it.member {
				v.Member = it.tag
			}
			c.active.paths[it.key] = v
			if e.s.features.PathLabels {
				if selector := e.selectors[it.selector]; selector != nil {
					if path := selector.cfg.paths[it.tag]; path != nil && len(path.labels) > 0 {
						if c.active.pathLabels == nil {
							c.active.pathLabels = map[string]map[string]string{}
						}
						c.active.pathLabels[it.key] = maps.Clone(path.labels)
					}
				}
			}
		}
	}
	run := c.active
	for _, j := range e.running {
		if j.reason == reasonCensus && j.censusRun == run {
			return e.censusIdleDeadline()
		}
	}
	for _, j := range e.queue {
		if j.reason == reasonCensus && j.censusRun == run && !j.invalidated {
			return e.censusIdleDeadline()
		}
	}
	for run.next < len(run.items) {
		it := run.items[run.next]
		run.next++
		row := run.paths[it.key]
		row.IsSelected = e.active(it.tag)
		if it.lazy && !e.censusOnDemandAllowed(it) {
			row.Result, row.Reason = "skipped", "on_demand"
			run.paths[it.key] = row
			continue
		}
		if p := e.paths[it.tag]; row.IsSelected && p != nil && !p.lastSuccess.IsZero() &&
			!p.lastSuccess.Before(c.networkSince) && now.Sub(p.lastSuccess) < e.s.census.startAfter {
			row.Result, row.Source = "ok", "recent"
			row.Ms = p.lastRTT.Milliseconds()
			run.paths[it.key] = row
			run.checked++
			continue
		}
		if run.probes >= maxCensusProbes {
			row.Result, row.Reason = "skipped", "limit"
			run.paths[it.key] = row
			continue
		}
		e.nextID++
		j := &probeJob{id: e.nextID, tag: it.tag, selector: it.selector, reason: reasonCensus,
			notBefore: now, generation: e.generation, candidateEpoch: e.candidateEpoch, censusRun: run, censusKey: it.key}
		e.queue = append(e.queue, j)
		return e.censusIdleDeadline()
	}
	e.finishCensus(run, now)
	return time.Time{}
}

func (e *engine) censusJobCurrent(j *probeJob) bool {
	return e.census != nil && e.census.active == j.censusRun && !j.invalidated && !j.forgotten &&
		j.generation == e.generation && j.candidateEpoch == e.candidateEpoch
}

func (e *engine) hasOrdinaryProbeWork(kept []*probeJob, now time.Time) bool {
	// Higher priority jobs have already been validated and compacted this tick.
	// The original backing slice may still contain jobs that were discarded.
	for _, j := range kept {
		if j.reason == reasonCensus || j.invalidated || (j.reason != reasonConnectivity && j.generation != e.generation) {
			continue
		}
		if j.reason == reasonCandidate && !e.candidateCurrent(j) || j.reason == reasonGroup && !e.groupProbeClaimed(j) {
			continue
		}
		if j.isUDP {
			if e.udpJobCurrent(j) {
				return true
			}
			continue
		}
		if !e.heldBackNow(j, e.idle(now)) {
			return true
		}
	}
	for _, j := range e.running {
		if j.reason != reasonCensus && !j.invalidated {
			return true
		}
	}
	return false
}

func (e *engine) beginCensus(j *probeJob, now time.Time) {
	e.running[j.id] = j
	j.profile = e.s.probeFor(j.tag)
	j.isProbeOnly = j.profile.isProbeOnly
	j.candidateEpoch = e.candidateEpoch
	// Only budget state is touched. Controller lastProbe/probing, proof,
	// stats and candidate verification keep their ordinary meaning.
	e.path(j.tag)
	j.censusRun.probes++
	row := j.censusRun.paths[j.censusKey]
	row.ObservedState = e.censusStateFor(j.tag)
	row.IsSelected = e.active(j.tag)
	j.censusRun.paths[j.censusKey] = row
	spacing := e.s.schedule.spacing
	if fraction := e.s.schedule.spacingJitter; fraction > 0 {
		spacing = time.Duration(float64(spacing) * (1 + fraction*(2*e.census.rnd.Float64()-1)))
	}
	j.censusSpacing = now.Add(spacing)
	e.spacedUntil = append(e.spacedUntil, j.censusSpacing)
	e.starts = append(e.starts, now)
	name := e.serverName(j.tag)
	e.startsByName[name] = append(e.startsByName[name], now)
	e.report.counts.CensusProbes++
	e.emit(now, "probe_start", map[string]any{"outbound": j.tag, "reason": "census", "balancer": j.selector, "running": e.runningProbes()})
}

func (e *engine) onCensusResult(j *probeJob, r probeResult, now time.Time) {
	if j.censusDone {
		return
	}
	j.censusDone = true
	delete(e.running, j.id)
	e.report.counts.CensusProbeBytes += r.bytes
	eventError := ""
	if !r.ok && !r.canceled {
		eventError = cmp.Or(r.class, "other")
	}
	e.emit(now, "probe_result", map[string]any{"outbound": j.tag, "reason": "census", "balancer": j.selector, "is_ok": r.ok,
		"rtt_ms": r.rtt.Milliseconds(), "bytes": r.bytes, "error": eventError, "is_stale": !e.censusJobCurrent(j),
		"profile": r.profile, "state": r.pathState().String(), "steps": r.eventSteps()})
	if !e.censusJobCurrent(j) {
		return
	}
	run := j.censusRun
	row := run.paths[j.censusKey]
	row.Source = "probe"
	row.Result = "failed"
	if r.ok || (r.state != statusUnknown && r.state != statusDown) {
		row.Result = "ok"
	} else {
		row.Error = r.class
		if row.Error == "" {
			row.Error = "other"
		}
	}
	if r.canceled {
		row.Result, row.Reason = "skipped", "interrupted"
		row.Error = ""
		run.complete = false
	}
	for _, step := range r.steps {
		row.ConnectMs += step.connect.Milliseconds()
		row.Ms += step.elapsed.Milliseconds()
	}
	if row.Ms == 0 {
		row.Ms = r.rtt.Milliseconds()
	}
	run.paths[j.censusKey] = row
	run.checked++
}

func (e *engine) preemptCensus(now time.Time) {
	for id, j := range e.running {
		if j.reason != reasonCensus || j.invalidated {
			continue
		}
		j.invalidated = true
		delete(e.running, id)
		if i := slices.Index(e.spacedUntil, j.censusSpacing); i >= 0 {
			e.spacedUntil = slices.Delete(e.spacedUntil, i, i+1)
		}
		if j.cancel != nil {
			j.cancel()
		}
		if e.census != nil && e.census.active == j.censusRun {
			row := j.censusRun.paths[j.censusKey]
			row.Result, row.Reason, row.Source = "skipped", "priority", "probe"
			j.censusRun.paths[j.censusKey] = row
			j.censusRun.complete = false
		}
	}
}

func (e *engine) interruptCensus(reason string, now time.Time) {
	if e.census == nil || e.census.active == nil {
		return
	}
	run := e.census.active
	e.preemptCensus(now)
	e.queue = slices.DeleteFunc(e.queue, func(j *probeJob) bool { return j.reason == reasonCensus && j.censusRun == run })
	for key, row := range run.paths {
		if row.Result == "pending" || row.Reason == "priority" {
			row.Result, row.Reason = "skipped", reason
			run.paths[key] = row
		}
	}
	run.complete = false
	e.finishCensus(run, now)
}

func (e *engine) interruptCensusForPath(tag string, now time.Time) {
	if e.census == nil || e.census.active == nil {
		return
	}
	for _, item := range e.census.active.items {
		if item.tag == tag {
			e.interruptCensus("path_changed", now)
			return
		}
	}
}

func (run *censusRun) view(now time.Time) censusView {
	end := run.ended
	if end.IsZero() {
		end = now
	}
	network := run.network
	network.Host, network.IP = maps.Clone(network.Host), maps.Clone(network.IP)
	var labels map[string]map[string]string
	if len(run.pathLabels) > 0 {
		labels = make(map[string]map[string]string, len(run.pathLabels))
		for key, values := range run.pathLabels {
			labels[key] = maps.Clone(values)
		}
	}
	return censusView{Network: network, PathLabels: labels, StartedMsAgo: max(now.Sub(run.started).Milliseconds(), 0),
		DurationMs: max(end.Sub(run.started).Milliseconds(), 0), IsComplete: run.complete, Paths: maps.Clone(run.paths)}
}

func (e *engine) finishCensus(run *censusRun, now time.Time) {
	if !run.ended.IsZero() {
		return
	}
	run.ended = now
	e.census.active = nil
	e.census.lastChecked = run.checked
	// Events retain every completion. Pending report snapshots are bounded;
	// the report builder consumes each snapshot once via takeCensus.
	if len(e.census.pending) < maxNetworks {
		e.census.pending = append(e.census.pending, run)
	}
	encoded, _ := json.Marshal(run.view(now))
	fields := map[string]any{}
	_ = json.Unmarshal(encoded, &fields)
	e.emit(now, "census", fields)
}

func (e *engine) peekCensus(now time.Time) *censusView {
	if e.census == nil || len(e.census.pending) == 0 {
		return nil
	}
	view := e.census.pending[0].view(now)
	for _, run := range e.census.pending[1:] {
		additional := run.view(now)
		view.Additional = append(view.Additional, &additional)
	}
	return &view
}

func (e *engine) takeCensus(now time.Time) *censusView {
	if e.census == nil || len(e.census.pending) == 0 {
		return nil
	}
	view := e.peekCensus(now)
	e.census.pending = nil
	return view
}

func (e *engine) censusStatus() *censusStatusView {
	if e.census == nil {
		return nil
	}
	v := &censusStatusView{IsSelected: e.census.selected, Checked: e.census.lastChecked}
	if run := e.census.active; run != nil {
		v.IsRunning, v.Checked = true, run.checked
	}
	return v
}
