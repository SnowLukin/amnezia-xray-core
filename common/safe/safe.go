// Package safe contains panic boundaries for runtime workers. Startup invariant
// checks deliberately remain outside these boundaries.
//
// A connection panic closes only that connection. A subsystem panic, or the
// same connection site panicking repeatedly, ends the process as an unrecovered
// panic would, so the host restarts the tunnel. The adaptive observer is the
// exception: its subsystem panic freezes the observer and the process lives.
package safe

import (
	"errors"
	"fmt"
	"log"
	"path"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const Connection = "connection"
const Subsystem = "subsystem"

const observer = "app/observatory/health/"

const (
	connectionPanicLimit  = 20
	connectionPanicWindow = time.Minute
)

var ErrPanic = errors.New("worker panic recovered")

var now = time.Now

// Failure contains only static source identifiers, never the panic value.
type Failure struct {
	Scope, Where string
	Number       uint64
}

var state struct {
	sync.Mutex
	count  uint64
	events []Failure
	broken map[string]bool
	active int
	recent map[string][]time.Time
}

var logSink atomic.Pointer[func(string)]

// SetLog sends recovery records to the core log instead of stderr; nil restores stderr.
func SetLog(sink func(message string)) {
	if sink == nil {
		logSink.Store(nil)
		return
	}
	logSink.Store(&sink)
}

// fatal carries an escalated panic through every outer boundary to the runtime.
type fatal struct{ value any }

func (f *fatal) Error() string { return fmt.Sprint(f.value) }

// Recover must be deferred directly by the worker, not by another defer.
// onPanic must release the worker's resources and unblock its waiters.
func Recover(scope, where string, onPanic func()) {
	r := recover()
	if r == nil {
		return
	}
	if f, ok := r.(*fatal); ok {
		panic(f)
	}
	if record(scope, where) {
		// Cleanup is skipped: it may wait on a lock the panicking worker still holds.
		panic(&fatal{r})
	}
	if onPanic != nil {
		cleanup(where, onPanic)
	}
}

func cleanup(where string, fn func()) {
	defer Recover(Connection, where+".cleanup", nil)
	fn()
}

func subsystemOf(where string) string {
	return path.Dir(strings.SplitN(where, ":", 2)[0])
}

// record reports whether the panic must end the process.
func record(scope, where string) (escalate bool) {
	contained := strings.HasPrefix(where, observer)
	state.Lock()
	state.count++
	failure := Failure{scope, where, state.count}
	switch {
	case scope == Subsystem && contained:
		if state.broken == nil {
			state.broken = map[string]bool{}
		}
		state.broken[subsystemOf(where)] = true
	case scope == Subsystem:
		escalate = true
	case !contained:
		escalate = repeated(where)
	}
	if len(state.events) == 256 {
		copy(state.events, state.events[1:])
		state.events = state.events[:255]
	}
	state.events = append(state.events, failure)
	state.Unlock()
	// runtime.Stack includes argument values. CallersFrames contains source only.
	pcs := make([]uintptr, 48)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(3, pcs)])
	var stack strings.Builder
	for {
		frame, more := frames.Next()
		fmt.Fprintf(&stack, "\n%s %s:%d", frame.Function, path.Base(frame.File), frame.Line)
		if !more {
			break
		}
	}
	message := fmt.Sprintf("panic_recovered scope=%s where=%s%s", scope, where, stack.String())
	if sink := logSink.Load(); sink != nil {
		(*sink)(message)
	} else {
		log.Print(message)
	}
	return escalate
}

// repeated counts connection panics of one site in a sliding window; the
// caller holds state. A deterministic bug there would fail every connection.
func repeated(where string) bool {
	t := now()
	times := state.recent[where]
	expired := 0
	for expired < len(times) && t.Sub(times[expired]) >= connectionPanicWindow {
		expired++
	}
	times = append(times[expired:], t)
	if state.recent == nil {
		state.recent = map[string][]time.Time{}
	}
	state.recent[where] = times
	return len(times) >= connectionPanicLimit
}

// Snapshot returns the process count and bounded events since cursor. A caller
// derives counters from Count even if the event ring overflowed.
func Snapshot(cursor uint64) (count uint64, events []Failure, broken []string) {
	state.Lock()
	defer state.Unlock()
	broken = []string{}
	for _, event := range state.events {
		if event.Number > cursor {
			events = append(events, event)
		}
	}
	for name := range state.broken {
		broken = append(broken, name)
	}
	sort.Strings(broken)
	return state.count, events, broken
}

func Broken(subsystem string) bool {
	state.Lock()
	defer state.Unlock()
	return state.broken[subsystem]
}

// Reset is for isolated tests. Production uses BeginRun and monotonic counts.
func Reset() {
	state.Lock()
	state.count = 0
	state.events = nil
	state.broken = nil
	state.active = 0
	state.recent = nil
	state.Unlock()
}

// Close tears down independently owned sides even when one closer panics.
// Interrupt takes precedence for pipes, whose Close may leave reads waiting.
func Close(resources ...any) {
	for _, resource := range resources {
		if resource == nil {
			continue
		}
		v := reflect.ValueOf(resource)
		if (v.Kind() == reflect.Pointer || v.Kind() == reflect.Func || v.Kind() == reflect.Interface) && v.IsNil() {
			continue
		}
		cleanup("common/safe:Close", func() {
			switch c := resource.(type) {
			case interface{ Interrupt() }:
				c.Interrupt()
			case interface{ Close() error }:
				_ = c.Close()
			case interface{ Close() }:
				c.Close()
			case func():
				c()
			default:
				if v.Kind() == reflect.Func && v.Type().NumIn() == 0 && v.Type().NumOut() == 0 {
					v.Call(nil)
				} else if TestPanicEnabled {
					panic(&fatal{fmt.Sprintf("safe.Close: %T has no Close or Interrupt", resource)})
				}
			}
		})
	}
}

// Run is used with an already captured callback at a goroutine boundary.
func Run(scope, where string, onPanic func(), fn func()) {
	if !Allowed(scope, where, onPanic) {
		return
	}
	defer Recover(scope, where, onPanic)
	fn()
}

func Closer(resources ...any) func() { return func() { Close(resources...) } }

// Allowed prevents a failed subsystem from launching further work. Only the
// adaptive observer can be failed: other subsystem panics end the process.
func Allowed(scope, where string, onStop func()) bool {
	if scope != Subsystem || !Broken(subsystemOf(where)) {
		return true
	}
	if onStop != nil {
		cleanup(where, onStop)
	}
	return false
}

// Error returns a stable error to parallel-task waiters after a recovered panic.
func Error(scope, where string, fn func() error) (err error) {
	defer Recover(scope, where, func() { err = ErrPanic })
	return fn()
}

// FunctionWhere names a callback's code, without inspecting captured values.
func FunctionWhere(fn any) string {
	name := runtime.FuncForPC(reflect.ValueOf(fn).Pointer()).Name()
	name = strings.TrimPrefix(name, "github.com/xtls/xray-core/")
	i := strings.Index(name, ".")
	if i < 0 {
		return "common/task/worker.go:" + name
	}
	return name[:i] + "/worker.go:" + name[i+1:]
}

// BeginRun clears the observer failure only when there is no other live core.
// The process sequence remains monotonic so existing observer cursors are valid.
func BeginRun() {
	state.Lock()
	defer state.Unlock()
	if state.active == 0 {
		state.broken = map[string]bool{}
	}
	state.active++
}
func EndRun() {
	state.Lock()
	defer state.Unlock()
	if state.active > 0 {
		state.active--
	}
}
