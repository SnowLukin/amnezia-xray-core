package health

import "time"

// takeBase compares the base config an applied answer names with the one
// the core started with. The base is that of the config, or that of a fresh
// initial set, the answer of the connect that brought the config. A set the
// host kept names the base of the backend in an earlier run, not the one
// this core runs with, so it never becomes the base; when it names another
// one, the host learns at the start of a change it put off.
//
// Another base is told once per value with provider_base_changed: the host
// fetches the config anew and restarts the core when that is cheap. An
// answer that names the base of the core again clears it, so a later
// change is told again. Only the value told last is kept, so a backend that
// names a new base every time grows nothing. The set applies whatever the
// base: the request tells the backend the base of the core, and it answers
// with outbounds that work there. Without a base nothing is told.
//
// It reports whether the answer names another base than the answer applied
// before, which the host keeps with the set.
func (e *engine) takeBase(p *provider, a *providerAnswer, now time.Time) bool {
	moved := a.baseVersion != p.answerBase
	p.answerBase = a.baseVersion
	switch {
	case a.baseVersion == "" || p.base == "":
	case a.baseVersion == p.base:
		p.baseReported = ""
	case a.baseVersion != p.baseReported:
		p.baseReported = a.baseVersion
		e.emit(now, "provider_base_changed", map[string]any{"provider": p.cfg.tag, "from": p.base, "to": a.baseVersion})
	}
	return moved
}

// baseFromInitial makes the base that of a fresh initial set when the
// config names none. It goes as soon as the set is read, before it is
// applied: one that does not build still came with the config and names
// it, and without the base no later change would be told. A set the host
// kept names an earlier config and is never the base.
func (e *engine) baseFromInitial(p *provider, a *providerAnswer) {
	if p.base == "" && a.receivedAt.IsZero() {
		p.base = a.baseVersion
	}
}

// baseFields tells in provider_apply the base config the answer named.
func baseFields(f map[string]any, a *providerAnswer) {
	if a.baseVersion != "" {
		f["base_version"] = a.baseVersion
	}
}
