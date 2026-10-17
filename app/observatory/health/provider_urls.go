package health

import (
	"net/url"
	"slices"
	"time"
)

// maxProviderURLs bounds the mirrors of a provider. They are tried one after
// another within every via with the timeout of an attempt each, so four on
// three roads already make a refresh of a minute and a half at worst.
const maxProviderURLs = 4

// SetProviderURLs replaces the mirrors of the provider with the list the host
// found, in the order to try them: the discovery of the app finds the domains
// of the backend while the core runs, and a restart would cost the user the
// connection. The whole list is replaced; the attempt under way finishes with
// the old address and the next one takes the new list. The backoff after
// failures on the old addresses ends, a pause the backend asked for does not,
// and no refresh starts: RequestProviderRefresh does. The core keeps the list
// for the run only and never sends or reports the addresses.
func (o *Observer) SetProviderURLs(provider string, urls []string) error {
	if len(urls) < 1 || len(urls) > maxProviderURLs {
		return NewCommandError("invalid_argument", "urls must hold 1 to 4 addresses")
	}
	for i, destination := range urls {
		if slices.Contains(urls[:i], destination) {
			return NewCommandError("invalid_argument", "urls must not repeat an address")
		}
		u, err := url.Parse(destination)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.Fragment != "" {
			return NewCommandError("invalid_argument", "urls must be HTTP(S) URLs without credentials or fragment")
		}
	}
	var err error
	func() {
		o.mu.Lock()
		defer o.mu.Unlock()

		err = o.e.setProviderURLs(provider, urls, o.now())

	}()
	if err != nil {
		return err
	}
	o.wake()
	return nil
}

func (e *engine) setProviderURLs(tag string, urls []string, now time.Time) error {
	p := e.provider(tag)
	if p == nil {
		return e.providerCommandError(tag)
	}
	if !p.cfg.plainHTTP {
		for _, destination := range urls {
			if u, _ := url.Parse(destination); u.Scheme != "https" {
				return NewCommandError("invalid_argument", "urls must be https unless isPlainHttpAllowed")
			}
		}
	}
	if slices.Equal(p.urls, urls) {
		return nil
	}
	p.urls = slices.Clone(urls)
	p.urlsGen++
	// The failures in a row were on the old addresses; the new ones get
	// their chance at once. A pause the backend or refused headers set
	// stays: the backend is the same.
	if p.lastClass != "" {
		p.failures = 0
		p.backoffUntil = time.Time{}
	}
	e.emit(now, "provider_urls", map[string]any{"provider": p.cfg.tag, "count": len(urls), "generation": p.urlsGen})
	return nil
}

// providerURLs is the list of mirrors a refresh tries, taken once at its
// start under the observer lock.
func (o *Observer) providerURLs(rt *providerRuntime) []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	return slices.Clone(rt.p.urls)
}
