package outbound

// Sleeper is implemented by outbound handlers that can release what they hold
// while idle, such as a tunnel and its network stack, and recreate it on
// demand. A handler may implement it while its proxy holds nothing of the
// kind; to find the proxies that do, check whether the proxy behind the
// handler (proxy.GetOutbound) implements Sleeper.
type Sleeper interface {
	// Sleep releases what the handler can recreate; the next connection
	// recreates it. It returns true when the handler holds nothing it could
	// release afterwards, which includes a handler that never held anything
	// and a closed one. It returns false and releases nothing while the
	// handler is in use, by a connection or by the setup of one; that passes,
	// and the caller may try again later.
	//
	// A connection that has just ended may still be closing inside a tunnel,
	// and Sleep cuts that short: sleep handlers that have been idle a while.
	Sleep() bool
}
