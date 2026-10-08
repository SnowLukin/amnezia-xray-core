package outbound

// Sleeper is implemented by outbound handlers that hold resources they can
// recreate on demand, such as a tunnel and its network stack.
type Sleeper interface {
	// Sleep releases those resources while no connection uses them; the next
	// connection recreates them. It returns false and releases nothing while
	// the handler is in use.
	Sleep() bool
}
