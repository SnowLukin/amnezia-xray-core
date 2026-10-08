//go:build !linux && !darwin

package health

import "syscall"

// kernelReceived is not available here; spliced connections are then not
// judged by silence at all.
func kernelReceived(syscall.RawConn) (uint64, bool) {
	return 0, false
}
