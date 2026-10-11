//go:build !linux && !darwin

package health

import "syscall"

func kernelTCPQuality(syscall.RawConn) tcpQualitySnapshot {
	return tcpQualitySnapshot{}
}
