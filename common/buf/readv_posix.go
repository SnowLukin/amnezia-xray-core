//go:build !windows && !wasm && !illumos && !openbsd
// +build !windows,!wasm,!illumos,!openbsd

package buf

import (
	"os"
	"syscall"
	"unsafe"
)

// releaseWhileWaiting: Read tells an empty socket from a failed one, so the buffers can go back to the pool until
// the socket has something to read
const releaseWhileWaiting = true

type posixReader struct {
	iovecs []syscall.Iovec
}

func (r *posixReader) Init(bs []*Buffer) {
	iovecs := r.iovecs
	if iovecs == nil {
		iovecs = make([]syscall.Iovec, 0, len(bs))
	}
	for idx, b := range bs {
		iovecs = append(iovecs, syscall.Iovec{
			Base: &b.v[0],
		})
		iovecs[idx].SetLen(int(Size))
	}
	r.iovecs = iovecs
}

func (r *posixReader) Read(fd uintptr) (int32, error) {
	for {
		n, _, e := syscall.Syscall(syscall.SYS_READV, fd, uintptr(unsafe.Pointer(&r.iovecs[0])), uintptr(len(r.iovecs)))
		switch e {
		case 0:
			return int32(n), nil
		case syscall.EINTR:
			continue
		case syscall.EAGAIN:
			return -1, nil
		}
		return -1, os.NewSyscallError("readv", e)
	}
}

func (r *posixReader) Clear() {
	for idx := range r.iovecs {
		r.iovecs[idx].Base = nil
	}
	r.iovecs = r.iovecs[:0]
}

func newMultiReader() multiReader {
	return &posixReader{}
}
