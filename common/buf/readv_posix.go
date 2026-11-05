//go:build !windows && !wasm && !illumos && !openbsd
// +build !windows,!wasm,!illumos,!openbsd

package buf

import (
	"syscall"
	"unsafe"
)

// releaseWhileWaiting: readv fails here only when the socket has nothing to read yet, so the buffers can go back
// to the pool until it has
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

func (r *posixReader) Read(fd uintptr) int32 {
	n, _, e := syscall.Syscall(syscall.SYS_READV, fd, uintptr(unsafe.Pointer(&r.iovecs[0])), uintptr(len(r.iovecs)))
	if e != 0 {
		return -1
	}
	return int32(n)
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
