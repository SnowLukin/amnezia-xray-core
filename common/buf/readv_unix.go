//go:build illumos
// +build illumos

package buf

import (
	"os"

	"golang.org/x/sys/unix"
)

// releaseWhileWaiting: Read tells an empty socket from a failed one, so the buffers can go back to the pool until
// the socket has something to read
const releaseWhileWaiting = true

type unixReader struct {
	iovs [][]byte
}

func (r *unixReader) Init(bs []*Buffer) {
	iovs := r.iovs
	if iovs == nil {
		iovs = make([][]byte, 0, len(bs))
	}
	for _, b := range bs {
		iovs = append(iovs, b.v)
	}
	r.iovs = iovs
}

func (r *unixReader) Read(fd uintptr) (int32, error) {
	for {
		n, e := unix.Readv(int(fd), r.iovs)
		switch e {
		case nil:
			return int32(n), nil
		case unix.EINTR:
			continue
		case unix.EAGAIN:
			return -1, nil
		}
		return -1, os.NewSyscallError("readv", e)
	}
}

func (r *unixReader) Clear() {
	// without the slices of the buffers, which go back to the pool while it waits
	clear(r.iovs)
	r.iovs = r.iovs[:0]
}

func newMultiReader() multiReader {
	return &unixReader{}
}
