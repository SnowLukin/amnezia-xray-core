//go:build !wasm && !openbsd
// +build !wasm,!openbsd

package buf

import (
	"io"
	"sync/atomic"
	"syscall"

	"github.com/xtls/xray-core/common/platform"
	"github.com/xtls/xray-core/features/stats"
)

type allocStrategy struct {
	current uint32
}

func (s *allocStrategy) Current() uint32 {
	return s.current
}

func (s *allocStrategy) Adjust(n uint32) {
	if n >= s.current {
		s.current *= 2
	} else {
		s.current = n
	}

	if s.current > 8 {
		s.current = 8
	}

	if s.current == 0 {
		s.current = 1
	}
}

func (s *allocStrategy) Alloc() []*Buffer {
	bs := make([]*Buffer, s.current)
	for i := range bs {
		bs[i] = New()
	}
	return bs
}

type multiReader interface {
	Init([]*Buffer)
	Read(fd uintptr) int32
	Clear()
}

// ReadVReader is a Reader that uses readv(2) syscall to read data.
type ReadVReader struct {
	io.Reader
	rawConn syscall.RawConn
	mr      multiReader
	alloc   allocStrategy
	counter stats.Counter
}

// NewReadVReader creates a new ReadVReader.
func NewReadVReader(reader io.Reader, rawConn syscall.RawConn, counter stats.Counter) *ReadVReader {
	return &ReadVReader{
		Reader:  reader,
		rawConn: rawConn,
		alloc: allocStrategy{
			current: 1,
		},
		mr:      newMultiReader(),
		counter: counter,
	}
}

// readMulti takes the buffers only once the socket has something to read: they go back to the pool every time it
// has not, so a connection that waits for its peer does not hold them
func (r *ReadVReader) readMulti() (MultiBuffer, error) {
	var bs []*Buffer
	var nBytes int32
	err := r.rawConn.Read(func(fd uintptr) bool {
		if bs == nil {
			bs = r.alloc.Alloc()
			r.mr.Init(bs)
		}
		n := r.mr.Read(fd)
		if n < 0 {
			if releaseWhileWaiting {
				r.mr.Clear()
				ReleaseMulti(MultiBuffer(bs))
				bs = nil
			}
			return false
		}

		nBytes = n
		return true
	})
	r.mr.Clear()

	if err != nil {
		ReleaseMulti(MultiBuffer(bs))
		return nil, err
	}

	if nBytes == 0 {
		ReleaseMulti(MultiBuffer(bs))
		return nil, io.EOF
	}

	nBuf := 0
	for nBuf < len(bs) {
		if nBytes <= 0 {
			break
		}
		end := nBytes
		if end > Size {
			end = Size
		}
		bs[nBuf].end = end
		nBytes -= end
		nBuf++
	}

	for i := nBuf; i < len(bs); i++ {
		bs[i].Release()
		bs[i] = nil
	}

	return MultiBuffer(bs[:nBuf]), nil
}

// ReadMultiBuffer implements Reader.
func (r *ReadVReader) ReadMultiBuffer() (MultiBuffer, error) {
	single := r.alloc.Current() == 1
	if single && !releaseWhileWaiting {
		b, err := ReadBuffer(r.Reader)
		if b.IsFull() {
			r.alloc.Adjust(1)
		}
		if r.counter != nil && b != nil {
			r.counter.Add(int64(b.Len()))
		}
		return MultiBuffer{b}, err
	}

	// one buffer is read through readv as well where it can wait without the buffer: a plain read takes it before
	mb, err := r.readMulti()
	if r.counter != nil && mb != nil {
		r.counter.Add(int64(mb.Len()))
	}
	if err != nil {
		return nil, err
	}
	if !single {
		r.alloc.Adjust(uint32(len(mb)))
	} else if mb[0].IsFull() {
		r.alloc.Adjust(1)
	}
	return mb, nil
}

var useReadv atomic.Bool

func useReadV() bool {
	return useReadv.Load()
}

func reloadEnvSettings() error {
	const defaultFlagValue = "NOT_DEFINED_AT_ALL"
	value := platform.NewEnvFlag(platform.UseReadV).GetValue(func() string { return defaultFlagValue })
	enabled := false
	switch value {
	case defaultFlagValue, "auto", "enable":
		enabled = true
	}
	useReadv.Store(enabled)
	return nil
}

func init() {
	platform.RegisterEnvReload(reloadEnvSettings)
}
