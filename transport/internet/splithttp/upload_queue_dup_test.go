package splithttp

import (
	"bytes"
	"io"
	"testing"
)

func TestUploadQueueDropsDuplicateSeq(t *testing.T) {
	q := NewUploadQueue(30)
	a := bytes.Repeat([]byte{'A'}, 16384)
	b := make([]byte, 16384)
	for i := range b {
		b[i] = byte(i % 251)
	}
	_ = q.Push(Packet{Payload: b, Seq: 1})
	_ = q.Push(Packet{Payload: append([]byte(nil), b...), Seq: 1})
	_ = q.Push(Packet{Payload: a, Seq: 0})
	var got []byte
	buf := make([]byte, 8192)
	for len(got) < len(a)+len(b) {
		n, err := q.Read(buf)
		got = append(got, buf[:n]...)
		if err != nil && err != io.EOF {
			t.Fatalf("read: %v", err)
		}
	}
	if !bytes.Equal(got, append(append([]byte(nil), a...), b...)) {
		t.Fatalf("stream corrupted: first bad byte at %d", firstDiff(got, append(append([]byte(nil), a...), b...)))
	}
}

func firstDiff(x, y []byte) int {
	for i := range x {
		if i >= len(y) || x[i] != y[i] {
			return i
		}
	}
	return -1
}
