package task_test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/xtls/xray-core/common"
	. "github.com/xtls/xray-core/common/task"
)

func TestPeriodicTaskStop(t *testing.T) {
	var value atomic.Int32
	task := &Periodic{
		Interval: time.Second * 2,
		Execute: func() error {
			value.Add(1)
			return nil
		},
	}
	common.Must(task.Start())
	time.Sleep(time.Second * 5)
	common.Must(task.Close())
	if value.Load() != 3 {
		t.Fatal("expected 3, but got ", value.Load())
	}
	time.Sleep(time.Second * 4)
	if value.Load() != 3 {
		t.Fatal("expected 3, but got ", value.Load())
	}
	common.Must(task.Start())
	time.Sleep(time.Second * 3)
	if value.Load() != 5 {
		t.Fatal("Expected 5, but ", value.Load())
	}
	common.Must(task.Close())
}
