package core_test

import (
	"strings"
	"sync"
	"testing"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/safe"
	. "github.com/xtls/xray-core/core"
)

type errorCapture struct {
	sync.Mutex
	errors []string
}

func (c *errorCapture) Handle(msg log.Message) {
	if m, ok := msg.(*log.GeneralMessage); ok && m.Severity == log.Severity_Error {
		c.Lock()
		c.errors = append(c.errors, m.String())
		c.Unlock()
	}
}

func TestRecoveredPanicReachesCoreErrorLog(t *testing.T) {
	safe.Reset()
	defer safe.Reset()
	defer safe.SetLog(nil)
	capture := &errorCapture{}
	log.RegisterHandler(capture)
	instance := new(Instance)
	common.Must(instance.Start())
	defer instance.Close()

	safe.Run(safe.Connection, "test/connection.go:handler", nil, func() { panic("secret.example") })

	capture.Lock()
	defer capture.Unlock()
	if len(capture.errors) != 1 || !strings.Contains(capture.errors[0], "panic_recovered scope=connection where=test/connection.go:handler") || strings.Contains(capture.errors[0], "secret") {
		t.Fatalf("%q", capture.errors)
	}
}
