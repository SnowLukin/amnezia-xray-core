//go:build resilience_test

package safe

import "testing"

func TestCloseRejectsUnknownResource(t *testing.T) {
	Reset()
	defer Reset()
	if !escapes(func() { Close(struct{}{}) }) {
		t.Fatal("resource without Close ignored")
	}
	if !escapes(func() { Run(Connection, "test/connection.go:handler", Closer(42), func() { panic("parse") }) }) {
		t.Fatal("cleanup swallowed an unknown resource")
	}
}
