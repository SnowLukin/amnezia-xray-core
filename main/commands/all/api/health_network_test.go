package api

import (
	"reflect"
	"testing"
)

func TestHealthNetworkArgumentsPreserveOpaqueAndEmptyIDs(t *testing.T) {
	for _, tc := range []struct {
		args   []string
		id     string
		labels map[string]string
	}{
		{nil, "", map[string]string{}},
		{[]string{""}, "", map[string]string{}},
		{[]string{"", "--label", "type=wifi"}, "", map[string]string{"type": "wifi"}},
		{[]string{"opaque", "--label=type=cellular", "--label", "operator=25001"}, "opaque", map[string]string{"type": "cellular", "operator": "25001"}},
		{[]string{"--label-net"}, "--label-net", map[string]string{}},
		{[]string{"--label", "type=wifi"}, "", map[string]string{"type": "wifi"}},
	} {
		id, labels, err := healthNetworkArguments(tc.args)
		if err != nil || id != tc.id || !reflect.DeepEqual(labels, tc.labels) {
			t.Fatalf("%q: id=%q labels=%v err=%v", tc.args, id, labels, err)
		}
	}
	for _, args := range [][]string{{"home", "--label"}, {"home", "--label", "=wifi"}, {"home", "--label=type"}, {"home", "unexpected"}} {
		if _, _, err := healthNetworkArguments(args); err == nil {
			t.Fatalf("invalid args accepted: %q", args)
		}
	}
}

func TestHealthTraceArguments(t *testing.T) {
	if req, err := healthTraceArguments([]string{"5", "--max-bytes=100"}); err != nil || req.After != 5 || req.MaxBytes != 100 {
		t.Fatalf("%v %v", req, err)
	}
	if _, err := healthTraceArguments([]string{"5", "9"}); err == nil {
		t.Fatal("second after accepted")
	}
}
