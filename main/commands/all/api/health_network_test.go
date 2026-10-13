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
