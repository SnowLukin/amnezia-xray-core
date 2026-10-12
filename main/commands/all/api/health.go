package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"unicode/utf8"

	healthService "github.com/xtls/xray-core/app/observatory/health/command"
	"github.com/xtls/xray-core/main/commands/base"
)

var cmdHealth = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api health [--server=127.0.0.1:8080] <status|events|network [id]|lost|present|pin <balancer> <outbound> [run]|release <balancer> [run]|refresh [provider]|report [take|final]|probe [--udp] <balancer> [outbound...]|headers <provider>|set <provider> [after]>",
	Short:       "Query and control the health observatory",
	Long: `
Query and control the health observatory. Output is JSON.

> Ensure that "HealthService" is enabled under "config.api.services".

Commands:

	status                      current state of outbounds and balancers
	events                      events since the previous call
	network [id]                the device moved to another network
	lost                        the device has no network
	present                     the user unlocked the device; queue checks
	                            of stale selected paths and return at once
	pin <balancer> <outbound> [run]
	                            keep the balancer on the outbound
	release <balancer> [run]    let the balancer choose again
	                            with run (from status), a core started
	                            again in the meantime ignores the request
	refresh [provider]          fetch the outbounds of a provider, or of all
	report [take|final]         the report a stop would bring now, without
	                            closing the window; take returns it when
	                            it is due and starts a new window, final
	                            is the last one before the core stops;
	                            null when there is nothing to report
	probe [--udp] <balancer> [outbound...]
	                            probe outbounds of the balancer now, by
	                            default the one it uses and its other
	                            candidates; waits for the result up to
	                            the timeout and prints the request with
	                            is_done false when it is not over yet
	headers <provider>          put headers over those of the config for
	                            the requests of the provider, such as a new
	                            token, from a JSON object on stdin:
	                            {"Authorization": "Bearer ..."}; an empty
	                            value removes one. Never from arguments,
	                            which the process list shows
	set <provider> [after]      the last set the provider applied, valid
	                            as its initial, with the token: {"seq": n,
	                            "set": {...}}; set is null when nothing
	                            changed since the seq after

Arguments:

	-s, -server <server:port>
		The API server address. Default 127.0.0.1:8080

	-t, -timeout <seconds>
		Timeout in seconds for calling API. Default 3
`,
	Run: executeHealth,
}

func executeHealth(cmd *base.Command, args []string) {
	setSharedFlags(cmd)
	cmd.Flag.Parse(args)
	a := cmd.Flag.Args()
	if len(a) == 0 {
		base.Fatalf("health: missing command")
	}
	var headers map[string]string
	if a[0] == "headers" {
		if len(a) < 2 {
			base.Fatalf("health headers: need provider")
		}
		// Before the dial, whose timeout covers the call: a person may be
		// typing the object.
		var err error
		if headers, err = providerHeaders(os.Stdin); err != nil {
			base.Fatalf("health headers: %s", err)
		}
	}
	conn, ctx, close := dialAPIServer()
	defer close()
	client := healthService.NewHealthServiceClient(conn)
	switch a[0] {
	case "status":
		r, err := client.GetStatus(ctx, &healthService.GetStatusRequest{})
		if err != nil {
			base.Fatalf("health status: %s", err)
		}
		fmt.Println(r.Json)
	case "events":
		r, err := client.DrainEvents(ctx, &healthService.DrainEventsRequest{})
		if err != nil {
			base.Fatalf("health events: %s", err)
		}
		fmt.Printf("{\"dropped\":%d,\"events\":%s}\n", r.Dropped, r.Json)
	case "present":
		if len(a) != 1 {
			base.Fatalf("health present: no arguments expected")
		}
		if _, err := client.UserPresent(ctx, &healthService.UserPresentRequest{}); err != nil {
			base.Fatalf("health present: %s", err)
		}
		fmt.Println("{}")
	case "network", "lost":
		req := &healthService.NetworkChangedRequest{Lost: a[0] == "lost"}
		if len(a) > 1 {
			req.Network = a[1]
		}
		if _, err := client.NetworkChanged(ctx, req); err != nil {
			base.Fatalf("health %s: %s", a[0], err)
		}
		fmt.Println("{}")
	case "pin", "release":
		req := &healthService.PinRequest{}
		if len(a) > 1 {
			req.Balancer = a[1]
		}
		run := 2
		if a[0] == "pin" {
			if len(a) < 3 {
				base.Fatalf("health pin: need balancer and outbound")
			}
			req.Outbound = a[2]
			run = 3
		}
		if len(a) > run {
			req.Run = a[run]
		}
		r, err := client.Pin(ctx, req)
		if err != nil {
			base.Fatalf("health %s: %s", a[0], err)
		}
		fmt.Printf("{\"ok\":%t}\n", r.Ok)
	case "refresh":
		req := &healthService.RefreshRequest{}
		if len(a) > 1 {
			req.Provider = a[1]
		}
		r, err := client.Refresh(ctx, req)
		if err != nil {
			base.Fatalf("health refresh: %s", err)
		}
		fmt.Printf("{\"ok\":%t}\n", r.Ok)
	case "report":
		req := &healthService.GetReportRequest{}
		if len(a) > 1 {
			switch a[1] {
			case "take":
				req.Take = true
			case "final":
				req.Final = true
			default:
				base.Fatalf("health report: unknown argument %s", a[1])
			}
		}
		r, err := client.GetReport(ctx, req)
		if err != nil {
			base.Fatalf("health report: %s", err)
		}
		if r.Json == "" {
			fmt.Println("null")
		} else {
			fmt.Println(r.Json)
		}
	case "probe":
		isUDP := false
		filtered := a[:1]
		for _, arg := range a[1:] {
			if arg == "--udp" {
				isUDP = true
			} else {
				filtered = append(filtered, arg)
			}
		}
		a = filtered
		if len(a) < 2 {
			base.Fatalf("health probe: need balancer")
		}
		req := &healthService.ProbeRequest{Balancer: a[1], Outbounds: a[2:], IsUdp: isUDP, WaitMs: int64(apiTimeout) * 1000}
		r, err := client.Probe(ctx, req)
		if err != nil {
			base.Fatalf("health probe: %s", err)
		}
		request := r.Json
		if request == "" {
			request = "null"
		}
		fmt.Printf("{\"id\":%d,\"run\":%q,\"request\":%s}\n", r.Id, r.Run, request)
	case "set":
		if len(a) < 2 {
			base.Fatalf("health set: need provider")
		}
		req := &healthService.GetProviderSetRequest{Provider: a[1]}
		if len(a) > 2 {
			after, err := strconv.ParseUint(a[2], 10, 64)
			if err != nil {
				base.Fatalf("health set: after must be a number")
			}
			req.After = after
		}
		r, err := client.GetProviderSet(ctx, req)
		if err != nil {
			base.Fatalf("health set: %s", err)
		}
		set := r.Json
		if set == "" {
			set = "null"
		}
		fmt.Printf("{\"seq\":%d,\"set\":%s}\n", r.Seq, set)
	case "headers":
		if _, err := client.SetProviderHeaders(ctx, &healthService.SetProviderHeadersRequest{Provider: a[1], Headers: headers}); err != nil {
			base.Fatalf("health headers: %s", err)
		}
		fmt.Println("{\"ok\":true}")
	default:
		base.Fatalf("health: unknown command %s", a[0])
	}
}

// maxHeadersInput bounds what providerHeaders reads: eight values of 4 KiB
// and their names fit many times over.
const maxHeadersInput = 256 << 10

// providerHeaders reads the headers for a provider as a JSON object of
// strings. A token never comes from the arguments, which the process list
// shows, and an error never quotes the input, which holds it.
func providerHeaders(in io.Reader) (map[string]string, error) {
	raw, err := io.ReadAll(io.LimitReader(in, maxHeadersInput+1))
	if err != nil {
		return nil, err
	}
	if len(raw) > maxHeadersInput {
		return nil, fmt.Errorf("stdin holds more than %d bytes", maxHeadersInput)
	}
	if !utf8.Valid(raw) {
		// The decoder would put U+FFFD in place of such bytes, and the
		// provider would send another token than the one given.
		return nil, errors.New("stdin is not valid UTF-8")
	}
	var headers map[string]string
	if json.Unmarshal(raw, &headers) != nil || headers == nil {
		return nil, errors.New(`stdin must hold a JSON object of strings, such as {"Authorization": "Bearer ..."}`)
	}
	return headers, nil
}
