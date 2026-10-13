package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"unicode/utf8"

	healthService "github.com/xtls/xray-core/app/observatory/health/command"
	"github.com/xtls/xray-core/main/commands/base"
)

var cmdHealth = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api health [--server=127.0.0.1:8080] <status|events|network [id]|lost|present|pin <balancer> <outbound> [run]|release <balancer> [run]|refresh [provider]|report [take|final]|probe [--udp] <balancer> [outbound...]|headers <provider>|set <provider> [after]|probe-result <run> <id>|memory --low=true>",
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
	                            as its initialAnswer, with the token: {"seq": n,
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

	var response *healthService.Result
	var err error
	switch a[0] {
	case "status":
		response, err = client.Status(ctx, &healthService.StatusRequest{})
	case "events":
		response, err = client.DrainEvents(ctx, &healthService.DrainEventsRequest{})
	case "network":
		req := &healthService.NetworkChangedRequest{}
		labels := map[string]string{}
		for i := 1; i < len(a); i++ {
			if a[i] == "--label" && i+1 < len(a) {
				i++
				pair := strings.SplitN(a[i], "=", 2)
				if len(pair) != 2 {
					base.Fatalf("health network: label needs key=value")
				}
				labels[pair[0]] = pair[1]
			} else if req.Network == "" {
				req.Network = a[i]
			} else {
				base.Fatalf("health network: unexpected argument")
			}
		}
		if len(labels) > 0 {
			response, err = client.NetworkChangedWithLabels(ctx, &healthService.NetworkChangedWithLabelsRequest{Network: req.Network, Labels: labels})
		} else {
			response, err = client.NetworkChanged(ctx, req)
		}
	case "lost":
		response, err = client.NetworkLost(ctx, &healthService.NetworkLostRequest{})
	case "present":
		response, err = client.UserPresent(ctx, &healthService.UserPresentRequest{})
	case "memory":
		if len(a) != 2 || (a[1] != "--low=true" && a[1] != "--low=false") {
			base.Fatalf("health memory: expected --low=true or --low=false")
		}
		response, err = client.SetMemoryLow(ctx, &healthService.SetMemoryLowRequest{IsLow: a[1] == "--low=true"})
	case "pin":
		if len(a) < 3 {
			base.Fatalf("health pin: need balancer and outbound")
		}
		req := &healthService.PinRequest{Balancer: a[1], Outbound: a[2]}
		if len(a) > 3 {
			req.Run = a[3]
		}
		response, err = client.Pin(ctx, req)
	case "release":
		if len(a) < 2 {
			base.Fatalf("health release: need balancer")
		}
		req := &healthService.ReleaseRequest{Balancer: a[1]}
		if len(a) > 2 {
			req.Run = a[2]
		}
		response, err = client.Release(ctx, req)
	case "refresh":
		req := &healthService.RequestProviderRefreshRequest{}
		if len(a) > 1 {
			req.Provider = a[1]
		}
		response, err = client.RequestProviderRefresh(ctx, req)
	case "report":
		if len(a) == 1 {
			response, err = client.PeekReport(ctx, &healthService.PeekReportRequest{})
		} else if a[1] == "take" || a[1] == "final" {
			response, err = client.TakeReport(ctx, &healthService.TakeReportRequest{IsFinal: a[1] == "final"})
		} else {
			base.Fatalf("health report: expected take or final")
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
		response, err = client.RequestProbe(ctx, &healthService.RequestProbeRequest{Balancer: a[1], Outbounds: a[2:], IsUdp: isUDP, WaitMs: int64(apiTimeout) * 1000})
	case "probe-result":
		if len(a) != 3 {
			base.Fatalf("health probe-result: need run and id")
		}
		id, parseErr := strconv.ParseUint(a[2], 10, 64)
		if parseErr != nil {
			base.Fatalf("health probe-result: id must be a number")
		}
		response, err = client.ProbeResult(ctx, &healthService.ProbeResultRequest{Run: a[1], Id: id})
	case "set":
		if len(a) < 2 {
			base.Fatalf("health set: need provider")
		}
		req := &healthService.ExportProviderSetRequest{Provider: a[1]}
		if len(a) > 2 {
			after, parseErr := strconv.ParseUint(a[2], 10, 64)
			if parseErr != nil {
				base.Fatalf("health set: after must be a number")
			}
			req.After = after
		}
		response, err = client.ExportProviderSet(ctx, req)
	case "headers":
		response, err = client.SetProviderHeaders(ctx, &healthService.SetProviderHeadersRequest{Provider: a[1], Headers: headers})
	default:
		base.Fatalf("health: unknown command %s", a[0])
	}
	if err != nil {
		base.Fatalf("health %s: %s", a[0], err)
	}
	fmt.Println(string(decodedHealthResult(response)))
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

// Render the shared Result without encoding its data object a second time.
func decodedHealthResult(r *healthService.Result) []byte {
	out := struct {
		Success bool                        `json:"success"`
		Run     string                      `json:"run,omitempty"`
		Data    json.RawMessage             `json:"data,omitempty"`
		Error   *healthService.CommandError `json:"error,omitempty"`
	}{r.Success, r.Run, json.RawMessage(r.Data), r.Error}
	raw, _ := json.Marshal(out)
	return raw
}
