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
	UsageLine:   "{{.Exec}} api health [--server=127.0.0.1:8080] <status|events|network [id]|lost|present|pin <balancer> <outbound> [run]|release <balancer> [run]|refresh [provider]|report [take|final]|probe [--udp] <balancer> [outbound...]|headers <provider>|urls <provider>|set <provider> [after]|trace [after] [--max-bytes=N]|header|probe-result <run> <id>|memory --low=true>",
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
	urls <provider>             replace the addresses of the provider with
	                            a JSON array of 1 to 4 URLs on stdin, in
	                            the order to try them
	set <provider> [after]      the last set the provider applied, valid
	                            as its initialAnswer, with the token: {"seq": n,
	                            "set": {...}}; no data when nothing changed
	                            since the seq after or no set was applied
	trace [after] [--max-bytes=N]
	                            trace records after the seq after, at most
	                            N bytes of them; is_more true asks to call
	                            again with the seq returned
	header                      the Xray-Adaptive header of this core

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
	var urls []string
	if a[0] == "headers" || a[0] == "urls" {
		if len(a) < 2 {
			base.Fatalf("health %s: need provider", a[0])
		}
		// Before the dial, whose timeout covers the call: a person may be
		// typing the input.
		var err error
		if a[0] == "headers" {
			err = stdinJSON(os.Stdin, &headers, `a JSON object of strings, such as {"Authorization": "Bearer ..."}`)
		} else {
			err = stdinJSON(os.Stdin, &urls, `a JSON array of strings, such as ["https://example.com/set"]`)
		}
		if err != nil {
			base.Fatalf("health %s: %s", a[0], err)
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

		var labels map[string]string
		req.Network, labels, err = healthNetworkArguments(a[1:])
		if err != nil {
			base.Fatalf("health network: %s", err)
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
	case "urls":
		response, err = client.SetProviderURLs(ctx, &healthService.SetProviderURLsRequest{Provider: a[1], Urls: urls})
	case "trace":
		req, parseErr := healthTraceArguments(a[1:])
		if parseErr != nil {
			base.Fatalf("health trace: expected [after] [--max-bytes=N]")
		}
		response, err = client.ExportTrace(ctx, req)
	case "header":
		response, err = client.AdaptiveHeader(ctx, &healthService.AdaptiveHeaderRequest{})
	default:
		base.Fatalf("health: unknown command %s", a[0])
	}
	if err != nil {
		base.Fatalf("health %s: %s", a[0], err)
	}
	fmt.Println(string(decodedHealthResult(response)))
}

// maxStdinInput bounds what stdinJSON reads: eight header values of 4 KiB
// and their names fit many times over.
const maxStdinInput = 256 << 10

// stdinJSON reads the headers or the addresses of a provider. A token never
// comes from the arguments, which the process list shows, and an error never
// quotes the input, which holds it.
func stdinJSON[T map[string]string | []string](in io.Reader, out *T, want string) error {
	raw, err := io.ReadAll(io.LimitReader(in, maxStdinInput+1))
	if err != nil {
		return err
	}
	if len(raw) > maxStdinInput {
		return fmt.Errorf("stdin holds more than %d bytes", maxStdinInput)
	}
	if !utf8.Valid(raw) {
		// The decoder would put U+FFFD in place of such bytes, and the
		// provider would send another token than the one given.
		return errors.New("stdin is not valid UTF-8")
	}
	if json.Unmarshal(raw, out) != nil || *out == nil {
		return errors.New("stdin must hold " + want)
	}
	return nil
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

func healthTraceArguments(args []string) (*healthService.ExportTraceRequest, error) {
	req := &healthService.ExportTraceRequest{}
	after := false
	for _, arg := range args {
		var err error
		if limit, ok := strings.CutPrefix(arg, "--max-bytes="); ok {
			var n uint64
			n, err = strconv.ParseUint(limit, 10, 32)
			req.MaxBytes = uint32(n)
		} else if !after {
			req.After, err = strconv.ParseUint(arg, 10, 64)
			after = true
		} else {
			err = strconv.ErrSyntax
		}
		if err != nil {
			return nil, err
		}
	}
	return req, nil
}

// Arguments follow the subcommand, after the shared flag parser has stopped.
// An explicitly empty opaque id remains an id, rather than a label argument.
func healthNetworkArguments(args []string) (string, map[string]string, error) {
	id := ""
	labels := make(map[string]string)
	start := 0
	if len(args) > 0 && args[0] != "--label" && !strings.HasPrefix(args[0], "--label=") {
		id, start = args[0], 1
	}
	for i := start; i < len(args); i++ {
		value := strings.TrimPrefix(args[i], "--label=")
		if args[i] == "--label" && i+1 < len(args) {
			i++
			value = args[i]
		} else if value == args[i] {
			return "", nil, errors.New("expected --label key=value")
		}
		key, val, ok := strings.Cut(value, "=")
		if !ok || key == "" {
			return "", nil, errors.New("expected --label key=value")
		}
		labels[key] = val
	}
	return id, labels, nil
}
