package api

import (
	"fmt"

	healthService "github.com/xtls/xray-core/app/observatory/health/command"
	"github.com/xtls/xray-core/main/commands/base"
)

var cmdHealth = &base.Command{
	CustomFlags: true,
	UsageLine:   "{{.Exec}} api health [--server=127.0.0.1:8080] <status|events|network [id]|lost|pin <balancer> <outbound>|release <balancer>>",
	Short:       "Query and control the health observatory",
	Long: `
Query and control the health observatory. Output is JSON.

> Ensure that "HealthService" is enabled under "config.api.services".

Commands:

	status                      current state of outbounds and balancers
	events                      events since the previous call
	network [id]                the device moved to another network
	lost                        the device has no network
	pin <balancer> <outbound>   keep the balancer on the outbound
	release <balancer>          let the balancer choose again

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
		if a[0] == "pin" {
			if len(a) < 3 {
				base.Fatalf("health pin: need balancer and outbound")
			}
			req.Outbound = a[2]
		}
		r, err := client.Pin(ctx, req)
		if err != nil {
			base.Fatalf("health %s: %s", a[0], err)
		}
		fmt.Printf("{\"ok\":%t}\n", r.Ok)
	default:
		base.Fatalf("health: unknown command %s", a[0])
	}
}
