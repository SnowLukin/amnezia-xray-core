# Runtime worker inventory

## Panic policy

- `connection`: the panic closes that connection or task and the process lives. If one site (`where`) panics 20 times within a minute, the process ends as below: a deterministic bug there would otherwise fail every connection while the tunnel looks connected.
- `subsystem` outside `app/observatory/health/`: the process ends as on an unrecovered panic, with exit code 2 and the stack trace on stderr, so the mobile system restarts the tunnel. Cleanup is skipped because it may wait on a lock the panicking worker still holds. A connection boundary around such code does not swallow the panic.
- `subsystem` under `app/observatory/health/`: the observer freezes its current choice, the process lives, and `Status.broken_subsystems` lists `app/observatory/health` until the next core start. It is the only entry that list can contain.
- Every recovered panic writes `panic_recovered scope=... where=...` with source frames, without the panic value, to the core error log (stderr before the first core start). The record of a panic that ends the process may be lost with the asynchronous log writer; the runtime trace on stderr stays.

Periodic tasks default to the owner function subsystem, so a panic in a cleanup task ends the process; connection ActivityTimer uses connection scope.

## Inventory

The table is a snapshot of baseline `8d8a2251`, checked through `2478ced5`, and line numbers refer to that baseline. It is not complete for the current head: later packages have go statements without a boundary, for example `proxy/masque`, `transport/internet/masque`, `transport/internet/xdrive`, `transport/internet/finalmask/xdns`, `transport/internet/finalmask/realm/client.go`, `proxy/tun/tun_windows.go`, and some listed files no longer exist. A panic in an unguarded goroutine ends the process, which matches the subsystem policy. Fixtures under testing/, the formatting CLI and the debug distro are process/tool entries. Dependency-owned goroutines are guarded at core callbacks/task/proxy boundaries; fatal runtime throws are outside recover.

Go launches at the baseline: {'subsystem': 54, 'connection': 81, 'process': 9}. Explicit runtime startup assertions remain unchanged.

| Function | Baseline line | Launch | Class | Existing recover | Boundary |
| --- | ---: | --- | --- | --- | --- |
| `app/commander/commander.go:c.Start` | 97 | go | subsystem | no | guarded worker; cleanup safe.Closer(l) |
| `app/commander/commander.go:c.Start` | 106 | go | subsystem | no | guarded worker; cleanup safe.Closer(listener) |
| `app/dispatcher/default.go:trackOnlineIP` | 228 | callback | connection | no | guarded worker; owner cleanup/error propagation |
| `app/dispatcher/default.go:d.Dispatch` | 288 | go | connection | no | guarded worker; cleanup safe.Closer(inbound.Reader,inbound.Writer,outbound.Reader,outbound.Writer) |
| `app/dispatcher/default.go:d.Dispatch` | 290 | go | connection | no | guarded worker; cleanup safe.Closer(inbound.Reader,inbound.Writer,outbound.Reader,outbound.Writer) |
| `app/dns/cache_controller.go:c.writeAndShrink` | 168 | go | subsystem | yes (file) | guarded worker; owner cleanup/error propagation |
| `app/dns/dns.go:asyncQueryAll` | 462 | go | subsystem | no | guarded worker; cleanup func(){ch<-queryResult{err:safe.ErrPanic,index:i}} |
| `app/dns/nameserver_cached.go:queryIP` | 38 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/dns/nameserver_doh.go:s.sendQuery` | 175 | go | subsystem | no | guarded worker; cleanup func(){if noResponseErrCh!=nil{noResponseErrCh<-safe.ErrPanic}} |
| `app/dns/nameserver_quic.go:s.sendQuery` | 103 | go | subsystem | no | guarded worker; cleanup func(){if noResponseErrCh!=nil{noResponseErrCh<-safe.ErrPanic}} |
| `app/dns/nameserver_tcp.go:s.sendQuery` | 138 | go | subsystem | no | guarded worker; cleanup func(){if noResponseErrCh!=nil{noResponseErrCh<-safe.ErrPanic}} |
| `app/geodata/download.go:c.Read` | 42 | callback | connection | no | guarded worker; cleanup safe.Closer(c) |
| `app/metrics/metrics.go:p.Start` | 64 | go | subsystem | no | guarded worker; cleanup safe.Closer(TCPlistener) |
| `app/metrics/metrics.go:p.Start` | 80 | go | subsystem | no | guarded worker; cleanup safe.Closer(listener) |
| `app/observatory/burst/healthping.go:h.StartScheduler` | 100 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/burst/healthping.go:h.StartScheduler` | 109 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/burst/healthping.go:h.StartScheduler` | 111 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/burst/healthping.go:h.doCheck` | 186 | callback | subsystem | no | guarded worker; cleanup func(){ch<-&rtt{handler:handler,value:rttFailed}} |
| `app/observatory/health/health.go:o.Start` | 203 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/health.go:o.loop` | 324 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/health.go:o.loop` | 327 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/health.go:o.loop` | 330 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/health.go:o.loop` | 333 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/health.go:o.loop` | 336 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/health.go:o.watch` | 541 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/probe.go:s.begin` | 57 | callback | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/probe.go:s.begin` | 59 | callback | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/probe.go:s.startRequestTimersLocked` | 115 | callback | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/probe.go:s.startRequestTimersLocked` | 116 | callback | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/probe.go:o.probeStepWithContext` | 256 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/provider_set.go:o.fetchOnce` | 798 | callback | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/provider_set.go:o.startDrain` | 1464 | callback | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/udp_probe.go:o.probeUDPDestination` | 131 | callback | connection | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/udp_probe.go:runSTUN` | 180 | go | connection | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/udp_probe.go:runSTUN` | 188 | go | connection | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/udp_probe.go:runSTUN` | 266 | callback | connection | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/health/udp_state.go:o.watchUDPEvidence` | 375 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/observatory/observer.go:o.Start` | 52 | go | subsystem | no | guarded worker; cleanup safe.Closer(o.finished) |
| `app/observatory/observer.go:o.background` | 97 | go | subsystem | no | guarded worker; cleanup func(){o.finished.Close();ch<-struct{}{}} |
| `app/proxyman/inbound/worker.go:w.Start` | 142 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `app/proxyman/inbound/worker.go:w.callback` | 332 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `app/proxyman/inbound/worker.go:w.Start` | 429 | go | subsystem | no | guarded worker; cleanup safe.Closer(w) |
| `app/proxyman/inbound/worker.go:w.Start` | 528 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `app/proxyman/outbound/handler.go:h.Dial` | 319 | go | connection | no | guarded worker; cleanup safe.Closer(uplinkReader,downlinkWriter) |
| `app/reverse/bridge.go:w.Dispatch` | 212 | go | connection | no | guarded worker; cleanup safe.Closer(downlinkReader,uplinkWriter) |
| `app/router/webhook.go:NewWebhookNotifier` | 82 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/router/webhook.go:h.Fire` | 106 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/stats/channel.go:c.Start` | 104 | go | subsystem | no | guarded worker; cleanup safe.Closer(c) |
| `app/stats/channel.go:c.publishNonBlocking` | 156 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `app/stats/channel.go:c.broadcastNonBlocking` | 171 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `common/buf/io.go:r.ReadMultiBufferTimeout` | 58 | go | connection | no | guarded worker; owner cleanup/error propagation |
| `common/buf/io.go:r.ReadMultiBufferTimeout` | 64 | go | connection | no | guarded worker; owner cleanup/error propagation |
| `common/log/logger.go:l.Handle` | 108 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `common/mux/client.go:m.DispatchIsolated` | 58 | callback | connection | no | guarded worker; owner cleanup/error propagation |
| `common/mux/client.go:f.createWithContext` | 192 | go | connection | no | guarded worker; cleanup safe.Closer(c,uplinkReader,downlinkWriter) |
| `common/mux/client.go:NewClientWorker` | 240 | go | connection | no | guarded worker; cleanup safe.Closer(c) |
| `common/mux/client.go:NewClientWorker` | 241 | go | connection | no | guarded worker; cleanup safe.Closer(c) |
| `common/mux/client.go:m.Dispatch` | 364 | go | connection | no | guarded worker; cleanup safe.Closer(s) |
| `common/mux/server.go:NewServerWorker` | 106 | go | connection | no | guarded worker; cleanup safe.Closer(worker) |
| `common/mux/server.go:NewServerWorker` | 107 | go | connection | no | guarded worker; cleanup safe.Closer(worker) |
| `common/mux/server.go:w.handleStatusNew` | 260 | go | connection | no | guarded worker; cleanup safe.Closer(x.Mux) |
| `common/mux/server.go:w.handleStatusNew` | 285 | go | connection | no | guarded worker; cleanup safe.Closer(s) |
| `common/mux/session.go:init` | 237 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `common/singbridge/dialer.go:d.DialContext` | 66 | go | connection | no | guarded worker; cleanup safe.Closer(downlinkReader,uplinkWriter) |
| `common/task/parallel.go:ParallelForN` | 33 | callback | connection | no | guarded worker; owner cleanup/error propagation |
| `common/task/periodic.go:t.checkedExecute` | 46 | callback | subsystem | no | guarded worker; owner cleanup/error propagation |
| `common/task/task.go:Run` | 27 | go | connection | no | guarded worker; owner cleanup/error propagation |
| `infra/vformat/main.go:RunMany` | 109 | go | process | no | startup/tool/fixture; outside production boundary |
| `main/distro/debug/debug.go:init` | 8 | go | process | no | startup/tool/fixture; outside production boundary |
| `proxy/blackhole/blackhole.go:h.Process` | 53 | go | connection | no | guarded worker; cleanup safe.Closer(link.Reader,link.Writer) |
| `proxy/dns/dns.go:h.Process` | 274 | go | connection | no | guarded worker; cleanup safe.Closer(link.Reader,link.Writer) |
| `proxy/freedom/freedom.go:h.Process` | 356 | callback | connection | no | guarded worker; owner cleanup/error propagation |
| `proxy/http/client.go:setUpHTTPTunnel` | 256 | go | connection | no | guarded worker; cleanup func(){pErr=safe.ErrPanic;safe.Close(pr,pw,rawConn);wg.Done()} |
| `proxy/socks/server.go:s.processTCP` | 174 | go | connection | no | guarded worker; cleanup func(){safe.Close(tempUDPConn,conn);errCh<-safe.ErrPanic} |
| `proxy/tun/stack_gvisor.go:t.Start` | 70 | go | connection | no | guarded worker; cleanup func(){r.Complete(true)} |
| `proxy/tun/stack_gvisor_endpoint.go:e.Attach` | 61 | go | subsystem | no | guarded worker; cleanup safe.Closer(e) |
| `proxy/tun/tun_darwin.go:t.Start` | 126 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `proxy/tun/tun_linux.go:t.Start` | 189 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `proxy/tun/udp_fullcone.go:u.HandlePacket` | 67 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `proxy/vless/encryption/server.go:i.Init` | 82 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `proxy/vless/inbound/inbound.go:New` | 206 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `proxy/vless/inbound/inbound.go:r.NewMux` | 710 | go | connection | no | guarded worker; owner cleanup/error propagation |
| `proxy/vless/inbound/inbound.go:h.acceptUnixSocketClients` | 760 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `proxy/vless/outbound/outbound.go:New` | 125 | go | subsystem | yes (file) | guarded worker; cleanup safe.Closer(handler.reverse) |
| `proxy/vless/outbound/outbound.go:h.Process` | 163 | go | subsystem | yes (file) | guarded worker; owner cleanup/error propagation |
| `proxy/vless/outbound/outbound.go:r.monitor` | 471 | go | connection | yes (file) | guarded worker; cleanup safe.Closer(reader1,reader2,writer1,writer2) |
| `proxy/wireguard/bind.go:b.Open` | 56 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `proxy/wireguard/client_observe.go:t.entered` | 293 | callback | subsystem | no | guarded worker; owner cleanup/error propagation |
| `proxy/wireguard/netstack.go:tnet.exchange` | 516 | callback | connection | no | guarded worker; owner cleanup/error propagation |
| `proxy/wireguard/netstack.go:tnet.LookupContextHost` | 683 | go | connection | no | guarded worker; cleanup func(){lane<-result{error:safe.ErrPanic}} |
| `proxy/wireguard/netstack.go:tnet.LookupContextHost` | 689 | go | connection | no | guarded worker; cleanup func(){lane<-result{error:safe.ErrPanic}} |
| `proxy/wireguard/tun.go:createForwarder` | 57 | go | connection | no | guarded worker; cleanup func(){r.Complete(true)} |
| `proxy/wireguard/tun.go:m.feed` | 145 | go | connection | no | guarded worker; cleanup safe.Closer(uc) |
| `testing/adaptive/pion_matrix/hub.go:newHostHub` | 33 | go | process | no | startup/tool/fixture; outside production boundary |
| `testing/adaptive/pion_matrix/hub.go:newHostHub` | 34 | go | process | no | startup/tool/fixture; outside production boundary |
| `testing/adaptive/pion_matrix/main.go:run` | 318 | go | process | no | startup/tool/fixture; outside production boundary |
| `testing/servers/http/http.go:s.Start` | 34 | go | process | no | startup/tool/fixture; outside production boundary |
| `testing/servers/tcp/tcp.go:server.StartContext` | 44 | go | process | no | startup/tool/fixture; outside production boundary |
| `testing/servers/tcp/tcp.go:server.acceptConnections` | 57 | go | process | no | startup/tool/fixture; outside production boundary |
| `testing/servers/udp/udp.go:server.Start` | 28 | go | process | no | startup/tool/fixture; outside production boundary |
| `transport/internet/browser_dialer/dialer.go:Reload` | 92 | go | subsystem | no | guarded worker; cleanup safe.Closer(server) |
| `transport/internet/dialer.go:redirect` | 123 | go | connection | no | guarded worker; cleanup safe.Closer(ur,dw) |
| `transport/internet/finalmask/header/custom/udp.go:c.ensureReader` | 398 | go | connection | no | guarded worker; cleanup safe.Closer(c) |
| `transport/internet/finalmask/realm/server.go:NewConnServer` | 77 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/realm/server.go:c.run` | 249 | go | connection | no | guarded worker; cleanup safe.Closer(c) |
| `transport/internet/finalmask/realm/server.go:c.run` | 250 | go | connection | no | guarded worker; cleanup safe.Closer(c) |
| `transport/internet/finalmask/realm/server.go:c.eventsLoop` | 351 | go | connection | no | guarded worker; cleanup safe.Closer(c) |
| `transport/internet/finalmask/salamander/conn.go:NewGeckoConnClient` | 112 | go | connection | no | guarded worker; cleanup safe.Closer(g) |
| `transport/internet/finalmask/xdns/client.go:NewConnClient` | 113 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xdns/client.go:NewConnClient` | 114 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xdns/server.go:NewConnServer` | 91 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xdns/server.go:NewConnServer` | 92 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xdns/server.go:NewConnServer` | 93 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xicmp/client.go:NewConnClient` | 87 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xicmp/client.go:NewConnClient` | 88 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xicmp/server.go:NewConnServer` | 75 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xicmp/server.go:NewConnServer` | 76 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xicmp/server.go:NewConnServer` | 77 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xicmp/server_oob.go:NewConnServer` | 84 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xicmp/server_oob.go:NewConnServer` | 85 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xicmp/server_oob.go:NewConnServer` | 86 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/finalmask/xmc/packet_stream.go:newPacketStream` | 47 | go | connection | no | guarded worker; cleanup safe.Closer(s) |
| `transport/internet/grpc/hub.go:Listen` | 104 | go | subsystem | no | guarded worker; cleanup safe.Closer(listener) |
| `transport/internet/happy_eyeballs.go:TcpRaceDial` | 87 | go | connection | no | guarded worker; owner cleanup/error propagation |
| `transport/internet/httpupgrade/hub.go:s.keepAccepting` | 113 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/httpupgrade/hub.go:ListenHTTPUpgrade` | 167 | go | subsystem | no | guarded worker; cleanup safe.Closer(serverInstance) |
| `transport/internet/hysteria/dialer.go:c.dial` | 247 | go | connection | no | guarded worker; cleanup safe.Closer(c.udpSM) |
| `transport/internet/hysteria/dialer.go:Dial` | 334 | go | subsystem | no | guarded worker; cleanup safe.Closer(manager) |
| `transport/internet/hysteria/hub.go:h.AuthHTTP` | 98 | go | connection | no | guarded worker; cleanup safe.Closer(udpSM) |
| `transport/internet/hysteria/hub.go:h.AuthHTTP` | 99 | go | connection | no | guarded worker; cleanup safe.Closer(udpSM) |
| `transport/internet/hysteria/hub.go:l.keepAccepting` | 181 | go | connection | no | guarded worker; cleanup func(){conn.CloseWithError(0,"worker panic recovered")} |
| `transport/internet/hysteria/hub.go:Listen` | 331 | go | subsystem | no | guarded worker; cleanup safe.Closer(l) |
| `transport/internet/hysteria/udphop/conn.go:NewUDPHopPacketConn` | 87 | go | connection | no | guarded worker; cleanup safe.Closer(hConn) |
| `transport/internet/hysteria/udphop/conn.go:NewUDPHopPacketConn` | 88 | go | connection | no | guarded worker; cleanup safe.Closer(hConn) |
| `transport/internet/hysteria/udphop/conn.go:u.hop` | 160 | go | connection | no | guarded worker; cleanup safe.Closer(u) |
| `transport/internet/kcp/connection.go:u.WakeUp` | 145 | go | connection | no | guarded worker; owner cleanup/error propagation |
| `transport/internet/kcp/connection.go:c.SetState` | 455 | go | connection | no | guarded worker; cleanup safe.Closer(c) |
| `transport/internet/kcp/dialer.go:fetchInput` | 24 | go | connection | no | guarded worker; cleanup func(){safe.Close(input);close(cache)} |
| `transport/internet/kcp/dialer.go:DialKCP` | 95 | go | connection | no | guarded worker; cleanup safe.Closer(conn,reader) |
| `transport/internet/kcp/listener.go:NewListener` | 58 | go | subsystem | no | guarded worker; cleanup safe.Closer(l) |
| `transport/internet/kcp/listener.go:l.Close` | 138 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/reality/reality.go:UClient` | 185 | go | connection | no | guarded worker; cleanup safe.Closer(uConn) |
| `transport/internet/reality/reality.go:UClient` | 269 | go | connection | no | guarded worker; cleanup safe.Closer(uConn) |
| `transport/internet/splithttp/client.go:c.OpenStream` | 71 | go | connection | yes (file) | guarded worker; cleanup safe.Closer(body,gotConn,wrc) |
| `transport/internet/splithttp/dialer.go:createHTTPClient` | 262 | callback | connection | no | guarded worker; owner cleanup/error propagation |
| `transport/internet/splithttp/dialer.go:Dial` | 502 | go | connection | no | guarded worker; cleanup safe.Closer(pipeReader,pipeWriter) |
| `transport/internet/splithttp/dialer.go:Dial` | 547 | go | connection | no | guarded worker; cleanup safe.Closer(pipeReader,pipeWriter,wroteRequest) |
| `transport/internet/splithttp/hub.go:h.upsertSession` | 79 | go | connection | no | guarded worker; cleanup safe.Closer(s.uploadQueue,shouldReap) |
| `transport/internet/splithttp/hub.go:h.upsertSession` | 83 | go | connection | no | guarded worker; cleanup safe.Closer(s.uploadQueue,shouldReap) |
| `transport/internet/splithttp/hub.go:h.ServeHTTP` | 221 | go | connection | no | guarded worker; cleanup safe.Closer(httpSC) |
| `transport/internet/splithttp/hub.go:ListenXH` | 524 | go | subsystem | no | guarded worker; cleanup safe.Closer(l) |
| `transport/internet/splithttp/hub.go:ListenXH` | 567 | go | subsystem | no | guarded worker; cleanup safe.Closer(l) |
| `transport/internet/tcp/hub.go:ListenTCP` | 82 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `transport/internet/tcp/hub.go:ListenTCP` | 97 | go | subsystem | no | guarded worker; cleanup safe.Closer(l) |
| `transport/internet/tcp/hub.go:v.keepAccepting` | 116 | go | connection | no | guarded worker; cleanup func(){safe.Close(conn)} |
| `transport/internet/tls/config.go:setupOcspTicker` | 97 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `transport/internet/tls/ech.go:QueryRecord` | 163 | go | subsystem | no | guarded worker; owner cleanup/error propagation |
| `transport/internet/tls/grpc.go:c.ClientHandshake` | 67 | go | connection | no | guarded worker; cleanup func(){safe.Close(conn);errChannel<-safe.ErrPanic;close(errChannel)} |
| `transport/internet/tls/tls.go:c.Close` | 37 | callback | connection | no | guarded worker; owner cleanup/error propagation |
| `transport/internet/tls/tls.go:c.Close` | 82 | callback | connection | no | guarded worker; owner cleanup/error propagation |
| `transport/internet/udp/dispatcher.go:v.getInboundRay` | 104 | go | connection | no | guarded worker; cleanup safe.Closer(entry.link.Reader,entry.link.Writer) |
| `transport/internet/udp/hub.go:ListenUDP` | 83 | go | subsystem | no | guarded worker; cleanup safe.Closer(hub) |
| `transport/internet/websocket/connection.go:NewConnection` | 29 | go | connection | no | guarded worker; cleanup safe.Closer(conn) |
| `transport/internet/websocket/hub.go:ListenWS` | 147 | go | subsystem | no | guarded worker; cleanup safe.Closer(l) |
