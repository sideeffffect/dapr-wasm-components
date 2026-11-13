# Dapr on TinyGo: feasibility test

> Source: own build experiments with TinyGo 0.44.0 + TinyGo source/issues
> Collected: 2026-11-13
> Published: 2026-11-13

Experiments in a scratch directory; no repo files changed.

## Verified

- TinyGo 0.44.0 (2026-10-28) has `wasip1` and `wasip2` targets, no `wasip3` (tinygo#5478). `targets/wasip2.json`: `goos: linux`, `goarch: arm`, `scheduler: asyncify`. Released TinyGo runs goroutines on asyncify, not CM threads. wasip3 / `wasiOnIdle` work is pending (PR #5848 WIP; tinygo#5796 "soon").
- protobuf-go builds; `proto.Marshal` panics `unimplemented: (reflect.Type).MethodByName()`. Removing ~6 lines in `internal/impl/message.go` (legacy `XXX_OneofWrappers` lookup) fixes it; binary, protojson, anypb then work.
- grpc-go first error: `internal/tcp_keepalive_unix.go:48:3: unknown field Control in struct literal of type net.Dialer`. With 5 build-tag patches (keepalive, channelz, syscall, readyreader) it builds (13 MB) and runs, then fails `Lookup of host name 'localhost' failed: Netdev not set` — TinyGo `net` has no wasi:sockets backend. x/net http2 does not compile against TinyGo net/http with a Go 1.27 GOROOT (`t1.Protocols undefined`); Go 1.26.9 needed.
- Minimal Dapr subset (`pkg/runtime` + `api/grpc` + redis + in-memory state): 1145 non-std packages. First error: `azcore/runtime/transport_default_http_client.go:27:3: unknown field ForceAttemptHTTP2 in struct literal of type http.Transport`; also `undefined: tls.RenegotiateFreelyAsClient` (redis state → common/component/redis → Azure azcore).

## Blockers

1. `crypto/tls` is a stub (`Handshake()` no-op, 890 lines); `net/http.Transport` is a 125-line netdev shim (Go: 3283).
2. No wasi:sockets in TinyGo `net` (netdev only; nop on wasip2).
3. reflect `Method`/`MethodByName`/`Call`/`MakeFunc` unimplemented (tinygo#3862 open, PR #5322 closed). In the subset: 10 packages use Method/MethodByName (k8s apimachinery, go-redis, gogo + golang/protobuf, go-restful, go-cmp), 9 use Call.
4. json-iterator / modern-go/reflect2 linkname `reflect.unsafe_New`, `mapassign`, `typelinks` — absent in TinyGo (redis state, cloudevents, k8s structured-merge-diff).
5. Scale: tinygo#5840 — ~1200 packages, wasm-opt killed at 27 GB. `goos=linux` drags in Linux-only code (x/sys/unix, procfs, fsnotify, logrus).

## Estimate (inference)

10–20 person-weeks: TinyGo wasi:sockets netdev 1–2 w, real TLS 3–6 w, reflect Method/Call (compiler work); Dapr fork stubbing k8s/Azure/OTel/Prometheus/json-iterator/MCP across ~200 packages 4–8 w. Upstreaming unlikely (k8s types in Dapr core); every Dapr release is a rebase.

Cheaper: stay on big Go and wait for GOOS=wasip3 (golang/go#77141); a slim TinyGo "Dapr-lite" (building blocks over small clients, 4–8 person-weeks, loses component breadth, still needs a netdev); wait for TinyGo wasip3.

Not tried: the full go-outbound module (TinyGo stops at the first failing package).
