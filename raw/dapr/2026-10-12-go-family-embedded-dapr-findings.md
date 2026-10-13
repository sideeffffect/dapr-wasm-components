# Embedding Dapr (github.com/dapr/dapr) in a wasm component — probe and implementation findings

> Source: hands-on probes and the go-family implementation session in this repository (PR https://github.com/sideeffffect/dapr-wasm-components/pull/12); commands run against Go 1.27.2, componentize-go v0.5.0, wasmtime 45.0.1, dapr v1.18.7, components-contrib v1.18.6
> Collected: 2026-10-12
> Published: 2026-10-12

## Toolchain

- `GOOS=wasip1 GOARCH=wasm go build ./cmd/daprd` of dapr/dapr (main, Oct 2026, `go 1.26.9` in go.mod) succeeds with Go 1.27.2: a 138 MB core module. Without `-tags stablecomponents|allcomponents` it panics at init (`cmd/daprd/components/zz_notag.go`).
- `componentize-go` v0.5.0 (Bytecode Alliance) builds components from **big Go** (needs Go ≥ 1.27.1; `go install github.com/bytecodealliance/componentize-go@v0.5.0` downloads the Rust binary). `componentize-go -d wit --world W bindings --pkg-name <go pkg>` generates library-mode bindings; `--generate-stubs` writes export stubs. Exports are implemented as package-level functions in `<pkg>/export_<iface>`; exported resources are structs with `pinner runtime.Pinner; handle int32` plus an `OnDrop()` method.
- `--export-pkg-name` splits resource glue and implementation into different packages that do not compile together (v0.5.0) — keep exports under `--pkg-name`.
- wit-bindgen-go 0.61.1 name clash: a variant case `constant(job-failure-policy-constant)` yields a tag constant `JobFailurePolicyConstant` that collides with the record type of the same name (does not compile).
- `<Res>FromOwnHandle(handle)` for an own handle of an exported resource coming back in (e.g. `static finish(this: encryptor)`) returns the object without recording the new handle, so `Drop()` cannot release it.
- A probe component exporting a WIT `store` interface backed by components-contrib `state/in-memory`: 6.3 MB; `wasmtime run --invoke 'get("k")'` → `ok(none)`. `--invoke` cannot address functions of exported *interfaces* (parse error on `:`).

## Networking

- Go's wasip1 `net` is an in-process fake (`net/net_fake.go`, `//go:build js || wasip1`): `net.Dial` to a real listening host port returns "connection refused".
- A `go build -overlay` can patch std files (GOROOT) but **not** files under GOMODCACHE ("Files beneath GOMODCACHE must not be replaced").
- Overlaying `net/net_fake.go` (`socket()`), `net/fd_fake.go` (netFD methods), `net/lookup_unix.go` (lookupIP/lookupHost) plus an added file with hooks that a user package registers via `//go:linkname` (push in net, pull in the user package) gives real TCP dial, listen/accept and DNS over `wasi:sockets@0.2` — verified: raw TCP echo, `net/http` GET, DNS of `localhost`, in-process listen+dial. TLS needs bundled roots (`golang.org/x/crypto/x509roots/fallback`): there is no system trust store.
- wasi:sockets IPv6 sockets are IPv6-only: binding `[::]` for Go's dual-stack wildcard `":port"` refuses IPv4 clients. Bind wildcard listens to `0.0.0.0`.
- Big Go on wasm runs all goroutines on one thread: socket waits must be non-blocking + `time.Sleep` backoff, or one blocked read freezes everything.

## Runtime

- `pkg/runtime.FromConfig` + `Run`, configured exactly like `cmd/daprd/app.runWithContext` and parsed with daprd's own `options.New(args)`, boots inside the component; with the app channel enabled it logs `dapr initialized. Status: Running. Init Elapsed 37ms`.
- `DaprRuntime` has no public accessor for its API object; its unexported fields `initComplete chan struct{}` and `daprGRPCAPI grpc.API` are read by reflection (checked at start).
- The `HotReload` feature is on by default (`config.defaultFeatures`) and starts an fsnotify watcher, which fails on wasip1 ("fsnotify not supported on the current platform") and aborts the runtime. Turned off by writing `config.defaultFeatures` through `go:linkname`.
- Of daprd's 54 `stablecomponents` registration files, 47 build for wasip1; not: `state_sqlite` (modernc.org/libc has no wasip1), `pubsub_pulsar`, `bindings_dubbo`, `bindings/pubsub_azure_eventhubs`, `bindings/state_azure_blobstorage`. `crypto_localstorage`, `crypto_jwks`, `nameresolution_hashicorp_consul`, `lock_redis`, `state_hashicorp_consul` and all `conversation_*` (allcomponents-only) build.
- mDNS name resolution fails (`zeroconf ... No supported interface`): no UDP multicast. `nameresolution.nameformat` returns one formatted address for any app id (no per-app port), so it cannot separate two apps on one host. Consul works; the runtime self-registers at `DAPR_HOST_IP` (`utils.GetHostAddress` reads it first), and Consul only returns instances whose health check passed (first check runs up to one interval, 15s, after registration).
- Dapr resolves every invocation target through the name resolver **before** checking for self-invocation (`directMessaging.Invoke` → `getRemoteApp`), so a resolver is needed even to invoke your own app id.
- Dapr's gRPC API reads request metadata from its own `pkg/api/grpc/metadata` context key, which daprd's server interceptor (`SetMetadataInContextUnary`) fills from the gRPC incoming metadata. Calling the API object in-process skips the interceptor: `InvokeService` then leaves the request metadata nil and `addCallerAndCalleeAppIDHeaderToMetadata` / `addForwardedHeadersToMetadata` panic ("assignment to entry in nil map"). Running the exported interceptor on the context fixes it; a real gRPC server never passes empty metadata.
- A Go GC can start inside `cabi_realloc` (the canonical ABI copying an argument in). Its start-the-world runs `netpoll(0)`, which on wasip1 calls `poll_oneoff` — an import call, forbidden during realloc: wasmtime traps "cannot leave component instance". componentize-go already pauses the adapter's monotonic clock for the same reason. Patching `runtime/netpoll_wasip1.go` to skip `poll_oneoff` when polling no fds with a zero timeout fixes it.
- Component size with the curated set: ~264 MB (`wasm-tools strip --all` saves only ~12 MB, the `name` section); first wasmtime compile ~30 s.

## Single-instance consequences

- A long-running app instance cannot receive a delivery while it is in a callback (it is on the stack). A self-invocation from a callback therefore can never be served and would deadlock; the go family fails it fast with `app-unreachable`. The sidecar families avoid this only because `wasmtime serve`/Spin instantiate per request.
- Go goroutines (subscribers, timers, heartbeats) run only while the Go component is on the stack (inside an export call such as `next-event` or a building-block call).

## Result

The shared real-Dapr scenario (`e2e/tests/common::run_mirrored_scenario`) passes with the go family: state + CAS + delete, binary byte-exact state, secrets, self-invocation fails fast, Redis pub/sub across two runtimes, CEL routing, bulk subscribe, cross-app invocation via Consul, streaming crypto (3 MiB), cron input binding (~106 s incl. two ~30 s compiles).
