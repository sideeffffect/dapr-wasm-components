# go family without async-typed WIT — research

> Source: web research (Component Model spec `main`, GitHub repos/issues/PRs/releases, project docs, Bytecode Alliance and wasi.dev articles). No spikes in this session.
> Collected: 2026-11-14
> Published: 2026-11-14

Question: can the go family's two limits (Dapr runs only while go-outbound is on the stack; the app cannot be re-entered) be lifted while the public WIT stays sync?

## Component Model spec (`design/mvp`, read 2026-11-14)

- Concurrency.md: "Since non-`async` functions are not allowed to block (including due to backpressure) and also don't pile up like `async` functions, non-`async` functions ignore backpressure (explicit and implicit) entirely."
- CanonicalABI.md, canonopt validation: "`async` - is only allowed when used with an `async` function type in `canon lift` or `canon lower`". So a sync-typed function cannot use the async ABI.
- Concurrency.md: "The new async ABI can be used alongside or instead of the existing WASI 0.2 'sync ABI' to call or implement any `async`-typed functions… both sync-ABI-calls-async-ABI and async-ABI-calls-sync-ABI pairings have well-defined, composable behavior".
- Concurrency.md, recursion with async types: "this call will deadlock if `component.wasm` implements `exp` using the sync or stackless async ABI and calls `imp` synchronously… if `exp` is implemented using the *stackful* async ABI, there is no automatic backpressure and it's up to the guest to handle the recursion without deadlock (as with the preceding synchronous case)".
- CanonicalABI.md: "because non-`async`-typed functions ignore backpressure entirely, they may reenter core wasm when an `async`-typed function would have been blocked by implicit backpressure."
- Concurrency.md: "Returning values by calling `task.return` allows a task to continue executing even after it has passed its initial results to the caller. This is also possible even with the sync ABI by using cooperative threads."
- Concurrency.md: "cooperative threads do not allow a single component instance to increase its internal parallelism".
- Concurrency.md, start functions: a component-level start function lifted with the async ABI "*may* block after calling `task.return`, and may thus serve as a long-running 'background task'".
- https://github.com/WebAssembly/component-model/blob/main/design/mvp/Concurrency.md , https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md

## wit-bindgen (Rust `generate!` docs, `main`)

- "Functions that are `async` in WIT get bindings using the component model async ABI by default, and all other functions get sync bindings. This option switches `async` functions back to sync bindings. `sync: true, // all bindings are sync`"; a per-function list is also accepted (`"import:…"`, `"export:…"`).
- https://github.com/bytecodealliance/wit-bindgen/blob/main/crates/guest-rust/src/lib.rs

## WASI 0.2 `pollable` does not compose

- wasi.dev WASI 0.3 overview (as summarised by a web search, not read in full): WASI 0.2 could express async but could not compose it across component boundaries. A component between the host and its caller could not forward a host `pollable` wake-up, because `pollable` is a resource of one component instance (the "sandwich problem"). WASI 0.3 moves the event loop into the host/Canonical ABI.
- https://wasi.dev/releases/wasi-p3 , https://bytecodealliance.org/articles/WASI-0.3

## Hosts: separate stores with typed sync WIT

- **wRPC.** v0.18.0 (2026-10-23, wasmtime 50): `wrpc-wasmtime tcp run --import <addr>` / `tcp serve --import <addr> --export <addr>` polyfill non-WASI imports over TCP; serving instantiates per call ("This serving method does not support guest-exported resources"). PR #1719 "refactor(wasmtime)!: drop wrpc:rpc and polyfilling", merged 2026-11-13: "The wRPC polyfilling feature was always fragile and a potential footgun. In wasip2, calling functions designed to return fast could potentially block and, even worse, calling polyfilled functions could result in a trap… No maintainer is currently actively working on this feature… therefore drop it". https://github.com/bytecodealliance/wrpc/pull/1719
- **wasmCloud 2.12.1** (2026-11-08, wasmtime 48). Services: "Components **cannot** call into the service via WIT interfaces. However, components **can** connect to TCP ports the service is listening on" (virtual pipes in the runtime). Host component plugins: one long-lived instance pinned under one store, optional `wasi:cli/run@0.3.0` background task, calls into workloads through `wasmcloud:host/workload-call` on a pooled instance or a fresh store; "Every function of a workload-facing interface must be an **`async func`**; a sync function is refused at plugin load"; resources cannot cross. https://github.com/wasmCloud/wasmcloud.com/blob/main/docs/overview/workloads/services.mdx , https://github.com/wasmCloud/wasmcloud.com/blob/main/docs/runtime/creating-host-component-plugins.mdx
- **Spin 4.3.0** (2026-11-05; `Cargo.toml` at tag `v4.3.0`: `wasmtime = "50.0.1"`). Service chaining is HTTP only, in memory, inside the host process; "A WASI P2 HTTP component is _not_ subject to instance reuse". Component dependencies are static composition. No typed cross-store call, no long-running component. https://github.com/spinframework/spin-docs/blob/main/content/v4/http-outbound.md
- **wasmtime CLI** (latest 50.0.1, 2026-10-29; 51.0.0-rc.1): `run` and `serve` take one component. Component Model issue #423 "runtime instantiation": open, no spec PR; Luke Wagner 2024-12-03: "once 0.3-async is closed out, this is one of the last significant feature I am quite keen to add… for a 1.0 MVP". https://github.com/WebAssembly/component-model/issues/423
- Others: Golem 1.6.3 (typed agent-to-agent RPC, code-first, durable agents); Obelisk (workflow-to-activity, no long-running component); Hyperlight-wasm (a Rust library, needs a custom host); NGINX Unit (archived 2025-10-08).

## Go toolchain and threads

- golang/go#28631 (wasm threads): open, Unplanned. #77141 (`wasip3`): open, no decision; 2026-02-18 "we should only implement wasip3 as and when it becomes clear that it is something that the larger Wasm community is settling on". #76775 / CL 729680 (`wasiOnIdle`): open. #65333 (`wasip2`): open, backlog.
- golang/go#81963 "Go Wasm Stack Switching": milestone Go 1.28, behind `GOWASM=stackswitch`; "The change is intended to be invisible to programmers and only to streamline the compiler implementation."
- `go:wasmexport` (proposal golang/go#65199): "all goroutines still running when the invocation of the `go:wasmexport` function returns will be paused until the control flow re-enters the Go application." Go blog 2025-02-13 says the same. No documented scheduler tick.
- componentize-go v0.5.3 (2026-10-30): nothing on background goroutines or timer-aware idle. go-pkg PR #19 (merged 2026-11-13) adds the TinyGo-only CM-threads path.
- shared-everything-threads: Phase 1. Bytecode Alliance, 2026-06-08: "Cooperative threads support will ship in a follow-up WASI P3 minor release. These advancements also pave the road for shared-everything threads later". wasmtime 50.0.1: component-model threading off by default, "_very_ incomplete". "A `Store` cannot be used simultaneously from multiple threads". https://bytecodealliance.org/articles/the-road-to-component-model-1-0
- WASIX: Wasmer docs: "Go's wasip1 port is single-threaded WASI, not WASIX." An experimental fork `wasix-org/go` has thread commits (2026-10-29), maturity unknown. Wasmer closed "WASI Preview 2 Support" (wasmerio/wasmer#4439) as not planned, so no Component Model.

## Analysis (this session, not from a source; not spiked)

- In one store, a sync-typed function cannot block at the Component Model level, so nothing else in the store runs during it except what it calls. Only a function with `async` in its type may block. This is why limit 1 has no in-store fix with sync types.
- Instantiation is acyclic, so every provider of the app's imports is instantiated before the app and cannot call the app. A nested delivery can only reach the app on the "return" path. One sync design follows: the app-side SDK waits for a building-block result in a loop and, while it waits, pulls events for this app and runs the handler as a plain in-instance call (a nested pump). It covers self-invocation and A → B → A, which are nested by nature. It needs an additive sync, pull-based interface in the app world (or a go-only import).
- `async func` in WIT is a type marker for "may block". App code can stay sync (`sync: true`). A sync-lifted app then lets Dapr run while the app waits on Dapr or p3 I/O, but still cannot be re-entered.
- Correction to the 2026-11-13 research file: Spin 4.3.0 pins wasmtime 50.0.1, not 49.0.3.
