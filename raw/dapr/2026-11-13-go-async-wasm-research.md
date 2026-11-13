# Go in wasm: async, threads and queues for the go family — research and spikes

> Source: web research (specs, GitHub repos/issues/PRs, release notes) and hands-on spikes in this repository (`spikes/go-async/`); wasmtime 49.0.2, componentize-go 0.5.3 (patched Go `go1.27.1-wasi-on-idle`), wit-bindgen 0.63, wac 0.10
> Collected: 2026-11-13
> Published: 2026-11-13

## WASI 0.3 and Component Model async

- WASI 0.3.0 was ratified in June 2026 (https://bytecodealliance.org/articles/WASI-0.3). The Component Model itself is still Phase 1 at the W3C.
- wasmtime 49: basic CM async on by default (follows `concurrency_support`); WASIp3 on by default in the CLI (`-S p3`). Separate opt-ins: `-W component-model-async-stackful`, `-W component-model-threading`, `-W component-model-more-async-builtins` (docs: "very incomplete"). The binary carries `wasi:cli/run@0.3.0`, `wasi:http/handler@0.3.0`, `wasi:clocks/*@0.3.0`.
- `wasmtime serve`: WASIp3 components are reused (defaults: 128 requests, 16 concurrent per instance; `--idle-instance-timeout`); WASIp2 = 1 request per instance. Instances are still recycled.
- Spin 4.1 (Wasmtime 46, WASI 0.3.0, P3 service chaining); 4.3.0 (2026-11-05) on Wasmtime 49.0.3 and WASI 0.3.2 WITs. Triggers are per-request.
- wac 0.10 checks async/sync function types; wit-bindgen Rust 0.63: async exports (callback ABI), `spawn_local`, and `sync: [...]` to sync-lower selected async imports.

## Reentrance (spec)

- CM PR #705 (merged 2026-08-28) "removes the `may_enter` flag+trap": recursive reentrance no longer traps; run-to-completion stays via implicit backpressure.
- Concurrency.md: recursion "will deadlock if `component.wasm` implements `exp` using the sync or stackless async ABI and calls `imp` synchronously… if `imp` is called asynchronously and `exp` doesn't wait for the result, there is no deadlock… if `exp` is implemented using the *stackful* async ABI, there is no automatic backpressure".
- Callback-ABI async exports hold the instance lock only "while running or blocked on the stack (not when having returned to the event loop)". "non-`async` functions ignore backpressure… entirely".
- CM PR #646: async ABI options only on `async`-typed functions — a caller can only call without blocking if the WIT function is `async func`.
- The spec test `test/async/reentrance.wast` passes on wasmtime 49.0.2.

## componentize-go and Go

- componentize-go ≥ 0.5.x supports async worlds: build tag `componentizego_async`, and it downloads a patched Go (`go1.27.1-wasi-on-idle`, https://github.com/dicej/go/releases/tag/go1.27.1-wasi-on-idle) unless `$GOROOT/src/runtime/lock_wasip1.go` contains `wasiOnIdle`.
- The patch (golang/go#76775, open) adds `runtime.wasiOnIdle(func() bool)`; `beforeIdle` returns its result. The go-pkg glue (`wit/async/async_callback.go`) runs each export in a goroutine, and when all goroutines are idle returns `WAIT|set` if the task has pending waitables, `EXIT` if it has none, `YIELD` on `Yield()`. The task state is one global, swapped on every callback.
- The hook ignores the next-timer delay (unlike `js`'s `beforeIdle`, which schedules a timeout): goroutines that wait only on Go timers stall until a waitable of a live task fires.
- TinyGo wasip3 (go-pkg, 2026-11-12) uses CM threads instead: every goroutine is a thread that blocks in `waitable-set.wait`.
- Upstream Go: `GOOS=wasip2` (#65333) and `wasip3/wasm` (#77141) open; wasm threads open since 2018 (#28631); an experimental stack-switching backend (#81963).

## Threads

- wasi-threads: "legacy proposal, retained for engines that can only support WASI v0.1"; wasmtime `-S threads` disables preview2.
- shared-everything-threads: Phase 1; wasmtime flag exists.
- Stack switching: Phase 3; work continues in wasmtime.
- CM `thread.*` built-ins (`thread.new-indirect`, `suspend`, `yield`, `context.get/set`…): cooperative only ("switching between threads only at explicit program points"); wasmtime 49 behind `-W component-model-threading`.
- Big Go has no wasm threads, so threads cannot run Dapr's goroutines in parallel; CM threads could at most map goroutines to fibers (what TinyGo does).

## Background work

- "Returning values by calling `task.return` allows a task to continue executing even after it has passed its initial results". A never-resolving async export keeps a task (and its event loop) alive under `wasmtime run`; `wasmtime serve`/Spin bound that with timeouts and recycling.
- Prior art: wasmCloud v2 "services" (one long-lived `wasi:cli/run` component per workload, reached over TCP only); componentize-go's wasip3 example (concurrent goroutine-based `wasi:http@0.3` handlers). No public project embeds a large Go daemon with timers as a p3 component.

## Spikes (spikes/go-async, wasmtime 49.0.2)

1. Go async `run` (keeps a 5 ms clock wait pending) + goroutine counting every 10 ms via `time.Sleep`; Rust p3 driver: +24 in 300 ms while the driver awaits p3 `wait-for`; +0 during a p2 blocking sleep (`std::thread::sleep`); +0 during a busy loop.
2. Go provider with async `invoke`/`next-event`, Rust app with async `on-invoke` calling `invoke(depth+1)`, Rust p3 pump dispatching concurrently: app → Go → same app instance nests 3 deep and returns 103. With sync-typed `invoke-sync`/`on-invoke-sync`: Go `fatal error: all goroutines are asleep - deadlock!`.
3. A sync-typed export calling p3 `wait-for` through a sync-lowered import: `wasm trap: cannot block a synchronous task before returning`.
- Go async exports must keep a waitable of their own pending while they wait on Go channels; otherwise the glue exits the task without `task.return`.
