# go-async spikes

Small, throw-away components that test what WASI 0.3 / Component Model async
can do for the go provider family. They are not part of any workspace and are
never published. Findings and the plan built on them:
[wiki/dapr/dapr-wasm-components-go-async.md](../../wiki/dapr/dapr-wasm-components-go-async.md).

Run all of them with `./run.sh`. It needs componentize-go ≥ 0.5.3 (it
downloads a patched Go 1.27.1 with `runtime.wasiOnIdle`), cargo with the
`wasm32-wasip2` target, wac ≥ 0.10 and wasmtime ≥ 49.

| Spike | What it composes | Result on wasmtime 49.0.2 |
|---|---|---|
| 1 `1-background` | Go component: async `run` (never returns, keeps a 5 ms clock wait pending) + a goroutine that counts every 10 ms with `time.Sleep`. Rust p3 driver measures the counter. | Counter advances **+24 in 300 ms** while the driver awaits a p3 `wait-for`; **+0** during a p2 blocking sleep; **+0** during a busy loop. |
| 3 `1-background` (`app3`, `driver3`) | A sync-typed export that calls the p3 `wait-for` through a sync-lowered import (the shape of today's `*-callback` functions). | `wasm trap: cannot block a synchronous task before returning`. |
| 2 `2-reentrance` | Go provider (`invoke`, `next-event` async), Rust app with async `on-invoke` that calls `invoke(depth+1)`, Rust p3 pump that pulls events and calls the app concurrently. | **app → Go → same app instance, 3 levels deep → 103**. One app instance, one Go runtime. |
| 2 with `sync` arg | The same with the sync-typed `invoke-sync` / `on-invoke-sync`. | Go: `fatal error: all goroutines are asleep - deadlock!` |

Notes:

- The Go async glue (go-pkg `wit/async`, callback ABI) exits a task once all
  goroutines are idle and the task has no pending waitable. So a Go async
  export must never wait on a plain Go channel only: it waits with a pending
  clock wait of its own (`block` in `2-reentrance/go/.../export.go`).
- Go timers do not wake the host: the patched runtime's idle hook ignores the
  next timer. Goroutines that sleep run only when some waitable of a live
  task fires, hence the clock wait in `run`.
