# scala-wasm: `Future`, the execution context and Component Model async gaps

> Source: read of scala-wasm (`d4e4feb`, 2026-11-10) and its issue tracker (issues #218, #250); Component Model spec `main` (Concurrency.md, CanonicalABI.md) as read on 2026-11-14. No code was run.
> Collected: 2026-11-14
> Published: 2026-11-14

Question: how do WIT `async`, the Component Model `future<T>` type and Scala's `Future` relate, and what does scala-wasm lack?

## scala-wasm source facts

- `library/src/main/scala/scala/scalajs/concurrent/QueueExecutionContext.scala`: `apply()` uses `linkTimeIf(moduleKind == MinimalWasmModule || moduleKind == WasmComponent) { single() } { …timeouts() / promises() }`. `SingleThreadedExecutionContext.execute` adds the task to a list and, if no drain is in progress, runs all tasks in a `while (tasks.nonEmpty)` loop before it returns.
- `scalalib/overrides/scala/concurrent/ExecutionContext.scala`: `Implicits.global` is `scala.scalajs.concurrent.JSExecutionContext.queue`. In component mode the failure reporter is a no-op ("On Wasm without JS there is no standard output").
- `test-suite/js/.../jsinterop/AsyncTest.scala`: the queue/timeout/promise execution-context tests are disabled for `WasmComponent`.
- IR interface types (`ir/.../WasmInterfaceTypes.scala`): primitives, `ListType`, records, tuples, variants, `ResultType`, enums, `OptionType`, flags, aliases, `ResourceType`, `FuncType(params, resultType)`. No future, stream or error-context type; a search of `ir` and the linker's webassembly backend for `futuretype`, `streamtype`, `errorcontext` finds nothing.
- `ComponentBinaryWriter.scala`: the only canonical option written is the string encoding (UTF-16). No `async`, no `callback`.
- No JSPI / `js.await` code in the component backend (JSPI is the mechanism on the JS host).

## scala-wasm issues

- #218 "ExecutionContext should probably also handle WASIp2 `Pollable`s" (open, 2026-05-21). Request: a scheduling loop for Scala tasks and WASI pollables (came up when porting the cats-effect test suite). Maintainer (collaborator), 2026-05-22: "I think this will have to be provided by an external library. We can't make the core dependent on a specific WASI API or version." The agreed direction: a library like `scala-js-macrotask-executor`, for WASIp2 "and WASIp3 once it's finished".
- #250 "How should applications differentiate between WASI versions?" (open, 2026-08-10). Maintainer: "Scala.js core must not know about WASI. It must only know about the Component Model." Proposed direction: user-defined link-time properties; the WASI binding library defines a well-known property.
- A search of the tracker for "async" finds only #74 (closed, WasmGC Canonical ABI options). No issue for Component Model async, the callback ABI, or WASI 0.3.

## Analysis (this session, not from a source; not tested)

- Three layers: WIT `async` is a marker on a function type ("may block"). `future<T>` is a WIT value type (a one-shot channel with a readable and a writable end, passed as a parameter or result). Scala `Future[T]` is a library type inside the guest; it is not visible outside the component.
- The Canonical ABI object closest to a Scala `Future` is the *subtask*: what an async-lowered import call returns when the callee does not finish at once. It is a waitable, not a WIT type.
- A bindings generator chooses the mapping. Other languages map an async-lowered import to their own type (Rust `async fn`, JS `Promise`); a Scala generator could map it to `Future[T]` or keep a blocking direct-style call.
- With the single-threaded execution context, a `Future` in a component is finished before the outermost `execute` returns. It gives no concurrency with the outside: nothing can complete a `Promise` later, because there is no event loop.
- The callback (stackless) async ABI fits `Future`-style code: the export returns to the host and the host calls a callback when a waitable is ready. Direct-style code needs the stackful ABI (wasmtime 49: opt-in `-W component-model-async-stackful`), Component Model threads (opt-in, incomplete) or core stack switching (Phase 3).
- By the spec text, a sync-lifted export of an `async func` may block, so it could start several imports with async lower and wait on them with `waitable-set.wait`. Not tested on wasmtime.
