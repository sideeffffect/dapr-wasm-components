# Async-typed WIT with sync-ABI guests — check for scala-wasm / dapr4s

> Source: hands-on check in this session (wasm-tools 1.261.0, wac-cli 0.10.0, wasmtime 49.0.2, wit-bindgen-scala 0.1.0) plus a read of scala-wasm (`d4e4feb`, 2026-11-10), wit-bindgen-scala (`6c8630b`, 2026-09-15) and sideeffffect/dapr4s PR #44 (branch `feat/wasm-component`)
> Collected: 2026-11-14
> Published: 2026-11-14

Question: what changes for a guest toolchain that knows only the sync ABI (scala-wasm, used by dapr4s PR #44) when the interfaces become `async func` (interfaces 0.8.0, PR #20)?

## Experiment 1: sync-ABI core modules against an async-typed WIT

- WIT: `interface api { get: async func(n: u32) -> u32; }`, `interface cb { on-invoke: async func(n: u32) -> u32; }`, `world app { import api; export cb; }`, `world provider { export api; }`.
- Two hand-written core modules (WAT) with the plain sync ABI: import `"t:b/api@0.1.0" "get"` as `(param i32) (result i32)`, export `"t:b/cb@0.1.0#on-invoke"`. No `[async-lower]` / `[async-lift]` names.
- `wasm-tools component embed wit --world <w>` then `wasm-tools component new`: accepted. `wasm-tools component wit` shows `get: async func(n: u32) -> u32;` and `on-invoke: async func(n: u32) -> u32;`. `wasm-tools print` shows `(canon lower (func $get))` and `(canon lift (core func …))` with no `async` option.
- `wac plug app.wasm --plug prov.wasm`: OK. `wasmtime run --invoke 'on-invoke(20)' composed.wasm` prints `41`.
- Control: the same app module embedded with the sync WIT (`func`, no `async`) does not plug into the async-typed provider: `error: the socket component had no matching imports for the plugs that were provided`.

## Experiment 2: wit-bindgen-scala 0.1.0 on `async func`

- A WIT with a record, a variant, `result<list<u8>, err>` and `async func` on an import and an export. `wit-bindgen-scala wit --world app --out-dir out` succeeds.
- The output is byte-identical (`diff -r`) to the output for the same WIT with plain `func`. The generator ignores `async`.
- Source: `TypeDefKind::Future(_) | TypeDefKind::Stream(_)` → `panic!("Unsupported type")`; `Type::ErrorContext` → panic. It pins `wit-bindgen-core = "=0.58.0"`.

## scala-wasm source facts

- No Component Model async code in the linker's component backend (`linker/.../backend/webassembly/component`, `wasmemitter`): a search for `async`, `task.return`, `waitable`, `callback`, `subtask`, `stream.new`, `future.new` finds only the JS loader and JSPI code.
- The IR type is `FuncType(params: List[ParamType], resultType: Option[ValType])` (`ir/.../WasmInterfaceTypes.scala`): there is no async flag. The linker reconstructs the component type from the IR (PR #254), so its own type section says plain `func`.
- Upstream PRs from dapr4s: #268 merged 2026-10-15, #269 merged 2026-11-09, #270 closed, #271 merged. Newest GitHub release: `v1.22.1-wasm.5` (2026-09-14).

## dapr4s PR #44 facts

- `wasm/wit/world.wit` is `include dapr-wasm-components:interfaces/app@0.7.5;` fetched with `wkg` from OCI. 15 hand-written `@WitExport(WitScope("dapr-wasm-components", "interfaces", "<iface>", "0.7.5"), "<fn>")` trampolines (Scala 2.13).
- Both build plugins strip the linker's component type and run `wasm-tools component embed --encoding utf16 <wit> --world <world>` + `wasm-tools component new`. The comment says the step retires with scala-wasm#269.
- The public API is direct-style by design (DESIGN.md Non-Goals: no `Future`-based API). On Scala.js it uses JSPI. The component runs under `wasmtime serve` with the wasi-http providers.

## Not tested

- A real scala-wasm core module against an async-typed WIT (only hand-written modules were used).
- dapr4s against interfaces 0.8.0 (not published to OCI yet).
