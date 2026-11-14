# scala-wasm `@WitAsync` pull request — implementation notes

> Source: hands-on work in this session on scala-wasm `d4e4feb` (branch `wit-async-func`, PR https://github.com/scala-wasm/scala-wasm/pull/273); wasmparser 0.262.0 and wit-parser 0.262.0 sources; wasmtime 44.0.2 and 49.0.2; cargo-component 0.21.1
> Collected: 2026-11-14
> Published: 2026-11-14

- Component binary: `functype ::= 0x40 …` for `func`, `0x43 …` for `async func` (wasmparser `readers/component/types.rs`: `byte @ (0x40 | 0x43)`, `async_: byte == 0x43`).
- wasmparser 0.262 enables `cm_async` by default. wasmtime 44.0.2 does not: a component with an async function type fails with "async component functions require the component model async feature" unless run with `-W component-model-async=y`. With the flag, a sync-ABI composition of async-typed functions runs (result `41`). wasmtime 49.0.2 runs it without the flag.
- wit-parser: `async` is allowed on freestanding functions, resource methods and static methods. Constructors and getters/setters cannot be async ("should not be possible to parse").
- cargo-component 0.21.1 (wit-bindgen-rt 0.39 era): for `async func` it generates sync bindings, but with broken names: the export glue calls `T::async_async_twice` while the trait declares `async_twice` (does not compile); an import becomes `async_async_add_one`.
- scala-wasm: the IR serializer appends tags; a new tag for the async variant keeps old IR readable. `wit.FuncType` is built in `GenWitInterop.scala` (imports, resource static methods, constructors, exports) and read in `Serializers.readWITFuncType`.
- Result in `examples/test-component-model`: `wasm-tools component wit` shows `export bare-add-one: async func(a: s32) -> s32;`; `canon lift` has no `async` option; `wasmtime run --invoke 'bare-add-one(41)'` prints `42` on wasmtime 44 (with the flag) and 49.
- Checks run locally: ir 122 tests (2.12, 2.13), compiler 296 (2.12, 2.13), linker 198 (2.12), scalafmt, testComponentModel fastLinkJS 2.12/2.13 + fullLinkJS 2.13, wasmSystemTest, `make run` 2.12/2.13. Upstream `headerCheck`/`scalastyle` already fail on untouched files and are not in CI.
