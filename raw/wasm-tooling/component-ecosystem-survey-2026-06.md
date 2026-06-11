# WebAssembly Component Model ecosystem survey (June 2026)

> Source: Research synthesis compiled 2026-06-11 from github.com/bytecodealliance (wasmtime, wit-bindgen, wasm-pkg-tools), github.com/WebAssembly/WASI, wasi.dev/roadmap, docs.rs/wasmtime, docs.wasmtime.dev, component-model.bytecodealliance.org, wasmcloud.com, bytecodealliance.org articles
> Collected: 2026-06-11
> Published: Unknown

## Version snapshot (verified via crates.io / GitHub releases, 2026-06-11)

| Tool | Latest stable | Released |
|---|---|---|
| wasmtime | 45.0.1 | 2026-06-05 |
| wit-bindgen | 0.58.0 | 2026-06-08 |
| wkg (wasm-pkg-tools) | 0.15.1 | 2026-05-28 |
| wac-cli | 0.10.0 | 2026-04-17 |
| cargo-component | 0.21.1 | 2025-03-18 (maintenance/transitional) |
| WASI 0.2.x latest | 0.2.12 | 2026-06-02 |
| WASI 0.3.0 | final, released 2026-06-11 | RCs since 2026-01 |

## WASI 0.2 vs 0.3

- WASI 0.2 is stable and remains the safe production target; the 0.2 line continues in parallel.
- WASI 0.3.0 final released 2026-06-11. 0.3 = native async at the Canonical ABI level: `async` funcs, `stream<T>`/`future<T>`; 0.2 interfaces refactored to use them.
- Wasmtime: experimental opt-in WASIp3 since wasmtime 37; wasmtime 43 tracks snapshot 0.3.0-rc-2026-03-15; wasmtime 45.0.1 predates final 0.3.0.
- Composing async components is preview-quality, not production-default: wasmCloud runs Rust HTTP p3 components behind `--features wasip3`; Bytecode Alliance reports cross-component async currently adds ~3.5x overhead even on sync call paths, optimization planned post-1.0. Component Model 1.0 to be "only the good parts" of P3; WASI 1.0 targeted late 2026.
- Caveat: blogs claiming "WASI 0.3 released February 2026" are wrong (that was the first RC).

## wit-bindgen for Rust guests (0.58.0)

```toml
[lib]
crate-type = ["cdylib"]
[dependencies]
wit-bindgen = "0.58"
```

```rust
wit_bindgen::generate!({
    world: "my-world",       // optional if only one world
    path: "wit",             // default ./wit
    generate_all,
});

struct Component;
impl exports::docs::adder::add::Guest for Component {
    fn add(a: u32, b: u32) -> u32 { a + b }
}
export!(Component);
```

`generate!` options: `world`, `path`, `inline`, `with`, `generate_all`, `skip`, `additional_derives`, `ownership`, `features`, `pub_export_macro`, `async` (bool or per-function list; unset = follow WIT annotations), etc.

Build: `wasm32-wasip2` is tier-2 Rust target since 1.82; `cargo build --target wasm32-wasip2` produces a component directly — no adapter, no cargo-component needed. cargo-component is in transition; modern recommendation is plain cargo + wit_bindgen + `wkg wit fetch`.

## wasmtime host embedding — sync WIT + async host

**A sync WIT interface CAN be implemented by async Rust host functions** (supported since wasmtime PR #2434). To the guest the call is still blocking; the runtime awaits the host future. bindgen docs: the async flag means "a Rust-level `async` function is used on the host… Note though that to WebAssembly itself the function will still be blocking."

Syntax change ≥ ~v34 (current 45.x): the old top-level `async: true` was replaced by flag maps:

```rust
bindgen!({
    path: "wit",
    world: "my-world",
    imports: { default: async | trappable },   // host trait methods become async fn
    exports: { default: async },               // guest exports called via async API
});
```

Flags per function or `default:`: `async`, `store`, `tracing`, `verbose_tracing`, `trappable`, `ignore_wit`, `exact`.

Runtime requirements:

```rust
let mut config = wasmtime::Config::new();
config.async_support(true);                      // REQUIRED; mismatch panics
let engine = Engine::new(&config)?;
let mut linker = Linker::<MyState>::new(&engine);
MyWorld::add_to_linker(&mut linker, |s| s)?;
let mut store = Store::new(&engine, MyState::default());
let instance = MyWorld::instantiate_async(&mut store, &component, &linker).await?;
let out = instance.call_run(&mut store).await?;
```

With `async_support(true)` you must use `_async` variants everywhere. Wasmtime futures are Send when store data is Send. Tune `Config::async_stack_size`.

## wkg publishing

```bash
wkg wit build --wit-dir wit          # emits ns:pkg@x.y.z.wasm (name from package decl)
wkg publish ns:pkg@x.y.z.wasm        # routes via namespace mapping in config
wkg publish component.wasm --package ns:pkg@x.y.z   # component binary
wkg oci push ghcr.io/user/name:0.1.0 component.wasm # raw OCI, no config needed
wkg get --format wit ns:pkg@x.y.z --output pkg.wit  # consume
wkg wit fetch                        # resolve wit/deps/ + wkg.lock
```

Config (`~/.config/wasm-pkg/config.toml`):

```toml
[namespace_registries]
yourns = { registry = "yourns", metadata = { preferredProtocol = "oci", oci = { registry = "ghcr.io", namespacePrefix = "your-gh-org/" } } }
```

Final OCI ref = `<oci.registry>/<namespacePrefix><namespace>/<package>:<version>`.

Auth for ghcr.io: wkg falls back to `~/.docker/config.json` credentials; locally `docker login ghcr.io -u USER -p <PAT with write:packages>`. GitHub Actions: `permissions: { packages: write, contents: read }` + docker/login-action with `username: ${{ github.actor }}`, `password: ${{ secrets.GITHUB_TOKEN }}`. First-time note: packages published by a token are private by default on ghcr; link to repo / set public in package settings (the `org.opencontainers.image.source` annotation from wkg.toml `repository` metadata links the package to a repo).

## OCI naming conventions (WASI precedent)

Official WASI WIT packages: `ghcr.io/webassembly/<namespace>/<package>:<semver>`, e.g. `ghcr.io/webassembly/wasi/http:0.2.3`. The `wasi.dev` registry name is an indirection via `.well-known/wasm-pkg/registry.json` → `{"oci": {"registry": "ghcr.io", "namespacePrefix": "webassembly/"}}`. WIT packages and components use the same OCI artifact format (`application/vnd.wasm.component.v1` media types) and naming scheme; tags are bare semver; one OCI repo per namespace/package.

## Prior art: Dapr × WebAssembly

- Dapr Wasm HTTP middleware (`middleware.http.wasm`): in-tree, production; http-wasm Handler ABI on embedded wazero — core wasm module ABI, NOT component model/WIT.
- deislabs/dapr-wasm-exp: DeisLabs experiment (state store plugin as wasm via Go plugin system); contemplated component model but never implemented it; archived 2025-07-14.
- second-state/dapr-wasm: older WasmEdge functions as Dapr microservices, pre-component-model.
- dapr/dapr issues #3496 (WASM for state/pubsub) and #5619 (wasm actor): deferred; pluggable components went gRPC/Unix-socket instead.
- **Net finding: no shipped project exposes Dapr building blocks as WASI-0.2 WIT interfaces to component guests — the space is open.** Closest adjacent art: wasmCloud's capability model over WIT (wasi:keyvalue, wasi:messaging, wasi:http) — worth borrowing design vocabulary.
