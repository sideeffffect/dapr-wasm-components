# dapr-wasm-components

[Dapr](https://dapr.io) building blocks exposed to [WebAssembly components](https://component-model.bytecodealliance.org/) as [WIT](https://github.com/WebAssembly/component-model/blob/main/design/mvp/WIT.md) interfaces, with a [wasmtime](https://wasmtime.dev)-based host that bridges them to a Dapr sidecar via the [Dapr Rust SDK](https://github.com/dapr/rust-sdk).

Write a portable, sandboxed wasm component that calls Dapr state stores, pub/sub, secrets, bindings, service invocation, and configuration — using plain **synchronous** function calls. The host implements those sync WIT imports with **async** host functions (tokio + the async-only Dapr Rust SDK); the guest blocks while the host awaits. This is a standard, fully supported wasmtime pattern on WASI 0.2, so no WASI 0.3 async is needed.

```
┌────────────────────────────┐     sync WIT calls     ┌──────────────────────────┐    gRPC     ┌──────────┐
│  your component            │  ───────────────────▶  │  dapr-wasm-host          │  ────────▶  │  Dapr    │
│  (wasm32-wasip2, Rust/...) │  ◀───────────────────  │  (wasmtime + Rust SDK)   │  ◀────────  │  sidecar │
│  imports dapr:client/*     │   topic events (export) │  async host functions    │  callbacks  │          │
└────────────────────────────┘                        └──────────────────────────┘             └──────────┘
```

## Repository layout

| Path | What |
|---|---|
| `wit/` | The `dapr:client@0.1.0` WIT package — the **component interfaces** |
| `host/` | `dapr-wasm-host` — wasmtime host implementing the interfaces with the Dapr Rust SDK |
| `examples/kv-demo/` | Example guest component (state roundtrip + pub/sub, Rust + wit-bindgen) |
| `wiki/`, `raw/` | LLM-maintained knowledge base with the research behind the design |

## Interfaces (`dapr:client@0.1.0`)

| Interface | Direction | Building block |
|---|---|---|
| `state` | import | State management: `get`, `save`, `save-bulk`, `delete` |
| `pubsub` | import | Pub/sub: `publish` |
| `secrets` | import | Secrets: `get-secret`, `get-bulk-secret` |
| `bindings` | import | Output bindings: `invoke-binding` |
| `invocation` | import | Service invocation: `invoke` |
| `configuration` | import | Configuration: `get` |
| `topic-handler` | **export** | Pub/sub deliveries: `list-topic-subscriptions`, `on-topic-event` |

Worlds: `library` (imports only) and `app` (imports + `topic-handler` export + a `run` entry point).

## Quick start

Prerequisites: Rust (≥ 1.88) with the `wasm32-wasip2` target, and the [Dapr CLI](https://docs.dapr.io/getting-started/) with a [initialized environment](https://docs.dapr.io/getting-started/install-dapr-selfhost/) (for the real sidecar).

```sh
rustup target add wasm32-wasip2

# Build the example component and the host
cargo build --release --target wasm32-wasip2 --manifest-path examples/Cargo.toml
cargo build --release

# Try it without Dapr (in-memory backend)
./target/release/dapr-wasm-host --backend memory examples/target/wasm32-wasip2/release/kv_demo.wasm

# Run it against a real Dapr sidecar; the host doubles as the callback app
# so pub/sub messages are delivered back into the component
dapr run --app-id kv-demo --app-port 50051 --app-protocol grpc \
  -- ./target/release/dapr-wasm-host examples/target/wasm32-wasip2/release/kv_demo.wasm
```

The host resolves the sidecar address like other Dapr SDKs: `DAPR_GRPC_ENDPOINT`, then `DAPR_GRPC_PORT` on localhost, then `http://127.0.0.1:50001`.

## Writing your own component (Rust)

```toml
[lib]
crate-type = ["cdylib"]

[dependencies]
wit-bindgen = "0.58"
```

```rust
wit_bindgen::generate!({ world: "app", path: "wit" });

use dapr::client::state;

struct Component;

impl Guest for Component {
    fn run() -> Result<String, String> {
        state::save("statestore", "key", b"value", None, &[], None)
            .map_err(|e| format!("{e:?}"))?;
        Ok("saved".into())
    }
}
// ... impl exports::dapr::client::topic_handler::Guest, then:
export!(Component);
```

Build with `cargo build --target wasm32-wasip2` — the output is a ready-to-run component.

## OCI distribution

CI publishes both modules to this repository's OCI registry (GitHub Container Registry) with [wkg](https://github.com/bytecodealliance/wasm-pkg-tools):

- **Interfaces**: `ghcr.io/sideeffffect/dapr-wasm-components/dapr/client:<version>`
- **Example component**: `ghcr.io/sideeffffect/dapr-wasm-components/dapr/kv-demo:<version>`

Fetch the interfaces for your own project:

```sh
wkg oci pull ghcr.io/sideeffffect/dapr-wasm-components/dapr/client:0.1.0 -o dapr-client.wasm
wasm-tools component wit dapr-client.wasm   # view as WIT text
```

## Status & limitations

Experimental. The Dapr Rust SDK itself is Alpha, and this project tracks what it can deliver (SDK 0.17):

- No client-level state transactions and no distributed lock (absent from the SDK).
- No input bindings via gRPC callbacks and no bulk subscribe (stubbed in the SDK).
- Configuration subscriptions and actors/jobs/crypto/conversation/workflow are not exposed yet.
- One component instance handles events sequentially (wasmtime stores are single-threaded).
- `topic-handler` events are delivered through the host's app-callback gRPC server; run the host with `--app-port <port> --app-protocol grpc` under `dapr run` (or equivalent K8s annotations).

## Development

```sh
cargo fmt --all && cargo clippy --all-targets -- -D warnings
cargo build --release --target wasm32-wasip2 --manifest-path examples/Cargo.toml
cargo test    # runs the example component against the in-memory backend
```

The `wiki/` directory is an [LLM-maintained knowledge base](AGENTS.md) holding the research this design is based on — start at [wiki/index.md](wiki/index.md).

## License

[Apache-2.0](LICENSE)
