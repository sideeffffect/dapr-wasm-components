# gRPC to Dapr from a Wasm component — feasibility research

> Compiled 2026-06-12 to answer: could we build a `dapr-wasm-components-*-grpc` sibling to
> `dapr-wasm-components-wasi-http`, i.e. a **pure-wasm** implementation of the
> `dapr-wasm-components:interfaces` WIT that talks to the Dapr sidecar's **gRPC** API
> (`:50001`) instead of its HTTP API (`:3500`)?
> Constraint from the user: the implementation must run **from wasm** — it need not literally
> use `wasi:http`, but it must not require a custom native host bridging to an SDK (that was the
> rejected v1 design).

## The four layers a Dapr gRPC client needs

Dapr's runtime gRPC API is the `DaprClient` service in `dapr/proto/runtime/v1/dapr.proto`
(messages also from `dapr/proto/common/v1`). It is **plain gRPC**: HTTP/2 (h2c — cleartext, no
TLS by default on `:50001`), 5-byte length-prefixed protobuf message framing, and
`grpc-status`/`grpc-message` returned in **HTTP/2 trailers**.

| Layer | Wasm story | Confidence |
|---|---|---|
| Protobuf codec (`prost`) | Pure Rust, compiles to `wasm32-wasip2` | 0.95 |
| gRPC stubs + framing (`tonic-build` codegen **without** `transport` feature; or hand-rolled framing) | Compiles to wasm — the documented "tonic in the browser" path: generate client without transport, supply your own `tower::Service` | 0.9 |
| HTTP/2 framing (HPACK, streams, flow control, trailers) | **The crux** — no wasm-native HTTP/2 in the standard toolchain; must be brought in | — |
| Byte transport (TCP to sidecar) | `wasi:sockets` (WASI 0.2 TCP) — supported by wasmtime, wasmCloud, Spin | 0.85 |

Key reframing of the known blocker: **tonic's codegen and codec compile to wasm fine; only
its tokio/hyper `transport` module does not.** "tonic can't go to wasm" is too strong — the
transport is the only part that can't.

## Transport options (the whole problem)

### Option A — `wasi:http` outgoing-handler. Mostly blocked today.
- `wasi:http` 0.2's WIT *does* model trailers (a `trailers` future on the body), and is
  HTTP-version-agnostic in the abstract — but the **host** decides whether HTTP/2 is spoken.
- **wasmtime's `wasi-http` host is HTTP/1.1 only** (built on hyper+tokio but no outbound
  h2/ALPN negotiation exposed). So the existing `wstd`/`wasi:http` approach **cannot reach
  Dapr's gRPC port on wasmtime**. This is the killer for portability — it breaks the "runs on
  *any* WASI 0.2 runtime" property that makes the wasi-http variant attractive.
- **Spin 3.4+ added outbound HTTP/2**, including cleartext h2c via the
  `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE` env var — exactly what Dapr's h2c needs. Fermyon's
  **`wasi-grpc`** crate is a guest-side library exposing `WasiGrpcEndpoint` (implements
  `tower_service::Service`) that bridges tonic to `wasi-hyperium` over Spin's outbound HTTP/2.
  Proves the pattern — but is **Spin-coupled** and not viable as a `dapr-wasm-components`
  foundation:
  - Confirmed requirements: Spin 3.4+; `allowed_outbound_hosts = ["http://[::1]:50051"]` in
    `spin.toml`; env var `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE=[::1]:50051` to switch the outbound
    path to cleartext h2c. Versions: `wasi-grpc` 0.1.0, `tonic` 0.13.1.
  - Cleartext h2c only (no TLS shown); streaming "limited" (the post reaches for `wasi-hyperium`
    *because* the spin-sdk's outbound streaming is limited).
  - The deeper objection: `wasi:http` 0.2's WIT cannot express "HTTP/2 with prior knowledge";
    Spin's outbound h2 is a host extension activated by a host-interpreted env var, so the
    protocol behavior lives in the host, not the `.wasm`. The same artifact on wasmtime gets
    HTTP/1.1 or a failed dial. This is the same host-lock v2 was built to escape (it just trades
    a custom wasmtime host for a custom Spin host) — and the project's E2E runs on the wasmtime
    CLI, where `wasi-grpc` won't run at all.
  - Activation also fights Dapr: `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE` fixes a `host:port` ahead of
    time, but the provider resolves the sidecar at runtime from `DAPR_GRPC_ENDPOINT`/`PORT`.
  - `wasi-hyperium`'s exact host binding (`wasi:sockets` vs a Spin-proprietary outbound import)
    is thinly documented publicly — that uncertainty is itself a reason not to build on it.
  - The *technique* (tonic no-transport + `tower::Service` bridge to a wasm-native h2) is sound
    and is what Option B reuses; only this crate's transport binding + host-env activation are
    disqualifying.

### Option B — `wasi:sockets` + a Rust HTTP/2 stack in the guest. The portable path.
- Bring your own HTTP/2: the **`h2`** crate is pure Rust. Run it over a `wasi:sockets` TCP
  stream, drive its futures with a wasi reactor (`wstd`'s reactor + `block_on` — the same
  blocking-sync trick the wasi-http impl already uses to keep the WIT synchronous), and put
  tonic's generated client on top via a custom `tower::Service` / `GrpcService`.
- This is conceptually what Fermyon's `wasi-grpc`/`wasi-hyperium` does, but swapping Spin's
  outbound-HTTP host dependency for runtime-agnostic `wasi:sockets`. Keeps the project's
  "pure wasm, any WASI 0.2 runtime" identity: the dependency becomes `wasi:sockets` (widely
  supported) rather than the host's HTTP/2.
- **Main engineering risk:** `h2` is written against `tokio`'s `AsyncRead`/`AsyncWrite` IO
  traits; `wstd` exposes its own IO traits over `wasi:sockets` pollables. You need a poll-based
  adapter from `wasi:sockets` to tokio's IO traits. Tokio's *traits* compile to wasm (it's
  tokio's runtime/net/time that don't), so the adapter is bounded work — and `wasi-hyperium` is
  prior art for exactly this glue. This adapter is the bulk of the novel effort.

### Option C — gRPC-Web. Rejected against vanilla Dapr.
- `tonic-web-wasm-client` proves gRPC-Web works over HTTP/1.1 (trailers base64-encoded in the
  body), which wasmtime *does* support. But **Dapr's runtime gRPC API does not speak
  gRPC-Web** — you'd need an Envoy / grpcwebproxy in front of every sidecar, defeating the
  sidecar model. Only worth recording as "considered and rejected."

## Why a gRPC variant is worth it (vs the HTTP impl)

- **Strongly-typed protobuf** — removes the HTTP variant's JSON value lossiness (the documented
  "binary state values don't roundtrip byte-exact" limitation).
- Lower per-call overhead.
- Access to **gRPC-only streaming surfaces**: `SubscribeConfigurationAlpha1` (server stream),
  `SubscribeTopicEventsAlpha1` (bidi pubsub streaming subscribe, since Dapr 1.14),
  streaming `EncryptAlpha1`/`DecryptAlpha1`.
- **Caveat:** streaming clashes with the project's synchronous WIT + `block_on` model. A v1
  gRPC variant would realistically cover the same **unary** surface as wasi-http; streaming
  waits for WASI Preview 3 native async/streams (in progress, e.g. on wasmCloud) to be
  ergonomic, or would need callback/async ABI the project deliberately avoids.

## Recommended shape

- **Reuse `dapr-wasm-components:interfaces` unchanged.** gRPC is a *second implementation*
  exporting the identical WIT — apps don't change, they `wac plug` a different provider.
- New crate `components/<name>-grpc/`: `prost` types from Dapr's proto + tonic codegen (no
  transport) + `h2` over `wasi:sockets` + `wstd` reactor/`block_on`.
- **Config (mirror the SDK):** `DAPR_GRPC_ENDPOINT` → `http://127.0.0.1:$DAPR_GRPC_PORT` →
  default `:50001`; `DAPR_API_TOKEN` sent as the `dapr-api-token` gRPC metadata header.
- **Errors:** map `grpc-status` codes → the WIT `error` variant — a cleaner 1:1 than the HTTP
  variant's status-class heuristic.
- **Naming:** since it needn't be literally "wasi" and the real import is `wasi:sockets`, prefer
  `dapr-wasm-components-wasi-sockets-grpc` (honest) or `dapr-wasm-components-grpc`. A future
  Spin-only `wasi-grpc`-backed build could be a separate artifact.

## Effort & risk

- Proto/codec/stubs: low risk, mechanical.
- `h2`-over-`wasi:sockets` + tokio-IO adapter + sync bridge: the real work, medium-high risk,
  roughly the size of the original wasi-http implementation again; de-risked by `wasi-hyperium`
  / `wasi-grpc` prior art.
- E2E: the existing daprd-via-testcontainers harness already exposes gRPC ports; point the new
  component at `:50001`.

## Sources

- Dapr proto: https://github.com/dapr/dapr/blob/master/dapr/proto/runtime/v1/dapr.proto
- Streaming subscribe (`SubscribeTopicEventsAlpha1`, Dapr 1.14): https://docs.dapr.io/developing-applications/building-blocks/pubsub/howto-publish-subscribe/
- Fermyon/Akamai `wasi-grpc` for Spin: https://www.akamai.com/blog/developers/introducing-wasi-grpc (orig. https://www.fermyon.com/blog/introducing-wasi-grpc)
- tonic wasm/browser support (no-transport codegen, `spawn`): https://github.com/hyperium/tonic , https://docs.rs/tonic/latest/tonic/
- tonic-web-wasm-client (gRPC-Web): https://crates.io/crates/tonic-web-wasm-client , https://github.com/devashishdxt/tonic-web-wasm-client
- wasmtime wasi-http is HTTP/1.1-only: https://docs.wasmtime.dev/api/wasmtime_wasi_http/index.html
- tokio WASI Preview 2 status: https://github.com/tokio-rs/tokio/issues/6323
- WASI / component model async status (P3): https://eunomia.dev/blog/2025/02/16/wasi-and-the-webassembly-component-model-current-status/ , https://wasmcloud.com/blog/wasi-p3-on-wasmcloud/
