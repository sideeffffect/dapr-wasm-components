# Talking to Dapr over gRPC from a Wasm component

> Sources: Dapr proto, tonic/h2/wstd docs, Fermyon `wasi-grpc`, wasmtime wasi-http docs; 2026-06-12
> Raw: [dapr-grpc-from-wasm-research-2026-06](../../raw/dapr/dapr-grpc-from-wasm-research-2026-06.md)

## Question

Could there be a **pure-wasm** `dapr-wasm-components-*-grpc` sibling to
[`dapr-wasm-components-wasi-http`](dapr-wasm-components-architecture.md) — a second
implementation of the same `dapr-wasm-components:interfaces` WIT that talks to the Dapr
sidecar's **gRPC** API (`:50001`) instead of its HTTP API (`:3500`), with no native host?

**Verdict:** feasible and worthwhile, but materially harder than the wasi-http sibling. The
hard part is not gRPC or protobuf — it is obtaining an **HTTP/2 transport that runs inside a
wasm component**. (Confidence 0.85.)

**Outcome (2026-06-12):** the Spin-only path was *built* as a deliberate proof of concept —
`dapr-wasm-components-wasi-grpc` (`components/wasi-grpc/`) implements all 13 interfaces on
Fermyon's `wasi-grpc` crate and passes a real-daprd E2E under Spin 4.0 (state byte-exact +
CAS, invocation, pub/sub). The Spin coupling documented below was accepted knowingly; the
`wasi:sockets`+`h2` option remains the portable follow-up. Implementation decisions and traps
live in [dapr-wasm-components Architecture](dapr-wasm-components-architecture.md).

## What Dapr's gRPC API requires

`DaprClient` in `dapr/proto/runtime/v1/dapr.proto` (+ `common/v1`) is **plain gRPC**: HTTP/2
(h2c — cleartext, no TLS by default on `:50001`), 5-byte length-prefixed protobuf frames, and
`grpc-status`/`grpc-message` in **HTTP/2 trailers**. A client therefore needs four layers:

1. **Protobuf codec** — `prost`. Pure Rust, compiles to `wasm32-wasip2`.
2. **gRPC stubs + framing** — `tonic-build` codegen *without* the `transport` feature (the
   documented "tonic in the browser" path), or hand-rolled length-prefix framing. Compiles to wasm.
3. **HTTP/2 framing** (HPACK, streams, flow control, trailers) — **the crux**; nothing
   wasm-native ships in the standard toolchain.
4. **Byte transport** — TCP via `wasi:sockets`.

The known blocker "tonic can't compile to wasm" is too strong: **only tonic's tokio/hyper
`transport` module can't** — its codegen and codec do. The work is supplying a wasm-native
transport beneath the generated client.

## Transport options

- **`wasi:http` outgoing-handler — mostly blocked today.** The WIT models trailers and is
  HTTP-version-agnostic, but the host decides the version. **wasmtime's `wasi-http` is HTTP/1.1
  only**, so the current `wstd`/`wasi:http` path cannot reach Dapr's gRPC port on wasmtime —
  breaking the "any WASI 0.2 runtime" promise. **Spin 3.4+ added outbound HTTP/2** (cleartext
  via `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE`); Fermyon's **`wasi-grpc`** crate (`WasiGrpcEndpoint:
  tower::Service` over `wasi-hyperium`) proves it — but it is **Spin-coupled**, not portable.
- **`wasi:sockets` + the `h2` crate in the guest — the portable path.** Bring your own HTTP/2:
  run the pure-Rust `h2` over a `wasi:sockets` TCP stream, drive it with `wstd`'s reactor +
  `block_on` (the same sync-over-async trick the wasi-http impl uses to keep the WIT
  synchronous), and put tonic's generated client on top via a custom `tower::Service`. Depends
  only on `wasi:sockets` (broadly supported), preserving the project's pure-wasm identity. **Main
  risk:** `h2` wants tokio's `AsyncRead`/`AsyncWrite` traits while `wstd` exposes its own — you
  write a poll-based adapter from `wasi:sockets` pollables to tokio's IO traits (the traits
  compile to wasm; only tokio's runtime/net/time don't). `wasi-hyperium` is prior art for this glue.
- **gRPC-Web — rejected.** Works over HTTP/1.1 (`tonic-web-wasm-client`), but **Dapr's gRPC API
  does not speak gRPC-Web**; it would need an Envoy/grpcwebproxy per sidecar.

## Why Fermyon's `wasi-grpc` crate is not a shortcut

Distinguish two things: the **technique** `wasi-grpc` embodies — tonic codegen with `transport`
off + a `tower::Service` bridging the generated client to a wasm-native HTTP/2 stack — is exactly
right, and Option B reuses it. The **crate itself** (`wasi-grpc` 0.1.0) is not viable as the
basis for a `dapr-wasm-components` provider, for reasons of *coupling*, not idea:

1. **It requires the Spin host — negating the reason v2 exists.** It needs Spin 3.4+ plus
   Spin-specific knobs: `allowed_outbound_hosts` in `spin.toml` and
   `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE` to switch the outbound path to cleartext h2c. v2 dropped
   the native wasmtime+SDK host precisely so the impl is pure wasm on *any* WASI 0.2 runtime;
   `wasi-grpc` just swaps "custom wasmtime host" for "custom Spin host". The project's E2E runs on
   the wasmtime CLI — `wasi-grpc` components won't run there at all.
2. **Its HTTP/2 is a Spin host *feature*, not a WASI contract.** `wasi:http` 0.2's WIT cannot
   express "HTTP/2 with prior knowledge"; Spin's outbound h2 is a host extension activated
   out-of-band by an env var the *host* interprets. So the protocol behavior lives in the host,
   not the artifact — the same `.wasm` on wasmtime gets HTTP/1.1 or a failed dial. Contrast
   Option B: the `h2` framing is **compiled into the guest**, and the host is only asked for a
   raw `wasi:sockets` TCP socket — a standardized capability every 0.2 runtime implements
   uniformly. That's the portability line: *standard capability grant* vs *proprietary
   behavior-changing env var*.
3. **Its activation model fights Dapr endpoint resolution.** `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE`
   names a fixed `host:port` ahead of time, but a Dapr provider resolves the sidecar at runtime
   from `DAPR_GRPC_ENDPOINT`/`DAPR_GRPC_PORT`. The sockets path just dials the resolved address.
4. **Early/narrow/single-vendor.** `wasi-grpc` 0.1.0, pinned `tonic` 0.13.1, Spin 3.4+; cleartext
   h2c only (no TLS shown); streaming explicitly "limited" (it reaches for `wasi-hyperium`
   *because* the spin-sdk's outbound streaming is limited). The project publishes one `.wasm` to
   OCI for arbitrary runtimes — that artifact can't inherit these constraints.

Fairness: if the deployment target *is* Spin, `wasi-grpc` is the most ergonomic option going, and
`wasi-hyperium` does real work driving hyper from a guest. (Whether `wasi-hyperium` ultimately
rides `wasi:sockets` or a Spin-proprietary outbound import is thinly documented — that
uncertainty is itself a reason not to build a foundation on it.) The point is narrow: its
transport binding and host-env activation don't meet the "any WASI 0.2 runtime, pure wasm, no
proprietary host knobs" bar — the same bar that killed v1's native host. The portable substitute
keeps the tonic-bridge pattern but moves HTTP/2 into the guest via `h2` over `wasi:sockets`.

## Why bother (vs the HTTP impl)

Strongly-typed protobuf (fixes the HTTP variant's JSON byte-roundtrip lossiness), lower
overhead, and gRPC-only **streaming** surfaces (`SubscribeConfigurationAlpha1`,
`SubscribeTopicEventsAlpha1` bidi pubsub since Dapr 1.14, streaming `Encrypt/DecryptAlpha1`).
**But** streaming clashes with the synchronous WIT + `block_on` design — a v1 gRPC variant would
cover the same **unary** surface as wasi-http; streaming waits for WASI Preview 3 async/streams.

## Recommended shape

- **Reuse `dapr-wasm-components:interfaces` unchanged** — gRPC is a second implementation
  exporting the identical WIT; apps just `wac plug` a different provider.
- New crate `components/<name>-grpc/`: `prost` types + tonic codegen (no transport) + `h2` over
  `wasi:sockets` + `wstd` `block_on`.
- **Config (mirror the SDK):** `DAPR_GRPC_ENDPOINT` → `:$DAPR_GRPC_PORT` → default `:50001`;
  `DAPR_API_TOKEN` as `dapr-api-token` gRPC metadata.
- **Errors:** `grpc-status` codes → WIT `error` variant (cleaner 1:1 than the HTTP status-class map).
- **Naming:** the real import is `wasi:sockets`, so `dapr-wasm-components-wasi-sockets-grpc` (or
  plain `-grpc`); a future Spin-only `wasi-grpc`-backed build would be a separate artifact.

Effort: codec/stubs are mechanical; the `h2`-over-`wasi:sockets` + tokio-IO adapter + sync bridge
is roughly the size of the original wasi-http impl, de-risked by `wasi-hyperium`/`wasi-grpc`.

## See Also

- [dapr-wasm-components Architecture](dapr-wasm-components-architecture.md)
- [Dapr HTTP API](dapr-http-api.md)
- [Dapr Rust SDK](dapr-rust-sdk.md) (the tonic/tokio SDK that *can't* go to wasm — the contrast)
- [Wasmtime Host Embedding](../wasm-tooling/wasmtime-host-embedding.md)
