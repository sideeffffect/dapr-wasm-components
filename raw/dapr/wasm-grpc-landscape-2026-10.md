# Wasm gRPC landscape — wider survey (2026-10-10)

> Compiled 2026-10-10. Question: is Spin limited/abandoned, and is there a better runtime or
> library for a gRPC-speaking wasm component (the `wasi-grpc-outbound` provider)?

## Spin

- Not abandoned. Releases: v4.0.0 (2026-04-20), v4.1.0 (2026-08-26), v4.2.0/4.2.1/4.2.2
  (2026-09-29 → 2026-10-06). Commits still landing in October 2026.
- Governance: CNCF Sandbox since 2025-01-21 (repo `spinframework/spin`). Akamai acquired
  Fermyon 2025-12-01 and said it keeps supporting Spin/SpinKube.
- Outbound h2c is still a dev knob: `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE`, one `host:port`.
- The limited part is the `fermyon/wasi-grpc` crate: last commit 2025-10-02 ("gRPC server",
  unpublished `wasi-grpc-server`), crates.io 0.1.0 pinned to tonic 0.13 / spin-executor 4.
  Sources: `gh release list -R spinframework/spin`, `gh api repos/fermyon/wasi-grpc/commits`,
  https://www.cncf.io/projects/spin/,
  https://www.ir.akamai.com/news-releases/news-release-details/akamai-technologies-announces-acquisition-function-service

## wasmCloud v2 (wash-runtime) — host-side gRPC over plain wasi:http

- Latest v2.10.3 (2026-10-02); monthly-ish releases.
- `crates/wash-runtime/src/host/http.rs`: `is_grpc_request()` checks
  `content-type: application/grpc*`. Such outgoing `wasi:http` requests take a "gRPC egress fast
  path": HTTP/2 via `hyper::client::conn::http2::handshake`, prior knowledge for `http://`
  (h2c), ALPN h2 for `https://`. Pooled per workload (`http_client.rs`, `http2_only(true)`).
  Any authority, no env var. Shared by the P2 and P3 egress paths.
- Inbound: `hyper_util::server::conn::auto::Builder` — serves h1 and h2c on the same port.
- Example `examples/grpc-hello-world`: component-client is **wstd 0.6 + tonic 0.14
  (`default-features = false, features = ["codegen"]`) + tonic-prost 0.14**, with a ~60-line
  `WasiGrpcService: tower_service::Service` that collects the tonic body and sends it with
  `wstd::http::Client`. Component-server serves gRPC through `wasi:http/incoming-handler`.
- Discussed on the 2026-01-07 community call (Lucas Fontes: "as long as the protocol version
  makes it to the Wasm interface, we're okay"); landed for v2.
- Open docs issue: https://github.com/wasmCloud/wasmCloud/issues/5650 (choosing WIT vs HTTP vs
  gRPC vs plugins).
  Sources: https://github.com/wasmCloud/wasmCloud/tree/main/examples/grpc-hello-world,
  https://wasmcloud.com/community/2026-01-07-community-meeting-transcript/

## Guest-side HTTP/2 (portable, needs only wasi:sockets)

- **tokio wasip2 networking:** https://github.com/tokio-rs/tokio/pull/7933 merged 2026-04-02,
  shipped in tokio 1.51 (latest 1.53.2, 2026-10-04). Needs `RUSTFLAGS="--cfg tokio_unstable"`.
  Builds on mio/socket2 wasip2 support. No `lookup_host` (DNS) yet.
  → In principle hyper's `client::conn::http2` over `tokio::net::TcpStream` now runs in a
  wasip2 guest on any runtime that grants `wasi:sockets`.
- **tonic `channel` on wasip2:** still fails to compile — tonic imports
  `tokio::net::UnixStream` on every non-Windows target. Fix PR
  https://github.com/grpc/grpc-rust/pull/2910 is OPEN (not merged as of 2026-10-10).
  Workaround: codegen-only tonic + own `tower::Service` over hyper http2 + tokio TcpStream.
- **anlaco/wasi-grpc:** https://github.com/anlaco/wasi-grpc — hand-written HTTP/2 + HPACK over
  raw `wasi:sockets` for wasmtime; unary only; created 2026-07-29, 0 stars, Apache-2.0, README
  in Spanish. Proof that the path works; not a dependency candidate.

## Standards / wasmtime

- wasi-http PR "Add incoming and outgoing handlers for HTTP/2"
  (https://github.com/WebAssembly/wasi-http/pull/129) was CLOSED unmerged 2025-11-25. No
  protocol-version selection in the WIT.
- wasmtime: no open issue for h2/h2c in the default `wasi-http` sender (search 2026-10-10).

## Other

- WASIX (Wasmer) has a gRPC tutorial (https://wasix.org/docs/language-guide/rust/tutorials/wasix-grpc)
  — WASIX is not WASI 0.2 / component model; off-target.
- gRPC-Web (`tonic-web-wasm-client`) — still not usable: daprd does not speak gRPC-Web.
