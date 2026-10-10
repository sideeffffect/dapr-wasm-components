# Wiki Log

## [2026-10-10] lint | 30+ issues found, all but 1 auto-fixed
- Links: README self-anchor (`#two-directions-…--both-typed`), dead Dapr gRPC API doc link, moved dapr4s `DaprApp.scala` link.
- Drift vs code: Architecture + Inbound Design (split providers, `@0.5.0`, five OCI packages, four workspaces, inbound status now implemented, gRPC error mapping per 0.5.0 `classify`); old provider names/paths and v1 host framing in the other eight articles; See Also cross-refs expanded.
- Outside wiki: README, ROADMAP, AGENTS.md, proto/README.md (wasi-grpc-inbound exists, spin E2E uses wasi-http-inbound, 0.5.0 "no option in success" wording).
- Reported, not fixed: wasi-grpc-outbound `get-secret` — a missing secret returns gRPC `Internal`, which `classify` treats as a trap, contrary to the `secrets.rs` comment.

## [2026-10-10] ingest | wider wasm gRPC landscape: Spin active (4.2.2, CNCF, Akamai) but fermyon/wasi-grpc stale; wasmCloud v2 routes `application/grpc` wasi:http egress over h2/h2c automatically (wstd + tonic 0.14 codegen example); tokio 1.51 wasip2 networking; tonic channel still blocked (grpc-rust#2910); wasi-http HTTP/2 PR closed.
- Updated: gRPC to Dapr from Wasm (wider landscape section)

## [2026-10-10] update | dependency bump: wit-bindgen 0.62, wasmtime 49, wac-graph 0.12, ureq 3, CI wasmtime 49.0.2 / Spin v4.2.2 / wkg 0.16.1 / wasm-tools 1.261.0, daprd 1.18.7. tonic/prost 0.13 + spin-executor 4 are held by wasi-grpc 0.1.0.
- Updated: Wasmtime Host Embedding (wasi-http 49 import move)
- Updated: wasm-pkg-tools (wkg) (`wkg wit build` deprecated)
- Updated: dapr-wasm-components Architecture (Spin pin, wkg build)

## [2026-10-10] update | upstream re-check: Spin 4.2.2 / wasmtime 49-50 / wasi-grpc / WASI 0.3.1 / Dapr 1.19.0-rc.2. No roadmap blocker removed.
- Updated: gRPC to Dapr from Wasm (upstream status section)
- Updated: Wasmtime Host Embedding (WASI 0.3 status)
- Updated: Dapr HTTP API (1.19 delta)

## [2026-06-15] update | dapr-wasm-components Architecture (error-model redesign: removed the single shared `error` variant; recoverable failures are now per-interface tier-2 + per-function tier-1 `variant`s, the success type holds ONLY the happy path — expected failures like key-not-found are error cases, NOT `option` in the success slot (the six `result<option<…>>` gets + lock's bool/unlock-status were all converted), and unrecoverable/infra failures trap instead of being typed — modelled on wasi-keyvalue; inbound callbacks use `types.app-error`; breaking, bumped package `0.4.0` → `0.5.0`; mirrored across both providers, both inbound providers, the dapr-app SDK, and the e2e mock/tests)

## [2026-06-14] update | dapr-wasm-components Architecture (world rename: provider worlds `dapr-outbound`/`dapr-inbound` → bare `outbound`/`inbound`; `app` kept; `dapr-` prefix dropped as redundant inside the `dapr-wasm-components:interfaces` package and now matches `compose.wac`'s `dapr:outbound`/`dapr:inbound` aliases; bindgen struct `DaprOutbound` → `Outbound`; breaking, bumped package `0.3.0` → `0.4.0`, released as v0.4.0; the 0.3.0 API-faithfulness pass is released separately as v0.3.0)

## [2026-06-13] new | dapr-wasm-components Inbound Design (typed Dapr→app inbound — seven *-callback interfaces + the dapr-app SDK, built and merged; worlds app/dapr-outbound/dapr-inbound; providers split outbound+inbound to break the app↔provider compose cycle; supersedes "outbound only")
- Supersedes: dapr-wasm-components Architecture (§"Outbound only" decision)

## [2026-06-13] lint | 4 issues found, 3 auto-fixed

## [2026-06-13] update | dapr-wasm-components Architecture (inbound-path proof: provider is outbound-only, host's wasi:http delivers inbound to the app — name collision wasi:http-interface vs wasi-http-provider; gRPC-inbound feasibility: Spin accepts h2c inbound but wasi-grpc is client-only so AppCallback is net-new; E2E mirroring: one microservice app two instances, shared run_mirrored_scenario, wasi-http⇄wasi-grpc differ only in provider+runtime)

## [2026-06-13] update | dapr-wasm-components Architecture (repo restructure: components/ = only the 3 published artifacts, demo apps → e2e/apps wasm-only workspace; WIT worlds imports→dapr-client, provider→dapr-server; README two-directions explanation; shared e2e test scaffolding in tests/common)

## [2026-06-12] ingest | dapr-wasm-components Architecture (wasi-grpc provider PoC: tonic+wasi-grpc+spin-executor stack, h2c exact-match trap, spin trigger subprocess trap, ephemeral-port trap, gRPC semantic gaps)
- Updated: gRPC to Dapr from Wasm (outcome: Spin-only PoC shipped)

## [2026-06-12] ingest | gRPC to Dapr from Wasm (feasibility: HTTP/2-from-wasm crux, wasi:sockets+h2, gRPC-Web rejected)
- Updated: dapr-wasm-components Architecture (cross-link to gRPC variant)

## [2026-06-12] update | gRPC to Dapr from Wasm (why Fermyon wasi-grpc crate is Spin-locked, not a shortcut)

## [2026-06-11] ingest | Component Model Overview
- Updated: WIT Format

## [2026-06-11] ingest | Dapr Rust SDK

## [2026-06-11] ingest | Dapr Rust SDK (API survey merge)

## [2026-06-11] ingest | wasm-pkg-tools (wkg)

## [2026-06-11] ingest | Wasmtime Host Embedding
- Updated: Component Model Overview
- Updated: WIT Format
- Updated: wasm-pkg-tools (wkg)

## [2026-06-11] ingest | Dapr × Wasm Prior Art

## [2026-06-11] ingest | dapr-wasm-components Architecture
- Updated: Wasmtime Host Embedding
- Updated: wasm-pkg-tools (wkg)

## [2026-06-12] ingest | dapr-wasm-components Architecture (real-Dapr E2E: daprd flags, sqlite nameres, pipe-drain trap)

## [2026-06-11] ingest | Dapr HTTP API
- Updated: dapr-wasm-components Architecture (v2 redesign: pure-wasm wasi:http implementation)
- Updated: Dapr Rust SDK (no longer used by this project)
- Updated: Wasmtime Host Embedding (wasi-http views, wstd, wac-graph)
- Updated: wasm-pkg-tools (wkg) (oci push vs publish, latest tags)
