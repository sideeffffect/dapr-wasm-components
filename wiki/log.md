# Wiki Log

## [2026-10-13] update | wasi-grpc-outbound: wstd transport replaces wasi-grpc + spin-executor; tonic/prost 0.14 for both gRPC providers.
- Updated: dapr-wasm-components Architecture (gRPC stack)
- Updated: gRPC to Dapr from Wasm (outcome)

## [2026-10-13] update | who-is-who: fermyon/wasi-grpc + wstd (guest libs, alternatives) vs Spin + wasmCloud v2 (hosts, alternatives); gRPC both-ways matrix per host.
- Updated: gRPC to Dapr from Wasm

## [2026-10-13] update | opened draft upstream PR fermyon/wasi-grpc#18 (tonic 0.14, spin-executor 5, pluggable PollableRegistry).
- Updated: gRPC to Dapr from Wasm

## [2026-10-13] update | outbound fixes for daprd 1.17 (found by dapr4s's imports probe): protojson 64-bit ints are JSON strings (converse usage/index — wasi-http now accepts number or string); jobs is `/v1.0-alpha1` only on daprd ≤ 1.17 (`/v1.0` + stable gRPC `ScheduleJob` arrive in 1.18) — wasi-http and wasi-grpc now use the alpha1 path/RPCs; a timer on an unhosted actor type is a 500 `ERR_ACTOR_TIMER_CREATE` — all three outbound providers map it to `permission-denied` like daprd's 403 for reminders
- Updated: Dapr HTTP API (jobs version map corrected; two new traps)
- Updated: dapr-wasm-components Architecture (timer mapping next to the ERR_SECRET_GET precedent)

## [2026-10-12] update | wasi-grpc crate anatomy: client is a ~60-line tower::Service over wasi-hyperium (plain wasi:http) + spin-executor; HTTP/2 is all host-side, so the crate cannot lift the Spin limit. wstd 0.6.8 surfaces incoming trailers.
- Updated: gRPC to Dapr from Wasm (wider landscape section)

## [2026-10-10] update | streaming crypto (interfaces 0.6.0): sync WIT resources encryptor/decryptor/crypto-output; daprd v1.18 HTTP crypto buffers its output (`io.ReadAll`) and caps input at `--max-body-size`, gRPC streams; wasi-grpc provider buffers; early-rejection trap (wasmtime `HttpProtocolError`) → metadata pre-check; component-not-found mapping fixed
- Updated: dapr-wasm-components Architecture (streaming crypto decision)
- Updated: Dapr HTTP API (crypto streams in, not out; crypto error codes)

## [2026-10-10] update | workflow deprecation scope (HTTP only; gRPC uses non-deprecated `*WorkflowBeta1`) + actor-hosting workaround for gRPC apps (`--out grpc --in http`, daprd `--app-protocol http`; `SubscribeActorEventsAlpha1` rejected)
- Updated: dapr-wasm-components Architecture (two key decisions)

## [2026-10-10] update | pub/sub bulk subscribe (interfaces 0.6.0): `topic-subscription.bulk-subscribe` + `on-bulk-topic-event`, both inbound providers, SDK default loops over `on-topic-event`; wire shapes checked against Dapr docs, v1.18 protos and daprd release-1.18 source
- Updated: dapr-wasm-components Inbound Design (new *Bulk subscribe* section; AppCallback mapping; limitations)
- Updated: dapr-wasm-components Architecture (E2E scenario bulk step)

## [2026-10-10] update | pub/sub CEL routing rules: `topic-subscription.routes` (`topic-routes {rules: list<topic-rule {match, path}>, default}`), breaking, package `0.5.0` → `0.6.0`. wasi-http-inbound advertises rule paths under its reserved pub/sub prefix and recovers `path` from the delivery URL; wasi-grpc-inbound emits `TopicRoutes`. SDK builder `TopicSubscription::new(..).with_rule(..).with_default_route(..)`. Verified by composed tests + both real-daprd E2Es.
- Updated: dapr-wasm-components Inbound Design (new §Pub/sub routing rules; v1 limitations)
- Updated: dapr-wasm-components Architecture (package version)

## [2026-10-10] update | dapr-wasm-components Architecture (missing secret = `secret-not-found` on both providers: real daprd sends ERR_SECRET_GET for a store's "not found" — HTTP 500 / gRPC Internal — not a 204; providers now sniff "not found" in the appended store error, other ERR_SECRET_GET traps; real-Dapr scenario checks it with `secretstores.local.file`)
- Updated: Dapr HTTP API (204-vs-500 caveat for secrets get)

## [2026-10-10] ingest | daprd input-binding reply handling (v1.18 source + real-daprd E2E). daprd ignores the HTTP binding reply body entirely; over gRPC it honours only to/data/concurrency, never store_name/states. Field names emitted by wasi-http-inbound match the AppResponse JSON tags — no provider bug.
- Raw: daprd-input-binding-reply-v1-18
- Updated: Dapr HTTP API (trap: binding reply ignored)
- Updated: dapr-wasm-components Inbound Design (new section "Binding reply: what daprd really does"; refreshed the stale wasi-grpc inbound limitation)

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

## [2026-10-12] ingest | dapr-wasm-components Architecture (interfaces 0.7.0: authority split, subscription resource, typed workflow-instance, no workflow-component)
- Updated: Dapr HTTP API (workflow get response keys and status strings, daprd release-1.18/1.19 source)
- Updated: dapr-wasm-components Inbound Design (configuration subscription resource, package 0.7.0)

## [2026-10-12] ingest | dapr-wasm-components go Family (Dapr embedded)
- Updated: dapr-wasm-components Architecture (six providers, go family)
- Updated: dapr-wasm-components Inbound Design (See Also: pull-based inbound)

## [2026-10-13] update | go Family generated code
- Updated: dapr-wasm-components go Family (checked-in generated Go, Go generated-code header fix)

## [2026-10-14] update | go Family in-process app callbacks
- Updated: dapr-wasm-components go Family (inbound via Go calls in gRPC app-protocol mode; HTTP app channel removed; wiring, Consul APP_PORT constraint, gaps)

## [2026-10-15] update | One OCI name for the interface package
- Updated: dapr-wasm-components Architecture (interface published only as `dapr-wasm-components/interfaces`, `latest` + semver; flat name is legacy)
