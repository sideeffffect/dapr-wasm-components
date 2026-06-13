# Roadmap / follow-ups

Tracked follow-up work for `dapr-wasm-components`, as of the two-typed-directions
redesign (package `dapr-wasm-components:interfaces@0.2.0`). Grouped by area; each
item notes *why* it's open and roughly *what* it needs. See
[wiki/dapr/dapr-wasm-components-architecture.md](wiki/dapr/dapr-wasm-components-architecture.md)
for the design rationale and [wiki/dapr/dapr-wasm-components-inbound-design.md](wiki/dapr/dapr-wasm-components-inbound-design.md)
for the inbound spec.

## Inbound (Dapr → app) completeness

- **Automated daprd `--app-protocol grpc` E2E for `wasi-grpc-inbound`.** The gRPC
  `AppCallback` server is verified over Spin's inbound h2c with `grpcurl`
  (`ListTopicSubscriptions`, `HealthCheck`), but there is no *standing* CI test
  that drives it through a real daprd configured for the gRPC app channel. Both
  shipping E2E suites use the HTTP inbound provider. Needs: a `--app-protocol`
  field on the test `DaprdConfig`, and a scenario variant that drives the app
  *through* daprd's invoke API (since the app no longer speaks HTTP directly when
  the app channel is gRPC).
- **Input-binding response field names.** `bindings-callback.on-binding-event` can
  ask the sidecar to persist state / forward to output bindings; the JSON shape
  the `wasi-http-inbound` provider emits follows the bindings API reference but is
  not yet integration-tested against a real input-binding component.
- **Pub/sub routing rules and bulk subscribe.** `pubsub-callback` models one route
  per topic (dispatch on `pubsub-name` + `topic`). Dapr's CEL routing rules
  (`TopicRoutes`) and bulk subscribe (`OnBulkTopicEvent`) are not exposed.

## gRPC provider

- **Known limitation — service-invocation status over gRPC.** Dapr's gRPC
  `InvokeResponse` cannot carry an HTTP status code, so `wasi-grpc-inbound` maps a
  non-2xx app response to a gRPC error, and `wasi-grpc-outbound` surfaces a
  non-2xx target as an error (success is always 200). Inherent to the gRPC API.
- **Outbound integration coverage.** Most `wasi-grpc-outbound` interfaces are
  compile-checked; only the E2E surface (state, service invocation, pub/sub,
  metadata) is integration-tested. The provider runs only on Spin ≥ 3.4 (outbound
  h2c) and its h2c allowlist holds a single authority.

## Outbound / API coverage

- **Conversation** models the alpha2 *text* subset — no tool calling yet.
- **Crypto** is one-shot (no streaming encrypt/decrypt).
- **Workflow** uses the `/v1.0` HTTP API, which Dapr documents as deprecated in
  favour of the SDKs.

## Packaging / DX

- **Publish the `dapr-app` SDK.** It is consumed today as a git/path dependency;
  publishing it (crates.io or a stable git tag) would make app authoring smoother.

## Recently shipped

- **Composition ergonomics.** The [`compose.sh`](compose.sh) wrapper turns the
  three-dependency `wac compose` into a one-liner: it resolves the outbound and
  inbound providers (a local `components/target/` release build when present,
  otherwise an OCI pull via `wkg`), picks transports independently (`--out`/`--in`,
  http or grpc), and runs `wac` against `compose.wac`. Verified to produce a valid
  `wasi:http/incoming-handler` server for all three transport combinations.
- **CloudEvent extensions over gRPC.** `wasi-grpc-inbound` now maps
  `TopicEventRequest.extensions` (a `google.protobuf.Struct`) into
  `topic-event.extensions`, stringifying each value the same way the HTTP
  provider does (string verbatim, otherwise JSON text) — implemented JSON-dep-free
  to keep the published component lean. Compile-checked (the gRPC provider's
  standing posture); exercised opportunistically by the spin E2E.
- **Configuration-update delivery (inbound).** The `wasi-http-inbound` router now
  recognises the sidecar's config-update push (`POST /configuration/<store>/<key>`
  carrying Dapr's `UpdateEvent` — `items` is a map keyed by configuration key) and
  dispatches it to `configuration-callback.on-configuration-event`. Verified by a
  composed in-process test (`e2e/tests/composed.rs`
  `inbound_configuration_update_is_delivered`), driven through the real inbound
  handler via the new `serve_inbound` harness helper (`e2e/src/lib.rs`) — which
  also unlocks in-process testing of the other inbound flows.

## Standing design constraints (not TODOs)

- **Actor *hosting* is HTTP app-channel only.** Dapr's gRPC `AppCallback` service
  has no actor methods, so `actors-callback` is reachable only via the
  `wasi-http-inbound` provider. (Actor *client* calls work over either transport
  through the outbound `actors` interface.)
- **Outbound and inbound are separate components** because a single bidirectional
  provider forms an `app ↔ provider` instantiation cycle the component model
  forbids. This is by design, not a limitation to remove.
