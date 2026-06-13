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
- **Configuration-update delivery.** `configuration-callback` (the inbound side of
  `configuration.subscribe`) is defined in WIT but the `wasi-http-inbound` router
  does not yet recognise the sidecar's config-update POST and dispatch it to
  `on-configuration-event`. Needs the route + the JSON→typed mapping. (Over gRPC,
  config updates are a server stream on the `Dapr` service, not an `AppCallback`
  flow, so the `wasi-grpc-inbound` provider deliberately does not carry them.)
- **Input-binding response field names.** `bindings-callback.on-binding-event` can
  ask the sidecar to persist state / forward to output bindings; the JSON shape
  the `wasi-http-inbound` provider emits follows the bindings API reference but is
  not yet integration-tested against a real input-binding component.
- **Pub/sub routing rules and bulk subscribe.** `pubsub-callback` models one route
  per topic (dispatch on `pubsub-name` + `topic`). Dapr's CEL routing rules
  (`TopicRoutes`) and bulk subscribe (`OnBulkTopicEvent`) are not exposed.
- **CloudEvent extensions over gRPC.** The `wasi-http-inbound` provider surfaces
  CloudEvent extension attributes in `topic-event.extensions`; the
  `wasi-grpc-inbound` provider currently passes them empty (the proto carries them
  as a `google.protobuf.Struct`, not yet mapped).

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
- **Composition ergonomics.** Composition uses the repo's [`compose.wac`](compose.wac)
  (`outbound → app → inbound`, acyclic). A thin wrapper or template could make the
  three-dependency `wac compose` invocation a one-liner.

## Standing design constraints (not TODOs)

- **Actor *hosting* is HTTP app-channel only.** Dapr's gRPC `AppCallback` service
  has no actor methods, so `actors-callback` is reachable only via the
  `wasi-http-inbound` provider. (Actor *client* calls work over either transport
  through the outbound `actors` interface.)
- **Outbound and inbound are separate components** because a single bidirectional
  provider forms an `app ↔ provider` instantiation cycle the component model
  forbids. This is by design, not a limitation to remove.
