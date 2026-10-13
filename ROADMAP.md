# Roadmap / follow-ups

Tracked follow-up work for `dapr-wasm-components`, current as of package
`dapr-wasm-components:interfaces@0.7.2`. Grouped by area; each item notes *why*
it's open and roughly *what* it needs. See
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

- **Crypto error mapping.** daprd reports a missing key (`ERR_CRYPTO_KEY`) and a
  failed decryption/unwrap (`ERR_CRYPTO`) as HTTP 500 / gRPC `Internal`, so both
  providers trap instead of returning `key-not-found` / `malformed-ciphertext`.
  Needs: a crypto-specific classifier that maps these error codes before the
  generic 5xx trap (HTTP: `errorCode`; gRPC: the `ErrorInfo` reason).
- **True streaming crypto over gRPC.** `wasi-grpc-outbound`'s `encryptor` /
  `decryptor` buffer the whole input and output, because one tonic bidi call
  cannot stay in flight across several export calls (`spin_executor::run` per
  call). daprd's gRPC handlers stream for real, so a provider that keeps the
  call alive (or WASI 0.3) would make this O(chunk).
- **Workflow** — `wasi-http-outbound` uses the `/v1.0` HTTP API, which Dapr
  documents as deprecated in favour of the SDKs. Dapr offers no non-deprecated
  HTTP replacement, and 1.19 still serves it. The deprecation affects only the
  HTTP provider: `wasi-grpc-outbound` calls the `*WorkflowBeta1` RPCs, which carry
  no `deprecated` option in the vendored v1.18 protos (`dapr.proto`; only the
  `*WorkflowAlpha1` set is deprecated).

- **Real-Dapr coverage for the 0.7.0 shapes.** The configuration
  `subscription` resource (subscribe → update push → drop → unsubscribe) and
  the typed `workflow-instance` are tested against the mock sidecar only. A
  real-Dapr check needs a configuration store component (e.g. Redis) and, for
  workflows, a workflow worker plus placement/scheduler — and authoring is
  out of scope until WASI 0.3 (dapr4s proposal W5).
- **Configuration subscribe over gRPC.** `wasi-grpc-outbound` still traps on
  `subscribe` (daprd streams updates on the `Dapr` service, not the app
  channel). The 0.7.0 resource shape would let a provider hold that stream
  per handle once WASI 0.3 allows it.

## go family (Dapr embedded)

- **Slimmer component set.** go-outbound is ~264 MB (Dapr runtime ~138 MB + all
  wasip1-buildable component SDKs). Offer a slim build (e.g. Redis, Postgres,
  Kafka, in-memory, local file/env secrets, Consul) via a second
  `gen-components.py` list.
- **Actors / workflow / jobs E2E.** Implemented and compiled, but the shared
  scenario does not run placement/scheduler; add them as testcontainers.
- **Upstream fixes** to drop local workarounds: wit-bindgen-go variant-case vs
  record name clash and `FromOwnHandle` not recording the handle
  (`scripts/fix-bindings.py`); componentize-go / Go `netpoll(0)` calling
  `poll_oneoff` from a GC inside `cabi_realloc` (`overlay/`); a Go wasip1 `net`
  backed by `wasi:sockets` instead of the std overlay.
- **Event-driven socket waits** (fold wasi:io pollables into Go's netpoll)
  instead of sleep-backoff polling, if latency matters.

## Packaging / DX

- **Publish the `dapr-app` SDK.** It is consumed today as a git/path dependency;
  publishing it (crates.io or a stable git tag) would make app authoring smoother.

## Recently shipped

- **Interfaces 0.7.0 (breaking) — harmony with dapr4s** (the
  [dapr4s proposal](https://github.com/sideeffffect/dapr4s/blob/master/docs/WASM-COMPONENTS-PROPOSAL.md),
  items W1–W4). **W1:** blocks with a read-only subset are split by
  authority: `state-read`/`state`, `secrets-read`/`secrets` (bulk),
  `configuration-read`/`configuration` (subscribe), `jobs-read`/`jobs`,
  `workflow-read`/`workflow`; `wit-component` keeps only called functions, so
  the import list is the authority manifest (checked by
  `e2e/tests/authority.rs` on the new `state-reader` fixture; type-only
  imports remain for interfaces the callbacks use). **W2:** `subscribe`
  returns a `subscription` resource; dropping it unsubscribes (a failed
  unsubscribe is logged, not trapped); the free `unsubscribe` is gone.
  **W3:** `workflow-instance` is typed (`workflow-name`, `runtime-status`
  enum incl. `canceled` and `unknown`, `input`, `output`, `failure`).
  **W4:** no `workflow-component` parameter (always `dapr`).

- **Interfaces 0.6.0 — streaming crypto (breaking).** `crypto` gained sync WIT
  resources `encryptor` / `decryptor` (constructor, `write(chunk)`,
  `finish` → `crypto-output`) and `crypto-output.read(max-len)`; the one-shot
  `encrypt`/`decrypt` stay. Write-all-then-read by construction (`finish`
  consumes the writer). `wasi-http-outbound` streams both HTTP bodies over raw
  `wasi:http` 0.2 handles kept in the resource; daprd v1.18 itself buffers the
  output and caps the HTTP input at `--max-body-size` (default 4Mi).
  **gRPC caveat:** `wasi-grpc-outbound` buffers in provider memory and runs one
  RPC at `finish` (now sent as 1 MiB `seq`-numbered messages, so > 4Mi works).
  `dapr-app` adds `crypto_io` (`std::io` adapters). Both providers now map
  daprd's `ERR_CRYPTO_PROVIDER_NOT_FOUND` to `component-not-found`. Covered by
  mock tests (4 MiB roundtrip, early rejection, composed kv-demo 3 MiB) and by
  both real-Dapr E2Es (`crypto.dapr.localstorage`, 3 MiB).

- **Interfaces 0.6.0 — pub/sub bulk subscribe.** `topic-subscription` gained an
  optional `bulk-subscribe: bulk-subscribe-config { enabled, max-messages-count,
  max-await-duration-ms }`, and `pubsub-callback` a new
  `on-bulk-topic-event(bulk-topic-event) -> list<bulk-topic-event-entry-response>`
  (each entry is a parsed CloudEvent or a raw payload; per-entry statuses reuse
  `topic-event-response`). `wasi-http-inbound` advertises `bulkSubscribe` on
  `/dapr/subscribe` with a separate bulk route and answers `{ statuses }`;
  `wasi-grpc-inbound` sets `TopicSubscription.bulk_subscribe` and serves
  `OnBulkTopicEvent` (+ the deprecated `OnBulkTopicEventAlpha1` fallback). The
  `dapr-app` SDK's default `on_bulk_topic_event` loops over `on_topic_event`
  (`bulk_to_single`), so existing apps need no change; `TopicSubscription::new(..)
  .with_bulk_subscribe(..)` opts in. Verified by composed in-process tests and by
  a bulk-subscribed `orders-bulk` topic in the shared real-Dapr scenario (both
  suites; the gRPC `OnBulkTopicEvent` server path is compile-checked, like the
  rest of `wasi-grpc-inbound`).

- **Interfaces 0.6.0 — pub/sub CEL routing rules (breaking record change).**
  `topic-subscription` gained `routes: option<topic-routes>` (`rules: list<topic-rule
  {match, path}>`, `default: option<string>`), faithful to the HTTP API's
  `routes: {rules, default}` and gRPC `TopicRoutes`. daprd evaluates the CEL
  rules and the matched path arrives in `topic-event.path`. `wasi-http-inbound`
  advertises rule paths under its reserved pub/sub prefix and recovers the path
  from the delivery URL; `wasi-grpc-inbound` emits `TopicRoutes` verbatim. The
  `dapr-app` SDK adds `TopicSubscription::new(..).with_rule(..).with_default_route(..)`.
  Verified by composed in-process tests and by both real-daprd E2Es (a priority
  order routed by a CEL rule on `event.data`).

- **Input-binding E2E + reply check.** Both real-Dapr suites now drive a
  `bindings.cron` input binding (`ticks`, `@every 1s`) into the consumer's
  `on-binding-event`. The reply field names (`storeName`, `state`, `to`,
  `data`, `concurrency`) were checked against the docs and the daprd /
  components-contrib source: they are correct, so no provider fix was needed.
  But **daprd 1.18 ignores the HTTP binding reply body**, and over gRPC it applies
  only `to` + `data` + `concurrency` (never `store_name`/`states`). The E2E
  pins this (a `storeName`/`state` reply is not persisted); a composed
  in-process test pins the reply shape. Details:
  [Inbound Design](wiki/dapr/dapr-wasm-components-inbound-design.md#binding-reply-what-daprd-really-does).
- **Interfaces 0.5.0 — error-model redesign (breaking).** Dropped the single
  shared 7-case `error` variant that every operation returned. Failures are now
  sorted by who acts on them: the success type is the happy path only (a missing
  value is an error case such as `state.get` → `get-error::key-not-found`, never an
  `option`), recoverable failures are per-interface + per-function
  `variant`s (e.g. `state.save` → `write-error { etag-mismatch, state }`), and
  unrecoverable/infra failures (sidecar down, 5xx, timeouts, I/O) are no longer
  typed at all — the provider traps. Inbound callbacks use `types.app-error`.
  Modelled on wasi-keyvalue. Mirrored across all four providers (outbound +
  inbound, HTTP + gRPC), the `dapr-app` SDK, and the e2e mock/tests.
- **Interfaces 0.4.0.** The WIT package was tightened across 0.3.0 → 0.4.0 to
  follow the Dapr HTTP API faithfully, the provider worlds were renamed
  `dapr-outbound`/`dapr-inbound` → `outbound`/`inbound`, and the published OCI
  interface artifact was renamed `…-interface` → `…-interfaces`.
- **Full alpha2 conversation.** The `conversation` interface now models the whole
  `/v1.0-alpha2` converse surface — content-part messages with role wrappers,
  tool definitions and tool calls, structured-output schema, and prompt-cache —
  not just the plain-text subset. Wired in both the `wasi-http-outbound` (JSON)
  and `wasi-grpc-outbound` (protobuf) providers.
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
  to keep the published component lean. Compile-checked only (the gRPC provider's
  standing posture): the spin E2E composes the wasi-http **inbound** provider, so
  it does not exercise this path (see the gRPC app-channel E2E item above).
- **Configuration-update delivery (inbound).** The `wasi-http-inbound` router now
  recognises the sidecar's config-update push (`POST /configuration/<store>/<key>`
  carrying Dapr's `UpdateEvent` — `items` is a map keyed by configuration key) and
  dispatches it to `configuration-callback.on-configuration-event`. Verified by a
  composed in-process test (`e2e/tests/composed.rs`
  `inbound_configuration_update_is_delivered`), driven through the real inbound
  handler via the new `serve_inbound` harness helper (`e2e/src/lib.rs`) — which
  also unlocks in-process testing of the other inbound flows.

## External blockers — last checked 2026-10-10

These are the upstream changes that would unblock items above. None of them has landed yet. Details: [wiki/dapr/dapr-grpc-from-wasm.md](wiki/dapr/dapr-grpc-from-wasm.md#upstream-status-re-check-2026-10-10) and [wiki/dapr/dapr-http-api.md](wiki/dapr/dapr-http-api.md#dapr-119-delta-v1190-rc2-checked-2026-10-10).

- **gRPC outside Spin / multi-authority h2c.** Wasmtime (up to 50-rc) still has HTTP/1-only outbound `wasi-http`. Spin 4.2 still uses the single-authority `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE`. `wasi-grpc` is still 0.1.0 (tonic 0.13). The only path we control is guest-side `h2` over `wasi:sockets`.
- **Actor hosting over gRPC.** Dapr 1.19 `AppCallback` still has no actor methods. Workaround: gRPC outbound + HTTP inbound (see *Standing design constraints*).
- **Streaming crypto.** Shipped in 0.6.0 with sync WIT resources (no WIT `stream` needed). Remaining upstream limits: daprd's HTTP crypto handler still buffers the whole output and caps the input at `--max-body-size`; true streaming over gRPC needs a call that survives across export calls (or WASI 0.3).
- **Workflow.** The `/v1.0` HTTP API is still deprecated but served in 1.19 (HTTP provider only; the gRPC provider uses the non-deprecated `*WorkflowBeta1` RPCs).

Additive Dapr 1.19 surface we could adopt (1.19 is still an RC): actor timer reads (`ListActorTimers` / `GetActorTimer`), conversation `max_tokens`, optional job `data`, and workflow `appID` targeting.

## Standing design constraints (not TODOs)

- **Actor *hosting* is HTTP app-channel only.** Dapr's gRPC `AppCallback` service
  has no actor methods, so `actors-callback` is reachable only via the
  `wasi-http-inbound` provider. (Actor *client* calls work over either transport
  through the outbound `actors` interface.) **Workaround for gRPC apps:** compose
  `wasi-grpc-outbound` + `wasi-http-inbound` (`./compose.sh my_app.wasm --out grpc
  --in http`) and run daprd with `--app-protocol http` — outbound goes over gRPC,
  actor hosting over the HTTP app channel. The gRPC-native path,
  `SubscribeActorEventsAlpha1` (an app-initiated bidi stream, `actors.proto`
  ~line 172), was rejected: it needs a long-lived bidi stream that request-scoped
  wasm/Spin HTTP triggers cannot hold, and it is alpha.
- **go family: one long-lived app instance.** A delivery cannot reach the app
  while it runs a callback, so self-invocation fails fast (`app-unreachable`)
  and A → B → A cycles hang without a resiliency timeout. Inherent to a single
  instance plus a sync WIT; per-request instances (the sidecar families) don't
  have it.
- **Outbound and inbound are separate components** because a single bidirectional
  provider forms an `app ↔ provider` instantiation cycle the component model
  forbids. This is by design, not a limitation to remove.
