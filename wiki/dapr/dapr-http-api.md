# Dapr HTTP API (sidecar REST)

> Sources: docs.dapr.io API reference (v1.18), 2026-06-11 survey; dapr/dapr v1.18.0 source `pkg/api/http/crypto.go` (2026-10-10); dapr/dapr release-1.18/1.19 workflow get source (2026-10-12); dapr/dapr v1.14.0–v1.18.7 `pkg/api/http/jobs.go`, v1.18.7 `pkg/actors/timers/timers.go` + `pkg/messages/predefined.go`, and live daprd 1.17.7 answers (2026-10-13)
> Raw: [dapr-http-api-reference-v1-18](../../raw/dapr/dapr-http-api-reference-v1-18.md), [daprd-input-binding-reply-v1-18](../../raw/dapr/daprd-input-binding-reply-v1-18.md), [daprd-workflow-get-properties-v1-18](../../raw/dapr/daprd-workflow-get-properties-v1-18.md)

## Overview

The sidecar serves a REST API on `http://localhost:3500` (default). This is what the `wasi-http-outbound` provider (`components/wasi-http-outbound/`) implements against; the `wasi-http-inbound` provider serves the reverse direction (the sidecar's HTTP app channel). The raw file holds the full endpoint-by-endpoint reference with exact field names; this article captures the version map and the traps.

## Version prefix map (v1.18)

| Building block | Prefix |
|---|---|
| state, pubsub (incl. **bulk publish**), bindings, secrets, configuration, invocation, actors, metadata, health, workflow | `/v1.0` |
| **jobs** | `/v1.0-alpha1` (1.14 – 1.18); 1.18 adds `/v1.0` and keeps `-alpha1` as a deprecated alias |
| state query, distributed lock, crypto | `/v1.0-alpha1` |
| conversation | `/v1.0-alpha2` (alpha1 deprecated) |

Bulk publish was alpha pre-1.16 — older sidecars need the `-alpha1` prefix. **Jobs is `/v1.0-alpha1` only on daprd ≤ 1.17** (checked in `pkg/api/http/jobs.go` of v1.14.0 – v1.17.16; the `/v1.0` routes and the stable gRPC `ScheduleJob`/`GetJob`/`DeleteJob` arrive in 1.18.0). On 1.17, `/v1.0/jobs/<name>` falls through to service invocation: `404 ERR_DIRECT_INVOKE` ("failed getting app id …"), which a `GET` would misread as "job not found". Both outbound providers therefore call the `-alpha1` path / `*JobAlpha1` RPCs. The workflow HTTP API is `/v1.0` but **deprecated** in favor of SDKs.

## Dapr 1.19 delta (v1.19.0-rc.2, checked 2026-10-10)

The changes are additive only. The `AppCallback` proto is byte-identical to 1.18; it has no actor methods and no new RPCs.

- **Actors:** new `GET /v1.0/actors/{type}/{id}/timers` and `.../timers/{name}`, with matching gRPC `ListActorTimers` and `GetActorTimer` (`actors.proto`).
- **Workflow:** an optional `?appID=` query parameter targets a workflow in another app. When the workflow FAILED, `properties` carry `dapr.workflow.failure.{error_type,error_message,stack_trace}`. The HTTP API is still deprecated but still served.
- **Jobs:** `data` is optional, and `schedule` accepts a `CRON_TZ=`/`TZ=` prefix.
- **Conversation:** still `/v1.0-alpha2`, with a new optional `max_tokens` field.
- **Unchanged:** crypto (still alpha; the HTTP handler still buffers its output, see below), bulk subscribe, routing rules, the input-binding response shape, and all alpha/beta statuses.
- **1.18.x patches** fixed bugs only: 1.18.6 strips a spoofed `Dapr-Caller-App-Id`, and 1.18.7 fixes concurrent requests on the HTTP app channel.

## Traps and asymmetries

- **204 means "not found"** on state get, secrets get, and actor state get — not an error. But 204 is also the *success* code for writes. Caveat for secrets: daprd only returns 204 when the store returns *no data*; most stores (e.g. `local.file`) report a missing secret as an error, which daprd turns into `500 ERR_SECRET_GET` (`failed getting secret with key <key> from secret store <store>: <store error>`).
- **64-bit integers are JSON strings** in every response daprd renders with protojson (the "universal" handlers: conversation, jobs, …): proto `int64`/`uint64` → `"4"`, per the proto3 JSON mapping. daprd 1.17.7's echo-component converse answer is `"usage":{"completionTokens":"2","promptTokens":"2","totalTokens":"4"}`, and a non-zero choice `index` is a string too. Parse such fields as "number or string" (`wasi-http-outbound` does since 2026-10-13; before, `converse` trapped on any answer with usage).
- **A timer for an actor type the app does not host is a 500**: `ERR_ACTOR_TIMER_CREATE`, `"error creating actor timer: can't create timer for actor <type>||<id>: actor type not registered"` (gRPC: `Internal`). The same condition for reminders is a **403** `ERR_ACTOR_REMINDER_NON_HOSTED` (gRPC: `PermissionDenied`), and unregistering a timer never fails. All providers map the timer case to `actors-error.permission-denied`, like the reminder case; any other `ERR_ACTOR_TIMER_CREATE` still traps.
- Bulk state get items carry the value under **`value`**; state *query* results use **`data`**.
- Delete etag goes in the **`If-Match` header**, not the body/query.
- Bulk publish entry id is **`entryId`**; partial failure = HTTP 500 + `failedEntries` array.
- Jobs schedule body is flat (no `{"job":...}` wrapper) and uses **snake_case** for `failure_policy`/`max_retries` while everything else is camelCase.
- Workflow start's query param is **`instanceID`** (capital ID); workflow component name must be `dapr`.
- Workflow get (`GET /v1.0/workflows/dapr/{instanceID}`) returns `instanceID`, `workflowName`, `createdAt`, `lastUpdatedAt`, `runtimeStatus` (one of `RUNNING, COMPLETED, CONTINUED_AS_NEW, FAILED, CANCELED, TERMINATED, PENDING, SUSPENDED`, else `UNKNOWN`) and a string map `properties` with `dapr.workflow.input`, `dapr.workflow.output`, `dapr.workflow.custom_status`, `dapr.workflow.failure.*` (each only when set). Source: [daprd workflow GetWorkflow properties](../../raw/dapr/daprd-workflow-get-properties-v1-18.md).
- Conversation alpha2 wraps messages in role keys (`ofUser`, `ofSystem`, …) with `content: [{"text": ...}]`; alpha1 used flat `{content, role, scrubPII}`.
- Crypto uses **PUT** with `dapr-key-name`/`dapr-key-wrap-algorithm` headers and raw octet-stream bodies.
- **Crypto streams in, not out** (v1.18.0 source, [`pkg/api/http/crypto.go`](https://github.com/dapr/dapr/blob/v1.18.0/pkg/api/http/crypto.go)): the request body is piped into the encryption, but the handler `io.ReadAll`s the whole output before it answers, and `MaxBodySizeMiddleware` caps the input at `--max-body-size` (default 4Mi). An unknown component is a **400 `ERR_CRYPTO_PROVIDER_NOT_FOUND`**, sent *before* the body is read. A missing key (`ERR_CRYPTO_KEY`) and a failed operation (`ERR_CRYPTO`) are **500**. The gRPC `EncryptAlpha1`/`DecryptAlpha1` stream both ways.
- Actor state transaction body is a bare array, unlike the state-store transaction's `{"operations": ...}` wrapper.
- Service invocation passes the target's status/body through verbatim — a 4xx/5xx from the target is a response, not a sidecar error.
- **Input-binding reply is documented but ignored over HTTP.** The bindings reference says the app can answer `POST /<binding>` with `{"storeName", "state", "to", "data", "concurrency"}` (these are the `bindings.AppResponse` JSON tags; `concurrency` is `parallel`/`sequential`). daprd 1.18 never parses the HTTP reply body, so none of it has an effect. Over gRPC (`OnBindingEvent`) daprd honours `to` + `data` + `concurrency` only; `store_name`/`states` are ignored. Both paths worked in daprd 1.0 and were lost by 1.5. Verified in source and empirically (see [Inbound Design](dapr-wasm-components-inbound-design.md#binding-reply-what-daprd-really-does)).
- Per-call component metadata = `?metadata.<field>=<value>` query params; API auth = `dapr-api-token` header.

## See Also

- [dapr-wasm-components Architecture](dapr-wasm-components-architecture.md)
- [gRPC to Dapr from Wasm](dapr-grpc-from-wasm.md) (the gRPC counterpart to this REST API)
- [Dapr Rust SDK](dapr-rust-sdk.md)
- [Inbound Design](dapr-wasm-components-inbound-design.md) (the HTTP app channel, sidecar → app)
