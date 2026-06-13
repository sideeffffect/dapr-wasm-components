# Dapr HTTP API (sidecar REST)

> Sources: docs.dapr.io API reference (v1.18), 2026-06-11 survey
> Raw: [dapr-http-api-reference-v1-18](../../raw/dapr/dapr-http-api-reference-v1-18.md)

## Overview

The sidecar serves a REST API on `http://localhost:3500` (default). This is what dapr-wasm-components-wasi-http implements against. The raw file holds the full endpoint-by-endpoint reference with exact field names; this article captures the version map and the traps.

## Version prefix map (v1.18)

| Building block | Prefix |
|---|---|
| state, pubsub (incl. **bulk publish**), bindings, secrets, configuration, invocation, actors, metadata, health, **jobs**, workflow | `/v1.0` |
| state query, distributed lock, crypto | `/v1.0-alpha1` |
| conversation | `/v1.0-alpha2` (alpha1 deprecated) |

Bulk publish and jobs were alpha pre-1.16 — older sidecars need the `-alpha1` prefixes. The workflow HTTP API is `/v1.0` but **deprecated** in favor of SDKs.

## Traps and asymmetries

- **204 means "not found"** on state get, secrets get, and actor state get — not an error. But 204 is also the *success* code for writes.
- Bulk state get items carry the value under **`value`**; state *query* results use **`data`**.
- Delete etag goes in the **`If-Match` header**, not the body/query.
- Bulk publish entry id is **`entryId`**; partial failure = HTTP 500 + `failedEntries` array.
- Jobs schedule body is flat (no `{"job":...}` wrapper) and uses **snake_case** for `failure_policy`/`max_retries` while everything else is camelCase.
- Workflow start's query param is **`instanceID`** (capital ID); workflow component name must be `dapr`.
- Conversation alpha2 wraps messages in role keys (`ofUser`, `ofSystem`, …) with `content: [{"text": ...}]`; alpha1 used flat `{content, role, scrubPII}`.
- Crypto uses **PUT** with `dapr-key-name`/`dapr-key-wrap-algorithm` headers and raw octet-stream bodies.
- Actor state transaction body is a bare array, unlike the state-store transaction's `{"operations": ...}` wrapper.
- Service invocation passes the target's status/body through verbatim — a 4xx/5xx from the target is a response, not a sidecar error.
- Per-call component metadata = `?metadata.<field>=<value>` query params; API auth = `dapr-api-token` header.

## See Also

- [dapr-wasm-components Architecture](dapr-wasm-components-architecture.md)
- [gRPC to Dapr from Wasm](dapr-grpc-from-wasm.md) (the gRPC counterpart to this REST API)
- [Dapr Rust SDK](dapr-rust-sdk.md)
