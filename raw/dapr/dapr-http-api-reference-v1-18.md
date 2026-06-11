# Dapr HTTP API reference survey (docs v1.18)

> Source: Research synthesis compiled 2026-06-11 from https://docs.dapr.io/reference/api/ (state_api, pubsub_api, bindings_api, secrets_api, configuration_api, service_invocation_api, distributed_lock_api, workflow_api, jobs_api, cryptography_api, conversation_api, actors_api, metadata_api, health_api), cross-verified against raw page HTML
> Collected: 2026-06-11
> Published: Unknown

Base: `http://localhost:<daprPort>` (default 3500). URL parameters case-sensitive.

## State — /v1.0 (query: /v1.0-alpha1)

- Get: `GET /v1.0/state/<store>/<key>?consistency=strong|eventual&metadata.*` → 200 raw JSON value + `ETag` header; **204 = key not found**; 400 store missing.
- Save: `POST /v1.0/state/<store>?metadata.*` body = JSON array: `[{ "key", "value", "etag", "metadata": {"ttlInSeconds": "300"}, "options": {"concurrency": "first-write|last-write", "consistency": "strong|eventual"} }]` → 204; 500 includes etag mismatch.
- Delete: `DELETE /v1.0/state/<store>/<key>` with optional `If-Match: <etag>` header, query `concurrency`, `consistency`, `metadata.*` → 204.
- Get bulk: `POST /v1.0/state/<store>/bulk` body `{"keys": [...], "parallelism": 10}` → 200 array; items use **`value`** (not `data`); missing keys come back without value/etag.
- Transaction: `POST /v1.0/state/<store>/transaction` body `{"operations": [{"operation": "upsert|delete", "request": {"key", "value", ...}}], "metadata": {...}}` → 204.
- Query (alpha): `POST /v1.0-alpha1/state/<store>/query?metadata.*` body = filter/sort/page JSON → 200 `{"results": [{"key", "data", "etag"}], "token"}` (query results use **`data`**).

## Pub/Sub

- Publish: `POST /v1.0/publish/<pubsub>/<topic>?metadata.*` body = raw event; Content-Type matters (`application/cloudevents+json` for own CloudEvent; `metadata.rawPayload=true` to skip wrapping) → 204; 403; 404.
- Bulk publish: `POST /v1.0/publish/bulk/<pubsub>/<topic>` — **plain /v1.0 in v1.18** (alpha pre-1.16). Body array with **`entryId`**, `event`, `contentType`, optional `metadata`. → 204 all delivered; 500 partial failure with `{"failedEntries": [{"entryId", "error"}], "errorCode": "ERR_PUBSUB_PUBLISH_MESSAGE"}`.

## Output bindings

`POST/PUT /v1.0/bindings/<name>` body `{"data": <json>, "metadata": {...}, "operation": "create"}` → 200 with optional payload / 204 empty.

## Secrets

- `GET /v1.0/secrets/<store>/<name>?metadata.*` → 200 flat string map; **204 secret not found**; 403.
- `GET /v1.0/secrets/<store>/bulk` → 200 map of name → map.

## Configuration — /v1.0

- Get: `GET /v1.0/configuration/<store>?key=k1&key=k2` (repeated `key` param; omit = all) → body map key → `{"value", ...}` (`version`/`metadata` may appear). Docs status table says "204 Get operation successful" while showing a JSON body — treat any 2xx as success.
- Subscribe/unsubscribe exist but push updates to the app channel.

## Service invocation

`GET/POST/PUT/DELETE/PATCH /v1.0/invoke/<appID>/method/<method-path>` — body/headers pass through verbatim; **target's status code and body returned as-is**; nested paths fine; cross-namespace `<appID>.<namespace>`.

## Distributed lock (alpha) — /v1.0-alpha1

- `POST /v1.0-alpha1/lock/<store>` body `{"resourceId", "lockOwner", "expiryInSeconds": 60}` → 200 `{"success": true|false}`.
- `POST /v1.0-alpha1/unlock/<store>` body `{"resourceId", "lockOwner"}` → 200 `{"status": 0..3}` — 0 success, 1 lock doesn't exist, 2 belongs to others, 3 internal error.

## Workflow — /v1.0, but the HTTP workflow API is marked Deprecated (manage via SDKs going forward)

`<workflowComponentName>` must be `dapr`.

- Start: `POST /v1.0/workflows/dapr/<name>/start[?instanceID=<id>]` (query param **instanceID**) body = input JSON → 202 `{"instanceID": "..."}`; 409 exists.
- Terminate/pause/resume/purge: `POST /v1.0/workflows/dapr/<instanceId>/<action>` → 202. Purge only for COMPLETED/FAILED/TERMINATED.
- Raise event: `POST /v1.0/workflows/dapr/<instanceID>/raiseEvent/<eventName>` → 202.
- Get: `GET /v1.0/workflows/dapr/<instanceId>` → 200 `{"createdAt", "instanceID", "lastUpdatedAt", "properties": {...}, "runtimeStatus"}`; statuses: RUNNING, COMPLETED, CONTINUED_AS_NEW, FAILED, CANCELED, TERMINATED, PENDING, SUSPENDED. (`dapr.workflow.*` property keys are runtime behavior, not documented contract.)

## Jobs — /v1.0 in v1.18 (alpha pre-1.16)

- Schedule: `POST /v1.0/jobs/<name>` — **flat body**: `{"data": <json>, "schedule": "@every 1m", "dueTime": "30s", "repeats": 5, "ttl": "1h", "overwrite": false, "failure_policy": {"constant": {"max_retries": 3, "interval": "10s"}} }` (failure_policy/max_retries snake_case; `drop` variant exists; data is arbitrary JSON, not protobuf-Any on HTTP) → 204.
- Get: `GET /v1.0/jobs/<name>` → 200 `{"name", "schedule", "repeats", "data"}`; 400 unknown.
- Delete: `DELETE /v1.0/jobs/<name>` → 204.

## Cryptography (alpha) — /v1.0-alpha1, verb PUT

- Encrypt: `PUT /v1.0-alpha1/crypto/<store>/encrypt` — required headers `dapr-key-name`, `dapr-key-wrap-algorithm` (A256KW, A128CBC, A192CBC, RSA-OAEP-256); optional `dapr-data-encryption-cipher` (aes-gcm | chacha20-poly1305), `dapr-omit-decryption-key-name`, `dapr-decryption-key-name`. Body raw octet-stream → 200 raw encrypted bytes (streamed).
- Decrypt: `PUT /v1.0-alpha1/crypto/<store>/decrypt` — header `dapr-key-name` (when needed); raw bytes → raw plaintext.

## Conversation — current /v1.0-alpha2 (alpha1 deprecated but served)

- alpha2: `POST /v1.0-alpha2/conversation/<llm>/converse` body `{"inputs": [{"messages": [{"ofUser": {"content": [{"text": "..."}]}}], "scrubPii": false}], "parameters": {}, "metadata": {}, "temperature": 0.7, "contextId", "tools", "toolChoice", "responseFormat"}`; role wrappers: ofUser/ofSystem/ofAssistant/ofTool/ofDeveloper. Response: `{"outputs": [{"choices": [{"finishReason": "stop|tool_calls", "message": {"content", "toolCalls"}}]}], "model", "usage"}`.
- alpha1 (deprecated): inputs `[{"content", "role", "scrubPII"}]` (capital PII), response `{"outputs": [{"result"}]}`.

## Actors

- Invoke: `POST/GET/PUT/DELETE /v1.0/actors/<type>/<id>/method/<method>` — body→actor, response = actor's.
- State get: `GET /v1.0/actors/<type>/<id>/state/<key>` → 200 value; **204 not found**; 400 actor not found.
- State transaction: `POST/PUT /v1.0/actors/<type>/<id>/state` — body = bare array `[{"operation": "upsert", "request": {"key", "value"}}, {"operation": "delete", "request": {"key"}}]` → 204.
- Reminders: `POST/PUT .../reminders/<name>` body `{"dueTime": "1m", "period": "20s" | "R5/PT10S", "ttl", "data"}` → 204; `GET` → `{"dueTime", "period", "data"}`; `DELETE` → 204.
- Timers: same as reminders **plus `"callback": "methodName"`**; create POST/PUT, DELETE.

## Metadata

- `GET /v1.0/metadata` → 200 `{id, runtimeVersion, enabledFeatures, actors, components, httpEndpoints, subscriptions, appConnectionProperties, scheduler, extended}`.
- `PUT /v1.0/metadata/{attribute}` — body = raw value, `Content-Type: text/plain` → 204; appears under `extended`.

## Health

- `GET /v1.0/healthz` → **204 healthy / 500 not**; waits for app channel.
- `GET /v1.0/healthz/outbound` → 204/500; doesn't require app channel.

## Common

- `dapr-api-token` header required on every request when daprd runs with DAPR_API_TOKEN; invalid → 401.
- `metadata.<field>=<value>` query params for per-call component metadata.
- CLI/injector set `DAPR_HTTP_PORT`/`DAPR_GRPC_PORT` in the app env; `DAPR_HTTP_ENDPOINT`/`DAPR_GRPC_ENDPOINT` are an SDK convention for remote sidecars.
- Error envelope: `{"errorCode": "ERR_...", "message": "..."}` (+ optional `details[]` richer errors).
