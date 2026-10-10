# daprd input-binding app reply handling (v1.18 source)

> Source: https://github.com/dapr/dapr/blob/release-1.18/pkg/runtime/processor/binding/send.go (`sendBindingEventToApp`, `onAppResponse`); https://github.com/dapr/components-contrib/blob/main/bindings/responses.go (`AppResponse`); https://github.com/dapr/docs/blob/v1.16/daprdocs/content/en/reference/api/bindings_api.md ("Binding endpoints"); for history, https://github.com/dapr/dapr/blob/release-1.0/pkg/runtime/runtime.go and release-1.5 / release-1.10 of the same file
> Collected: 2026-10-10
> Published: Unknown

## Documented reply shape (bindings API reference)

The app may answer a `POST /<binding-name>` with a JSON body that asks the sidecar to save state and to forward data to output bindings:

```json
{
  "storeName": "stateStore",
  "state": [{ "key": "...", "value": ... }],
  "to": ["storage", "queue"],
  "concurrency": "parallel",
  "data": ...
}
```

If `concurrency` is not set, output bindings are called in sequence.

## Struct with the JSON tags (components-contrib `bindings/responses.go`)

```go
type AppResponse struct {
	Data        interface{}        `json:"data"`
	To          []string           `json:"to"`
	StoreName   string             `json:"storeName"`
	State       []state.SetRequest `json:"state"`
	Concurrency string             `json:"concurrency"`
}
```

`concurrency` values: `"parallel"` and `"sequential"` (lower case; constants in `pkg/runtime/processor/binding/binding.go`).

## What daprd 1.18 actually does (`sendBindingEventToApp`)

- A local `var response bindings.AppResponse` starts empty.
- **gRPC app channel** (`OnBindingEvent`): daprd copies only `Concurrency` (`PARALLEL` → `"parallel"`, else `"sequential"`), `To`, and `Data` (JSON-decoded if it parses) from `BindingEventResponse`. It never reads `store_name` or `states`.
- **HTTP app channel**: daprd reads the reply body with `resp.RawDataFull()` only to return it and to put it into the error text of a non-2xx reply. It never unmarshals the body into `response`.
- At the end: `if len(response.State) > 0 || len(response.To) > 0 { onAppResponse(...) }`. `onAppResponse` saves `State` into `StoreName` (bulk set) and calls each `To` binding with the `create` operation and JSON-encoded `Data`.

Result: over HTTP, nothing in the reply has an effect. Over gRPC, `to` + `data` + `concurrency` have an effect; `store_name` + `states` do not.

## History

- release-1.0 `runtime.go`: the HTTP path did `json.Unmarshal(resp.Message().Data.Value, &response)`, and the gRPC path copied `States` into `response.State`.
- release-1.5 and release-1.10: both of these are gone; only the gRPC `To`/`Data`/`Concurrency` copy stays. The code moved to `pkg/runtime/processor/binding/send.go` later with the same logic (checked on release-1.18 and master, 2026-10-10).

## Empirical check (this repo, 2026-10-10)

daprd 1.18.7 with a `bindings.cron` input binding (`@every 1s`) and the `wasi-http-inbound` provider. The app replied `{"storeName":"statestore","state":[{"key":"binding-response","value":{...}}]}` to each event. After 61 delivered events, `binding-response` was not in the state store.
