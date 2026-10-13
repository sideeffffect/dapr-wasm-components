# Interfaces at a glance (`dapr-wasm-components:interfaces@0.7.1`)

Diagrams of the WIT package in this directory. The `.wit` files are the source of truth. The [README](../../README.md#interfaces-dapr-wasm-componentsinterfaces070) has the tables. Its [runtime diagram](../../README.md#two-directions-calling-dapr-vs-being-called-by-dapr--both-typed) shows how the composed component talks to the sidecar.

1. [Worlds](#1-worlds): who imports and who exports each interface.
2. [Type dependencies](#2-type-dependencies): the `use` graph, including the authority split.
3. [Outbound building blocks](#3-outbound-building-blocks): the functions and resources an app calls.
4. [Inbound callbacks](#4-inbound-callbacks): the functions an app implements.
5. [Resource lifecycles](#5-resource-lifecycles): `configuration.subscription` and the streaming `crypto` resources.

## 1. Worlds

Three worlds. An app imports the building blocks and exports the callbacks. The `outbound` provider exports the building blocks. The `inbound` provider imports the callbacks. So the composition graph `outbound → app → inbound` has no cycle.

```mermaid
flowchart LR
    outbound["<b>world outbound</b><br/>(provider)<br/>exports the building blocks"]

    subgraph app["world app"]
        direction TB
        subgraph imports["imports: building blocks"]
            direction TB
            reads["<b>read halves</b><br/>state-read · secrets-read<br/>configuration-read · jobs-read<br/>workflow-read"]
            fulls["<b>full halves</b><br/>state · secrets<br/>configuration · jobs<br/>workflow"]
            singles["<b>no read-only subset</b><br/>pubsub · bindings · invocation<br/>lock · crypto · conversation<br/>actors · runtime"]
            types["<b>types</b><br/>metadata · json · app-error"]
        end
        subgraph exports["exports: callbacks"]
            direction TB
            cbs["invocation-callback<br/>pubsub-callback<br/>bindings-callback<br/>jobs-callback<br/>actors-callback<br/>configuration-callback<br/>health-callback"]
        end
    end

    inbound["<b>world inbound</b><br/>(provider)<br/>imports the *-callback"]

    outbound == "satisfies imports" ==> imports
    exports == "called by" ==> inbound
```

Each provider family has one component per world:

| World | Components |
|---|---|
| `outbound` | `wasi-http-outbound`, `wasi-grpc-outbound`, `go-outbound` |
| `app` | your app (with the `dapr-app` SDK), `e2e/apps/*` |
| `inbound` | `wasi-http-inbound`, `wasi-grpc-inbound`, `go-inbound` |

The `go` pair is a special case. `go-outbound` embeds the Dapr runtime itself, so there is no sidecar. `go-inbound` gets the deliveries from `go-outbound` through a private interface that is not part of this package, so the two must be used together. The `go-inbound` world (in `components/go-outbound/wit/go.wit`) includes the `inbound` world.

## 2. Type dependencies

Arrows point from the interface that has the `use` to the interface that owns the type. Edges to `types` are not shown. These interfaces use `types` (`metadata`, `json`, `app-error`): all of them except `crypto`, `lock`, `jobs`, `workflow-read` and `workflow`.

A `<x>-read` half owns the types that both halves share, so the full half `<x>` points to it. The import list of a component is its authority manifest: an app that only reads state imports no `state` function.

```mermaid
flowchart LR
    classDef read fill:#e3f2fd,stroke:#1565c0
    classDef full fill:#fff3e0,stroke:#e65100
    classDef cb fill:#e8f5e9,stroke:#2e7d32

    state:::full -- "state-error, state-options, state-item" --> state-read:::read
    secrets:::full -- "secret, secrets-error" --> secrets-read:::read
    configuration:::full -- "configuration-error" --> configuration-read:::read
    jobs:::full -- "jobs-error, job" --> jobs-read:::read
    workflow:::full -- "workflow-error" --> workflow-read:::read

    bindings-callback:::cb -- "state-item" --> state-read
    configuration-callback:::cb -- "configuration-item" --> configuration-read
    invocation-callback:::cb -- "http-verb, http-response" --> invocation
```

Legend: blue = read half, orange = full half, green = callback.

## 3. Outbound building blocks

The functions an app imports. Arguments and most return types are left out. Most functions return `result<…, E>`, where `E` is a per-function or per-interface error variant. Infrastructure failures (sidecar not reachable, timeout) trap; they are not error values. The `string` in a setup error case (`store-not-found`, `permission-denied`, …) is the sidecar's message, for logs only. See the README [error model](../../README.md#error-model).

```mermaid
classDiagram
    direction LR

    class state_read["state-read"] {
        get()
        get-bulk()
        query()
    }
    class state {
        save()
        delete()
        execute-transaction()
    }
    class secrets_read["secrets-read"] {
        get-secret()
    }
    class secrets {
        get-bulk-secret()
    }
    class configuration_read["configuration-read"] {
        get()
    }
    class configuration {
        subscribe() subscription
    }
    class subscription {
        <<resource>>
        id() string
    }
    class jobs_read["jobs-read"] {
        get()
    }
    class jobs {
        schedule()
        delete()
    }
    class workflow_read["workflow-read"] {
        get() workflow-instance
    }
    class workflow {
        start()
        terminate()
        raise-event()
        pause()
        resume()
        purge()
    }

    state ..> state_read : use
    secrets ..> secrets_read : use
    configuration ..> configuration_read : use
    jobs ..> jobs_read : use
    workflow ..> workflow_read : use
    configuration *-- subscription
```

```mermaid
classDiagram
    direction TB

    class pubsub {
        publish()
        publish-bulk()
    }
    class bindings {
        invoke-binding()
    }
    class invocation {
        invoke()
    }
    class lock {
        try-lock()
        unlock()
    }
    class conversation {
        converse()
    }
    class runtime {
        get-metadata()
        set-metadata-label()
        healthz()
        outbound-healthz()
    }
    class actors {
        invoke()
        get-state()
        execute-state-transaction()
        register-reminder()
        get-reminder()
        unregister-reminder()
        register-timer()
        unregister-timer()
    }
```

`crypto` has one-shot functions and three streaming resources:

```mermaid
classDiagram
    direction LR

    class crypto {
        encrypt()
        decrypt()
    }
    class encryptor {
        <<resource>>
        constructor(component-name, options)
        write(chunk)
        finish(this)$ crypto-output
    }
    class decryptor {
        <<resource>>
        constructor(component-name, key-name)
        write(chunk)
        finish(this)$ crypto-output
    }
    class crypto_output["crypto-output"] {
        <<resource>>
        read(max-len) option~list~u8~~
    }

    crypto *-- encryptor
    crypto *-- decryptor
    crypto *-- crypto_output
    encryptor ..> crypto_output : finish
    decryptor ..> crypto_output : finish
```

## 4. Inbound callbacks

The functions an app exports. The inbound provider calls them. Every export is mandatory, because the component model has no optional exports. The `dapr-app` SDK gives a default for each one, so an app overrides only what it uses.

```mermaid
classDiagram
    direction LR

    class invocation_callback["invocation-callback"] {
        on-invoke(invoke-request)
    }
    class pubsub_callback["pubsub-callback"] {
        list-topic-subscriptions()
        on-topic-event(topic-event)
        on-bulk-topic-event(bulk-topic-event)
    }
    class bindings_callback["bindings-callback"] {
        list-input-bindings()
        on-binding-event(binding-event)
    }
    class jobs_callback["jobs-callback"] {
        on-job-event(job-event)
    }
    class actors_callback["actors-callback"] {
        config()
        on-invoke()
        on-timer()
        on-reminder()
        deactivate()
    }
    class configuration_callback["configuration-callback"] {
        on-configuration-event(configuration-update)
    }
    class health_callback["health-callback"] {
        health-check()
    }
```

Two kinds of callback:

- **Discovery** (`list-topic-subscriptions`, `list-input-bindings`, `actors-callback.config`): the sidecar asks at startup what the app wants.
- **Delivery** (all `on-*`, `deactivate`, `health-check`): the sidecar sends one event or request.

## 5. Resource lifecycles

### `configuration.subscription`

`subscribe` returns a `subscription`. Updates come in through `configuration-callback.on-configuration-event`. Drop the resource to unsubscribe.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Active : configuration.subscribe()
    Active --> Active : on-configuration-event (callback)
    Active --> [*] : drop → unsubscribe
```

### Streaming `crypto`

The `encryptor` and `decryptor` resources take the input chunk by chunk. `finish` is static: it consumes the resource and returns a `crypto-output`. Read the result chunk by chunk until `read` returns `none`.

```mermaid
stateDiagram-v2
    direction LR
    [*] --> Writing : encryptor(...) / decryptor(...)
    Writing --> Writing : write(chunk)
    Writing --> Reading : finish(this) → crypto-output
    Writing --> [*] : drop (abort)
    Reading --> Reading : read(max-len) → some(bytes)
    Reading --> [*] : read → none, or drop
```
