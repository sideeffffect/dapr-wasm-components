# Dapr Rust SDK — official docs (v1.18)

> Source: https://v1-18.docs.dapr.io/developing-applications/sdks/rust/ and https://v1-18.docs.dapr.io/developing-applications/sdks/rust/rust-client/
> Collected: 2026-06-11
> Published: Unknown

## Dapr Rust SDK (overview page)

The Dapr Rust SDK is a client library designed to help developers build Dapr applications using Rust. It aims to support all public Dapr APIs while emphasizing idiomatic Rust patterns and developer productivity.

**Alpha Release**: "The Dapr Rust-SDK is currently in Alpha. Work is underway to bring it to a stable release and will likely involve breaking changes."

The Rust Client SDK enables invocation of public Dapr APIs. Current documentation version: v1.18 (preview); latest stable: v1.17.

## Getting Started with the Dapr Rust SDK Client (rust-client page)

### Prerequisites

- The Dapr CLI installed
- An initialized Dapr environment
- Rust installed on your system

### Installation

Add Dapr to your `Cargo.toml` dependencies:

```toml
[dependencies]
dapr = "0.16"
```

You can import the client with:

```rust
use dapr::Client as DaprClient;
```

### Instantiating the Client

Basic connection to the Dapr sidecar:

```rust
let addr = "https://127.0.0.1".to_string();

let mut client = dapr::Client::<dapr::client::TonicClient>::connect(addr, port).await?;
```

To specify a custom port:

```rust
let mut client = dapr::Client::<dapr::client::TonicClient>::connect_with_port(
    addr,
    "3500".to_string()
).await?;
```

### Building Blocks

#### Service Invocation (gRPC)

Invoke methods on other services running with Dapr sidecars:

```rust
let response = client
    .invoke_service("service-to-invoke", "method-to-invoke", Some(data))
    .await
    .unwrap();
```

#### State Management

The SDK provides three core state operations:

```rust
let store_name = String::from("statestore");
let key = String::from("hello");
let val = String::from("world").into_bytes();

// Save state
client
    .save_state(store_name, key, val, None, None, None)
    .await?;

// Retrieve state
let get_response = client
    .get_state("statestore", "hello", None)
    .await?;

// Delete state
client
    .delete_state("statestore", "hello", None)
    .await?;
```

The `save_bulk_states` method handles multiple state operations simultaneously.

#### Publish/Subscribe

Publishing messages to topics:

```rust
let pubsub_name = "pubsub-name".to_string();
let pubsub_topic = "topic-name".to_string();
let pubsub_content_type = "text/plain".to_string();

let data = "content".to_string().into_bytes();
client
    .publish_event(pubsub_name, pubsub_topic, pubsub_content_type, data, None)
    .await?;
```

### Note

The Dapr Rust SDK is currently in Alpha and may undergo breaking changes before reaching stable release.
