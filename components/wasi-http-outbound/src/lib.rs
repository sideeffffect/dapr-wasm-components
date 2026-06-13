//! dapr-wasm-components-wasi-http-outbound
//!
//! A pure-wasm Dapr provider component for the **outbound** direction. It
//! exports every building-block interface of the `dapr-wasm-components:interfaces`
//! WIT package (world `outbound`) and implements them by calling the Dapr
//! sidecar's HTTP API through `wasi:http` outgoing requests (via the `wstd`
//! client). Compose it with an application that imports those interfaces, and
//! run on any WASI 0.2 runtime with `wasi:http` support — no native host, no
//! Rust SDK.
//!
//! The inbound direction (Dapr → app) is a separate component,
//! `dapr-wasm-components-wasi-http-inbound`; the two are split so the
//! composition graph (`outbound → app → inbound`) stays acyclic.

mod sidecar;

mod actors;
mod bindings_block;
mod configuration;
mod conversation;
mod crypto;
mod invocation;
mod jobs;
mod lock;
mod pubsub;
mod runtime;
mod secrets;
mod state;
mod workflow;

mod wit {
    wit_bindgen::generate!({
        world: "outbound",
        path: "../wit",
        default_bindings_module: "crate::wit",
    });
}

pub(crate) use wit::exports::dapr_wasm_components::interfaces as exports;
pub(crate) use wit::exports::dapr_wasm_components::interfaces::types;

pub(crate) struct Component;

wit::export!(Component);
