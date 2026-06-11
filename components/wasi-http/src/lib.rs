//! dapr-wasm-components-wasi-http
//!
//! A pure-wasm Dapr provider component. It exports every interface of the
//! `dapr-wasm-components:interfaces` WIT package and implements them by
//! calling the Dapr sidecar's HTTP API through `wasi:http` outgoing
//! requests (via the `wstd` client). Compose it with any application
//! component that imports those interfaces, and run the result on any
//! WASI 0.2 runtime with `wasi:http` support — no native host, no Rust SDK.

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
        world: "provider",
        path: "../../wit",
        default_bindings_module: "crate::wit",
    });
}

pub(crate) use wit::exports::dapr_wasm_components::interfaces as exports;
pub(crate) use wit::exports::dapr_wasm_components::interfaces::types;

pub(crate) struct Component;

wit::export!(Component);
