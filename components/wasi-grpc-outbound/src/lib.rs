//! dapr-wasm-components-wasi-grpc
//!
//! A pure-wasm Dapr provider component. It exports every interface of the
//! `dapr-wasm-components:interfaces` WIT package and implements them by
//! calling the Dapr sidecar's gRPC API (`DaprClient` on :50001) through
//! `wasi:http` outgoing requests, using tonic's generated client over the
//! `wasi-grpc` transport.
//!
//! gRPC needs HTTP/2 end-to-end; `wasi:http` 0.2 leaves the HTTP version
//! to the host, and today only Spin >= 3.4 speaks outbound cleartext
//! HTTP/2 — enabled per-authority via the host-process env var
//! `SPIN_OUTBOUND_H2C_PRIOR_KNOWLEDGE` (it must equal the authority of
//! `DAPR_GRPC_ENDPOINT` byte-for-byte). On other runtimes (wasmtime, ...)
//! this component instantiates but calls fail with `unavailable`. The
//! wasi-http sibling provider is the portable choice; this one trades
//! portability for typed protobuf (byte-exact values) on Spin.

mod anyjson;
mod proto;
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
