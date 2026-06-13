//! prost/tonic code generated from the vendored Dapr protos (`../../proto/`,
//! dapr/dapr v1.18.0). Checked in so the build needs no protoc/codegen —
//! see `../../proto/README.md` for how to regenerate.

// The generated code is not ours to lint.
#![allow(clippy::all, clippy::pedantic)]

pub mod dapr {
    pub mod proto {
        pub mod common {
            pub mod v1 {
                include!("dapr.proto.common.v1.rs");
            }
        }
        pub mod runtime {
            pub mod v1 {
                include!("dapr.proto.runtime.v1.rs");
            }
        }
    }
}

/// The Dapr runtime API messages + client (`dapr.proto.runtime.v1`).
pub use dapr::proto::runtime::v1 as runtime;

/// The shared Dapr messages (`dapr.proto.common.v1`).
pub use dapr::proto::common::v1 as common;
