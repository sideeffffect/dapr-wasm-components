//! E2E fixture: a service-invocation handler that only *reads* state.
//!
//! The `app` world imports both `state-read` and `state`, but `wit-component`
//! keeps only the functions the app calls, so this component imports
//! `state-read` with just `get` and no `state` at all. `e2e/tests/authority.rs`
//! checks that: a component's import list is its authority manifest.

use dapr_app::callback::invocation_callback::{HttpResponse, InvokeRequest};
use dapr_app::dapr::state_read;
use dapr_app::DaprApp;

struct App;

impl DaprApp for App {
    /// Invoking method `<key>` returns the value stored under `key` (404 when
    /// absent).
    fn on_invoke(request: InvokeRequest) -> HttpResponse {
        match state_read::get("statestore", &request.method, None, &[]) {
            Ok(response) => HttpResponse {
                status: 200,
                headers: Vec::new(),
                body: response.data,
            },
            Err(state_read::GetError::KeyNotFound) => HttpResponse {
                status: 404,
                headers: Vec::new(),
                body: Vec::new(),
            },
            Err(error) => HttpResponse {
                status: 500,
                headers: Vec::new(),
                body: format!("{error:?}").into_bytes(),
            },
        }
    }
}

dapr_app::export_app!(App);
