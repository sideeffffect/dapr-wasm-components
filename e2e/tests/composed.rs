//! Composition test: plug kv-demo into the wasi-http provider with
//! wac-graph (the programmatic `wac plug`) and run the resulting command
//! component against the mock sidecar — the same artifact a user would run
//! with `wasmtime run -S http`.

use wasmtime::component::Component;
use wasmtime::Store;

use dapr_wasm_components_e2e::mock::MockSidecar;
use dapr_wasm_components_e2e::{
    compose, engine, http_inbound_path, http_outbound_path, kv_demo_path, linker,
    microservice_path, serve_inbound, Ctx,
};

#[tokio::test]
async fn composed_kv_demo_runs() {
    let sidecar = MockSidecar::start().await.unwrap();

    let app_bytes = std::fs::read(kv_demo_path()).expect("kv-demo component not built");
    let outbound_bytes =
        std::fs::read(http_outbound_path()).expect("http-outbound provider not built");
    let composed = compose::plug(app_bytes, outbound_bytes).expect("composition failed");

    let engine = engine().unwrap();
    let component = Component::new(&engine, &composed).unwrap();
    let linker = linker(&engine).unwrap();

    let env = vec![("DAPR_HTTP_ENDPOINT".to_string(), sidecar.endpoint.clone())];
    let mut store = Store::new(&engine, Ctx::new(&env));

    let command =
        wasmtime_wasi::p2::bindings::Command::instantiate_async(&mut store, &component, &linker)
            .await
            .unwrap();
    let result = command.wasi_cli_run().call_run(&mut store).await.unwrap();
    assert!(result.is_ok(), "kv-demo exited with failure");

    let recorded = sidecar.recorded.lock().unwrap();
    // kv-demo deleted its key, so state must be empty again...
    assert!(recorded.state.is_empty());
    // ...and exactly one event was published.
    assert_eq!(recorded.published.len(), 1);
    assert_eq!(recorded.published[0].topic, "kv-demo");
}

/// The full bidirectional composition (`outbound → app → inbound`) must encode
/// to a valid server component that exports `wasi:http/incoming-handler`.
#[tokio::test]
async fn full_composition_is_valid() {
    let app = std::fs::read(microservice_path()).expect("microservice component not built");
    let outbound = std::fs::read(http_outbound_path()).expect("http-outbound not built");
    let inbound = std::fs::read(http_inbound_path()).expect("http-inbound not built");

    let composed =
        compose::plug_full(app, outbound, inbound).expect("full composition failed to encode");

    // Component::new fully validates the encoded component bytes.
    let engine = engine().unwrap();
    Component::new(&engine, &composed).expect("composed component is invalid");
}

/// Inbound config-update delivery: a sidecar `POST /configuration/<store>/<key>`
/// carrying Dapr's `UpdateEvent` must reach the app's `on-configuration-event`.
/// The microservice persists each delivered item to state, so we assert the
/// resulting state write landed on the mock sidecar.
#[tokio::test]
async fn inbound_configuration_update_is_delivered() {
    let sidecar = MockSidecar::start().await.unwrap();

    let app = std::fs::read(microservice_path()).expect("microservice component not built");
    let outbound = std::fs::read(http_outbound_path()).expect("http-outbound not built");
    let inbound = std::fs::read(http_inbound_path()).expect("http-inbound not built");
    let composed = compose::plug_full(app, outbound, inbound).expect("full composition failed");

    // Dapr's UpdateEvent: `items` is a map keyed by configuration key.
    let body = serde_json::json!({
        "id": "sub-1",
        "items": {
            "feature-flag": { "value": "enabled", "version": "v2" }
        }
    })
    .to_string()
    .into_bytes();

    let (status, _) = serve_inbound(
        &composed,
        &sidecar.endpoint,
        "POST",
        "/configuration/configstore/feature-flag",
        "application/json",
        body,
    )
    .await
    .expect("inbound handler errored");
    assert_eq!(status, 200, "config-update delivery should return 200");

    let recorded = sidecar.recorded.lock().unwrap();
    let stored = recorded
        .state
        .get(&("statestore".to_string(), "config-feature-flag".to_string()))
        .expect("app did not persist the delivered config item");
    assert_eq!(stored, &serde_json::json!("enabled"));
}
