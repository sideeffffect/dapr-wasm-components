//! Direct tests of the wasi-http provider's exports against the mock sidecar.

use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::invocation::HttpVerb;
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::jobs::Job;
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::lock::UnlockStatus;
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::state::StateItem;
use dapr_wasm_components_e2e::load_provider;
use dapr_wasm_components_e2e::mock::MockSidecar;

#[tokio::test]
async fn state_roundtrip() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let state = provider.dapr_wasm_components_interfaces_state();

    let value = br#"{"message":"hello"}"#.to_vec();
    state
        .call_save(
            &mut store,
            "statestore",
            &[StateItem {
                key: "k1".to_string(),
                value: value.clone(),
                etag: None,
                metadata: Vec::new(),
                options: None,
            }],
            &Vec::new(),
        )
        .await
        .unwrap()
        .unwrap();

    let got = state
        .call_get(&mut store, "statestore", "k1", None, &Vec::new())
        .await
        .unwrap()
        .unwrap()
        .expect("key should exist");
    assert_eq!(got.data, value);
    assert_eq!(got.etag.as_deref(), Some("42"));

    let missing = state
        .call_get(&mut store, "statestore", "nope", None, &Vec::new())
        .await
        .unwrap()
        .unwrap();
    assert!(missing.is_none(), "missing key should be none");

    let bulk = state
        .call_get_bulk(
            &mut store,
            "statestore",
            &["k1".to_string(), "nope".to_string()],
            None,
            &Vec::new(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(bulk.len(), 2);
    assert_eq!(bulk[0].key, "k1");
    assert_eq!(bulk[0].data, value);
    assert_eq!(bulk[1].key, "nope");
    assert!(bulk[1].data.is_empty());

    state
        .call_delete(&mut store, "statestore", "k1", None, None, &Vec::new())
        .await
        .unwrap()
        .unwrap();
    assert!(sidecar.recorded.lock().unwrap().state.is_empty());
}

#[tokio::test]
async fn pubsub_publish_records_content_type() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();

    provider
        .dapr_wasm_components_interfaces_pubsub()
        .call_publish(
            &mut store,
            "pubsub",
            "orders",
            br#"{"order":1}"#,
            "application/json",
            &vec![("rawPayload".to_string(), "true".to_string())],
        )
        .await
        .unwrap()
        .unwrap();

    let recorded = sidecar.recorded.lock().unwrap();
    assert_eq!(recorded.published.len(), 1);
    let event = &recorded.published[0];
    assert_eq!(event.pubsub, "pubsub");
    assert_eq!(event.topic, "orders");
    assert_eq!(event.content_type, "application/json");
    assert_eq!(event.body, br#"{"order":1}"#);
}

#[tokio::test]
async fn secrets() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let secrets = provider.dapr_wasm_components_interfaces_secrets();

    let secret = secrets
        .call_get_secret(&mut store, "vault", "db", &Vec::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        secret,
        vec![("password".to_string(), "hunter2".to_string())]
    );

    let missing = secrets
        .call_get_secret(&mut store, "vault", "missing", &Vec::new())
        .await
        .unwrap();
    assert!(
        missing.is_err(),
        "missing secret should be a not-found error"
    );
}

#[tokio::test]
async fn lock_and_unlock() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let lock = provider.dapr_wasm_components_interfaces_lock();

    let acquired = lock
        .call_try_lock(&mut store, "lockstore", "resource", "owner", 60)
        .await
        .unwrap()
        .unwrap();
    assert!(acquired);

    let status = lock
        .call_unlock(&mut store, "lockstore", "resource", "owner")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(status, UnlockStatus::Success);
}

#[tokio::test]
async fn invocation_passes_non_2xx_through() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();

    let response = provider
        .dapr_wasm_components_interfaces_invocation()
        .call_invoke(
            &mut store,
            "other-app",
            "orders/42",
            HttpVerb::Post,
            &vec![("x-mock-status".to_string(), "418".to_string())],
            None,
            b"teapot?",
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(response.status, 418);
    assert_eq!(response.body, b"teapot?");
    assert!(response
        .headers
        .iter()
        .any(|(name, value)| name == "x-echo-path" && value == "orders/42"));
}

#[tokio::test]
async fn workflow_start() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();

    let instance_id = provider
        .dapr_wasm_components_interfaces_workflow()
        .call_start(&mut store, "dapr", "order-processing", None, br#"{"id":1}"#)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(instance_id, "wf-123");
}

#[tokio::test]
async fn jobs_roundtrip() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let jobs = provider.dapr_wasm_components_interfaces_jobs();

    jobs.call_schedule(
        &mut store,
        "nightly",
        &Job {
            schedule: Some("@every 1m".to_string()),
            repeats: Some(5),
            due_time: None,
            ttl: None,
            data: Some(r#"{"value":42}"#.to_string()),
            failure_policy: None,
        },
        false,
    )
    .await
    .unwrap()
    .unwrap();

    let job = jobs.call_get(&mut store, "nightly").await.unwrap().unwrap();
    assert_eq!(job.schedule.as_deref(), Some("@every 1m"));
    assert_eq!(job.repeats, Some(5));
    assert_eq!(job.data.as_deref(), Some(r#"{"value":42}"#));
}

#[tokio::test]
async fn runtime_health_and_metadata() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let runtime = provider.dapr_wasm_components_interfaces_runtime();

    assert!(runtime.call_healthz(&mut store).await.unwrap());
    assert!(runtime.call_outbound_healthz(&mut store).await.unwrap());

    let metadata = runtime
        .call_get_metadata(&mut store)
        .await
        .unwrap()
        .unwrap();
    assert!(metadata.contains("mock-sidecar"));

    runtime
        .call_set_metadata_label(&mut store, "is-blue", "yes")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        sidecar
            .recorded
            .lock()
            .unwrap()
            .metadata_labels
            .get("is-blue"),
        Some(&"yes".to_string())
    );
}

#[tokio::test]
async fn unreachable_sidecar_is_unavailable() {
    // Port 9 (discard) — nothing is listening there.
    let (mut store, provider) = load_provider("http://127.0.0.1:9").await.unwrap();

    let result = provider
        .dapr_wasm_components_interfaces_state()
        .call_get(&mut store, "statestore", "k", None, &Vec::new())
        .await
        .unwrap();
    assert!(result.is_err(), "unreachable sidecar should be an error");
}
