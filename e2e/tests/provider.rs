//! Direct tests of the wasi-http provider's exports against the mock sidecar.

use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::conversation::{
    ContentPart, ConversationInput, ConversationOptions, Message, ParticipantMessage, Tool,
    ToolFunction,
};
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::invocation::HttpVerb;
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::jobs::Job;
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::lock::{
    TryLockError, UnlockError,
};
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::secrets::GetSecretError;
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::state::{
    GetError, StateItem,
};
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
        .expect("key should exist");
    assert_eq!(got.data, value);
    assert_eq!(got.etag.as_deref(), Some("42"));

    let missing = state
        .call_get(&mut store, "statestore", "nope", None, &Vec::new())
        .await
        .unwrap();
    assert!(
        matches!(missing, Err(GetError::KeyNotFound)),
        "missing key should be key-not-found, got {missing:?}"
    );

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
    assert_eq!(bulk[0].value, value);
    assert_eq!(bulk[1].key, "nope");
    assert!(bulk[1].value.is_empty());

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
            Some("application/json"),
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
        .expect("secret should exist");
    assert_eq!(
        secret,
        vec![("password".to_string(), "hunter2".to_string())]
    );

    let missing = secrets
        .call_get_secret(&mut store, "vault", "missing", &Vec::new())
        .await
        .unwrap();
    assert!(
        matches!(missing, Err(GetSecretError::SecretNotFound)),
        "missing secret should be secret-not-found, got {missing:?}"
    );
}

#[tokio::test]
async fn conversation_converse() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let conversation = provider.dapr_wasm_components_interfaces_conversation();

    let inputs = vec![ConversationInput {
        messages: vec![Message::User(ParticipantMessage {
            name: None,
            content: vec![ContentPart {
                text: "hi".to_string(),
            }],
        })],
        scrub_pii: None,
    }];
    let options = ConversationOptions {
        context_id: None,
        parameters: None,
        metadata: Vec::new(),
        scrub_pii: None,
        temperature: Some(0.5),
        tools: vec![Tool {
            function: ToolFunction {
                name: "lookup".to_string(),
                description: None,
                parameters: Some(r#"{"type":"object"}"#.to_string()),
            },
        }],
        tool_choice: Some("auto".to_string()),
        response_format: None,
        prompt_cache_retention: None,
    };

    let response = conversation
        .call_converse(&mut store, "openai", &inputs, Some(&options))
        .await
        .unwrap()
        .unwrap();

    // Response shape: choices, tool calls, usage, context id all parsed.
    assert_eq!(response.context_id.as_deref(), Some("ctx-1"));
    assert_eq!(response.outputs.len(), 1);
    let output = &response.outputs[0];
    assert_eq!(output.model.as_deref(), Some("gpt-test"));
    let choice = &output.choices[0];
    assert_eq!(choice.finish_reason, "tool_calls");
    assert_eq!(choice.message.content, "hello");
    assert_eq!(choice.message.tool_calls[0].id.as_deref(), Some("call-1"));
    assert_eq!(choice.message.tool_calls[0].function.name, "lookup");
    let usage = output.usage.as_ref().expect("usage present");
    assert_eq!(usage.total_tokens, 8);
    assert_eq!(
        usage.prompt_tokens_details.as_ref().unwrap().cached_tokens,
        2
    );

    // Request shape: role wrapper, content parts, options serialized faithfully.
    let recorded = sidecar.recorded.lock().unwrap();
    let request = &recorded.converse_requests[0];
    assert_eq!(
        request["inputs"][0]["messages"][0]["ofUser"]["content"][0]["text"],
        "hi"
    );
    assert_eq!(request["temperature"], 0.5);
    assert_eq!(request["toolChoice"], "auto");
    assert_eq!(request["tools"][0]["function"]["name"], "lookup");
    assert_eq!(
        request["tools"][0]["function"]["parameters"]["type"],
        "object"
    );
}

#[tokio::test]
async fn lock_and_unlock() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let lock = provider.dapr_wasm_components_interfaces_lock();

    // Acquired -> Ok(()).
    lock.call_try_lock(&mut store, "lockstore", "resource", "owner", 60)
        .await
        .unwrap()
        .expect("lock should be acquired");

    // Already held -> Err(not-acquired). The mock keys "contended" to a
    // `success: false` response.
    let contended = lock
        .call_try_lock(&mut store, "lockstore", "contended", "owner", 60)
        .await
        .unwrap();
    assert!(
        matches!(contended, Err(TryLockError::NotAcquired)),
        "contended lock should be not-acquired, got {contended:?}"
    );

    // Unlock success -> Ok(()).
    lock.call_unlock(&mut store, "lockstore", "resource", "owner")
        .await
        .unwrap()
        .expect("unlock should succeed");

    // Unlock of a non-existent lock -> Err(lock-does-not-exist).
    let missing = lock
        .call_unlock(&mut store, "lockstore", "missing", "owner")
        .await
        .unwrap();
    assert!(
        matches!(missing, Err(UnlockError::LockDoesNotExist)),
        "unlocking a missing lock should be lock-does-not-exist, got {missing:?}"
    );

    // Unlock of a lock owned by someone else -> Err(lock-belongs-to-others).
    let others = lock
        .call_unlock(&mut store, "lockstore", "others", "owner")
        .await
        .unwrap();
    assert!(
        matches!(others, Err(UnlockError::LockBelongsToOthers)),
        "unlocking another owner's lock should be lock-belongs-to-others, got {others:?}"
    );
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

    let job = jobs
        .call_get(&mut store, "nightly")
        .await
        .unwrap()
        .expect("scheduled job should exist");
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
async fn unreachable_sidecar_traps() {
    // Port 9 (discard) — nothing is listening there. An unreachable sidecar is
    // an unrecoverable (tier-3) failure: the provider traps rather than
    // returning a recoverable error value, so the call itself errors.
    let (mut store, provider) = load_provider("http://127.0.0.1:9").await.unwrap();

    let call = provider
        .dapr_wasm_components_interfaces_state()
        .call_get(&mut store, "statestore", "k", None, &Vec::new())
        .await;
    assert!(
        call.is_err(),
        "unreachable sidecar should trap, surfacing as an errored call"
    );
}
