//! Direct tests of the wasi-http provider's exports against the mock sidecar.

use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::actors::{
    ActorsError, Timer,
};
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::conversation::{
    ContentPart, ConversationInput, ConversationOptions, Message, ParticipantMessage, Tool,
    ToolFunction,
};
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::invocation::HttpVerb;
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::jobs::Job;
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::lock::{
    TryLockError, UnlockError,
};
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::secrets_read::GetSecretError;
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::state_read::{GetError, StateItem};
use dapr_wasm_components_e2e::bindings::exports::dapr_wasm_components::interfaces::workflow_read::{
    GetError as WorkflowGetError, RuntimeStatus,
};
use dapr_wasm_components_e2e::load_provider;
use dapr_wasm_components_e2e::mock::{
    MockSidecar, MOCK_BROKEN_CONFIG_STORE, MOCK_BROKEN_TIMER_ACTOR_TYPE, MOCK_HOSTED_ACTOR_TYPE,
};

#[tokio::test]
async fn state_roundtrip() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let state = provider.dapr_wasm_components_interfaces_state();
    let state_read = provider.dapr_wasm_components_interfaces_state_read();

    let value = br#"{"message":"hello"}"#.to_vec();
    store
        .run_concurrent(async |a| {
            state
                .call_save(
                    a,
                    "statestore".to_string(),
                    vec![StateItem {
                        key: "k1".to_string(),
                        value: value.clone(),
                        etag: None,
                        metadata: Vec::new(),
                        options: None,
                    }],
                    Vec::new(),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let got = store
        .run_concurrent(async |a| {
            state_read
                .call_get(
                    a,
                    "statestore".to_string(),
                    "k1".to_string(),
                    None,
                    Vec::new(),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap()
        .expect("key should exist");
    assert_eq!(got.data, value);
    assert_eq!(got.etag.as_deref(), Some("42"));

    let missing = store
        .run_concurrent(async |a| {
            state_read
                .call_get(
                    a,
                    "statestore".to_string(),
                    "nope".to_string(),
                    None,
                    Vec::new(),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(missing, Err(GetError::KeyNotFound)),
        "missing key should be key-not-found, got {missing:?}"
    );

    let bulk = store
        .run_concurrent(async |a| {
            state_read
                .call_get_bulk(
                    a,
                    "statestore".to_string(),
                    vec!["k1".to_string(), "nope".to_string()],
                    None,
                    Vec::new(),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(bulk.len(), 2);
    assert_eq!(bulk[0].key, "k1");
    assert_eq!(bulk[0].value, value);
    assert_eq!(bulk[1].key, "nope");
    assert!(bulk[1].value.is_empty());

    store
        .run_concurrent(async |a| {
            state
                .call_delete(
                    a,
                    "statestore".to_string(),
                    "k1".to_string(),
                    None,
                    None,
                    Vec::new(),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(sidecar.recorded.lock().unwrap().state.is_empty());
}

#[tokio::test]
async fn pubsub_publish_records_content_type() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();

    store
        .run_concurrent(async |a| {
            provider
                .dapr_wasm_components_interfaces_pubsub()
                .call_publish(
                    a,
                    "pubsub".to_string(),
                    "orders".to_string(),
                    br#"{"order":1}"#.to_vec(),
                    Some("application/json".to_string()),
                    vec![("rawPayload".to_string(), "true".to_string())],
                )
                .await
        })
        .await
        .unwrap()
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
    let secrets = provider.dapr_wasm_components_interfaces_secrets_read();

    let secret = store
        .run_concurrent(async |a| {
            secrets
                .call_get_secret(a, "vault".to_string(), "db".to_string(), Vec::new())
                .await
        })
        .await
        .unwrap()
        .unwrap()
        .expect("secret should exist");
    assert_eq!(
        secret,
        vec![("password".to_string(), "hunter2".to_string())]
    );

    let missing = store
        .run_concurrent(async |a| {
            secrets
                .call_get_secret(a, "vault".to_string(), "missing".to_string(), Vec::new())
                .await
        })
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(missing, Err(GetSecretError::SecretNotFound)),
        "missing secret should be secret-not-found, got {missing:?}"
    );

    // Real daprd: a store's "not found" arrives as a 500 ERR_SECRET_GET.
    let missing = store
        .run_concurrent(async |a| {
            secrets
                .call_get_secret(
                    a,
                    "vault".to_string(),
                    "missing-in-store".to_string(),
                    Vec::new(),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(missing, Err(GetSecretError::SecretNotFound)),
        "store-reported missing secret should be secret-not-found, got {missing:?}"
    );
}

#[tokio::test]
async fn secret_store_failure_traps() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    // Any other ERR_SECRET_GET is a store backend failure: unrecoverable.
    let call = store
        .run_concurrent(async |a| {
            provider
                .dapr_wasm_components_interfaces_secrets_read()
                .call_get_secret(a, "vault".to_string(), "store-down".to_string(), Vec::new())
                .await
        })
        .await
        // A trap ends the whole concurrent run: it surfaces as its error.
        .and_then(|call| call);
    assert!(
        call.is_err(),
        "store backend failure should trap, got {call:?}"
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

    let response = store
        .run_concurrent(async |a| {
            conversation
                .call_converse(
                    a,
                    "openai".to_string(),
                    inputs.clone(),
                    Some(options.clone()),
                )
                .await
        })
        .await
        .unwrap()
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
    store
        .run_concurrent(async |a| {
            lock.call_try_lock(
                a,
                "lockstore".to_string(),
                "resource".to_string(),
                "owner".to_string(),
                60,
            )
            .await
        })
        .await
        .unwrap()
        .unwrap()
        .expect("lock should be acquired");

    // Already held -> Err(not-acquired). The mock keys "contended" to a
    // `success: false` response.
    let contended = store
        .run_concurrent(async |a| {
            lock.call_try_lock(
                a,
                "lockstore".to_string(),
                "contended".to_string(),
                "owner".to_string(),
                60,
            )
            .await
        })
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(contended, Err(TryLockError::NotAcquired)),
        "contended lock should be not-acquired, got {contended:?}"
    );

    // Unlock success -> Ok(()).
    store
        .run_concurrent(async |a| {
            lock.call_unlock(
                a,
                "lockstore".to_string(),
                "resource".to_string(),
                "owner".to_string(),
            )
            .await
        })
        .await
        .unwrap()
        .unwrap()
        .expect("unlock should succeed");

    // Unlock of a non-existent lock -> Err(lock-does-not-exist).
    let missing = store
        .run_concurrent(async |a| {
            lock.call_unlock(
                a,
                "lockstore".to_string(),
                "missing".to_string(),
                "owner".to_string(),
            )
            .await
        })
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(missing, Err(UnlockError::LockDoesNotExist)),
        "unlocking a missing lock should be lock-does-not-exist, got {missing:?}"
    );

    // Unlock of a lock owned by someone else -> Err(lock-belongs-to-others).
    let others = store
        .run_concurrent(async |a| {
            lock.call_unlock(
                a,
                "lockstore".to_string(),
                "others".to_string(),
                "owner".to_string(),
            )
            .await
        })
        .await
        .unwrap()
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

    let response = store
        .run_concurrent(async |a| {
            provider
                .dapr_wasm_components_interfaces_invocation()
                .call_invoke(
                    a,
                    "other-app".to_string(),
                    "orders/42".to_string(),
                    HttpVerb::Post,
                    vec![("x-mock-status".to_string(), "418".to_string())],
                    None,
                    b"teapot?".to_vec(),
                )
                .await
        })
        .await
        .unwrap()
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

    let instance_id = store
        .run_concurrent(async |a| {
            provider
                .dapr_wasm_components_interfaces_workflow()
                .call_start(
                    a,
                    "order-processing".to_string(),
                    None,
                    br#"{"id":1}"#.to_vec(),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(instance_id, "wf-123");
}

/// `subscribe` returns a `subscription` handle; dropping it unsubscribes.
/// A failed unsubscribe is logged, not trapped: the drop still returns.
#[tokio::test]
async fn configuration_subscription_drop_unsubscribes() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let configuration = provider.dapr_wasm_components_interfaces_configuration();

    let subscription = store
        .run_concurrent(async |a| {
            configuration
                .call_subscribe(
                    a,
                    "configstore".to_string(),
                    vec!["flag".to_string()],
                    Vec::new(),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let id = configuration
        .subscription()
        .call_id(&mut store, subscription)
        .await
        .unwrap();
    assert_eq!(id, "sub-1");
    assert!(sidecar
        .recorded
        .lock()
        .unwrap()
        .configuration_unsubscribes
        .is_empty());

    subscription.resource_drop_async(&mut store).await.unwrap();
    assert_eq!(
        sidecar.recorded.lock().unwrap().configuration_unsubscribes,
        vec![("configstore".to_string(), "sub-1".to_string())]
    );

    let broken = store
        .run_concurrent(async |a| {
            configuration
                .call_subscribe(a, MOCK_BROKEN_CONFIG_STORE.to_string(), vec![], Vec::new())
                .await
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    broken
        .resource_drop_async(&mut store)
        .await
        .expect("a failed unsubscribe must not trap");
    assert_eq!(
        sidecar.recorded.lock().unwrap().configuration_unsubscribes[1],
        (MOCK_BROKEN_CONFIG_STORE.to_string(), "sub-2".to_string())
    );
}

/// `get` lifts daprd's flat `dapr.workflow.*` properties into the typed
/// `workflow-instance` fields and keeps only the rest in `properties`.
#[tokio::test]
async fn workflow_get_is_typed() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let workflow = provider.dapr_wasm_components_interfaces_workflow_read();

    let failed = store
        .run_concurrent(async |a| workflow.call_get(a, "wf-failed".to_string()).await)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(failed.instance_id, "wf-failed");
    assert_eq!(failed.workflow_name, "order-processing");
    assert_eq!(failed.runtime_status, RuntimeStatus::Failed);
    assert_eq!(failed.input.as_deref(), Some(br#"{"id":1}"#.as_slice()));
    assert_eq!(failed.output, None);
    let failure = failed.failure.expect("failed instance has failure details");
    assert_eq!(failure.error_type, "PaymentDeclined");
    assert_eq!(failure.message, "card declined");
    assert_eq!(failure.stack_trace.as_deref(), Some("at charge()"));
    assert_eq!(
        failed.properties,
        vec![(
            "dapr.workflow.custom_status".to_string(),
            r#""charging""#.to_string()
        )]
    );

    // An unrecognised status is `unknown`, with the raw string kept.
    let stalled = store
        .run_concurrent(async |a| workflow.call_get(a, "wf-stalled".to_string()).await)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(stalled.runtime_status, RuntimeStatus::Unknown);
    assert_eq!(stalled.output.as_deref(), Some(b"42".as_slice()));
    assert!(stalled.failure.is_none());
    assert_eq!(
        stalled.properties,
        vec![(
            "dapr.workflow.runtime_status".to_string(),
            "UNKNOWN".to_string()
        )]
    );

    let missing = store
        .run_concurrent(async |a| workflow.call_get(a, "nope".to_string()).await)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(missing, Err(WorkflowGetError::NotFound(_))),
        "missing instance should be not-found, got {missing:?}"
    );
}

#[tokio::test]
async fn jobs_roundtrip() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    let jobs = provider.dapr_wasm_components_interfaces_jobs();

    store
        .run_concurrent(async |a| {
            jobs.call_schedule(
                a,
                "nightly".to_string(),
                Job {
                    schedule: Some("@every 1m".to_string()),
                    repeats: Some(5),
                    due_time: None,
                    ttl: None,
                    data: Some(r#"{"value":42}"#.to_string()),
                    failure_policy: None,
                }
                .clone(),
                false,
            )
            .await
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let job = store
        .run_concurrent(async |a| {
            provider
                .dapr_wasm_components_interfaces_jobs_read()
                .call_get(a, "nightly".to_string())
                .await
        })
        .await
        .unwrap()
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

    assert!(store
        .run_concurrent(async |a| runtime.call_healthz(a).await)
        .await
        .unwrap()
        .unwrap());
    assert!(store
        .run_concurrent(async |a| runtime.call_outbound_healthz(a).await)
        .await
        .unwrap()
        .unwrap());

    let metadata = store
        .run_concurrent(async |a| runtime.call_get_metadata(a).await)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(metadata.contains("mock-sidecar"));

    store
        .run_concurrent(async |a| {
            runtime
                .call_set_metadata_label(a, "is-blue".to_string(), "yes".to_string())
                .await
        })
        .await
        .unwrap()
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

    let call = store
        .run_concurrent(async |a| {
            provider
                .dapr_wasm_components_interfaces_state_read()
                .call_get(
                    a,
                    "statestore".to_string(),
                    "k".to_string(),
                    None,
                    Vec::new(),
                )
                .await
        })
        .await
        // A trap ends the whole concurrent run: it surfaces as its error.
        .and_then(|call| call);
    assert!(
        call.is_err(),
        "unreachable sidecar should trap, surfacing as an errored call"
    );
}

fn timer() -> Timer {
    Timer {
        due_time: Some("10s".to_string()),
        period: None,
        ttl: None,
        data: None,
        callback: None,
    }
}

#[tokio::test]
async fn register_timer_on_a_hosted_actor_type() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    store
        .run_concurrent(async |a| {
            provider
                .dapr_wasm_components_interfaces_actors()
                .call_register_timer(
                    a,
                    MOCK_HOSTED_ACTOR_TYPE.to_string(),
                    "a-1".to_string(),
                    "t".to_string(),
                    timer(),
                )
                .await
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn register_timer_on_an_unhosted_actor_type_is_permission_denied() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    // daprd answers HTTP 500 ERR_ACTOR_TIMER_CREATE "... actor type not
    // registered": the caller's mistake, not a sidecar fault — a typed error,
    // not a trap.
    let result = store
        .run_concurrent(async |a| {
            provider
                .dapr_wasm_components_interfaces_actors()
                .call_register_timer(
                    a,
                    "ProbeActor".to_string(),
                    "probe-1".to_string(),
                    "t".to_string(),
                    timer(),
                )
                .await
        })
        .await
        .unwrap()
        .expect("an unhosted actor type must not trap");
    match result {
        Err(ActorsError::PermissionDenied(message)) => assert_eq!(
            message,
            "error creating actor timer: can't create timer for actor ProbeActor||probe-1: actor type not registered"
        ),
        other => panic!("expected permission-denied, got {other:?}"),
    }
}

#[tokio::test]
async fn register_timer_storage_failure_traps() {
    let sidecar = MockSidecar::start().await.unwrap();
    let (mut store, provider) = load_provider(&sidecar.endpoint).await.unwrap();
    // Any other ERR_ACTOR_TIMER_CREATE is a sidecar fault: unrecoverable.
    let call = store
        .run_concurrent(async |a| {
            provider
                .dapr_wasm_components_interfaces_actors()
                .call_register_timer(
                    a,
                    MOCK_BROKEN_TIMER_ACTOR_TYPE.to_string(),
                    "a-1".to_string(),
                    "t".to_string(),
                    timer(),
                )
                .await
        })
        .await
        // A trap ends the whole concurrent run: it surfaces as its error.
        .and_then(|call| call);
    assert!(
        call.is_err(),
        "timer storage failure should trap, got {call:?}"
    );
}
