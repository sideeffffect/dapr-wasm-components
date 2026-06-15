//! Conversation (alpha2) — https://docs.dapr.io/reference/api/conversation_api/
//!
//! Full alpha2 surface: content-part messages with role wrappers, tool
//! definitions and tool calls, structured-output schema, prompt-cache
//! retention, and per-token usage in the response.

use serde::Deserialize;
use serde_json::json;
use wstd::http::Method;

use crate::exports::conversation::{
    AssistantMessage, Choice, CompletionTokensDetails, ContentPart, ConversationError,
    ConversationInput, ConversationOptions, ConversationOutput, ConversationResponse,
    ConverseError, Guest, Message, ParticipantMessage, PromptTokensDetails, ResultMessage, Tool,
    ToolCall, ToolCallFunction, ToolMessage, Usage,
};
use crate::sidecar::{seg, DaprFailure, Sidecar};
use crate::Component;

/// Map a recoverable failure to the conversation setup/config error.
fn conversation_error(f: DaprFailure) -> ConversationError {
    if f.is_permission() {
        ConversationError::PermissionDenied(f.message)
    } else {
        ConversationError::ComponentNotFound(f.message)
    }
}

/// Map a recoverable failure of a converse.
fn converse_error(f: DaprFailure) -> ConverseError {
    if f.status == 404 {
        return ConverseError::ContextNotFound(f.message);
    }
    if f.error_code
        .as_deref()
        .is_some_and(|c| c.contains("CONTENT") || c.contains("FILTER"))
    {
        return ConverseError::ContentFiltered(f.message);
    }
    if f.status == 400 {
        return ConverseError::InvalidRequest(f.message);
    }
    ConverseError::Conversation(conversation_error(f))
}

fn content_json(content: &[ContentPart]) -> serde_json::Value {
    serde_json::Value::Array(
        content
            .iter()
            .map(|part| json!({ "text": part.text }))
            .collect(),
    )
}

fn tool_calls_json(tool_calls: &[ToolCall]) -> serde_json::Value {
    serde_json::Value::Array(
        tool_calls
            .iter()
            .map(|call| {
                let mut object = json!({
                    "function": {
                        "name": call.function.name,
                        "arguments": call.function.arguments,
                    }
                });
                if let Some(id) = &call.id {
                    object["id"] = json!(id);
                }
                object
            })
            .collect(),
    )
}

fn participant_json(message: &ParticipantMessage) -> serde_json::Value {
    let mut object = json!({ "content": content_json(&message.content) });
    if let Some(name) = &message.name {
        object["name"] = json!(name);
    }
    object
}

fn assistant_json(message: &AssistantMessage) -> serde_json::Value {
    let mut object = json!({ "content": content_json(&message.content) });
    if let Some(name) = &message.name {
        object["name"] = json!(name);
    }
    if !message.tool_calls.is_empty() {
        object["toolCalls"] = tool_calls_json(&message.tool_calls);
    }
    object
}

fn tool_message_json(message: &ToolMessage) -> serde_json::Value {
    let mut object = json!({
        "name": message.name,
        "content": content_json(&message.content),
    });
    if let Some(tool_id) = &message.tool_id {
        object["toolId"] = json!(tool_id);
    }
    object
}

fn message_json(message: &Message) -> serde_json::Value {
    match message {
        Message::Developer(m) => json!({ "ofDeveloper": participant_json(m) }),
        Message::System(m) => json!({ "ofSystem": participant_json(m) }),
        Message::User(m) => json!({ "ofUser": participant_json(m) }),
        Message::Assistant(m) => json!({ "ofAssistant": assistant_json(m) }),
        Message::Tool(m) => json!({ "ofTool": tool_message_json(m) }),
    }
}

fn tools_json(tools: &[Tool]) -> serde_json::Value {
    let mut out = Vec::with_capacity(tools.len());
    for tool in tools {
        let mut function = json!({ "name": tool.function.name });
        if let Some(description) = &tool.function.description {
            function["description"] = json!(description);
        }
        if let Some(parameters) = &tool.function.parameters {
            function["parameters"] = serde_json::from_str(parameters)
                .unwrap_or_else(|e| panic!("tool parameters is not valid JSON: {e}"));
        }
        out.push(json!({ "function": function }));
    }
    serde_json::Value::Array(out)
}

impl Guest for Component {
    fn converse(
        component_name: String,
        inputs: Vec<ConversationInput>,
        options: Option<ConversationOptions>,
    ) -> Result<ConversationResponse, ConverseError> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0-alpha2/conversation/{}/converse",
            seg(&component_name)
        );

        let inputs_json: Vec<serde_json::Value> = inputs
            .iter()
            .map(|input| {
                let messages: Vec<serde_json::Value> =
                    input.messages.iter().map(message_json).collect();
                let mut object = json!({ "messages": messages });
                if let Some(scrub) = input.scrub_pii {
                    object["scrubPii"] = json!(scrub);
                }
                object
            })
            .collect();

        let mut body = json!({ "inputs": inputs_json });
        if let Some(options) = &options {
            if let Some(context_id) = &options.context_id {
                body["contextId"] = json!(context_id);
            }
            if let Some(parameters) = &options.parameters {
                body["parameters"] = serde_json::from_str(parameters)
                    .unwrap_or_else(|e| panic!("parameters is not valid JSON: {e}"));
            }
            if !options.metadata.is_empty() {
                body["metadata"] = serde_json::Value::Object(
                    options
                        .metadata
                        .iter()
                        .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                        .collect(),
                );
            }
            if let Some(scrub) = options.scrub_pii {
                body["scrubPii"] = json!(scrub);
            }
            if let Some(temperature) = options.temperature {
                body["temperature"] = json!(temperature);
            }
            if !options.tools.is_empty() {
                body["tools"] = tools_json(&options.tools);
            }
            if let Some(tool_choice) = &options.tool_choice {
                body["toolChoice"] = json!(tool_choice);
            }
            if let Some(response_format) = &options.response_format {
                body["responseFormat"] = serde_json::from_str(response_format)
                    .unwrap_or_else(|e| panic!("response-format is not valid JSON: {e}"));
            }
            if let Some(retention) = &options.prompt_cache_retention {
                body["promptCacheRetention"] = json!(retention);
            }
        }

        let response = sidecar
            .json(Method::POST, &path, &body)
            .map_err(converse_error)?;
        let parsed: ResponseJson = serde_json::from_slice(&response.body)
            .unwrap_or_else(|e| panic!("unexpected converse response: {e}"));
        Ok(parsed.into())
    }
}

#[derive(Deserialize)]
struct ResponseJson {
    #[serde(default)]
    outputs: Vec<OutputJson>,
    #[serde(default, rename = "contextId")]
    context_id: Option<String>,
}

#[derive(Deserialize)]
struct OutputJson {
    #[serde(default)]
    choices: Vec<ChoiceJson>,
    #[serde(default)]
    model: Option<String>,
    #[serde(default)]
    usage: Option<UsageJson>,
}

#[derive(Deserialize)]
struct ChoiceJson {
    #[serde(default, rename = "finishReason")]
    finish_reason: String,
    #[serde(default)]
    index: i64,
    #[serde(default)]
    message: Option<MessageJson>,
}

#[derive(Deserialize)]
struct MessageJson {
    #[serde(default)]
    content: String,
    #[serde(default, rename = "toolCalls")]
    tool_calls: Vec<ToolCallJson>,
}

#[derive(Deserialize)]
struct ToolCallJson {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    function: Option<ToolCallFunctionJson>,
}

#[derive(Default, Deserialize)]
struct ToolCallFunctionJson {
    #[serde(default)]
    name: String,
    #[serde(default)]
    arguments: String,
}

#[derive(Deserialize)]
struct UsageJson {
    #[serde(default, rename = "completionTokens")]
    completion_tokens: u64,
    #[serde(default, rename = "promptTokens")]
    prompt_tokens: u64,
    #[serde(default, rename = "totalTokens")]
    total_tokens: u64,
    #[serde(default, rename = "completionTokensDetails")]
    completion_tokens_details: Option<CompletionDetailsJson>,
    #[serde(default, rename = "promptTokensDetails")]
    prompt_tokens_details: Option<PromptDetailsJson>,
}

#[derive(Deserialize)]
struct PromptDetailsJson {
    #[serde(default, rename = "audioTokens")]
    audio_tokens: u64,
    #[serde(default, rename = "cachedTokens")]
    cached_tokens: u64,
}

#[derive(Deserialize)]
struct CompletionDetailsJson {
    #[serde(default, rename = "acceptedPredictionTokens")]
    accepted_prediction_tokens: u64,
    #[serde(default, rename = "audioTokens")]
    audio_tokens: u64,
    #[serde(default, rename = "reasoningTokens")]
    reasoning_tokens: u64,
    #[serde(default, rename = "rejectedPredictionTokens")]
    rejected_prediction_tokens: u64,
}

impl From<ResponseJson> for ConversationResponse {
    fn from(response: ResponseJson) -> Self {
        ConversationResponse {
            outputs: response.outputs.into_iter().map(Into::into).collect(),
            context_id: response.context_id,
        }
    }
}

impl From<OutputJson> for ConversationOutput {
    fn from(output: OutputJson) -> Self {
        ConversationOutput {
            choices: output.choices.into_iter().map(Into::into).collect(),
            model: output.model,
            usage: output.usage.map(Into::into),
        }
    }
}

impl From<ChoiceJson> for Choice {
    fn from(choice: ChoiceJson) -> Self {
        let message = choice.message.unwrap_or(MessageJson {
            content: String::new(),
            tool_calls: Vec::new(),
        });
        Choice {
            finish_reason: choice.finish_reason,
            index: choice.index,
            message: ResultMessage {
                content: message.content,
                tool_calls: message.tool_calls.into_iter().map(Into::into).collect(),
            },
        }
    }
}

impl From<ToolCallJson> for ToolCall {
    fn from(call: ToolCallJson) -> Self {
        let function = call.function.unwrap_or_default();
        ToolCall {
            id: call.id,
            function: ToolCallFunction {
                name: function.name,
                arguments: function.arguments,
            },
        }
    }
}

impl From<UsageJson> for Usage {
    fn from(usage: UsageJson) -> Self {
        Usage {
            completion_tokens: usage.completion_tokens,
            prompt_tokens: usage.prompt_tokens,
            total_tokens: usage.total_tokens,
            completion_tokens_details: usage.completion_tokens_details.map(|d| {
                CompletionTokensDetails {
                    accepted_prediction_tokens: d.accepted_prediction_tokens,
                    audio_tokens: d.audio_tokens,
                    reasoning_tokens: d.reasoning_tokens,
                    rejected_prediction_tokens: d.rejected_prediction_tokens,
                }
            }),
            prompt_tokens_details: usage.prompt_tokens_details.map(|d| PromptTokensDetails {
                audio_tokens: d.audio_tokens,
                cached_tokens: d.cached_tokens,
            }),
        }
    }
}
