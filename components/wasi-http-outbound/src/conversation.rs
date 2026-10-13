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
        Ok(parse_response(&response.body))
    }
}

/// Parse daprd's converse response body. A body that does not parse is a
/// malformed response (tier 3) and panics.
fn parse_response(body: &[u8]) -> ConversationResponse {
    let parsed: ResponseJson = serde_json::from_slice(body)
        .unwrap_or_else(|e| panic!("unexpected converse response: {e}"));
    parsed.into()
}

/// Deserializers for proto `int64`/`uint64` fields. daprd renders the converse
/// response with protojson, which writes 64-bit integers as JSON *strings*
/// (`"totalTokens":"4"`) — see
/// https://protobuf.dev/programming-guides/json/. Strings are what daprd
/// sends; plain JSON numbers are accepted too (protojson parsers accept both).
mod proto_int {
    use serde::de::Error;
    use serde::{Deserialize, Deserializer};

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumberOrString<T> {
        Number(T),
        String(String),
    }

    fn parse<'de, D, T>(deserializer: D) -> Result<T, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de> + std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        match NumberOrString::<T>::deserialize(deserializer)? {
            NumberOrString::Number(n) => Ok(n),
            NumberOrString::String(s) => s.parse().map_err(D::Error::custom),
        }
    }

    pub fn u64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        parse(deserializer)
    }

    pub fn i64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<i64, D::Error> {
        parse(deserializer)
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
    #[serde(default, deserialize_with = "proto_int::i64")]
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
    #[serde(
        default,
        rename = "completionTokens",
        deserialize_with = "proto_int::u64"
    )]
    completion_tokens: u64,
    #[serde(default, rename = "promptTokens", deserialize_with = "proto_int::u64")]
    prompt_tokens: u64,
    #[serde(default, rename = "totalTokens", deserialize_with = "proto_int::u64")]
    total_tokens: u64,
    #[serde(default, rename = "completionTokensDetails")]
    completion_tokens_details: Option<CompletionDetailsJson>,
    #[serde(default, rename = "promptTokensDetails")]
    prompt_tokens_details: Option<PromptDetailsJson>,
}

#[derive(Deserialize)]
struct PromptDetailsJson {
    #[serde(default, rename = "audioTokens", deserialize_with = "proto_int::u64")]
    audio_tokens: u64,
    #[serde(default, rename = "cachedTokens", deserialize_with = "proto_int::u64")]
    cached_tokens: u64,
}

#[derive(Deserialize)]
struct CompletionDetailsJson {
    #[serde(
        default,
        rename = "acceptedPredictionTokens",
        deserialize_with = "proto_int::u64"
    )]
    accepted_prediction_tokens: u64,
    #[serde(default, rename = "audioTokens", deserialize_with = "proto_int::u64")]
    audio_tokens: u64,
    #[serde(
        default,
        rename = "reasoningTokens",
        deserialize_with = "proto_int::u64"
    )]
    reasoning_tokens: u64,
    #[serde(
        default,
        rename = "rejectedPredictionTokens",
        deserialize_with = "proto_int::u64"
    )]
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

#[cfg(test)]
mod tests {
    use super::parse_response;

    /// daprd 1.17.7's answer for the `conversation.echo` component, verbatim.
    /// daprd renders the response with protojson, which writes the proto
    /// `uint64`/`int64` fields (token counts, the choice index) as JSON
    /// strings.
    const ECHO_RESPONSE: &str = r#"{"outputs":[{"choices":[{"finishReason":"stop", "message":{"content":"hello world"}}], "usage":{"completionTokens":"2", "promptTokens":"2", "totalTokens":"4"}}]}"#;

    #[test]
    fn accepts_protojson_string_integers() {
        let response = parse_response(ECHO_RESPONSE.as_bytes());
        let output = &response.outputs[0];
        assert_eq!(output.choices[0].message.content, "hello world");
        let usage = output.usage.as_ref().expect("usage present");
        assert_eq!(usage.completion_tokens, 2);
        assert_eq!(usage.prompt_tokens, 2);
        assert_eq!(usage.total_tokens, 4);
    }

    #[test]
    fn accepts_string_integers_in_details_and_index() {
        let response = parse_response(
            br#"{"outputs":[{"choices":[{"finishReason":"stop","index":"1"}],"usage":{
                "promptTokensDetails":{"audioTokens":"1","cachedTokens":"2"},
                "completionTokensDetails":{"acceptedPredictionTokens":"3","audioTokens":"4",
                    "reasoningTokens":"5","rejectedPredictionTokens":"6"}}}]}"#,
        );
        let output = &response.outputs[0];
        assert_eq!(output.choices[0].index, 1);
        let usage = output.usage.as_ref().expect("usage present");
        let prompt = usage
            .prompt_tokens_details
            .as_ref()
            .expect("prompt details");
        assert_eq!((prompt.audio_tokens, prompt.cached_tokens), (1, 2));
        let completion = usage
            .completion_tokens_details
            .as_ref()
            .expect("completion details");
        assert_eq!(
            (
                completion.accepted_prediction_tokens,
                completion.audio_tokens,
                completion.reasoning_tokens,
                completion.rejected_prediction_tokens
            ),
            (3, 4, 5, 6)
        );
    }

    #[test]
    fn still_accepts_json_numbers() {
        let response = parse_response(
            br#"{"outputs":[{"choices":[{"index":2}],"usage":{"completionTokens":5,"promptTokens":3,"totalTokens":8}}]}"#,
        );
        let output = &response.outputs[0];
        assert_eq!(output.choices[0].index, 2);
        let usage = output.usage.as_ref().expect("usage present");
        assert_eq!(
            (
                usage.completion_tokens,
                usage.prompt_tokens,
                usage.total_tokens
            ),
            (5, 3, 8)
        );
    }
}
