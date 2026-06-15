//! Conversation (alpha2) over gRPC — `ConverseAlpha2`.
//!
//! Full alpha2 surface: content-part messages with role wrappers, tool
//! definitions and tool calls, structured-output schema, prompt-cache
//! retention, and per-token usage in the response.

use std::collections::HashMap;

use crate::anyjson::{json_to_struct, pack_json, pack_protojson_wrapper};
use crate::exports::conversation::{
    Choice, CompletionTokensDetails, ContentPart, ConversationError, ConversationInput,
    ConversationOptions, ConversationOutput, ConversationResponse, ConverseError, Guest, Message,
    PromptTokensDetails, ResultMessage, Tool, ToolCall, ToolCallFunction, Usage,
};
use crate::proto::runtime as pb;
use crate::sidecar::{metadata_map, DaprFailure, Sidecar};
use crate::Component;

fn conversation_error(f: DaprFailure) -> ConversationError {
    if f.is_permission() {
        ConversationError::PermissionDenied(f.message)
    } else {
        ConversationError::ComponentNotFound(f.message)
    }
}

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

/// Parse a JSON object into a `Struct`; malformed JSON the app supplied is a
/// programming error, so it traps.
fn json_to_struct_or_panic(json_text: &str) -> prost_types::Struct {
    json_to_struct(json_text).unwrap_or_else(|e| panic!("{e}"))
}

fn content_pb(content: &[ContentPart]) -> Vec<pb::ConversationMessageContent> {
    content
        .iter()
        .map(|part| pb::ConversationMessageContent {
            text: part.text.clone(),
        })
        .collect()
}

fn tool_calls_pb(calls: &[ToolCall]) -> Vec<pb::ConversationToolCalls> {
    use pb::conversation_tool_calls::ToolTypes;
    calls
        .iter()
        .map(|call| pb::ConversationToolCalls {
            id: call.id.clone(),
            tool_types: Some(ToolTypes::Function(pb::ConversationToolCallsOfFunction {
                name: call.function.name.clone(),
                arguments: call.function.arguments.clone(),
            })),
        })
        .collect()
}

fn message_pb(message: &Message) -> pb::ConversationMessage {
    use pb::conversation_message::MessageTypes;
    let message_types = match message {
        Message::Developer(m) => MessageTypes::OfDeveloper(pb::ConversationMessageOfDeveloper {
            name: m.name.clone(),
            content: content_pb(&m.content),
        }),
        Message::System(m) => MessageTypes::OfSystem(pb::ConversationMessageOfSystem {
            name: m.name.clone(),
            content: content_pb(&m.content),
        }),
        Message::User(m) => MessageTypes::OfUser(pb::ConversationMessageOfUser {
            name: m.name.clone(),
            content: content_pb(&m.content),
        }),
        Message::Assistant(m) => MessageTypes::OfAssistant(pb::ConversationMessageOfAssistant {
            name: m.name.clone(),
            content: content_pb(&m.content),
            tool_calls: tool_calls_pb(&m.tool_calls),
        }),
        Message::Tool(m) => MessageTypes::OfTool(pb::ConversationMessageOfTool {
            tool_id: m.tool_id.clone(),
            name: m.name.clone(),
            content: content_pb(&m.content),
        }),
    };
    pb::ConversationMessage {
        message_types: Some(message_types),
    }
}

fn input_pb(input: &ConversationInput) -> pb::ConversationInputAlpha2 {
    pb::ConversationInputAlpha2 {
        messages: input.messages.iter().map(message_pb).collect(),
        scrub_pii: input.scrub_pii,
    }
}

fn tools_pb(tools: &[Tool]) -> Vec<pb::ConversationTools> {
    use pb::conversation_tools::ToolTypes;
    tools
        .iter()
        .map(|tool| {
            let parameters = tool
                .function
                .parameters
                .as_deref()
                .map(json_to_struct_or_panic);
            pb::ConversationTools {
                tool_types: Some(ToolTypes::Function(pb::ConversationToolsFunction {
                    name: tool.function.name.clone(),
                    description: tool.function.description.clone(),
                    parameters,
                })),
            }
        })
        .collect()
}

/// `prompt-cache-retention` is a Go-style duration string ("24h") in the WIT
/// contract; the proto wants a `google.protobuf.Duration`. The HTTP provider
/// passes the string through verbatim — humantime's grammar is close to but
/// not exactly Go's `time.ParseDuration`.
fn duration_pb(text: &str) -> prost_types::Duration {
    let duration = humantime::parse_duration(text)
        .unwrap_or_else(|e| panic!("invalid prompt-cache-retention {text:?}: {e}"));
    prost_types::Duration {
        seconds: i64::try_from(duration.as_secs())
            .unwrap_or_else(|_| panic!("prompt-cache-retention {text:?} is out of range")),
        nanos: duration.subsec_nanos() as i32,
    }
}

/// The WIT contract carries `options.parameters` as one JSON object; the
/// proto wants `map<string, Any>`. Values written in the protojson wrapper
/// form the Dapr docs use (`{"@type": ".../google.protobuf.Int64Value",
/// "value": "100"}`) are packed as that wrapper — like daprd decodes them
/// from the HTTP API; anything else is packed as
/// `Any(google.protobuf.Value)`.
fn parameters_pb(parameters: &str) -> HashMap<String, prost_types::Any> {
    let parsed: serde_json::Value = serde_json::from_str(parameters)
        .unwrap_or_else(|e| panic!("parameters is not valid JSON: {e}"));
    let serde_json::Value::Object(entries) = parsed else {
        panic!("parameters must be a JSON object");
    };
    entries
        .iter()
        .map(|(key, value)| {
            let any = match value {
                serde_json::Value::Object(object) => match pack_protojson_wrapper(object) {
                    Some(wrapped) => wrapped.unwrap_or_else(|e| panic!("{e}")),
                    None => pack_json(&value.to_string()).unwrap_or_else(|e| panic!("{e}")),
                },
                other => pack_json(&other.to_string()).unwrap_or_else(|e| panic!("{e}")),
            };
            (key.clone(), any)
        })
        .collect()
}

fn tool_call_wit(call: pb::ConversationToolCalls) -> ToolCall {
    use pb::conversation_tool_calls::ToolTypes;
    let function = match call.tool_types {
        Some(ToolTypes::Function(function)) => ToolCallFunction {
            name: function.name,
            arguments: function.arguments,
        },
        None => ToolCallFunction {
            name: String::new(),
            arguments: String::new(),
        },
    };
    ToolCall {
        id: call.id,
        function,
    }
}

fn usage_wit(usage: pb::ConversationResultAlpha2CompletionUsage) -> Usage {
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

fn choice_wit(choice: pb::ConversationResultChoices) -> Choice {
    let message = choice.message.unwrap_or_default();
    Choice {
        finish_reason: choice.finish_reason,
        index: choice.index,
        message: ResultMessage {
            content: message.content,
            tool_calls: message.tool_calls.into_iter().map(tool_call_wit).collect(),
        },
    }
}

fn output_wit(output: pb::ConversationResultAlpha2) -> ConversationOutput {
    ConversationOutput {
        choices: output.choices.into_iter().map(choice_wit).collect(),
        model: output.model,
        usage: output.usage.map(usage_wit),
    }
}

impl Guest for Component {
    fn converse(
        component_name: String,
        inputs: Vec<ConversationInput>,
        options: Option<ConversationOptions>,
    ) -> Result<ConversationResponse, ConverseError> {
        let sidecar = Sidecar::from_env();
        let inputs: Vec<pb::ConversationInputAlpha2> = inputs.iter().map(input_pb).collect();
        let options = options.unwrap_or(ConversationOptions {
            context_id: None,
            parameters: None,
            metadata: Vec::new(),
            scrub_pii: None,
            temperature: None,
            tools: Vec::new(),
            tool_choice: None,
            response_format: None,
            prompt_cache_retention: None,
        });
        let parameters = match &options.parameters {
            Some(parameters) => parameters_pb(parameters),
            None => HashMap::new(),
        };
        let tools = tools_pb(&options.tools);
        let response_format = options
            .response_format
            .as_deref()
            .map(json_to_struct_or_panic);
        let prompt_cache_retention = options.prompt_cache_retention.as_deref().map(duration_pb);
        let metadata = metadata_map(&options.metadata);
        let response = sidecar
            .unary(
                pb::ConversationRequestAlpha2 {
                    name: component_name,
                    context_id: options.context_id,
                    inputs,
                    parameters,
                    metadata,
                    scrub_pii: options.scrub_pii,
                    temperature: options.temperature,
                    tools,
                    tool_choice: options.tool_choice,
                    response_format,
                    prompt_cache_retention,
                },
                |mut client, request| async move { client.converse_alpha2(request).await },
            )
            .map_err(converse_error)?;
        Ok(ConversationResponse {
            outputs: response.outputs.into_iter().map(output_wit).collect(),
            context_id: response.context_id,
        })
    }
}
