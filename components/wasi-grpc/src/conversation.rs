//! Conversation (alpha2) over gRPC — `ConverseAlpha2`.
//!
//! Text subset of the alpha2 API, like the wasi-http provider: tool
//! calling, structured outputs and prompt-cache retention are not exposed
//! by the WIT contract. Tool-role messages travel as `OfTool` with an
//! empty tool name (proto3 default — wire-identical to what the HTTP
//! provider's `ofTool` JSON decodes to); validation is the backend's.

use std::collections::HashMap;

use crate::anyjson::{pack_json, pack_protojson_wrapper};
use crate::exports::conversation::{
    ConversationInput, ConversationMessage, ConversationOptions, ConversationResult,
    ConversationRole, Guest,
};
use crate::proto::runtime as pb;
use crate::proto::runtime::conversation_message::MessageTypes;
use crate::sidecar::{metadata_map, opt_string, Sidecar};
use crate::types::Error;
use crate::Component;

fn message_pb(message: &ConversationMessage) -> Result<pb::ConversationMessage, Error> {
    let content = vec![pb::ConversationMessageContent {
        text: message.content.clone(),
    }];
    let message_types = match message.role {
        ConversationRole::User => MessageTypes::OfUser(pb::ConversationMessageOfUser {
            name: None,
            content,
        }),
        ConversationRole::System => MessageTypes::OfSystem(pb::ConversationMessageOfSystem {
            name: None,
            content,
        }),
        ConversationRole::Assistant => {
            MessageTypes::OfAssistant(pb::ConversationMessageOfAssistant {
                name: None,
                content,
                tool_calls: Vec::new(),
            })
        }
        ConversationRole::Developer => {
            MessageTypes::OfDeveloper(pb::ConversationMessageOfDeveloper {
                name: None,
                content,
            })
        }
        ConversationRole::Tool => MessageTypes::OfTool(pb::ConversationMessageOfTool {
            tool_id: None,
            name: String::new(),
            content,
        }),
    };
    Ok(pb::ConversationMessage {
        message_types: Some(message_types),
    })
}

fn input_pb(input: &ConversationInput) -> Result<pb::ConversationInputAlpha2, Error> {
    Ok(pb::ConversationInputAlpha2 {
        messages: input
            .messages
            .iter()
            .map(message_pb)
            .collect::<Result<_, _>>()?,
        scrub_pii: input.scrub_pii,
    })
}

/// The WIT contract carries `options.parameters` as one JSON object; the
/// proto wants `map<string, Any>`. Values written in the protojson wrapper
/// form the Dapr docs use (`{"@type": ".../google.protobuf.Int64Value",
/// "value": "100"}`) are packed as that wrapper — like daprd decodes them
/// from the HTTP API; anything else is packed as
/// `Any(google.protobuf.Value)`.
fn parameters_pb(parameters: &str) -> Result<HashMap<String, prost_types::Any>, Error> {
    let parsed: serde_json::Value = serde_json::from_str(parameters)
        .map_err(|e| Error::InvalidArgument(format!("parameters is not valid JSON: {e}")))?;
    let serde_json::Value::Object(entries) = parsed else {
        return Err(Error::InvalidArgument(
            "parameters must be a JSON object".to_string(),
        ));
    };
    entries
        .iter()
        .map(|(key, value)| {
            let any = match value {
                serde_json::Value::Object(object) => match pack_protojson_wrapper(object) {
                    Some(wrapped) => wrapped?,
                    None => pack_json(&value.to_string())?,
                },
                other => pack_json(&other.to_string())?,
            };
            Ok((key.clone(), any))
        })
        .collect()
}

impl Guest for Component {
    fn converse(
        component_name: String,
        inputs: Vec<ConversationInput>,
        options: Option<ConversationOptions>,
    ) -> Result<Vec<ConversationResult>, Error> {
        let sidecar = Sidecar::from_env()?;
        let inputs: Vec<pb::ConversationInputAlpha2> =
            inputs.iter().map(input_pb).collect::<Result<_, _>>()?;
        let options = options.unwrap_or(ConversationOptions {
            temperature: None,
            scrub_pii: None,
            parameters: None,
            metadata: Vec::new(),
            context_id: None,
        });
        let parameters = match &options.parameters {
            Some(parameters) => parameters_pb(parameters)?,
            None => HashMap::new(),
        };
        let response = sidecar.unary(
            pb::ConversationRequestAlpha2 {
                name: component_name,
                context_id: options.context_id,
                inputs,
                parameters,
                metadata: metadata_map(&options.metadata),
                scrub_pii: options.scrub_pii,
                temperature: options.temperature,
                // Not in the WIT contract (text subset).
                tools: Vec::new(),
                tool_choice: None,
                response_format: None,
                prompt_cache_retention: None,
            },
            |mut client, request| async move { client.converse_alpha2(request).await },
        )?;
        // Flatten outputs[].choices[] in order; the response context-id is
        // dropped, as in the wasi-http provider.
        Ok(response
            .outputs
            .into_iter()
            .flat_map(|output| output.choices)
            .map(|choice| ConversationResult {
                content: choice
                    .message
                    .map(|message| message.content)
                    .unwrap_or_default(),
                finish_reason: opt_string(choice.finish_reason),
            })
            .collect())
    }
}
