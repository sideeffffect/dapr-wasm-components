//! Conversation (alpha2) — https://docs.dapr.io/reference/api/conversation_api/

use serde::Deserialize;
use serde_json::json;
use wstd::http::Method;

use crate::exports::conversation::{
    ConversationInput, ConversationOptions, ConversationResult, ConversationRole, Guest,
};
use crate::sidecar::{seg, Sidecar};
use crate::types::Error;
use crate::Component;

fn role_key(role: ConversationRole) -> &'static str {
    match role {
        ConversationRole::User => "ofUser",
        ConversationRole::System => "ofSystem",
        ConversationRole::Assistant => "ofAssistant",
        ConversationRole::Tool => "ofTool",
        ConversationRole::Developer => "ofDeveloper",
    }
}

impl Guest for Component {
    fn converse(
        component_name: String,
        inputs: Vec<ConversationInput>,
        options: Option<ConversationOptions>,
    ) -> Result<Vec<ConversationResult>, Error> {
        let sidecar = Sidecar::from_env();
        let path = format!(
            "/v1.0-alpha2/conversation/{}/converse",
            seg(&component_name)
        );

        let inputs_json: Vec<serde_json::Value> = inputs
            .iter()
            .map(|input| {
                let messages: Vec<serde_json::Value> = input
                    .messages
                    .iter()
                    .map(|message| {
                        json!({
                            role_key(message.role): { "content": [ { "text": message.content } ] }
                        })
                    })
                    .collect();
                let mut object = json!({ "messages": messages });
                if let Some(scrub) = input.scrub_pii {
                    object["scrubPii"] = json!(scrub);
                }
                object
            })
            .collect();

        let mut body = json!({ "inputs": inputs_json });
        if let Some(options) = &options {
            if let Some(temperature) = options.temperature {
                body["temperature"] = json!(temperature);
            }
            if let Some(scrub) = options.scrub_pii {
                body["scrubPii"] = json!(scrub);
            }
            if let Some(parameters) = &options.parameters {
                body["parameters"] = serde_json::from_str(parameters).map_err(|e| {
                    Error::InvalidArgument(format!("parameters is not valid JSON: {e}"))
                })?;
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
            if let Some(context_id) = &options.context_id {
                body["contextId"] = json!(context_id);
            }
        }

        let response = sidecar.json(Method::POST, &path, &body)?;

        #[derive(Deserialize)]
        struct ConverseJson {
            #[serde(default)]
            outputs: Vec<OutputJson>,
        }
        #[derive(Deserialize)]
        struct OutputJson {
            #[serde(default)]
            choices: Vec<ChoiceJson>,
        }
        #[derive(Deserialize)]
        struct ChoiceJson {
            #[serde(default, rename = "finishReason")]
            finish_reason: Option<String>,
            #[serde(default)]
            message: Option<MessageJson>,
        }
        #[derive(Deserialize)]
        struct MessageJson {
            #[serde(default)]
            content: Option<String>,
        }

        let parsed: ConverseJson = serde_json::from_slice(&response.body)
            .map_err(|e| Error::Internal(format!("unexpected converse response: {e}")))?;
        Ok(parsed
            .outputs
            .into_iter()
            .flat_map(|output| output.choices)
            .map(|choice| ConversationResult {
                content: choice
                    .message
                    .and_then(|message| message.content)
                    .unwrap_or_default(),
                finish_reason: choice.finish_reason,
            })
            .collect())
    }
}
