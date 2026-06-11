//! The Dapr app-callback gRPC service: the sidecar connects to this server
//! to discover topic subscriptions and deliver pub/sub messages, which are
//! forwarded to the guest component's exported `topic-handler` interface.

use std::sync::Arc;

use dapr_sdk::dapr::proto::common::v1::{InvokeRequest, InvokeResponse};
use dapr_sdk::dapr::proto::runtime::v1::app_callback_server::AppCallback;
use dapr_sdk::dapr::proto::runtime::v1::{
    topic_event_response::TopicEventResponseStatus, BindingEventRequest, BindingEventResponse,
    ListInputBindingsResponse, ListTopicSubscriptionsResponse, TopicEventRequest,
    TopicEventResponse, TopicSubscription,
};
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

use crate::bindings::exports::dapr::client::topic_handler;
use crate::runner::GuestRunner;

pub struct GuestCallbackService {
    runner: Arc<Mutex<GuestRunner>>,
    /// Captured once at startup, before the server starts.
    subscriptions: Vec<topic_handler::TopicSubscription>,
}

impl GuestCallbackService {
    pub fn new(
        runner: Arc<Mutex<GuestRunner>>,
        subscriptions: Vec<topic_handler::TopicSubscription>,
    ) -> Self {
        Self {
            runner,
            subscriptions,
        }
    }
}

#[tonic::async_trait]
impl AppCallback for GuestCallbackService {
    async fn on_invoke(
        &self,
        _request: Request<InvokeRequest>,
    ) -> Result<Response<InvokeResponse>, Status> {
        Err(Status::unimplemented(
            "service invocation into wasm components is not supported yet",
        ))
    }

    async fn list_topic_subscriptions(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ListTopicSubscriptionsResponse>, Status> {
        let subscriptions = self
            .subscriptions
            .iter()
            .map(|s| TopicSubscription {
                pubsub_name: s.pubsub_name.clone(),
                topic: s.topic.clone(),
                metadata: s.metadata.iter().cloned().collect(),
                ..Default::default()
            })
            .collect();
        Ok(Response::new(ListTopicSubscriptionsResponse {
            subscriptions,
        }))
    }

    async fn on_topic_event(
        &self,
        request: Request<TopicEventRequest>,
    ) -> Result<Response<TopicEventResponse>, Status> {
        let request = request.into_inner();
        let event = topic_handler::TopicEvent {
            id: request.id,
            source: request.source,
            event_type: request.r#type,
            spec_version: request.spec_version,
            data_content_type: request.data_content_type,
            data: request.data,
            topic: request.topic,
            pubsub_name: request.pubsub_name,
            path: request.path,
        };

        let response = self
            .runner
            .lock()
            .await
            .on_topic_event(&event)
            .await
            .map_err(|e| Status::internal(format!("guest failed to handle topic event: {e:#}")))?;

        let status = match response {
            topic_handler::TopicEventResponse::Success => TopicEventResponseStatus::Success,
            topic_handler::TopicEventResponse::Retry => TopicEventResponseStatus::Retry,
            topic_handler::TopicEventResponse::Drop => TopicEventResponseStatus::Drop,
        };
        Ok(Response::new(TopicEventResponse {
            status: status as i32,
        }))
    }

    async fn list_input_bindings(
        &self,
        _request: Request<()>,
    ) -> Result<Response<ListInputBindingsResponse>, Status> {
        Ok(Response::new(ListInputBindingsResponse {
            bindings: Vec::new(),
        }))
    }

    async fn on_binding_event(
        &self,
        _request: Request<BindingEventRequest>,
    ) -> Result<Response<BindingEventResponse>, Status> {
        Err(Status::unimplemented(
            "input bindings are not supported yet",
        ))
    }
}
