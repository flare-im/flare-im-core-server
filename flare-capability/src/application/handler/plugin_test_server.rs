//! A plugin endpoint for handler tests: records every call (operation, ticket, payload) and answers
//! with `respond(operation, payload)`. An error `grpc:<code>` or `grpc:<code>:<reason>` (code
//! `permission_denied`, `unauthenticated`, `failed_precondition`, `unavailable`) answers that gRPC
//! status instead of `ok: false`, with `reason` as its `ErrorDetail.reason`.
use flare_grpc_proto::capability::{
    GenericRequest, GenericResponse,
    extension_plugin_server::{ExtensionPlugin, ExtensionPluginServer},
};
use flare_im_capability_core::plugin::invocation::PLUGIN_CALL_HEADER;
use serde_json::Value;
use std::sync::{Arc, Mutex};
use tonic::{Request, Response, Status};

pub(crate) type Respond = dyn Fn(&str, &Value) -> Result<Value, String> + Send + Sync;

#[derive(Debug, Clone)]
pub(crate) struct RecordedCall {
    pub operation: String,
    pub ticket: String,
    pub payload: Value,
    pub metadata: std::collections::HashMap<String, String>,
}

struct Endpoint {
    calls: Arc<Mutex<Vec<RecordedCall>>>,
    respond: Arc<Respond>,
}

#[tonic::async_trait]
impl ExtensionPlugin for Endpoint {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        let ticket = request
            .metadata()
            .get(PLUGIN_CALL_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = request.into_inner();
        let payload: Value = body
            .payload
            .as_ref()
            .and_then(|any| serde_json::from_slice(&any.value).ok())
            .unwrap_or(Value::Null);
        self.calls.lock().unwrap().push(RecordedCall {
            operation: body.operation.clone(),
            ticket,
            payload: payload.clone(),
            metadata: body.metadata.clone(),
        });
        let (ok, data, error_code) = match (self.respond)(&body.operation, &payload) {
            Ok(data) => (true, data, String::new()),
            Err(code) if code.starts_with("grpc:") => return Err(scripted_status(&code[5..])),
            Err(code) => (false, Value::Null, code),
        };
        Ok(Response::new(GenericResponse {
            ok,
            request_id: body.request_id,
            payload: Some(prost_types::Any {
                type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
                value: serde_json::to_vec(&data).unwrap(),
            }),
            error_code,
            error_message: String::new(),
        }))
    }
}

/// `<code>[:<reason>]` as a status (see the module docs).
fn scripted_status(spec: &str) -> Status {
    use flare_server_core::error::{ErrorBuilder, ErrorCode};
    let (code, reason) = spec.split_once(':').unwrap_or((spec, ""));
    let code = match code {
        "permission_denied" => tonic::Code::PermissionDenied,
        "unauthenticated" => tonic::Code::Unauthenticated,
        "failed_precondition" => tonic::Code::FailedPrecondition,
        _ => tonic::Code::Unavailable,
    };
    if reason.is_empty() {
        return Status::new(code, "scripted refusal");
    }
    flare_server_core::error::grpc::with_code(
        Status::from(ErrorBuilder::new(ErrorCode::PermissionDenied, reason).build_error()),
        code,
    )
}

pub(crate) struct PluginServer {
    pub authority: String,
    pub calls: Arc<Mutex<Vec<RecordedCall>>>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl PluginServer {
    pub(crate) async fn start(respond: Arc<Respond>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let (stop, rx) = tokio::sync::oneshot::channel();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(ExtensionPluginServer::new(Endpoint {
                    calls: calls.clone(),
                    respond,
                }))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        rx.await.ok();
                    },
                ),
        );
        Self {
            authority: format!("http://{addr}"),
            calls,
            stop: Some(stop),
        }
    }
    pub(crate) fn calls(&self) -> Vec<RecordedCall> {
        self.calls.lock().unwrap().clone()
    }
}

impl Drop for PluginServer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
