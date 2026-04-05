//! Tonic 框架的 RPC 客户端实现
//!
//! 本模块提供基于 tonic 的 RPC 客户端具体实现。

use crate::domain::model::MessageSyncResult;
use crate::domain::repository::MessageProvider;
use crate::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::storage::QueryMessagesRequest;
use flare_server_core::client::set_context_metadata;
use flare_server_core::context::Context;
use flare_im_core::ServiceClient;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::{Channel, Endpoint};
use tonic::Request;

/// 存储服务客户端（基于 tonic）
///
/// 使用 tonic 框架实现的存储服务客户端，
/// 通过 gRPC 调用下游的 Storage Reader 服务。
pub struct StorageReaderClient {
    service_name: String,
    service_client: Arc<Mutex<Option<ServiceClient>>>,
}

fn grpc_call_failed(op: &str, status: tonic::Status) -> crate::error::FlareError {
    ErrorBuilder::new(
        ErrorCode::ServiceUnavailable,
        format!("storage reader {} failed", op),
    )
    .details(status.to_string())
    .build_error()
}

impl StorageReaderClient {
    /// 创建新的存储服务客户端
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            service_client: Arc::new(Mutex::new(None)),
        }
    }

    /// 使用 ServiceClient 创建新的存储服务客户端（推荐）
    pub fn with_service_client(service_client: ServiceClient) -> Self {
        Self {
            service_name: String::new(),
            service_client: Arc::new(Mutex::new(Some(service_client))),
        }
    }

    async fn client(&self) -> Result<StorageReaderServiceClient<Channel>> {
        let mut service_client_guard = self.service_client.lock().await;
        if service_client_guard.is_none() {
            if self.service_name.is_empty() {
                return Err(
                    ErrorBuilder::new(
                        ErrorCode::ConfigurationError,
                        "storage_reader_service is not configured",
                    )
                    .build_error(),
                );
            }

            let discover = flare_im_core::discovery::create_discover(&self.service_name)
                .await
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::ServiceUnavailable, "create service discover")
                })?;

            if let Some(discover) = discover {
                *service_client_guard = Some(ServiceClient::new(discover));
            } else {
                let addr = std::env::var("STORAGE_READER_GRPC_ADDR")
                    .ok()
                    .unwrap_or_else(|| "127.0.0.1:60083".to_string());
                let endpoint = Endpoint::from_shared(format!("http://{}", addr))
                    .map_err(|e| {
                        map_infra_error(e, ErrorCode::ConfigurationError, "create endpoint")
                    })?;
                let channel = endpoint.connect().await.map_err(|e| {
                    map_infra_error(e, ErrorCode::NetworkError, "connect storage reader")
                })?;
                tracing::warn!(address = %addr, "Using STORAGE_READER_GRPC_ADDR fallback");
                return Ok(StorageReaderServiceClient::new(channel));
            }
        }

        let service_client = service_client_guard.as_mut().ok_or_else(|| {
            ErrorBuilder::new(ErrorCode::ConfigurationError, "service client not initialized")
                .build_error()
        })?;
        let channel = service_client
            .get_channel()
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::ServiceUnavailable,
                    "get channel from service discovery",
                )
            })?;

        tracing::debug!("Got channel for storage reader service from service discovery");

        Ok(StorageReaderServiceClient::new(channel))
    }

    fn last_timestamp(messages: &[flare_proto::common::Message]) -> Option<i64> {
        messages
            .last()
            .and_then(|msg| msg.timestamp.as_ref())
            .map(|ts| ts.seconds * 1_000 + (ts.nanos as i64 / 1_000_000))
    }

    fn build_request(
        _ctx: &Context,
        conversation_id: &str,
        since_ts: i64,
        cursor: Option<&str>,
        limit: i32,
    ) -> QueryMessagesRequest {
        QueryMessagesRequest {
            conversation_id: conversation_id.to_string(),
            start_time: since_ts,
            end_time: 0,
            limit,
            cursor: cursor.unwrap_or_default().to_string(),
            pagination: None,
        }
    }

    fn last_seq(messages: &[flare_proto::common::Message]) -> Option<i64> {
        messages
            .last()
            .and_then(|msg| flare_im_core::utils::extract_seq_from_message(msg))
    }

    fn map_response(resp: flare_grpc_proto::storage::QueryMessagesResponse) -> MessageSyncResult {
        let server_cursor_ts = Self::last_timestamp(&resp.messages);
        let server_cursor_seq = Self::last_seq(&resp.messages);
        MessageSyncResult {
            messages: resp.messages,
            next_cursor: if resp.next_cursor.is_empty() {
                None
            } else {
                Some(resp.next_cursor)
            },
            server_cursor_ts,
            server_cursor_seq,
        }
    }
}

impl MessageProvider for StorageReaderClient {
    async fn sync_messages(
        &self,
        ctx: &Context,
        conversation_id: &str,
        since_ts: i64,
        cursor: Option<&str>,
        limit: i32,
    ) -> Result<MessageSyncResult> {
        let mut client = self.client().await?;
        let mut request = Request::new(Self::build_request(
            ctx,
            conversation_id,
            since_ts,
            cursor,
            limit,
        ));
        set_context_metadata(&mut request, ctx);
        let response = client
            .query_messages(request)
            .await
            .map_err(|e| grpc_call_failed("query_messages", e))?
            .into_inner();
        Ok(Self::map_response(response))
    }

    async fn recent_messages(
        &self,
        ctx: &Context,
        conversation_ids: &[String],
        limit_per_session: i32,
        client_cursor: &HashMap<String, i64>,
    ) -> Result<Vec<flare_proto::common::Message>> {
        use tokio::task::JoinSet;

        let mut join_set = JoinSet::new();
        let service_name = self.service_name.clone();
        let service_client = Arc::clone(&self.service_client);
        let ctx = ctx.clone();

        for conversation_id in conversation_ids {
            let conversation_id = conversation_id.clone();
            let since_ts = client_cursor.get(&conversation_id).copied().unwrap_or(0);
            let limit = limit_per_session;
            let service_name = service_name.clone();
            let service_client = Arc::clone(&service_client);
            let task_ctx = ctx.clone();

            join_set.spawn(async move {
                let mut service_client_guard = service_client.lock().await;
                if service_client_guard.is_none() && !service_name.is_empty() {
                    let discover = flare_im_core::discovery::create_discover(&service_name)
                        .await
                        .map_err(|e| {
                            map_infra_error(
                                e,
                                ErrorCode::ServiceUnavailable,
                                "create service discover",
                            )
                        })?;

                    if let Some(discover) = discover {
                        *service_client_guard = Some(ServiceClient::new(discover));
                    }
                }

                let channel: Channel = if let Some(service_client) = service_client_guard.as_mut() {
                    match service_client.get_channel().await {
                        Ok(ch) => ch,
                        Err(_e) => {
                            let addr = std::env::var("STORAGE_READER_GRPC_ADDR")
                                .ok()
                                .unwrap_or_else(|| "127.0.0.1:50091".to_string());
                            let endpoint = Endpoint::from_shared(format!("http://{}", addr))
                                .map_err(|err| {
                                    map_infra_error(err, ErrorCode::ConfigurationError, "endpoint")
                                })?;
                            endpoint.connect().await.map_err(|err| {
                                map_infra_error(err, ErrorCode::NetworkError, "connect")
                            })?
                        }
                    }
                } else {
                    let addr = std::env::var("STORAGE_READER_GRPC_ADDR")
                        .ok()
                        .unwrap_or_else(|| "127.0.0.1:50091".to_string());
                    let endpoint = Endpoint::from_shared(format!("http://{}", addr))
                        .map_err(|err| {
                            map_infra_error(err, ErrorCode::ConfigurationError, "endpoint")
                        })?;
                    endpoint.connect().await.map_err(|err| {
                        map_infra_error(err, ErrorCode::NetworkError, "connect")
                    })?
                };

                let mut client = StorageReaderServiceClient::new(channel);
                let mut request = Request::new(Self::build_request(
                    &task_ctx,
                    &conversation_id,
                    since_ts,
                    None,
                    limit,
                ));
                set_context_metadata(&mut request, &task_ctx);
                let response = client
                    .query_messages(request)
                    .await
                    .map_err(|e| grpc_call_failed("query_messages", e))?
                    .into_inner();
                Ok::<Vec<flare_proto::common::Message>, crate::error::FlareError>(response.messages)
            });
        }

        let mut messages = Vec::new();
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(msgs)) => {
                    messages.extend(msgs);
                }
                Ok(Err(e)) => {
                    tracing::warn!(error = %e, "Failed to fetch recent messages for one session");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "Task join error while fetching recent messages");
                }
            }
        }

        messages.sort_by(|a, b| {
            let a_ts = a
                .timestamp
                .as_ref()
                .map(|ts| ts.seconds * 1_000_000_000 + ts.nanos as i64)
                .unwrap_or(0);
            let b_ts = b
                .timestamp
                .as_ref()
                .map(|ts| ts.seconds * 1_000_000_000 + ts.nanos as i64)
                .unwrap_or(0);
            b_ts.cmp(&a_ts)
        });

        Ok(messages)
    }

    async fn sync_messages_by_seq(
        &self,
        ctx: &Context,
        conversation_id: &str,
        after_seq: i64,
        before_seq: Option<i64>,
        limit: i32,
    ) -> Result<MessageSyncResult> {
        let mut client = self.client().await?;

        let mut request = Request::new(flare_grpc_proto::storage::QueryMessagesBySeqRequest {
            conversation_id: conversation_id.to_string(),
            after_seq,
            before_seq: before_seq.unwrap_or(0),
            limit,
            user_id: ctx.user_id().map(|s| s.to_string()).unwrap_or_default(),
        });

        set_context_metadata(&mut request, ctx);

        let response = client
            .query_messages_by_seq(request)
            .await
            .map_err(|e| grpc_call_failed("query_messages_by_seq", e))?
            .into_inner();

        let server_cursor_ts = Self::last_timestamp(&response.messages);
        let server_cursor_seq = if response.last_seq > 0 {
            Some(response.last_seq)
        } else {
            None
        };

        Ok(MessageSyncResult {
            messages: response.messages,
            next_cursor: if response.next_cursor.is_empty() {
                None
            } else {
                Some(response.next_cursor)
            },
            server_cursor_ts,
            server_cursor_seq,
        })
    }
}
