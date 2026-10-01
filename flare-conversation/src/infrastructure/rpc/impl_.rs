//! Tonic 框架的 RPC 客户端实现
//!
//! 本模块提供基于 tonic 的 RPC 客户端具体实现。

use crate::domain::model::MessageSyncResult;
use crate::domain::repository::MessageProvider;
use flare_grpc_proto::storage::QueryMessagesRequest;
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_im_service_kit::ServiceClient;
use flare_server_core::client::set_context_metadata;
use flare_server_core::context::Context;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result, map_infra_error};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tonic::Request;
use tonic::transport::Channel;

/// 单次批量拉取的并发上限。
///
/// 这里按会话数扇出，会话数由客户端决定（同步一屏会话列表就可能是几百个）。
/// 不设上限的话，一个用户的一次同步就能给 storage-reader 发去几百个并发请求。
/// 与 flare-message-ingest 里 MAX_BATCH_SEND_CONCURRENCY 同量级。
const MAX_SYNC_FANOUT_CONCURRENCY: usize = 32;

/// 存储服务客户端（基于 tonic）
///
/// 使用 tonic 框架实现的存储服务客户端，
/// 通过 gRPC 调用下游的 Storage Reader 服务。地址在构造时一次定下：要么一个固定地址的
/// 懒连接通道（单条查询与批量扇出共用，不逐次建连），要么服务发现；调用路径上不再读环境
/// 变量，也不再回退到别的端口。
pub struct StorageReaderClient {
    target: StorageReaderTarget,
}

enum StorageReaderTarget {
    /// 固定地址的懒连接通道（建连上限与 HTTP/2 探活见 `StaticChannelPolicy`）。
    Static { url: String, channel: Channel },
    /// 服务发现。
    Discovery {
        service_name: String,
        client: Arc<Mutex<ServiceClient>>,
    },
}

fn grpc_call_failed(op: &str, status: tonic::Status) -> flare_server_core::error::FlareError {
    ErrorBuilder::new(
        ErrorCode::ServiceUnavailable,
        format!("storage reader {} failed", op),
    )
    .details(status.to_string())
    .build_error()
}

impl StorageReaderClient {
    /// 经服务发现找 Storage Reader。
    pub fn with_service_client(
        service_name: impl Into<String>,
        service_client: ServiceClient,
    ) -> Self {
        Self {
            target: StorageReaderTarget::Discovery {
                service_name: service_name.into(),
                client: Arc::new(Mutex::new(service_client)),
            },
        }
    }

    /// 直连固定地址（`http://` 或 `https://`）。通道懒连接，构造时不连下游。
    pub fn with_static_endpoint(url: &str) -> Result<Self> {
        let channel =
            flare_im_service_kit::discovery::connect_static_channel_lazy(url).map_err(|e| {
                map_infra_error(e, ErrorCode::ConfigurationError, "storage reader endpoint")
            })?;
        Ok(Self {
            target: StorageReaderTarget::Static {
                url: url.to_string(),
                channel,
            },
        })
    }

    /// 固定地址模式下的地址；服务发现模式为 `None`。
    pub fn static_endpoint(&self) -> Option<&str> {
        match &self.target {
            StorageReaderTarget::Static { url, .. } => Some(url),
            StorageReaderTarget::Discovery { .. } => None,
        }
    }

    /// 本次调用用的通道。服务发现模式下只在取通道时持锁，RPC 往返不持锁。
    async fn channel(&self) -> Result<Channel> {
        match &self.target {
            StorageReaderTarget::Static { channel, .. } => Ok(channel.clone()),
            StorageReaderTarget::Discovery {
                service_name,
                client,
            } => {
                let mut client = client.lock().await;
                let channel = flare_im_service_kit::discovery::get_discovered_channel_with_timeout(
                    service_name,
                    &mut client,
                )
                .await
                .map_err(|e| {
                    map_infra_error(
                        e,
                        ErrorCode::ServiceUnavailable,
                        "get channel from service discovery",
                    )
                })?;
                tracing::trace!("Got channel for storage reader service from service discovery");
                Ok(channel)
            }
        }
    }

    pub(crate) async fn client(&self) -> Result<StorageReaderServiceClient<Channel>> {
        Ok(StorageReaderServiceClient::new(self.channel().await?))
    }

    fn last_timestamp(messages: &[flare_proto::common::Message]) -> Option<i64> {
        messages
            .last()
            .map(|msg| msg.created_at)
            .filter(|ts| *ts > 0)
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
            include_burned_placeholder: false,
        }
    }

    fn last_seq(messages: &[flare_proto::common::Message]) -> Option<i64> {
        messages
            .last()
            .and_then(flare_im_contracts::utils::extract_seq_from_message)
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

        // 扇出前取一次通道：下游不可用时整批直接失败，而不是逐个会话各失败一次。
        // 通道自带连接池，clone 很便宜；服务发现模式下每个会话仍各取一次，以便在实例间分摊。
        let first_channel = self.channel().await?;

        let mut join_set = JoinSet::new();
        let fanout_permits = Arc::new(Semaphore::new(MAX_SYNC_FANOUT_CONCURRENCY));
        let discovery = match &self.target {
            StorageReaderTarget::Static { .. } => None,
            StorageReaderTarget::Discovery {
                service_name,
                client,
            } => Some((service_name.clone(), Arc::clone(client))),
        };
        let ctx = ctx.clone();

        for conversation_id in conversation_ids {
            let conversation_id = conversation_id.clone();
            let after_seq = client_cursor
                .get(&conversation_id)
                .copied()
                .unwrap_or(0)
                .max(0);
            let limit = limit_per_session;
            let discovery = discovery.clone();
            let first_channel = first_channel.clone();
            let task_ctx = ctx.clone();

            let permit = Arc::clone(&fanout_permits);
            join_set.spawn(async move {
                // 上限之内才开始干活；超出的任务在这里排队而不是一起压向下游。
                let _permit = permit
                    .acquire_owned()
                    .await
                    .map_err(|err| map_infra_error(err, ErrorCode::InternalError, "semaphore"))?;

                // 服务发现的锁只在取通道时持有。此前它一直活到 async 块结束、
                // **横跨整个 gRPC 往返**——于是这段「并行扇出」实际是串行的：
                // N 个会话就是 N 次顺序 RPC，而代码看起来是并发的。
                // 固定地址直接用它的通道。服务发现失败就让这个会话失败（与单条查询一致），
                // 不再回退到环境变量或默认端口。
                let channel: Channel = match discovery {
                    None => first_channel,
                    Some((service_name, client)) => {
                        let mut client = client.lock().await;
                        flare_im_service_kit::discovery::get_discovered_channel_with_timeout(
                            &service_name,
                            &mut client,
                        )
                        .await
                        .map_err(|e| {
                            map_infra_error(
                                e,
                                ErrorCode::ServiceUnavailable,
                                "get channel from service discovery",
                            )
                        })?
                    }
                };

                let mut client = StorageReaderServiceClient::new(channel);
                if after_seq > 0 {
                    let mut request =
                        Request::new(flare_grpc_proto::storage::QueryMessagesBySeqRequest {
                            conversation_id: conversation_id.to_string(),
                            after_seq,
                            before_seq: 0,
                            limit,
                            user_id: task_ctx
                                .user_id()
                                .map(|s| s.to_string())
                                .unwrap_or_default(),
                            include_burned_placeholder: false,
                        });
                    set_context_metadata(&mut request, &task_ctx);
                    let response = client
                        .query_messages_by_seq(request)
                        .await
                        .map_err(|e| grpc_call_failed("query_messages_by_seq", e))?
                        .into_inner();
                    Ok::<Vec<flare_proto::common::Message>, flare_server_core::error::FlareError>(
                        response.messages,
                    )
                } else {
                    let mut request = Request::new(Self::build_request(
                        &task_ctx,
                        &conversation_id,
                        0,
                        None,
                        limit,
                    ));
                    set_context_metadata(&mut request, &task_ctx);
                    let response = client
                        .query_messages(request)
                        .await
                        .map_err(|e| grpc_call_failed("query_messages", e))?
                        .into_inner();
                    Ok::<Vec<flare_proto::common::Message>, flare_server_core::error::FlareError>(
                        response.messages,
                    )
                }
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

        messages.sort_by_key(|m| std::cmp::Reverse(m.created_at));

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
            include_burned_placeholder: false,
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
