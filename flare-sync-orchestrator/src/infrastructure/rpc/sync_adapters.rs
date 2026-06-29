//! gRPC 出站适配器：发现下游 Channel 并调用 Conversation（读 + 游标写）/ Storage Reader。
//!
//! 本模块实现 application 层定义的 Port trait，使用 tonic 框架。

use flare_grpc_proto::conversation::conversation_manage_service_client::ConversationManageServiceClient;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_grpc_proto::conversation::{
    ConversationBootstrapRequest, ConversationBootstrapResponse, GetConversationDetailRequest,
    GetConversationDetailResponse, ListConversationParticipantsRequest,
    ListConversationParticipantsResponse, UpdateConversationUserSettingsRequest,
    UpdateConversationUserSettingsResponse, UpdateCursorRequest,
};
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::storage::{
    GetConversationMessageHeadRequest, QueryConversationEventsRequest, QueryMessagesBySeqRequest,
};
use flare_im_contracts::Ctx;
use flare_im_contracts::service_names::{CONVERSATION, STORAGE_READER, get_service_name};
use flare_proto::Message;
use flare_server_core::client::request_with_context;
use flare_server_core::error::FlareError;
use redis::aio::ConnectionManager;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock};
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::warn;

use crate::application::error::{discovery_unavailable, flare_from_tonic_status};

use crate::application::ports::{
    ConversationEventReadPort, ConversationSyncPort, ConversationVersionChange,
    ConversationVersionIndexPort, QueryEventsPage, StorageConversationMessageHead, StorageReadPort,
};

const DEFAULT_TENANT_ID: &str = "0";

/// gRPC 同步适配器（基于 tonic）
///
/// 实现 application 层的 Port trait，通过 gRPC 调用下游服务。
#[derive(Clone, Copy, Default)]
pub struct GrpcSyncAdapters;

impl GrpcSyncAdapters {
    fn channel_cache() -> &'static Mutex<HashMap<String, Channel>> {
        static CACHE: OnceLock<Mutex<HashMap<String, Channel>>> = OnceLock::new();
        CACHE.get_or_init(|| Mutex::new(HashMap::new()))
    }

    async fn create_channel(service_name: &str) -> Result<Channel, FlareError> {
        if let Some(channel) = Self::channel_cache()
            .lock()
            .await
            .get(service_name)
            .cloned()
        {
            return Ok(channel);
        }

        let fallback = flare_im_service_kit::discovery::default_static_grpc_fallback(service_name);
        let channel = flare_im_service_kit::discovery::connect_grpc_channel_with_fallback(
            service_name,
            fallback,
        )
        .await
        .map_err(|e| discovery_unavailable(service_name, e))?;

        let mut cache = Self::channel_cache().lock().await;
        Ok(cache
            .entry(service_name.to_string())
            .or_insert_with(|| channel.clone())
            .clone())
    }

    async fn conversation_read_client() -> Result<ConversationReadServiceClient<Channel>, FlareError>
    {
        let name = get_service_name(CONVERSATION);
        let ch = Self::create_channel(&name).await?;
        Ok(ConversationReadServiceClient::new(ch))
    }

    async fn conversation_manage_client()
    -> Result<ConversationManageServiceClient<Channel>, FlareError> {
        let name = get_service_name(CONVERSATION);
        let ch = Self::create_channel(&name).await?;
        Ok(ConversationManageServiceClient::new(ch))
    }

    async fn storage_client() -> Result<StorageReaderServiceClient<Channel>, FlareError> {
        let name = get_service_name(STORAGE_READER);
        let ch = Self::create_channel(&name).await?;
        Ok(StorageReaderServiceClient::new(ch))
    }

    fn conversation_version_redis_client() -> Option<Arc<redis::Client>> {
        static CLIENT: OnceLock<Option<Arc<redis::Client>>> = OnceLock::new();
        CLIENT
            .get_or_init(|| {
                let Ok(url) = std::env::var("SYNC_ORCHESTRATOR_REDIS_URL") else {
                    return None;
                };

                match redis::Client::open(url.as_str()) {
                    Ok(client) => Some(Arc::new(client)),
                    Err(error) => {
                        warn!(error = %error, "invalid sync orchestrator Redis URL");
                        None
                    }
                }
            })
            .clone()
    }

    async fn conversation_version_redis_connection() -> Result<Option<ConnectionManager>, FlareError>
    {
        let Some(client) = Self::conversation_version_redis_client() else {
            return Ok(None);
        };
        ConnectionManager::new(client.as_ref().clone())
            .await
            .map(Some)
            .map_err(|err| {
                FlareError::system(format!("Redis conversation version index connect: {err}"))
            })
    }

    fn tenant_id(ctx: &Ctx) -> String {
        ctx.tenant_id()
            .filter(|tenant_id| !tenant_id.trim().is_empty())
            .unwrap_or(DEFAULT_TENANT_ID)
            .to_string()
    }

    fn conversation_sync_state_key(tenant_id: &str, conversation_id: &str) -> String {
        format!("sync:conversation:{tenant_id}:{conversation_id}:state")
    }

    fn normalized_known_conversation_versions(known: &[(String, u64)]) -> Vec<(String, u64)> {
        let mut versions: BTreeMap<String, u64> = BTreeMap::new();
        for (conversation_id, version) in known {
            let conversation_id = conversation_id.trim();
            if conversation_id.is_empty() {
                continue;
            }
            versions
                .entry(conversation_id.to_string())
                .and_modify(|known_version| *known_version = (*known_version).max(*version))
                .or_insert(*version);
        }
        versions.into_iter().collect()
    }

    fn parse_u64_field(
        state: &HashMap<String, String>,
        field: &str,
        conversation_id: &str,
    ) -> Result<Option<u64>, FlareError> {
        state
            .get(field)
            .map(|value| {
                value.parse::<u64>().map_err(|err| {
                    FlareError::system(format!(
                        "Redis conversation version index invalid {field} for conversation_id={conversation_id}: {err}"
                    ))
                })
            })
            .transpose()
    }

    fn parse_i64_field(
        state: &HashMap<String, String>,
        field: &str,
        conversation_id: &str,
    ) -> Result<Option<i64>, FlareError> {
        state
            .get(field)
            .map(|value| {
                value.parse::<i64>().map_err(|err| {
                    FlareError::system(format!(
                        "Redis conversation version index invalid {field} for conversation_id={conversation_id}: {err}"
                    ))
                })
            })
            .transpose()
    }

    fn conversation_change_from_state(
        conversation_id: &str,
        known_version: u64,
        state: &HashMap<String, String>,
    ) -> Result<Option<ConversationVersionChange>, FlareError> {
        let Some(version) = Self::parse_u64_field(state, "version", conversation_id)? else {
            return Ok(None);
        };
        if version <= known_version {
            return Ok(None);
        }

        let stored_conversation_id = state
            .get("conversation_id")
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .unwrap_or(conversation_id)
            .to_string();

        Ok(Some(ConversationVersionChange {
            conversation_id: stored_conversation_id,
            version,
            max_conversation_seq: Self::parse_u64_field(
                state,
                "max_conversation_seq",
                conversation_id,
            )?
            .unwrap_or_default(),
            updated_at_ms: Self::parse_i64_field(state, "updated_at_ms", conversation_id)?
                .unwrap_or_default(),
        }))
    }
}

impl ConversationSyncPort for GrpcSyncAdapters {
    async fn conversation_bootstrap(
        &self,
        ctx: &Ctx,
        req: ConversationBootstrapRequest,
    ) -> Result<ConversationBootstrapResponse, FlareError> {
        let mut client = Self::conversation_read_client().await?;
        let resp = client
            .conversation_bootstrap(request_with_context(req, ctx))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?;
        Ok(resp.into_inner())
    }

    async fn update_sync_cursor(
        &self,
        ctx: &Ctx,
        req: UpdateCursorRequest,
    ) -> Result<(), FlareError> {
        let mut client = Self::conversation_manage_client().await?;
        client
            .update_cursor(request_with_context(req, ctx))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?;
        Ok(())
    }

    async fn conversation_detail(
        &self,
        ctx: &Ctx,
        req: GetConversationDetailRequest,
    ) -> Result<GetConversationDetailResponse, FlareError> {
        let mut client = Self::conversation_read_client().await?;
        let resp = client
            .get_conversation_detail(request_with_context(req, ctx))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?;
        Ok(resp.into_inner())
    }

    async fn list_conversation_participants(
        &self,
        ctx: &Ctx,
        req: ListConversationParticipantsRequest,
    ) -> Result<ListConversationParticipantsResponse, FlareError> {
        let mut client = Self::conversation_read_client().await?;
        let resp = client
            .list_conversation_participants(request_with_context(req, ctx))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?;
        Ok(resp.into_inner())
    }

    async fn update_conversation_user_settings(
        &self,
        ctx: &Ctx,
        req: UpdateConversationUserSettingsRequest,
    ) -> Result<UpdateConversationUserSettingsResponse, FlareError> {
        let mut client = Self::conversation_manage_client().await?;
        let resp = client
            .update_conversation_user_settings(request_with_context(req, ctx))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?;
        Ok(resp.into_inner())
    }
}

impl StorageReadPort for GrpcSyncAdapters {
    async fn query_messages_by_seq(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        user_id: &str,
    ) -> Result<(Vec<Message>, i64), FlareError> {
        let mut client = Self::storage_client().await?;
        let resp = client
            .query_messages_by_seq(request_with_context(
                QueryMessagesBySeqRequest {
                    conversation_id: conversation_id.to_string(),
                    after_seq,
                    before_seq,
                    limit,
                    user_id: user_id.to_string(),
                    include_burned_placeholder: false,
                },
                ctx,
            ))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?
            .into_inner();
        let last_seq = resp.last_seq;
        Ok((resp.messages, last_seq))
    }

    async fn get_conversation_message_head(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<StorageConversationMessageHead, FlareError> {
        let mut client = Self::storage_client().await?;
        let resp = client
            .get_conversation_message_head(request_with_context(
                GetConversationMessageHeadRequest {
                    conversation_id: conversation_id.to_string(),
                },
                ctx,
            ))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?
            .into_inner();
        Ok(StorageConversationMessageHead {
            max_seq: resp.max_seq,
            last_message_id: resp.last_message_id,
            last_timestamp: resp.last_timestamp,
        })
    }
}

impl ConversationEventReadPort for GrpcSyncAdapters {
    async fn query_events_page(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
        after_seq: i64,
        before_seq: i64,
        limit: i32,
        event_types: &[i32],
        include_deleted: bool,
    ) -> Result<QueryEventsPage, FlareError> {
        let _ = include_deleted;
        let mut client = Self::storage_client().await?;
        let resp = client
            .query_conversation_events(request_with_context(
                QueryConversationEventsRequest {
                    conversation_id: conversation_id.to_string(),
                    after_seq,
                    before_seq,
                    limit,
                    event_type_filter: event_types.to_vec(),
                },
                ctx,
            ))
            .await
            .map_err(|e| flare_from_tonic_status(&e))?
            .into_inner();
        Ok(QueryEventsPage {
            events: resp.events,
            last_seq: resp.last_seq,
            has_more: resp.has_more,
            next_cursor: resp.next_cursor,
        })
    }
}

impl ConversationVersionIndexPort for GrpcSyncAdapters {
    async fn diff_known_conversation_versions(
        &self,
        ctx: &Ctx,
        known: &[(String, u64)],
    ) -> Result<Vec<ConversationVersionChange>, FlareError> {
        let known = Self::normalized_known_conversation_versions(known);
        if known.is_empty() {
            return Ok(Vec::new());
        }

        let Some(mut conn) = Self::conversation_version_redis_connection().await? else {
            return Ok(Vec::new());
        };

        let tenant_id = Self::tenant_id(ctx);
        let mut pipe = redis::pipe();
        for (conversation_id, _) in &known {
            pipe.cmd("HGETALL").arg(Self::conversation_sync_state_key(
                &tenant_id,
                conversation_id,
            ));
        }

        let states: Vec<HashMap<String, String>> =
            pipe.query_async(&mut conn).await.map_err(|err| {
                FlareError::system(format!(
                    "Redis conversation version index diff failed tenant_id={tenant_id} conversation_count={}: {err}",
                    known.len()
                ))
            })?;

        let mut changes = Vec::new();
        for ((conversation_id, known_version), state) in known.into_iter().zip(states) {
            if let Some(change) =
                Self::conversation_change_from_state(&conversation_id, known_version, &state)?
            {
                changes.push(change);
            }
        }
        Ok(changes)
    }
}
