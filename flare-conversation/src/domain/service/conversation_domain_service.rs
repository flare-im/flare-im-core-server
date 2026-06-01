//! 会话领域服务 - 包含所有业务逻辑实现

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::error::{ErrorBuilder, ErrorCode, Result, require_user_id};
use flare_core::common::conversation::{
    generate_ai_conversation_id, generate_customer_conversation_id, generate_group_conversation_id,
    generate_single_chat_conversation_id, generate_system_conversation_id,
    generate_temp_conversation_id, validate_conversation_id,
};
use flare_proto::common::Message;
use flare_proto::common::message_content::Content;
use flare_proto::message_content_ext::decode_message_content;
use flare_server_core::context::Context;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::domain::model::{
    ConflictResolutionPolicy, Conversation, ConversationDomainConfig, ConversationFilter,
    ConversationLifecycleState, ConversationParticipant, ConversationPolicy, ConversationSort,
    ConversationSummary, ConversationType, ConversationVisibility, DevicePresence, DeviceState,
    MessageSyncResult,
};
use crate::domain::repository::{
    ConversationRepository, MessageProvider, PresenceRepository, PresenceUpdate,
};

/// 会话领域服务 - 包含所有业务逻辑
///
/// 使用泛型参数以获得更好的性能（静态分发）
/// - CR: ConversationRepository（必需）
/// - PR: PresenceRepository（必需）
/// - MP: MessageProvider（可选）
///
/// 性能优势：
/// - 零开销的静态分发（相比动态分发快 5-10%）
/// - 编译时类型检查
/// - 更好的内联优化
/// - 符合 Rust 2024 原生 async fn in traits 规范
///
/// 为什么使用泛型而不是 dyn Trait：
/// 1. Rust 2024 原生 async fn 不支持 dyn 兼容性
/// 2. 泛型提供零成本抽象，性能更好
/// 3. 类型安全，编译期检查
/// 4. 虽然需要配置层运行时选择实现，但可以通过类型别名简化使用
pub struct ConversationDomainService<CR, PR, MP> {
    conversation_repo: Arc<CR>,
    presence_repo: Arc<PR>,
    message_provider: Option<Arc<MP>>,
    config: ConversationDomainConfig,
}

/// 会话引导输出
pub struct ConversationBootstrapOutput {
    pub summaries: Vec<ConversationSummary>,
    pub recent_messages: Vec<Message>,
    pub cursor_map: HashMap<String, i64>,
    pub devices: Vec<DevicePresence>,
    pub policy: ConversationPolicy,
}

impl<CR: ConversationRepository, PR: PresenceRepository, MP: MessageProvider>
    ConversationDomainService<CR, PR, MP>
{
    pub fn new(
        conversation_repo: Arc<CR>,
        presence_repo: Arc<PR>,
        message_provider: Option<Arc<MP>>,
        config: ConversationDomainConfig,
    ) -> Self {
        Self {
            conversation_repo,
            presence_repo,
            message_provider,
            config,
        }
    }

    /// 会话引导（业务逻辑）
    pub async fn bootstrap_conversation(
        &self,
        ctx: &Context,
        client_cursor: HashMap<String, i64>,
        include_recent: bool,
        recent_limit: Option<i32>,
    ) -> Result<ConversationBootstrapOutput> {
        let bootstrap = self
            .conversation_repo
            .load_bootstrap(ctx, &client_cursor)
            .await?;

        let mut summaries = bootstrap.summaries;

        // 优化：按优先级排序（未读会话优先，然后按更新时间降序）
        summaries.sort_by(|a, b| {
            // 优先级1：未读数（未读会话优先）
            let a_unread = a.unread_count;
            let b_unread = b.unread_count;
            if a_unread != b_unread {
                return b_unread.cmp(&a_unread); // 未读数多的优先
            }

            // 优先级2：更新时间（最新的优先）
            let a_ts = a.server_cursor_ts.unwrap_or(0);
            let b_ts = b.server_cursor_ts.unwrap_or(0);
            b_ts.cmp(&a_ts)
        });

        // 优化：限制返回的会话数量（默认最多 100 个，避免响应过大）
        let max_conversations = self.config.max_bootstrap_conversations.unwrap_or(100);
        if summaries.len() > max_conversations {
            summaries.truncate(max_conversations);
        }

        // 仅在需要 recent 数据时补充最后一条消息，避免 bootstrap 首屏同步超时。
        if include_recent {
            if let Some(provider) = &self.message_provider {
                // 为每个会话获取最后一条消息（如果有）
                for summary in &mut summaries {
                    if summary.last_message_id.is_none() {
                        let visible_after_seq = summary.visible_after_seq.max(0);
                        let max_seq = summary.last_message_seq.unwrap_or_default().max(0);
                        if max_seq > 0 && visible_after_seq >= max_seq {
                            continue;
                        }

                        // 尝试获取最后一条消息信息
                        if let Ok(sync_result) = provider
                            .sync_messages(ctx, &summary.conversation_id, 0, None, 1)
                            .await
                        {
                            if let Some(last_msg) = sync_result.messages.first() {
                                summary.last_message_id = Some(last_msg.server_id.clone());

                                // 转换 Timestamp 为 DateTime<Utc>
                                summary.last_message_time =
                                    last_msg.timestamp.as_ref().and_then(|ts| {
                                        chrono::TimeZone::timestamp_opt(
                                            &chrono::Utc,
                                            ts.seconds,
                                            ts.nanos as u32,
                                        )
                                        .single()
                                    });

                                summary.last_sender_id = Some(last_msg.sender_id.clone());
                                summary.last_message_type = Some(last_msg.message_type() as i32);

                                // Message.content 为按 message_content.proto 序列化的 bytes
                                summary.last_content_type = if last_msg.content.is_empty() {
                                    None
                                } else {
                                    decode_message_content(&last_msg.content)
                                        .ok()
                                        .and_then(|mc| {
                                            mc.content.map(|c| match c {
                                                Content::Text(_) => "text".to_string(),
                                                Content::Image(_) => "image".to_string(),
                                                Content::Video(_) => "video".to_string(),
                                                Content::Audio(_) => "audio".to_string(),
                                                Content::File(_) => "file".to_string(),
                                                Content::Location(_) => "location".to_string(),
                                                Content::Card(_) => "card".to_string(),
                                                Content::Sticker(_) => "sticker".to_string(),
                                                Content::Emoji(_) => "emoji".to_string(),
                                                Content::Quote(_) => "quote".to_string(),
                                                Content::LinkCard(_) => "link_card".to_string(),
                                                Content::Forward(_) => "forward".to_string(),
                                                Content::Thread(_) => "thread".to_string(),
                                                Content::MiniProgram(_) => {
                                                    "mini_program".to_string()
                                                }
                                                Content::RichText(_) => "rich_text".to_string(),
                                                Content::ImageGroup(_) => "image_group".to_string(),
                                                Content::System(_) => "system".to_string(),
                                                Content::Notification(_) => {
                                                    "notification".to_string()
                                                }
                                                Content::Vote(_) => "vote".to_string(),
                                                Content::Task(_) => "task".to_string(),
                                                Content::Schedule(_) => "schedule".to_string(),
                                                Content::Announcement(_) => {
                                                    "announcement".to_string()
                                                }
                                                Content::Custom(_) => "custom".to_string(),
                                                Content::Placeholder(_) => {
                                                    "placeholder".to_string()
                                                }
                                            })
                                        })
                                };

                                // 更新server_cursor_ts为最后消息的时间戳
                                if let Some(ts) = last_msg.timestamp.as_ref() {
                                    summary.server_cursor_ts =
                                        Some(ts.seconds * 1_000 + (ts.nanos as i64 / 1_000_000));
                                }
                            }
                        }

                        // 未读数已在 load_bootstrap 中从数据库读取（基于 seq）
                        // 这里不再需要重新计算
                    }
                }
            }
        }

        let mut recent_messages = Vec::new();
        if include_recent {
            if let Some(provider) = &self.message_provider {
                let conversation_ids: Vec<String> = summaries
                    .iter()
                    .map(|s| s.conversation_id.clone())
                    .collect();
                if !conversation_ids.is_empty() {
                    recent_messages = provider
                        .recent_messages(
                            ctx,
                            &conversation_ids,
                            recent_limit.unwrap_or(self.config.recent_message_limit),
                            &bootstrap.cursor_map,
                        )
                        .await
                        .unwrap_or_default();
                }
            }
        }

        let user_id = require_user_id(ctx)?;
        let devices = self
            .presence_repo
            .list_devices(ctx, &user_id)
            .await
            .unwrap_or_default();

        Ok(ConversationBootstrapOutput {
            summaries,
            recent_messages,
            cursor_map: bootstrap.cursor_map,
            devices,
            policy: bootstrap.policy,
        })
    }

    /// 列出会话（业务逻辑）
    pub async fn list_conversations(
        &self,
        ctx: &Context,
        cursor: Option<&str>,
        limit: i32,
    ) -> Result<(Vec<ConversationSummary>, Option<String>, bool)> {
        let bootstrap = self
            .conversation_repo
            .load_bootstrap(ctx, &HashMap::new())
            .await?;

        let mut summaries = bootstrap.summaries;
        let (pivot_ts, pivot_id) = parse_cursor(cursor);

        if let Some(ts) = pivot_ts {
            summaries.retain(|summary| match summary.server_cursor_ts {
                Some(summary_ts) if summary_ts < ts => true,
                Some(summary_ts) if summary_ts == ts => summary.conversation_id > pivot_id,
                Some(_) => false,
                None => false,
            });
        }

        let limit = limit.max(1) as usize;
        let has_more = summaries.len() > limit;
        summaries.truncate(limit);

        let next_cursor = summaries.last().and_then(|summary| {
            summary
                .server_cursor_ts
                .map(|ts| format!("{}:{}", ts, summary.conversation_id))
        });

        Ok((summaries, next_cursor, has_more))
    }

    /// 同步消息（业务逻辑）
    pub async fn sync_messages(
        &self,
        ctx: &Context,
        conversation_id: &str,
        since_ts: i64,
        cursor: Option<&str>,
        limit: i32,
    ) -> Result<MessageSyncResult> {
        let provider = self.message_provider.as_ref().ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "message provider not configured",
            )
            .build_error()
        })?;
        provider
            .sync_messages(ctx, conversation_id, since_ts, cursor, limit)
            .await
    }

    /// 更新游标（业务逻辑）
    pub async fn update_cursor(
        &self,
        ctx: &Context,
        conversation_id: &str,
        sync_seq: i64,
    ) -> Result<()> {
        self.conversation_repo
            .update_cursor(ctx, conversation_id, sync_seq)
            .await
    }

    /// 更新设备状态（业务逻辑）
    pub async fn update_presence(
        &self,
        ctx: &Context,
        device_id: &str,
        platform: Option<String>,
        state: DeviceState,
        conflict_resolution: Option<ConflictResolutionPolicy>,
        notify_conflict: bool,
        conflict_reason: Option<String>,
    ) -> Result<()> {
        let user_id = require_user_id(ctx)?;
        let update = PresenceUpdate {
            user_id: user_id.to_string(),
            device_id: device_id.to_string(),
            device_platform: platform,
            state,
            conflict_resolution,
            notify_conflict,
            conflict_reason,
        };
        self.presence_repo.update_presence(ctx, update).await
    }

    /// 强制会话同步（业务逻辑）
    pub async fn force_conversation_sync(
        &self,
        ctx: &Context,
        conversation_ids: &[String],
        reason: Option<&str>,
    ) -> Result<Vec<String>> {
        if conversation_ids.is_empty() {
            return Ok(Vec::new());
        }

        let bootstrap = self
            .conversation_repo
            .load_bootstrap(ctx, &HashMap::new())
            .await?;

        let known: HashSet<String> = bootstrap
            .summaries
            .iter()
            .map(|summary| summary.conversation_id.clone())
            .collect();

        let missing: Vec<String> = conversation_ids
            .iter()
            .filter(|conversation_id| !known.contains(*conversation_id))
            .cloned()
            .collect();

        let user_id = require_user_id(ctx)?;
        if missing.is_empty() {
            debug!(
                user_id = %user_id,
                conversations = ?conversation_ids,
                reason = reason.unwrap_or(""),
                "force conversation sync requested"
            );
        } else {
            warn!(
                user_id = %user_id,
                missing = ?missing,
                reason = reason.unwrap_or(""),
                "force conversation sync encountered unknown conversations"
            );
        }

        Ok(missing)
    }

    /// 创建会话（业务逻辑）
    ///
    /// 如果 attributes 中包含 "conversation_id" 且会话不存在，则使用指定的 conversation_id
    /// 否则生成新的 UUID 作为 conversation_id
    ///
    /// 如果会话已存在，则更新参与者，确保所有参与者都在会话中
    pub async fn create_conversation(
        &self,
        ctx: &Context,
        conversation_type: ConversationType,
        business_type: String,
        participants: Vec<ConversationParticipant>,
        mut attributes: HashMap<String, String>,
        visibility: ConversationVisibility,
        stored_channel_id: String,
    ) -> Result<Conversation> {
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        let normalized_channel_id = match conversation_type {
            ConversationType::Single => String::new(),
            _ => stored_channel_id,
        };
        // 尝试从 attributes 中提取指定的 conversation_id
        if let Some(requested_conversation_id) = attributes.remove("conversation_id") {
            // 验证会话ID格式（如果格式不正确，记录警告但继续处理，保持向后兼容）
            if let Err(e) = validate_conversation_id(&requested_conversation_id) {
                warn!(
                    conversation_id = %requested_conversation_id,
                    error = %e,
                    "Invalid session ID format, but continuing for backward compatibility"
                );
            }

            // 检查会话是否已存在
            if let Ok(Some(existing_session)) = self
                .conversation_repo
                .get_conversation(ctx, &requested_conversation_id)
                .await
            {
                // 会话已存在，更新参与者（确保所有参与者都在会话中）
                debug!(
                    conversation_id = %requested_conversation_id,
                    participant_count = participants.len(),
                    "Conversation already exists, ensuring all participants are added"
                );

                // 获取需要添加的参与者（不在现有参与者列表中的）
                let existing_participant_ids: std::collections::HashSet<String> = existing_session
                    .participants
                    .iter()
                    .map(|p| p.user_id.clone())
                    .collect();

                let participants_to_add: Vec<ConversationParticipant> = participants
                    .into_iter()
                    .filter(|p| !existing_participant_ids.contains(&p.user_id))
                    .collect();

                if !participants_to_add.is_empty() {
                    debug!(
                        conversation_id = %requested_conversation_id,
                        new_participants = participants_to_add.len(),
                        "Adding new participants to existing session"
                    );
                    self.conversation_repo
                        .manage_participants(
                            ctx,
                            &requested_conversation_id,
                            &participants_to_add,
                            &[],
                            &[],
                        )
                        .await?;
                }

                // 返回现有会话
                Ok(existing_session)
            } else {
                // 会话不存在，使用指定的 conversation_id 创建新会话
                debug!(
                    conversation_id = %requested_conversation_id,
                    "Creating new session with provided conversation_id from attributes"
                );
                let session = Conversation {
                    tenant_id: tenant_id.to_string(),
                    conversation_id: requested_conversation_id.clone(),
                    conversation_type,
                    business_type,
                    channel_id: normalized_channel_id,
                    display_name: None,
                    attributes,
                    participants,
                    visibility,
                    lifecycle_state: ConversationLifecycleState::Active,
                    policy: None,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                };

                self.conversation_repo
                    .create_conversation(ctx, &session)
                    .await?;
                debug!(conversation_id = %requested_conversation_id, "Conversation created with provided conversation_id");
                Ok(session)
            }
        } else {
            // 没有指定 conversation_id，根据会话类型生成
            let conversation_id = match conversation_type {
                ConversationType::Single => {
                    // 单聊：从参与者中提取两个用户ID，使用 generate_single_chat_conversation_id
                    if participants.len() != 2 {
                        return Err(ErrorBuilder::new(
                            ErrorCode::InvalidParameter,
                            format!(
                                "single chat must have exactly 2 participants, got {}",
                                participants.len()
                            ),
                        )
                        .build_error());
                    }
                    let user1 = &participants[0].user_id;
                    let user2 = &participants[1].user_id;
                    generate_single_chat_conversation_id(user1, user2)
                }
                ConversationType::Group => {
                    // 群聊：使用 UUID 作为 group_id，或从 attributes 中获取
                    let group_id = attributes
                        .get("group_id")
                        .cloned()
                        .unwrap_or_else(|| Uuid::new_v4().to_string());
                    generate_group_conversation_id(&group_id)
                }
                ConversationType::Ai => {
                    // AI 助手：从参与者中提取用户ID，ai_scope 从 attributes 或默认值
                    let user_id = participants
                        .first()
                        .map(|p| p.user_id.as_str())
                        .unwrap_or_else(|| ctx.user_id().unwrap_or("unknown"));
                    let ai_scope = attributes
                        .get("ai_scope")
                        .map(|s| s.as_str())
                        .unwrap_or("0");
                    generate_ai_conversation_id(user_id, ai_scope)
                }
                ConversationType::System => {
                    // 系统会话：system_id 从 attributes 或使用 tenant_id
                    let system_id = attributes
                        .get("system_id")
                        .cloned()
                        .or_else(|| ctx.tenant_id().map(|s| s.to_string()))
                        .unwrap_or_else(|| "0".to_string());
                    let scope = attributes.get("scope").cloned();
                    generate_system_conversation_id(&system_id, scope)
                }
                ConversationType::Customer => {
                    // 客服会话：customer_id 和 channel 从 attributes 获取
                    let customer_id = attributes
                        .get("customer_id")
                        .cloned()
                        .unwrap_or_else(|| Uuid::new_v4().to_string());
                    let channel = attributes
                        .get("channel")
                        .cloned()
                        .unwrap_or_else(|| "0".to_string());
                    generate_customer_conversation_id(&customer_id, &channel)
                }
                ConversationType::Temp => generate_temp_conversation_id(),
                ConversationType::Unspecified => {
                    // 默认使用UUID（向后兼容）
                    warn!(
                        conversation_type = ?conversation_type,
                        "Unspecified session type, using UUID for conversation_id (backward compatibility)"
                    );
                    Uuid::new_v4().to_string()
                }
            };

            let session = Conversation {
                tenant_id: tenant_id.to_string(),
                conversation_id: conversation_id.clone(),
                conversation_type,
                business_type,
                channel_id: normalized_channel_id,
                display_name: None,
                attributes,
                participants,
                visibility,
                lifecycle_state: ConversationLifecycleState::Active,
                policy: None,
                created_at: chrono::Utc::now(),
                updated_at: chrono::Utc::now(),
            };

            self.conversation_repo
                .create_conversation(ctx, &session)
                .await?;
            debug!(
                conversation_id = %conversation_id,
                "Conversation created with generated conversation_id"
            );
            Ok(session)
        }
    }

    /// 获取会话（业务逻辑）
    pub async fn get_conversation(
        &self,
        ctx: &Context,
        conversation_id: &str,
    ) -> Result<Option<Conversation>> {
        self.conversation_repo
            .get_conversation(ctx, conversation_id)
            .await
    }

    /// 更新会话（业务逻辑）
    pub async fn update_conversation(
        &self,
        ctx: &Context,
        conversation_id: &str,
        display_name: Option<String>,
        attributes: Option<HashMap<String, String>>,
        visibility: Option<ConversationVisibility>,
        lifecycle_state: Option<ConversationLifecycleState>,
    ) -> Result<Conversation> {
        let mut conversation = self
            .conversation_repo
            .get_conversation(ctx, conversation_id)
            .await?
            .ok_or_else(|| {
                ErrorBuilder::new(ErrorCode::MessageNotFound, "conversation not found")
                    .details(format!("conversation_id={}", conversation_id))
                    .build_error()
            })?;

        if let Some(name) = display_name {
            conversation.display_name = Some(name);
        }
        if let Some(attrs) = attributes {
            conversation.attributes = attrs;
        }
        if let Some(vis) = visibility {
            conversation.visibility = vis;
        }
        if let Some(state) = lifecycle_state {
            conversation.lifecycle_state = state;
        }
        conversation.updated_at = chrono::Utc::now();

        self.conversation_repo
            .update_conversation(ctx, &conversation)
            .await?;
        debug!(conversation_id = %conversation_id, "Conversation updated");
        Ok(conversation)
    }

    /// 删除会话（业务逻辑）
    pub async fn delete_conversation(
        &self,
        ctx: &Context,
        conversation_id: &str,
        hard_delete: bool,
    ) -> Result<()> {
        self.conversation_repo
            .delete_conversation(ctx, conversation_id, hard_delete)
            .await?;
        debug!(conversation_id = %conversation_id, hard_delete = hard_delete, "Conversation deleted");
        Ok(())
    }

    /// 管理参与者（业务逻辑）
    pub async fn manage_participants(
        &self,
        ctx: &Context,
        conversation_id: &str,
        to_add: Vec<ConversationParticipant>,
        to_remove: Vec<String>,
        role_updates: Vec<(String, Vec<String>)>,
    ) -> Result<Vec<ConversationParticipant>> {
        let participants = self
            .conversation_repo
            .manage_participants(ctx, conversation_id, &to_add, &to_remove, &role_updates)
            .await?;
        debug!(
            conversation_id = %conversation_id,
            added = to_add.len(),
            removed = to_remove.len(),
            role_updates = role_updates.len(),
            "Participants managed"
        );
        Ok(participants)
    }

    pub async fn list_conversation_participants(
        &self,
        ctx: &Context,
        conversation_id: &str,
        cursor: Option<&str>,
        limit: i32,
        include_removed: bool,
    ) -> Result<crate::domain::model::ConversationParticipantsPage> {
        self.conversation_repo
            .list_conversation_participants(ctx, conversation_id, cursor, limit, include_removed)
            .await
    }

    /// 批量确认（业务逻辑）
    pub async fn batch_acknowledge(
        &self,
        ctx: &Context,
        cursors: Vec<(String, i64)>,
    ) -> Result<()> {
        let user_id = require_user_id(ctx)?;
        self.conversation_repo
            .batch_acknowledge(ctx, &cursors)
            .await?;
        debug!(user_id = %user_id, count = cursors.len(), "Batch acknowledge completed");
        Ok(())
    }

    /// 标记消息为已读（业务逻辑）
    ///
    /// 更新用户的 last_read_msg_seq，并重新计算未读数
    pub async fn mark_as_read(&self, ctx: &Context, conversation_id: &str, seq: i64) -> Result<()> {
        let user_id = ctx.user_id().unwrap_or("0");
        self.conversation_repo
            .mark_as_read(ctx, conversation_id, seq)
            .await?;
        debug!(
            user_id = %user_id,
            conversation_id = %conversation_id,
            seq,
            "Marked messages as read"
        );
        Ok(())
    }

    pub async fn update_user_settings(
        &self,
        ctx: &Context,
        patch: &crate::domain::model::UpdateConversationUserSettingsPatch,
    ) -> Result<crate::domain::model::ConversationUserSettings> {
        self.conversation_repo
            .update_user_settings(ctx, patch)
            .await
    }

    /// 应用消息事件（写时维护未读计数）。
    pub async fn apply_message_event(
        &self,
        ctx: &Context,
        conversation_id: &str,
        sender_id: &str,
        seq: i64,
        status: i32,
    ) -> Result<()> {
        self.conversation_repo
            .apply_message_event(ctx, conversation_id, sender_id, seq, status)
            .await
    }

    pub async fn get_unread_count(&self, ctx: &Context, conversation_id: &str) -> Result<i32> {
        self.conversation_repo
            .get_unread_count(ctx, conversation_id)
            .await
    }

    /// 搜索会话（业务逻辑）
    pub async fn search_conversations(
        &self,
        ctx: &Context,
        filters: Vec<ConversationFilter>,
        sort: Vec<ConversationSort>,
        limit: usize,
        offset: usize,
    ) -> Result<(Vec<ConversationSummary>, usize)> {
        self.conversation_repo
            .search_conversations(ctx, &filters, &sort, limit, offset)
            .await
    }
}

fn parse_cursor(cursor: Option<&str>) -> (Option<i64>, String) {
    if let Some(cursor) = cursor {
        if let Some((ts, id)) = cursor.split_once(':') {
            if let Ok(parsed) = ts.parse::<i64>() {
                return (Some(parsed), id.to_string());
            }
        }
    }
    (None, String::new())
}
