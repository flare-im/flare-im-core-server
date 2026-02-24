//! 推送领域服务 - 包含所有业务逻辑实现

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use flare_im_core::gateway::GatewayRouterTrait;
use flare_im_core::hooks::HookDispatcher;
use flare_im_core::metrics::PushServerMetrics;
use flare_proto::common::Message;
use flare_proto::push::{PushMessageRequest, PushNotificationRequest};
use flare_server_core::error::Result;
use futures::future;
use prost::Message as ProstMessage;
use tokio::sync::RwLock;
use tracing::{error, info, instrument, warn};

use crate::config::PushServerConfig;
use crate::domain::model::PushDispatchTask;
use crate::domain::repository::{OnlineStatusRepository, PushTaskPublisher};
use crate::infrastructure::ack_tracker::AckTracker;
use crate::infrastructure::message_state::{MessageStateTracker, MessageStatus};
use crate::infrastructure::retry::RetryPolicy;

/// 消息去重缓存（基于 message_id + user_id）
type MessageDedupCache = Arc<RwLock<HashMap<String, Instant>>>;

/// 推送领域服务 - 包含所有业务逻辑
pub struct PushDomainService {
    config: Arc<PushServerConfig>,
    online_repo: Arc<dyn OnlineStatusRepository>,
    task_publisher: Arc<dyn PushTaskPublisher>,
    hooks: Arc<HookDispatcher>,
    gateway_router: Arc<dyn GatewayRouterTrait>,
    state_tracker: Arc<MessageStateTracker>,
    ack_tracker: Arc<AckTracker>,
    retry_policy: RetryPolicy,
    metrics: Arc<PushServerMetrics>,
    /// 消息去重缓存（防止重复推送）
    dedup_cache: MessageDedupCache,
}

impl PushDomainService {
    pub fn new(
        config: Arc<PushServerConfig>,
        online_repo: Arc<dyn OnlineStatusRepository>,
        task_publisher: Arc<dyn PushTaskPublisher>,
        hooks: Arc<HookDispatcher>,
        gateway_router: Arc<dyn GatewayRouterTrait>,
        state_tracker: Arc<MessageStateTracker>,
        ack_tracker: Arc<AckTracker>,
        metrics: Arc<PushServerMetrics>,
    ) -> Self {
        let retry_policy = RetryPolicy::from_config(
            config.push_retry_max_attempts,
            config.push_retry_initial_delay_ms,
            config.push_retry_max_delay_ms,
            config.push_retry_backoff_multiplier,
        );

        Self {
            config,
            online_repo,
            task_publisher,
            hooks,
            gateway_router,
            state_tracker,
            ack_tracker,
            retry_policy,
            metrics,
            dedup_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 分发推送消息（业务逻辑）- 从 Kafka 消费
    #[instrument(skip(self), fields(
        user_count = request.user_ids.len(),
        message_id = %request.message.as_ref().map(|m| m.server_id.as_str()).unwrap_or(""),
        sender_id = %request.message.as_ref().map(|m| m.sender_id.as_str()).unwrap_or(""),
        receiver_id = %request.message.as_ref().map(|m| m.receiver_id.as_str()).unwrap_or(""),
    ))]
    pub async fn dispatch_push_message(&self, request: PushMessageRequest) -> Result<()> {
        // 验证消息完整性：receiver_id 和 channel_id 不能同时为空
        if let Some(ref message) = request.message {
            // 单聊消息：必须提供 receiver_id
            if message.conversation_type == 1 {
                if message.receiver_id.is_empty() {
                    error!(
                        message_id = %message.server_id,
                        conversation_id = %message.conversation_id,
                        sender_id = %message.sender_id,
                        user_ids = ?request.user_ids,
                        "Single chat message missing receiver_id in push service"
                    );
                    return Err(flare_server_core::error::ErrorBuilder::new(
                        flare_server_core::error::ErrorCode::InvalidParameter,
                        format!("Single chat message must provide receiver_id. message_id={}, conversation_id={}, sender_id={}", 
                            message.server_id, message.conversation_id, message.sender_id)
                    ).build_error());
                }

                // 如果 user_ids 为空，说明消息编排服务没有正确设置，这是错误
                if request.user_ids.is_empty() {
                    error!(
                        message_id = %message.server_id,
                        receiver_id = %message.receiver_id,
                        "user_ids is empty in PushMessageRequest, message orchestrator should set it"
                    );
                    return Err(flare_server_core::error::ErrorBuilder::new(
                        flare_server_core::error::ErrorCode::InvalidParameter,
                        format!("user_ids cannot be empty for single chat message. message_id={}, receiver_id={}", 
                            message.server_id, message.receiver_id)
                    ).build_error());
                }
            }
            // 群聊/频道消息：必须提供 channel_id
            else if message.conversation_type == 2 || message.conversation_type == 3 {
                if message.channel_id.is_empty() {
                    return Err(flare_server_core::error::ErrorBuilder::new(
                        flare_server_core::error::ErrorCode::InvalidParameter,
                        "Group/channel message must provide channel_id",
                    )
                    .build_error());
                }
            }

            // 注意：已移除消息去重逻辑
            // ACK 机制已经保证消息可靠性：
            // 1. 客户端收到消息后发送 ACK
            // 2. Gateway 通过 Push Proxy → Kafka → Push Server 上报 ACK
            // 3. Push Server 确认 ACK 后停止重试
            // 4. 如果 ACK 超时，Push Server 会重试推送（最多重试 N 次）
            // 因此不需要额外的去重逻辑，ACK 机制已经保证了消息的可靠性和幂等性
        }

        // 验证 user_ids 不为空
        if request.user_ids.is_empty() {
            return Err(flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "user_ids cannot be empty after deduplication. All recipients were filtered out as duplicates"
            ).build_error());
        }

        // 将 PushMessageRequest 转换为 PushDispatchTask 并批量处理
        let tasks = self.convert_message_request_to_tasks(&request)?;
        self.process_tasks(tasks).await
    }

    #[instrument(skip(self, ctx), fields(message_id = %ack.server_msg_id))]
    pub async fn handle_client_ack(
        &self,
        ctx: &flare_server_core::context::Context,
        ack: &flare_proto::common::SendEnvelopeAck,
    ) -> Result<()> {
        let user_id = ctx.user_id().ok_or_else(|| {
            flare_server_core::error::ErrorBuilder::new(
                flare_server_core::error::ErrorCode::InvalidParameter,
                "user_id is required in context"
            )
            .build_error()
        })?;
        let start_time = Instant::now();
        info!(
            message_id = %ack.server_msg_id,
            user_id = %user_id,
            timestamp_ms = start_time.elapsed().as_millis(),
            "Received client ACK from Gateway"
        );

        if let Ok(confirmed) = self.ack_tracker.confirm_ack(ctx, &ack.server_msg_id).await {
            let duration_ms = start_time.elapsed().as_millis();
            if confirmed {
                info!(
                    message_id = %ack.server_msg_id,
                    user_id = %user_id,
                    duration_ms = duration_ms,
                    "Client ACK confirmed, stopping retry"
                );
            } else {
                warn!(
                    message_id = %ack.server_msg_id,
                    user_id = %user_id,
                    duration_ms = duration_ms,
                    "ACK not found or already confirmed"
                );
            }
        } else {
            let duration_ms = start_time.elapsed().as_millis();
            warn!(
                message_id = %ack.server_msg_id,
                user_id = %user_id,
                duration_ms = duration_ms,
                "Failed to confirm ACK"
            );
        }

        Ok(())
    }

    /// 分发推送通知（业务逻辑）- 从 Kafka 消费
    #[instrument(skip(self), fields(user_count = request.user_ids.len()))]
    pub async fn dispatch_push_notification(&self, request: PushNotificationRequest) -> Result<()> {
        // 将 PushNotificationRequest 转换为 PushDispatchTask 并批量处理
        let tasks = self.convert_notification_request_to_tasks(&request)?;
        self.process_tasks(tasks).await
    }

    /// 批量处理推送任务（业务逻辑）- Kafka 消费的主要逻辑
    #[instrument(skip(self), fields(batch_size = batch.len()))]
    pub async fn process_push_tasks_batch(&self, batch: Vec<Vec<u8>>) -> Result<()> {
        // 1. 反序列化批量任务
        let mut tasks = Vec::new();
        for payload in batch {
            match serde_json::from_slice::<PushDispatchTask>(&payload) {
                Ok(task) => tasks.push(task),
                Err(e) => {
                    warn!(error = %e, "Failed to deserialize push task, skipping");
                    continue;
                }
            }
        }

        if tasks.is_empty() {
            return Ok(());
        }

        info!(task_count = tasks.len(), "Processing batch of push tasks");

        // 2. 批量处理任务
        self.process_tasks(tasks).await
    }

    /// 处理推送任务的核心逻辑
    #[instrument(skip(self), fields(task_count = tasks.len()))]
    async fn process_tasks(&self, tasks: Vec<PushDispatchTask>) -> Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }

        // 1. 提取所有用户ID（去重）
        let user_ids: Vec<String> = tasks
            .iter()
            .flat_map(|task| vec![task.user_id.clone()])
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        // 2. 批量查询在线状态（低延迟优化）
        let online_status_map = self
            .online_repo
            .batch_get_online_status(&user_ids)
            .await
            .map_err(|e| {
                flare_server_core::error::ErrorBuilder::new(
                    flare_server_core::error::ErrorCode::ServiceUnavailable,
                    "Failed to batch query online status",
                )
                .details(e.to_string())
                .build_error()
            })?;

        let query_start = Instant::now();
        let online_count = online_status_map.values().filter(|s| s.online).count();
        let offline_count = online_status_map.values().filter(|s| !s.online).count();
        let query_duration_ms = query_start.elapsed().as_millis();
        info!(
            total_users = user_ids.len(),
            online_users = online_count,
            offline_users = offline_count,
            query_duration_ms = query_duration_ms,
            "Batch queried online status"
        );

        // 3. 按 gateway_id 分组在线用户（批量推送优化）
        let mut gateway_groups: HashMap<String, Vec<(String, PushDispatchTask)>> = HashMap::new();
        let mut offline_tasks: Vec<PushDispatchTask> = Vec::new();

        for task in tasks {
            let user_id = &task.user_id;
            let status = online_status_map.get(user_id);

            if let Some(status) = status {
                if status.online {
                    // 在线用户：按 gateway_id 分组
                    if let Some(gateway_id) = &status.gateway_id {
                        gateway_groups
                            .entry(gateway_id.clone())
                            .or_insert_with(Vec::new)
                            .push((user_id.clone(), task));
                    } else {
                        warn!(
                            user_id = %user_id,
                            "Online user has no gateway_id, treating as offline"
                        );
                        offline_tasks.push(task);
                    }
                } else {
                    // 离线用户
                    offline_tasks.push(task);
                }
            } else {
                // 未查询到状态，视为离线
                warn!(user_id = %user_id, "User status not found, treating as offline");
                offline_tasks.push(task);
            }
        }

        info!(
            gateway_count = gateway_groups.len(),
            gateway_ids = ?gateway_groups.keys().collect::<Vec<_>>(),
            offline_count = offline_tasks.len(),
            "Grouped tasks by gateway and offline status"
        );

        // 4. 并发推送到多个网关（低延迟优化）
        let mut push_tasks = Vec::new();
        for (gateway_id, user_tasks) in gateway_groups {
            let router = Arc::clone(&self.gateway_router);
            let state_tracker = Arc::clone(&self.state_tracker);
            let ack_tracker = Arc::clone(&self.ack_tracker);
            let metrics = Arc::clone(&self.metrics);
            let task_publisher = Arc::clone(&self.task_publisher);
            let gateway_id_clone = gateway_id.clone();

            let retry_policy_clone = self.retry_policy.clone();
            push_tasks.push(tokio::spawn(async move {
                Self::push_to_gateway_batch(
                    router,
                    &gateway_id_clone,
                    user_tasks,
                    state_tracker,
                    ack_tracker,
                    metrics,
                    task_publisher,
                    retry_policy_clone,
                )
                .await
            }));
        }

        // 等待所有推送完成
        let push_results = future::join_all(push_tasks).await;
        for result in push_results {
            if let Err(e) = result {
                error!(error = %e, "Gateway push task panicked");
            }
        }

        // 5. 处理离线用户（根据消息类型）
        if !offline_tasks.is_empty() {
            self.handle_offline_tasks(offline_tasks).await?;
        }

        Ok(())
    }

    /// 批量推送到网关（按 gateway_id 分组）
    #[instrument(skip(router, state_tracker, ack_tracker, metrics, task_publisher, retry_policy), fields(gateway_id = %gateway_id, user_count = user_tasks.len()))]
    async fn push_to_gateway_batch(
        router: Arc<dyn GatewayRouterTrait>,
        gateway_id: &str,
        user_tasks: Vec<(String, PushDispatchTask)>,
        state_tracker: Arc<MessageStateTracker>,
        ack_tracker: Arc<AckTracker>,
        metrics: Arc<PushServerMetrics>,
        task_publisher: Arc<dyn PushTaskPublisher>,
        retry_policy: RetryPolicy,
    ) -> Result<()> {
        // 按用户分组任务（一个用户可能有多个任务）
        // 保留 user_groups 用于后续查找 task 的 message_type
        let mut user_groups: HashMap<String, Vec<PushDispatchTask>> = HashMap::new();
        for (user_id, task) in user_tasks {
            user_groups
                .entry(user_id)
                .or_insert_with(Vec::new)
                .push(task);
        }

        // 优化：按用户分组，每个用户推送其所有消息
        // 如果同一用户有多条消息，分别推送每条消息（真正的批量推送）
        let mut user_message_map: HashMap<String, Vec<(String, Message)>> = HashMap::new();

        // 收集所有用户的消息（反序列化）
        for (user_id, tasks) in &user_groups {
            let mut messages = Vec::new();
            for task in tasks {
                // P2优化：零拷贝反序列化
                match Message::decode(task.message.as_slice()) {
                    Ok(msg) => {
                        messages.push((task.message_id.clone(), msg));
                    }
                    Err(e) => {
                        warn!(
                            user_id = %user_id,
                            message_id = %task.message_id,
                            error = %e,
                            "Failed to decode message, skipping"
                        );
                        // 更新状态为失败
                        state_tracker
                            .update_status(
                                &task.message_id,
                                user_id,
                                MessageStatus::Failed,
                                Some(format!("Failed to decode message: {}", e)),
                            )
                            .await;
                    }
                }
            }
            if !messages.is_empty() {
                user_message_map.insert(user_id.clone(), messages);
            }
        }

        if user_message_map.is_empty() {
            return Ok(());
        }

        // 为每个用户构建推送请求（支持一个用户多条消息）
        let mut all_message_ids = Vec::new();
        for (user_id, messages) in &user_message_map {
            for (message_id, _) in messages {
                all_message_ids.push((user_id.clone(), message_id.clone()));
            }
        }

        // 构建批量推送请求：每个用户推送其所有消息
        // 优化：如果同一用户有多条消息，可以合并为一次推送（Gateway 支持）
        let push_request = {
            let target_user_ids: Vec<String> = user_message_map.keys().cloned().collect();
            // 取第一个用户的第一条消息作为推送消息（Gateway 当前接口限制）
            // 但所有 message_id 都记录在 metadata 中
            let first_message = user_message_map
                .values()
                .next()
                .and_then(|msgs| msgs.first())
                .map(|(_, msg)| msg.clone());

            let all_message_ids_str: Vec<String> = all_message_ids
                .iter()
                .map(|(_, msg_id)| msg_id.clone())
                .collect();

            flare_proto::access_gateway::PushMessageRequest {
                request_id: ulid::Ulid::new().to_string(), // 添加必填字段
                target_user_ids,
                message: first_message,
                options: None,

                metadata: {
                    let mut meta = HashMap::new();
                    meta.insert("message_ids".to_string(), all_message_ids_str.join(","));
                    meta
                },
            }
        };

        // 更新所有消息状态为推送中
        for (user_id, message_id) in &all_message_ids {
            state_tracker
                .update_status(message_id, user_id, MessageStatus::Pushing, None)
                .await;
        }

        // 通过 Gateway Router 路由推送（带智能重试）
        let push_result =
            crate::infrastructure::retry::execute_with_retry(&retry_policy, || async {
                router
                    .route_push_message(gateway_id, push_request.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("Gateway push failed: {}", e))
            })
            .await;

        match push_result {
            Ok(response) => {
                // 处理推送结果
                let mut success_count = 0;
                let mut fail_count = 0;

                for result in &response.results {
                    let user_id = &result.user_id;
                    let status_value = result.status as i32;
                    match status_value {
                        1 | 2 => {
                            // PushStatusSuccess = 1, PushStatusPartial = 2
                            // 推送成功
                            success_count += 1;

                            // 从 metadata 中提取所有 message_id（必需字段，不再降级）
                            let message_ids_str: Vec<String> = push_request
                                .metadata
                                .get("message_ids")
                                .ok_or_else(|| {
                                    flare_server_core::error::ErrorBuilder::new(
                                        flare_server_core::error::ErrorCode::InvalidParameter,
                                        "message_ids must be in metadata",
                                    )
                                    .build_error()
                                })?
                                .split(',')
                                .map(|s| s.to_string())
                                .collect();

                            // 更新所有消息状态
                            for message_id in &message_ids_str {
                                state_tracker
                                    .update_status(message_id, user_id, MessageStatus::Pushed, None)
                                    .await;

                                // 注册待确认的ACK（从 task 构建 Context）
                                let ctx = flare_server_core::context::Context::root()
                                    .with_user_id(user_id.clone());
                                if let Err(e) =
                                    ack_tracker.register_pending_ack(&ctx, message_id).await
                                {
                                    tracing::warn!(
                                        error = %e,
                                        message_id = %message_id,
                                        user_id = %user_id,
                                        "Failed to register pending ACK"
                                    );
                                }
                            }

                            // 更新指标
                            metrics
                                .online_push_success_total
                                .with_label_values(&[user_id])
                                .inc();
                        }
                        v if v == 3 => {
                            // PushStatusUserOffline = 3
                            // 用户离线，创建离线推送任务
                            fail_count += 1;

                            // 从 user_message_map 获取该用户的所有消息
                            if let Some(messages) = user_message_map.get(user_id) {
                                for (message_id, _) in messages {
                                    // 从 user_groups 查找对应的 task 以获取 message_type
                                    if let Some(tasks) = user_groups.get(user_id) {
                                        if let Some(task) =
                                            tasks.iter().find(|t| &t.message_id == message_id)
                                        {
                                            if task.message_type == "Normal" {
                                                // 普通消息：创建离线推送任务
                                                if let Err(e) = task_publisher.publish(task).await {
                                                    warn!(
                                                        user_id = %user_id,
                                                        message_id = %message_id,
                                                        error = %e,
                                                        "Failed to create offline task"
                                                    );
                                                }
                                            } else {
                                                // 通知消息：直接舍弃
                                                state_tracker
                                                    .update_status(
                                                        message_id,
                                                        user_id,
                                                        MessageStatus::Expired,
                                                        Some("Notification discarded for offline user".to_string()),
                                                    )
                                                    .await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            // 推送失败
                            fail_count += 1;

                            // 更新该用户所有消息状态为失败
                            if let Some(messages) = user_message_map.get(user_id) {
                                for (message_id, _) in messages {
                                    state_tracker
                                        .update_status(
                                            message_id,
                                            user_id,
                                            MessageStatus::Failed,
                                            Some(result.error_message.clone()),
                                        )
                                        .await;

                                    // 检查消息类型，决定是否创建离线任务
                                    if let Some(tasks) = user_groups.get(user_id) {
                                        if let Some(task) =
                                            tasks.iter().find(|t| &t.message_id == message_id)
                                        {
                                            if task.message_type == "Normal" {
                                                // 普通消息：创建离线推送任务
                                                if let Err(e) = task_publisher.publish(task).await {
                                                    warn!(
                                                        user_id = %user_id,
                                                        message_id = %message_id,
                                                        error = %e,
                                                        "Failed to create offline task"
                                                    );
                                                }
                                            } else {
                                                // 通知消息：直接舍弃
                                                state_tracker
                                                    .update_status(
                                                        message_id,
                                                        user_id,
                                                        MessageStatus::Expired,
                                                        Some(format!(
                                                            "Notification discarded: {}",
                                                            result.error_message
                                                        )),
                                                    )
                                                    .await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                info!(
                    gateway_id = %gateway_id,
                    success_count,
                    fail_count,
                    "Batch push completed"
                );
            }
            Err(e) => {
                // 推送失败，更新所有消息状态为失败
                error!(
                    gateway_id = %gateway_id,
                    error = %e,
                    "Failed to push to gateway"
                );

                for (user_id, messages) in &user_message_map {
                    for (message_id, _) in messages {
                        state_tracker
                            .update_status(
                                message_id,
                                user_id,
                                MessageStatus::Failed,
                                Some(e.to_string()),
                            )
                            .await;

                        // 检查消息类型，决定是否创建离线任务
                        if let Some(tasks) = user_groups.get(user_id) {
                            if let Some(task) = tasks.iter().find(|t| &t.message_id == message_id) {
                                if task.message_type == "Normal" {
                                    // 普通消息：创建离线推送任务
                                    if let Err(e) = task_publisher.publish(task).await {
                                        warn!(
                                            user_id = %user_id,
                                            message_id = %message_id,
                                            error = %e,
                                            "Failed to create offline task"
                                        );
                                    }
                                } else {
                                    // 通知消息：直接舍弃
                                    state_tracker
                                        .update_status(
                                            message_id,
                                            user_id,
                                            MessageStatus::Expired,
                                            Some(
                                                "Notification discarded due to push failure"
                                                    .to_string(),
                                            ),
                                        )
                                        .await;
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// 处理离线任务（根据消息类型）
    #[instrument(skip(self), fields(offline_count = offline_tasks.len()))]
    async fn handle_offline_tasks(&self, offline_tasks: Vec<PushDispatchTask>) -> Result<()> {
        let mut normal_tasks = Vec::new();
        let mut notification_tasks = Vec::new();

        // 按消息类型分类
        for task in offline_tasks {
            if task.message_type == "Notification" {
                notification_tasks.push(task);
            } else {
                normal_tasks.push(task);
            }
        }

        // 普通消息：生成离线推送任务
        if !normal_tasks.is_empty() {
            self.task_publisher
                .publish_offline_batch(&normal_tasks)
                .await
                .map_err(|e| {
                    flare_server_core::error::ErrorBuilder::new(
                        flare_server_core::error::ErrorCode::ServiceUnavailable,
                        "Failed to publish offline tasks",
                    )
                    .details(e.to_string())
                    .build_error()
                })?;

            // 更新状态
            for task in &normal_tasks {
                self.state_tracker
                    .update_status(
                        &task.message_id,
                        &task.user_id,
                        MessageStatus::Pending,
                        None,
                    )
                    .await;
            }

            info!(
                offline_task_count = normal_tasks.len(),
                "Created offline push tasks for normal messages"
            );
        }

        // 通知消息：直接舍弃
        if !notification_tasks.is_empty() {
            for task in &notification_tasks {
                self.state_tracker
                    .update_status(
                        &task.message_id,
                        &task.user_id,
                        MessageStatus::Expired,
                        Some("Notification message discarded for offline user".to_string()),
                    )
                    .await;
            }

            info!(
                notification_count = notification_tasks.len(),
                "Discarded notification messages for offline users"
            );
        }

        Ok(())
    }

    /// 创建离线推送任务（辅助方法）
    async fn create_offline_task(&self, task: PushDispatchTask) -> Result<()> {
        // 注意：这里应该发布到离线推送队列，而不是主队列
        // publish_offline_batch 是批量方法，这里单个任务使用 publish_offline_batch
        self.task_publisher
            .publish_offline_batch(&[task])
            .await
            .map_err(|e| {
                flare_server_core::error::ErrorBuilder::new(
                    flare_server_core::error::ErrorCode::ServiceUnavailable,
                    "Failed to create offline task",
                )
                .details(e.to_string())
                .build_error()
            })
    }

    /// 将 PushMessageRequest 转换为 PushDispatchTask 列表
    ///
    /// 优化：消息类型提前判断（P2优化）
    /// - 从 Message 的 message_type 字段直接判断，避免反序列化整个消息
    fn convert_message_request_to_tasks(
        &self,
        request: &PushMessageRequest,
    ) -> Result<Vec<PushDispatchTask>> {
        // P2优化：消息类型提前判断
        // 从 request.message 中快速提取 message_type，避免完整反序列化
        let (message_type, is_notification) = if let Some(ref message) = request.message {
            // 快速判断：从 message.message_type 枚举值判断
            use flare_proto::common::MessageType;
            let msg_type =
                MessageType::try_from(message.message_type).unwrap_or(MessageType::Unspecified);

            let is_notification =
                matches!(msg_type, MessageType::Notification | MessageType::Typing);

            let persist_if_offline = request
                .options
                .as_ref()
                .map(|o| o.persist_if_offline)
                .unwrap_or(!is_notification);

            let msg_type_str = if is_notification {
                "Notification"
            } else if persist_if_offline {
                "Normal"
            } else {
                "Notification"
            };

            (msg_type_str, is_notification)
        } else {
            // 如果没有 message，根据 options 判断
            let persist_if_offline = request
                .options
                .as_ref()
                .map(|o| o.persist_if_offline)
                .unwrap_or(true);
            (
                if persist_if_offline {
                    "Normal"
                } else {
                    "Notification"
                },
                !persist_if_offline,
            )
        };

        // P2优化：零拷贝序列化（延迟序列化，只在需要时序列化）
        // 注意：这里 message 字段存储的是 PushMessageRequest 的序列化 bytes
        // 如果 request.message 已存在，直接使用；否则延迟序列化
        let message_bytes = if let Some(ref msg) = request.message {
            // 如果 message 已存在，序列化为 bytes（用于后续传递）
            // 注意：这里可以优化为零拷贝，但 prost 需要序列化
            let mut buf = Vec::with_capacity(msg.encoded_len());
            msg.encode(&mut buf).map_err(|e| {
                flare_server_core::error::ErrorBuilder::new(
                    flare_server_core::error::ErrorCode::InternalError,
                    "Failed to encode message",
                )
                .details(e.to_string())
                .build_error()
            })?;
            buf
        } else {
            Vec::new()
        };

        let mut tasks = Vec::with_capacity(request.user_ids.len());
        for user_id in &request.user_ids {
            tasks.push(PushDispatchTask {
                user_id: user_id.clone(),
                message_id: uuid::Uuid::new_v4().to_string(),
                message_type: message_type.to_string(),
                message: message_bytes.clone(), // 复用序列化后的 bytes
                notification: None,
                headers: HashMap::new(),
                metadata: HashMap::new(),
                online: false, // 将在查询在线状态后更新
                tenant_id: None,
                require_online: request
                    .options
                    .as_ref()
                    .map(|o| o.require_online)
                    .unwrap_or(false),
                persist_if_offline: !is_notification,
                priority: request.options.as_ref().map(|o| o.priority).unwrap_or(5),
                context: None,
            });
        }

        Ok(tasks)
    }

    /// 将 PushNotificationRequest 转换为 PushDispatchTask 列表
    fn convert_notification_request_to_tasks(
        &self,
        request: &PushNotificationRequest,
    ) -> Result<Vec<PushDispatchTask>> {
        let mut tasks = Vec::new();
        for user_id in &request.user_ids {
            let notification =
                request
                    .notification
                    .as_ref()
                    .map(|n| crate::domain::model::DispatchNotification {
                        title: n.title.clone(),
                        body: n.body.clone(),
                        data: n.data.clone(),
                        metadata: n.metadata.clone(),
                    });

            tasks.push(PushDispatchTask {
                user_id: user_id.clone(),
                message_id: uuid::Uuid::new_v4().to_string(),
                message_type: "Notification".to_string(),
                message: Vec::new(), // 通知消息不需要 message 字段
                notification,
                headers: HashMap::new(),
                metadata: HashMap::new(),
                online: false,
                tenant_id: None,
                require_online: request
                    .options
                    .as_ref()
                    .map(|o| o.require_online)
                    .unwrap_or(false),
                persist_if_offline: false, // 通知消息不持久化
                priority: request.options.as_ref().map(|o| o.priority).unwrap_or(5),
                context: None,
            });
        }

        Ok(tasks)
    }

    pub async fn get_message_status(
        &self,
        ctx: &flare_server_core::context::Context,
        message_id: &str,
    ) -> Result<MessageStatus> {
        self.state_tracker
            .get_status(ctx, message_id)
            .await
            .map(|state| state.status)
            .ok_or_else(|| {
                flare_server_core::error::ErrorBuilder::new(
                    flare_server_core::error::ErrorCode::InvalidParameter,
                    "Message status not found",
                )
                .build_error()
            })
    }

    // 注意：CID 是不可逆的，无法从 CID 中提取用户/群信息
    // 所有降级处理和提取函数已移除
    // 消息进入时必须验证完整性：单聊提供 receiver_id，群聊/频道提供 channel_id
}
