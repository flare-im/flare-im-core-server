//! 推送领域服务 - 包含所有业务逻辑实现

use std::collections::HashMap;
use std::sync::Arc;

use flare_im_core::gateway::GatewayRouterTrait;
use flare_im_core::hooks::HookDispatcher;
use flare_im_core::metrics::PushWorkerMetrics;
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tracing::{error, info, instrument, warn};

use crate::config::PushWorkerConfig;
use crate::domain::model::PushDispatchTask;
use crate::domain::repository::{
    AckPublisher, DlqPublisher, OfflinePushSender, OnlinePushSender, PushAckEvent,
};
use crate::infrastructure::hook::{HookExecutor, build_delivery_context, build_delivery_event};
use crate::infrastructure::retry::{RetryPolicy, RetryableError};

/// 推送领域服务 - 包含所有业务逻辑
pub struct PushDomainService {
    config: Arc<PushWorkerConfig>,
    online_sender: Arc<dyn OnlinePushSender>,
    offline_sender: Arc<dyn OfflinePushSender>,
    ack_publisher: Arc<dyn AckPublisher>,
    dlq_publisher: Arc<dyn DlqPublisher>,
    gateway_router: Option<Arc<dyn GatewayRouterTrait>>,
    hooks: Arc<HookDispatcher>,
    hook_executor: Arc<HookExecutor>,
    retry_policy: RetryPolicy,
    metrics: Arc<PushWorkerMetrics>,
}

impl PushDomainService {
    pub fn new(
        config: Arc<PushWorkerConfig>,
        online_sender: Arc<dyn OnlinePushSender>,
        offline_sender: Arc<dyn OfflinePushSender>,
        ack_publisher: Arc<dyn AckPublisher>,
        dlq_publisher: Arc<dyn DlqPublisher>,
        gateway_router: Option<Arc<dyn GatewayRouterTrait>>,
        hooks: Arc<HookDispatcher>,
        hook_executor: Arc<HookExecutor>,
        metrics: Arc<PushWorkerMetrics>,
    ) -> Self {
        let retry_policy = RetryPolicy::from_config(
            config.push_retry_max_attempts,
            config.push_retry_initial_delay_ms,
            config.push_retry_max_delay_ms,
            config.push_retry_backoff_multiplier,
        );

        Self {
            config,
            online_sender,
            offline_sender,
            ack_publisher,
            dlq_publisher,
            gateway_router,
            hooks,
            hook_executor,
            retry_policy,
            metrics,
        }
    }

    /// 执行推送任务（业务逻辑）- 单个任务
    #[instrument(skip(self), fields(user_id = %task.user_id, message_id = %task.message_id, online = task.online))]
    pub async fn execute_push_task(&self, task: PushDispatchTask) -> Result<()> {
        let start = std::time::Instant::now();

        // 提取租户ID和平台用于指标
        let tenant_id = task.tenant_id.as_deref().unwrap_or("unknown");
        let platform = task
            .metadata
            .get("platform")
            .map(|s| s.as_str())
            .unwrap_or("unknown");

        // 如果要求在线但用户离线，跳过
        if task.require_online && !task.online {
            info!(user_id = %task.user_id, "skip offline task due to require_online=true");
            return Ok(());
        }

        // 执行推送（带重试）
        let result = if task.online {
            // 在线推送：通过 Gateway Router 路由到 Access Gateway
            self.execute_online_push(&task).await
        } else if task.persist_if_offline {
            // 离线推送：通过外部渠道（APNs/FCM/WebPush）
            self.execute_offline_push(&task).await
        } else {
            warn!(user_id = %task.user_id, "offline task dropped because persist_if_offline=false");
            return Ok(());
        };

        // 记录推送耗时
        let duration = start.elapsed();
        self.metrics
            .push_duration_seconds
            .with_label_values(&[platform, tenant_id])
            .observe(duration.as_secs_f64());

        // 处理推送结果
        match result {
            Ok(_) => {
                // 推送成功，上报ACK
                self.publish_ack(&task, true, None).await?;

                // 记录离线推送成功（仅离线推送）
                if !task.online {
                    self.metrics
                        .offline_push_success_total
                        .with_label_values(&[platform, tenant_id])
                        .inc();
                }

                // PostDelivery Hook
                let ctx = build_delivery_context(tenant_id, &task);
                let event =
                    build_delivery_event(&task, if task.online { "online" } else { "offline" });
                if let Err(e) = self.hook_executor.post_delivery(&ctx, &event).await {
                    warn!(error = %e, "PostDelivery hook execution failed");
                }

                Ok(())
            }
            Err(e) => {
                // 推送失败，上报ACK并发送到死信队列
                let error_str = e.to_string();
                error!(
                    message_id = %task.message_id,
                    user_id = %task.user_id,
                    error = %error_str,
                    "Push failed after retries"
                );

                // 记录离线推送失败（仅离线推送）
                if !task.online {
                    // 注意：metrics 的 label_values 需要 &str，error_str 已经是 String
                    let error_label = error_str.as_str();
                    self.metrics
                        .offline_push_failure_total
                        .with_label_values(&[platform, error_label, tenant_id])
                        .inc();
                }

                // 上报失败ACK
                let _ = self.publish_ack(&task, false, Some(&error_str)).await;

                // 发送到死信队列
                self.dlq_publisher.publish_to_dlq(&task, &error_str).await?;

                // 记录死信队列消息数
                self.metrics
                    .dlq_messages_total
                    .with_label_values(&[error_str.as_str(), tenant_id])
                    .inc();

                Ok(()) // 返回Ok，避免重复处理
            }
        }
    }

    /// 批量执行推送任务（业务逻辑）- Kafka 消费的主要逻辑
    #[instrument(skip(self), fields(batch_size = tasks.len()))]
    pub async fn execute_push_tasks_batch(&self, tasks: Vec<PushDispatchTask>) -> Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }

        // 并发处理任务
        let mut handles = Vec::new();
        for task in tasks {
            let service = self.clone_for_task();
            handles.push(tokio::spawn(async move {
                service.execute_push_task(task).await
            }));
        }

        // 等待所有任务完成
        let mut success_count = 0;
        let mut fail_count = 0;
        for handle in handles {
            match handle.await {
                Ok(Ok(_)) => success_count += 1,
                Ok(Err(e)) => {
                    error!(error = %e, "Task execution failed");
                    fail_count += 1;
                }
                Err(e) => {
                    error!(error = %e, "Task join error");
                    fail_count += 1;
                }
            }
        }

        info!(success_count, fail_count, "Batch push tasks completed");

        Ok(())
    }

    /// 执行在线推送（通过 Gateway Router）
    #[instrument(skip(self))]
    async fn execute_online_push(&self, task: &PushDispatchTask) -> Result<()> {
        // 如果有 Gateway Router，使用它路由推送
        if let Some(router) = &self.gateway_router {
            // 需要查询用户的 gateway_id（从 Signaling Online 服务）
            // 简化处理：假设 task.metadata 中包含 gateway_id
            let gateway_id = task.metadata.get("gateway_id").ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::InvalidParameter,
                    "gateway_id not found in task metadata",
                )
                .build_error()
            })?;

            // 构建 PushMessageRequest
            let push_request = self.build_push_message_request(task)?;

            // 通过 Gateway Router 路由推送
            match router.route_push_message(gateway_id, push_request).await {
                Ok(response) => {
                    // 检查推送结果
                    if response.results.is_empty() {
                        return Err(ErrorBuilder::new(
                            ErrorCode::ServiceUnavailable,
                            "No push results returned",
                        )
                        .build_error());
                    }

                    // 检查是否有失败的用户
                    let mut has_failure = false;
                    for result in &response.results {
                        let status_value = result.status as i32;
                        if status_value == 3 {
                            // PushStatusUserOffline = 3
                            has_failure = true;
                            break;
                        }
                    }

                    if has_failure {
                        // 部分用户离线，降级到离线推送
                        warn!(
                            user_id = %task.user_id,
                            "User went offline during push, downgrading to offline push"
                        );
                        return self.execute_offline_push(task).await;
                    }

                    Ok(())
                }
                Err(e) => {
                    // Gateway Router 错误，降级到离线推送
                    warn!(
                        error = %e,
                        user_id = %task.user_id,
                        "Gateway Router error, downgrading to offline push"
                    );
                    self.execute_offline_push(task).await
                }
            }
        } else {
            // 没有 Gateway Router，使用 OnlinePushSender（可能是 Noop）
            self.execute_with_retry(|| self.online_sender.send(task))
                .await
                .map_err(|e| {
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "Online push failed")
                        .details(e)
                        .build_error()
                })
        }
    }

    /// 执行离线推送（通过外部渠道）
    #[instrument(skip(self))]
    async fn execute_offline_push(&self, task: &PushDispatchTask) -> Result<()> {
        self.execute_with_retry(|| self.offline_sender.send(task))
            .await
            .map_err(|e| {
                ErrorBuilder::new(ErrorCode::ServiceUnavailable, "Offline push failed")
                    .details(e)
                    .build_error()
            })
    }

    /// 带重试的执行推送
    async fn execute_with_retry<F, Fut>(&self, mut f: F) -> std::result::Result<(), String>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let mut attempt = 0;
        let mut last_error = None;

        while attempt < self.retry_policy.max_attempts {
            match f().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    let error_str = e.to_string();
                    let err = anyhow::Error::from(e);
                    if err.is_retryable() && attempt < self.retry_policy.max_attempts - 1 {
                        // 可重试的错误，等待后重试
                        let delay = self.retry_policy.calculate_delay(attempt);
                        tokio::time::sleep(delay).await;
                        last_error = Some(error_str);
                        attempt += 1;
                        continue;
                    } else {
                        // 永久失败或达到最大重试次数
                        return Err(error_str);
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| "Max retries exceeded".to_string()))
    }

    /// 构建 PushMessageRequest（用于 Gateway Router）
    fn build_push_message_request(
        &self,
        task: &PushDispatchTask,
    ) -> Result<flare_proto::access_gateway::PushMessageRequest> {
        // task.message 是 Vec<u8>，我们需要将其作为自定义内容
        let message_content = if !task.message.is_empty() {
            Some(flare_proto::common::MessageContent {
                content: Some(flare_proto::common::message_content::Content::Custom(
                    flare_proto::common::CustomContent {
                        r#type: "application/octet-stream".to_string(),
                        payload: task.message.clone(),
                        description: String::new(),
                        metadata: std::collections::HashMap::new(),
                        extensions: Vec::new(), // 添加缺失的 extensions 字段
                    },
                )),
                extensions: Vec::new(), // 添加缺失的 extensions 字段
            })
        } else {
            None
        };

        Ok(flare_proto::access_gateway::PushMessageRequest {
            target_user_ids: vec![task.user_id.clone()],
            message: Some(flare_proto::common::Message {
                server_id: task.message_id.clone(),
                conversation_id: String::new(),
                client_msg_id: String::new(),
                sender_id: String::new(),
                receiver_id: String::new(), // 离线推送：receiver_id 由任务决定
                channel_id: String::new(),  // 离线推送：channel_id 由任务决定
                source: 1,                  // MessageSource::User
                seq: 0,
                timestamp: Some(prost_types::Timestamp {
                    seconds: chrono::Utc::now().timestamp(),
                    nanos: 0,
                }),
                conversation_type: 1, // ConversationType::Single = 1
                message_type: 0, // MessageType::Unspecified = 0
                business_type: String::new(),
                content: message_content,
                content_type: 1, // ContentType::PlainText
                attachments: vec![],
                quote: None,
                status: 1,       // MessageStatus::Created = 1
                extra: HashMap::new(),
                attributes: HashMap::new(),
                is_recalled: false,
                current_edit_version: 0, // 未编辑
                last_edited_at: None,    // 未编辑
                recalled_at: None,
                recall_reason: String::new(),
                is_burn_after_read: false,
                burn_after_seconds: 0,
                tenant: String::new(),
                audit: None,
                tags: Vec::new(),
                visibility: HashMap::new(),
                read_by: Vec::new(),
                timeline: None,
                offline_push_info: None,
                edit_history: Vec::new(),
                reactions: Vec::new(),
                extensions: Vec::new(), // 添加缺失的 extensions 字段
            }),
            options: None,

            metadata: HashMap::new(),
            request_id: uuid::Uuid::new_v4().to_string(), // 添加缺失的 request_id 字段
        })
    }

    /// 上报ACK
    async fn publish_ack(
        &self,
        task: &PushDispatchTask,
        success: bool,
        error: Option<&str>,
    ) -> Result<()> {
        let event = PushAckEvent {
            message_id: task.message_id.clone(),
            user_id: task.user_id.clone(),
            success,
            error: error.map(|s| s.to_string()),
            timestamp: chrono::Utc::now().timestamp(),
        };

        self.ack_publisher.publish_ack(&event).await
    }

    /// 克隆服务用于并发任务（只克隆必要的 Arc）
    fn clone_for_task(&self) -> Self {
        Self {
            config: Arc::clone(&self.config),
            online_sender: Arc::clone(&self.online_sender),
            offline_sender: Arc::clone(&self.offline_sender),
            ack_publisher: Arc::clone(&self.ack_publisher),
            dlq_publisher: Arc::clone(&self.dlq_publisher),
            gateway_router: self.gateway_router.as_ref().map(|r| Arc::clone(r)),
            hooks: Arc::clone(&self.hooks),
            hook_executor: Arc::clone(&self.hook_executor),
            retry_policy: self.retry_policy.clone(),
            metrics: Arc::clone(&self.metrics),
        }
    }

    /// 获取推送任务状态
    pub async fn get_push_task_status(
        &self,
        message_id: &str,
        user_id: &str,
    ) -> Result<Option<String>> {
        // 这是一个占位实现，实际实现需要查询存储或缓存来获取任务状态
        // 在实际系统中，这可能涉及查询Redis、数据库或其他存储系统
        Ok(None)
    }

    /// 获取推送统计信息
    pub async fn get_push_statistics(
        &self,
        _query: &crate::application::queries::QueryPushStatisticsQuery,
    ) -> Result<crate::application::dto::PushStatsResponse> {
        // 这是一个占位实现，实际实现需要从指标系统或存储中获取统计数据
        Ok(crate::application::dto::PushStatsResponse {
            total_pushes: 0,
            successful_pushes: 0,
            failed_pushes: 0,
            average_latency_ms: 0.0,
            p99_latency_ms: 0.0,
        })
    }
}
