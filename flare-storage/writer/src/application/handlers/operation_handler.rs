//! 操作消息命令处理器 - 专门处理消息操作（撤回、编辑、删除等）

use anyhow::Result;
use flare_im_core::metrics::StorageWriterMetrics;
use std::sync::Arc;
use std::time::Instant;
use tracing::instrument;

use crate::application::commands::ProcessMessageOperationCommand;
use crate::domain::model::PersistenceResult;
use crate::domain::service::MessageOperationDomainService;

/// 操作消息命令处理器
///
/// 专门处理消息操作命令（撤回、编辑、删除等）
pub struct MessageOperationCommandHandler {
    operation_service: Arc<MessageOperationDomainService>,
    metrics: Arc<StorageWriterMetrics>,
}

impl MessageOperationCommandHandler {
    pub fn new(
        operation_service: Arc<MessageOperationDomainService>,
        metrics: Arc<StorageWriterMetrics>,
    ) -> Self {
        Self {
            operation_service,
            metrics,
        }
    }

    /// 处理消息操作命令（撤回、编辑、删除等）
    #[instrument(skip(self), fields(operation_type = %command.operation.operation_type))]
    pub async fn handle(
        &self,
        command: ProcessMessageOperationCommand,
    ) -> Result<PersistenceResult> {
        let start = Instant::now();

        // 处理操作
        self.operation_service
            .process_operation(command.operation.clone(), &command.message)
            .await?;

        // 构建结果（操作消息不创建新消息，返回操作的目标消息ID）
        let result = PersistenceResult {
            conversation_id: command.message.conversation_id.clone(),
            message_id: command.operation.target_message_id.clone(),
            timeline: Default::default(),
            deduplicated: false,
        };

        // 记录指标
        let duration = start.elapsed();
        self.metrics
            .messages_persisted_duration_seconds
            .observe(duration.as_secs_f64());
        self.metrics
            .messages_persisted_total
            .with_label_values(&["operation"])
            .inc();

        tracing::info!(
            message_id = %result.message_id,
            operation_type = %command.operation.operation_type,
            duration_ms = duration.as_millis(),
            "Message operation processed successfully"
        );

        Ok(result)
    }
}