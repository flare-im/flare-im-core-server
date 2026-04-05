//! 领域事件命令处理器：将 Event 应用到存储（撤回/编辑/删除/已读/反应/置顶/标记）

use flare_im_core::error::{ErrorCode, Result, map_infra_error};
use flare_im_core::metrics::StorageWriterMetrics;
use flare_im_core::Ctx;
use std::sync::Arc;
use std::time::Instant;
use tracing::instrument;

use crate::application::commands::ProcessEventCommand;
use crate::domain::model::PersistenceResult;
use crate::domain::service::EventApplicationService;
use crate::domain::service::event_handlers;
use crate::infrastructure::persistence::repository::event_stream::PostgresEventStreamStore;
use crate::infrastructure::persistence::repository::postgres_store::PostgresMessageStore;

// 类型别名
type ArchiveRepo = PostgresMessageStore;
type EventStreamRepo = PostgresEventStreamStore;
type EventApplicationServiceType = EventApplicationService<ArchiveRepo, EventStreamRepo>;

pub struct MessageOperationCommandHandler {
    event_service: Arc<EventApplicationServiceType>,
    metrics: Arc<StorageWriterMetrics>,
}

impl MessageOperationCommandHandler {
    pub fn new(
        event_service: Arc<EventApplicationServiceType>,
        metrics: Arc<StorageWriterMetrics>,
    ) -> Self {
        Self {
            event_service,
            metrics,
        }
    }

    #[instrument(skip(self))]
    pub async fn handle(
        &self,
        ctx: &Ctx,
        command: ProcessEventCommand,
    ) -> Result<PersistenceResult> {
        let start = Instant::now();
        let event = command.event;

        self.event_service.process_event(ctx, &event).await.map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;

        let message_id = event_handlers::primary_message_id_for_metrics(&event);

        let duration = start.elapsed();
        self.metrics
            .messages_persisted_duration_seconds
            .observe(duration.as_secs_f64());
        self.metrics
            .messages_persisted_total
            .with_label_values(&["event"])
            .inc();

        Ok(PersistenceResult {
            conversation_id: event.conversation_id,
            message_id,
            timeline: Default::default(),
            deduplicated: false,
        })
    }
}
