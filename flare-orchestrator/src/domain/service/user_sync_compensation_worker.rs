use std::sync::Arc;
use std::time::Duration;

use flare_server_core::context::Context;
use flare_server_core::error::Result;

use crate::domain::repository::{
    UserSyncCompensationKind, UserSyncCompensationRepository, UserSyncCompensationTask,
    UserSyncIndexRepository,
};

const DEFAULT_BATCH_SIZE: usize = 64;
const DEFAULT_RETRY_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_INTERVAL: Duration = Duration::from_secs(5);

pub struct UserSyncCompensationWorker {
    repository: Arc<dyn UserSyncCompensationRepository>,
    user_sync_index: Arc<dyn UserSyncIndexRepository>,
    batch_size: usize,
    retry_delay: Duration,
    interval: Duration,
}

impl UserSyncCompensationWorker {
    pub fn new(
        repository: Arc<dyn UserSyncCompensationRepository>,
        user_sync_index: Arc<dyn UserSyncIndexRepository>,
    ) -> Self {
        Self {
            repository,
            user_sync_index,
            batch_size: DEFAULT_BATCH_SIZE,
            retry_delay: DEFAULT_RETRY_DELAY,
            interval: DEFAULT_INTERVAL,
        }
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    pub async fn replay_once(&self) -> Result<usize> {
        let tasks = self.repository.claim_due(self.batch_size).await?;
        let task_count = tasks.len();
        for task in tasks {
            self.replay_task(task).await;
        }
        Ok(task_count)
    }

    async fn replay_task(&self, task: UserSyncCompensationTask) {
        let ctx = Arc::new(Context::root().with_tenant_id(task.tenant_id.clone()));
        let result = match task.kind {
            UserSyncCompensationKind::EagerUserChanges => {
                self.user_sync_index
                    .record_conversation_change(
                        &ctx,
                        &task.user_ids,
                        &task.conversation_id,
                        task.max_conversation_seq,
                        task.occurred_at_ms,
                    )
                    .await
            }
            UserSyncCompensationKind::ConversationVersionBump => self
                .user_sync_index
                .record_conversation_version_bump(
                    &ctx,
                    &task.conversation_id,
                    task.max_conversation_seq,
                    task.occurred_at_ms,
                )
                .await
                .map(|_| ()),
        };

        match result {
            Ok(()) => {
                if let Err(error) = self.repository.mark_completed(&task.task_id).await {
                    tracing::warn!(
                        error = %error,
                        task_id = %task.task_id,
                        "failed to mark user_sync compensation task completed"
                    );
                }
            }
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    task_id = %task.task_id,
                    conversation_id = %task.conversation_id,
                    attempts = task.attempts,
                    "user_sync compensation replay failed; task rescheduled"
                );
                if let Err(mark_error) = self
                    .repository
                    .mark_failed(
                        task,
                        &error.to_string(),
                        self.retry_delay.as_millis() as i64,
                    )
                    .await
                {
                    tracing::warn!(
                        error = %mark_error,
                        "failed to reschedule user_sync compensation task"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use flare_im_contracts::Ctx;
    use flare_server_core::error::FlareError;
    use std::sync::Mutex;

    use crate::domain::repository::ConversationChange;

    #[derive(Default)]
    struct MemoryCompensationRepository {
        tasks: Mutex<Vec<UserSyncCompensationTask>>,
        completed: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl UserSyncCompensationRepository for MemoryCompensationRepository {
        async fn enqueue(&self, task: UserSyncCompensationTask) -> Result<()> {
            self.tasks.lock().expect("tasks lock").push(task);
            Ok(())
        }

        async fn claim_due(&self, limit: usize) -> Result<Vec<UserSyncCompensationTask>> {
            let mut tasks = self.tasks.lock().expect("tasks lock");
            let claim_count = limit.min(tasks.len());
            Ok(tasks.drain(0..claim_count).collect())
        }

        async fn mark_completed(&self, task_id: &str) -> Result<()> {
            self.completed
                .lock()
                .expect("completed lock")
                .push(task_id.to_string());
            Ok(())
        }

        async fn mark_failed(
            &self,
            mut task: UserSyncCompensationTask,
            error: &str,
            _retry_after_ms: i64,
        ) -> Result<()> {
            task.attempts += 1;
            task.last_error = Some(error.to_string());
            self.tasks.lock().expect("tasks lock").push(task);
            Ok(())
        }
    }

    struct RecordingUserSyncIndex {
        fail: bool,
        events: Mutex<Vec<String>>,
    }

    impl RecordingUserSyncIndex {
        fn new(fail: bool) -> Self {
            Self {
                fail,
                events: Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl UserSyncIndexRepository for RecordingUserSyncIndex {
        async fn record_conversation_change(
            &self,
            _ctx: &Ctx,
            user_ids: &[String],
            conversation_id: &str,
            max_conversation_seq: u64,
            _occurred_at_ms: i64,
        ) -> Result<()> {
            if self.fail {
                return Err(FlareError::system("injected compensation failure"));
            }
            self.events.lock().expect("events lock").push(format!(
                "eager:{conversation_id}:{max_conversation_seq}:{}",
                user_ids.len()
            ));
            Ok(())
        }

        async fn record_conversation_version_bump(
            &self,
            _ctx: &Ctx,
            conversation_id: &str,
            max_conversation_seq: u64,
            _occurred_at_ms: i64,
        ) -> Result<u64> {
            if self.fail {
                return Err(FlareError::system("injected compensation failure"));
            }
            self.events
                .lock()
                .expect("events lock")
                .push(format!("version:{conversation_id}:{max_conversation_seq}"));
            Ok(1)
        }

        async fn diff_changed_conversations(
            &self,
            _ctx: &Ctx,
            _known: &[(String, u64)],
        ) -> Result<Vec<ConversationChange>> {
            Ok(Vec::new())
        }
    }

    fn ctx() -> Ctx {
        Arc::new(Context::root().with_tenant_id("0"))
    }

    #[tokio::test]
    async fn replay_once_replays_and_acks_eager_task() {
        let repository = Arc::new(MemoryCompensationRepository::default());
        let user_sync_index = Arc::new(RecordingUserSyncIndex::new(false));
        repository
            .enqueue(
                UserSyncCompensationTask::eager_user_changes(
                    &ctx(),
                    &["u2".to_string(), "u1".to_string()],
                    "c1",
                    42,
                    1_000,
                    0,
                )
                .expect("task"),
            )
            .await
            .expect("enqueue");
        let worker = UserSyncCompensationWorker::new(repository.clone(), user_sync_index.clone());

        let replayed = worker.replay_once().await.expect("replay");

        assert_eq!(replayed, 1);
        assert_eq!(
            user_sync_index
                .events
                .lock()
                .expect("events lock")
                .as_slice(),
            &["eager:c1:42:2"]
        );
        assert_eq!(
            repository.completed.lock().expect("completed lock").len(),
            1
        );
    }

    #[tokio::test]
    async fn replay_once_reschedules_failed_task() {
        let repository = Arc::new(MemoryCompensationRepository::default());
        let user_sync_index = Arc::new(RecordingUserSyncIndex::new(true));
        repository
            .enqueue(
                UserSyncCompensationTask::conversation_version_bump(
                    &ctx(),
                    "c-large",
                    900,
                    1_000,
                    0,
                )
                .expect("task"),
            )
            .await
            .expect("enqueue");
        let worker = UserSyncCompensationWorker::new(repository.clone(), user_sync_index);

        let replayed = worker.replay_once().await.expect("replay");

        assert_eq!(replayed, 1);
        let tasks = repository.tasks.lock().expect("tasks lock");
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].attempts, 1);
        assert!(tasks[0].last_error.as_deref().is_some());
    }
}
