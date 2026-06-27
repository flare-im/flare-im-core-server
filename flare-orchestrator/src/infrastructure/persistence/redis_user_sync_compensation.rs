use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use redis::aio::ConnectionManager;

use crate::domain::repository::{UserSyncCompensationRepository, UserSyncCompensationTask};
use flare_server_core::error::{FlareError, Result};

const PENDING_KEY: &str = "sync:user:compensation:pending";
const TASK_KEY_PREFIX: &str = "sync:user:compensation:task";
const CLAIM_VISIBILITY_MS: i64 = 30_000;

pub struct RedisUserSyncCompensationRepository {
    client: Arc<redis::Client>,
}

impl RedisUserSyncCompensationRepository {
    pub fn new(client: Arc<redis::Client>) -> Self {
        Self { client }
    }

    async fn get_connection(&self) -> Result<ConnectionManager> {
        ConnectionManager::new(self.client.as_ref().clone())
            .await
            .map_err(|err| {
                FlareError::system(format!("Redis user sync compensation connect: {err}"))
            })
    }

    fn task_key(task_id: &str) -> String {
        format!("{TASK_KEY_PREFIX}:{task_id}")
    }

    fn now_millis() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or_default()
    }

    fn encode_task(task: &UserSyncCompensationTask) -> Result<String> {
        serde_json::to_string(task).map_err(|err| {
            FlareError::system(format!(
                "serialize user_sync compensation task {}: {err}",
                task.task_id
            ))
        })
    }

    fn decode_task(task_id: &str, payload: &str) -> Result<UserSyncCompensationTask> {
        serde_json::from_str(payload).map_err(|err| {
            FlareError::system(format!(
                "deserialize user_sync compensation task {task_id}: {err}"
            ))
        })
    }
}

#[async_trait]
impl UserSyncCompensationRepository for RedisUserSyncCompensationRepository {
    async fn enqueue(&self, task: UserSyncCompensationTask) -> Result<()> {
        let payload = Self::encode_task(&task)?;
        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("HSET")
            .arg(Self::task_key(&task.task_id))
            .arg("payload")
            .arg(payload)
            .cmd("ZADD")
            .arg(PENDING_KEY)
            .arg(task.due_at_ms)
            .arg(&task.task_id);

        let _: Vec<redis::Value> = pipe.query_async(&mut conn).await.map_err(|err| {
            FlareError::system(format!(
                "enqueue user_sync compensation task {}: {err}",
                task.task_id
            ))
        })?;
        Ok(())
    }

    async fn claim_due(&self, limit: usize) -> Result<Vec<UserSyncCompensationTask>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let now = Self::now_millis();
        let mut conn = self.get_connection().await?;
        let task_ids: Vec<String> = redis::cmd("ZRANGEBYSCORE")
            .arg(PENDING_KEY)
            .arg("-inf")
            .arg(now)
            .arg("LIMIT")
            .arg(0)
            .arg(limit)
            .query_async(&mut conn)
            .await
            .map_err(|err| {
                FlareError::system(format!("claim user_sync compensation tasks: {err}"))
            })?;

        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        let claim_until = now.saturating_add(CLAIM_VISIBILITY_MS);
        let mut visibility_pipe = redis::pipe();
        visibility_pipe.atomic();
        for task_id in &task_ids {
            visibility_pipe
                .cmd("ZADD")
                .arg(PENDING_KEY)
                .arg(claim_until)
                .arg(task_id);
        }
        let _: Vec<redis::Value> = visibility_pipe
            .query_async(&mut conn)
            .await
            .map_err(|err| {
                FlareError::system(format!(
                    "mark user_sync compensation tasks claimed count={}: {err}",
                    task_ids.len()
                ))
            })?;

        let mut payload_pipe = redis::pipe();
        for task_id in &task_ids {
            payload_pipe
                .cmd("HGET")
                .arg(Self::task_key(task_id))
                .arg("payload");
        }
        let payloads: Vec<Option<String>> =
            payload_pipe.query_async(&mut conn).await.map_err(|err| {
                FlareError::system(format!(
                    "load user_sync compensation task payloads count={}: {err}",
                    task_ids.len()
                ))
            })?;

        let mut tasks = Vec::new();
        for (task_id, payload) in task_ids.into_iter().zip(payloads) {
            let Some(payload) = payload else {
                continue;
            };
            tasks.push(Self::decode_task(&task_id, &payload)?);
        }
        Ok(tasks)
    }

    async fn mark_completed(&self, task_id: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let mut pipe = redis::pipe();
        pipe.atomic()
            .cmd("ZREM")
            .arg(PENDING_KEY)
            .arg(task_id)
            .cmd("DEL")
            .arg(Self::task_key(task_id));
        let _: Vec<redis::Value> = pipe.query_async(&mut conn).await.map_err(|err| {
            FlareError::system(format!(
                "complete user_sync compensation task {task_id}: {err}"
            ))
        })?;
        Ok(())
    }

    async fn mark_failed(
        &self,
        mut task: UserSyncCompensationTask,
        error: &str,
        retry_after_ms: i64,
    ) -> Result<()> {
        task.attempts = task.attempts.saturating_add(1);
        task.last_error = Some(error.chars().take(512).collect());
        task.due_at_ms = Self::now_millis().saturating_add(retry_after_ms.max(0));
        self.enqueue(task).await
    }
}
