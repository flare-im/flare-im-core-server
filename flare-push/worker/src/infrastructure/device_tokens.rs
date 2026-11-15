//! Device-scoped registry lookups and per-registration vendor-acceptance receipts.
//!
//! A binding its device has not inspected for the inactivity window (or, never inspected,
//! not registered within it) is absent: lookups skip it and remove it with the same
//! conditional removal a vendor-reported dead token gets.
use crate::domain::DeviceTokenRepository;
use flare_im_contracts::domain::push_binding::{
    push_binding_inactive, push_binding_inactive_cutoff_ms,
};
use flare_im_contracts::{
    Ctx, DevicePushToken, device_push_token_registry_field, device_push_token_registry_key,
};
use flare_proto::PushTaskEnvelope;
use flare_server_core::error::{ErrorCode, FlareError};
use redis::aio::ConnectionManager;
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const IO_BUDGET: Duration = Duration::from_secs(3);
const RECEIPT_TTL_SECS: u64 = 7 * 24 * 3600;
const MAX_RECORD_BYTES: usize = 16 * 1024;
/// Vendor channels a device can be bound through; the registry hash field is `provider:device_id`.
const DEVICE_PROVIDERS: [&str; 3] = ["fcm", "apns", "getui"];
/// Reads only the target device's fields (one per vendor channel), so the cost of a task does
/// not grow with how many devices the user has registered. Oversized records fail inside Redis
/// instead of crossing the wire.
const LIST_DEVICE: &str = r#"
local rows = {}
for i = 1, #ARGV do
  local raw = redis.call('HGET', KEYS[1], ARGV[i])
  if raw then
    if #raw > 16384 then return redis.error_reply('record limit') end
    rows[#rows + 1] = raw
  end
end
return rows
"#;
/// Removes the observed generation only: same registration id and token. With ARGV[4] set it
/// is an inactivity removal and also requires the row to be still inactive (last active before
/// that cutoff), so an inspection that refreshed the binding meanwhile keeps it.
const REMOVE_OBSERVED: &str = r#"
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local ok, row = pcall(cjson.decode, raw)
if ok and row.registration_id == ARGV[2] and row.token == ARGV[3] then
  if ARGV[4] ~= '' then
    local active = math.max(tonumber(row.last_seen_ms) or 0, tonumber(row.registered_at_ms) or 0)
    if active >= tonumber(ARGV[4]) then return 0 end
  end
  local slotraw=redis.call('GET',KEYS[2])
  if slotraw then local slot=cjson.decode(slotraw); if slot.binding_id==ARGV[2] then slot.registry=nil;redis.call('SET',KEYS[2],cjson.encode(slot),'EX',86400) end end
  redis.call('SET',KEYS[3],'{"revoked":true}','EX',86400)
  return redis.call('HDEL', KEYS[1], ARGV[1])
end
return 0
"#;

pub struct RedisDeviceTokenRepository {
    conn: ConnectionManager,
    key_prefix: String,
    /// Days without activity after which a binding is absent.
    inactive_days: u32,
}

fn unavailable() -> FlareError {
    FlareError::localized(ErrorCode::ServiceUnavailable, "device registry unavailable")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

impl RedisDeviceTokenRepository {
    pub async fn connect(
        redis_url: &str,
        key_prefix: String,
        inactive_days: u32,
    ) -> Result<Self, FlareError> {
        let client = redis::Client::open(redis_url).map_err(|_| unavailable())?;
        let conn = tokio::time::timeout(IO_BUDGET, client.get_connection_manager())
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
        Ok(Self {
            conn,
            key_prefix,
            inactive_days,
        })
    }

    /// The conditional removal of one observed binding generation. `inactive_before` set: only
    /// while the row is still last active before it.
    async fn remove_observed(
        &self,
        token: &DevicePushToken,
        inactive_before: Option<i64>,
    ) -> Result<(), FlareError> {
        let key =
            device_push_token_registry_key(&self.key_prefix, &token.tenant_id, &token.user_id);
        let field = device_push_token_registry_field(&token.provider, &token.device_id);
        let mut conn = self.conn.clone();
        let _: i64 = tokio::time::timeout(
            IO_BUDGET,
            redis::Script::new(REMOVE_OBSERVED)
                .key(key)
                .key(flare_im_contracts::domain::push_binding::push_binding_slot(
                    &self.key_prefix,
                    &token.tenant_id,
                    &token.provider,
                    &token.device_id,
                ))
                .key(
                    flare_im_contracts::domain::push_binding::push_binding_command(
                        &self.key_prefix,
                        &token.registration_id,
                    ),
                )
                .arg(field)
                .arg(&token.registration_id)
                .arg(&token.token)
                .arg(
                    inactive_before
                        .map(|cutoff| cutoff.to_string())
                        .unwrap_or_default(),
                )
                .invoke_async(&mut conn),
        )
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?;
        Ok(())
    }

    fn receipt_key(&self, token: &DevicePushToken, task: &PushTaskEnvelope) -> String {
        format!(
            "{}:accepted:{}",
            self.key_prefix.trim_end_matches(':'),
            receipt_digest(token, task)
        )
    }
}

/// Receipt identity: the exact binding generation plus the task.
///
/// The router's `idempotency_key` names the task and stays the same across broker
/// redeliveries and republishes, while other envelope bytes (expiry, headers) may differ
/// between two publishes of one decision; when present it is the task identity. A task
/// without one is identified by its content.
fn receipt_digest(token: &DevicePushToken, task: &PushTaskEnvelope) -> String {
    let mut hash = Sha256::new();
    for value in [
        &token.tenant_id,
        &token.user_id,
        &token.provider,
        &token.device_id,
        &token.registration_id,
        &token.token,
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    // The leading byte keeps the two identity kinds apart: a key can never equal a content hash.
    if task.idempotency_key.is_empty() {
        hash.update([0u8]);
        hash_task_content(&mut hash, task);
    } else {
        hash.update([1u8]);
        hash.update((task.idempotency_key.len() as u64).to_be_bytes());
        hash.update(task.idempotency_key.as_bytes());
    }
    hex::encode(hash.finalize())
}

fn hash_task_content(hash: &mut Sha256, task: &PushTaskEnvelope) {
    for value in [
        &task.tenant_id,
        &task.user_id,
        &task.message_id,
        &task.conversation_id,
        &task.device_id,
        &task.collapse_key,
        &task.thread_key,
    ] {
        hash.update((value.len() as u64).to_be_bytes());
        hash.update(value.as_bytes());
    }
    hash.update(task.priority.to_be_bytes());
    hash.update(task.payload_kind.to_be_bytes());
    // A loud notification, its quiet refresh and its recall replacement can share a message id.
    hash.update(task.disposition.to_be_bytes());
    hash.update(task.alert_class.to_be_bytes());
    hash.update(task.count.to_be_bytes());
    hash.update([u8::from(task.badge.is_some())]);
    hash.update(task.badge.unwrap_or_default().to_be_bytes());
    hash.update(task.conversation_seq.to_be_bytes());
    hash.update([u8::from(task.expire_at.is_some())]);
    hash.update(task.expire_at.unwrap_or_default().to_be_bytes());
    hash.update((task.push_payload.len() as u64).to_be_bytes());
    hash.update(&task.push_payload);
    // Protobuf map order is not canonical across broker decodes.
    let headers: std::collections::BTreeMap<_, _> = task.headers.iter().collect();
    for (key, value) in headers {
        for part in [key, value] {
            hash.update((part.len() as u64).to_be_bytes());
            hash.update(part.as_bytes());
        }
    }
}

#[async_trait::async_trait]
impl DeviceTokenRepository for RedisDeviceTokenRepository {
    async fn is_current(&self, token: &DevicePushToken) -> Result<bool, FlareError> {
        let mut conn = self.conn.clone();
        tokio::time::timeout(IO_BUDGET,redis::Script::new("local raw=redis.call('GET',KEYS[1]);if not raw then return 0 end local row=cjson.decode(raw);return row.registry and row.binding_id==ARGV[1] and row.user_id==ARGV[2] and 1 or 0")
            .key(flare_im_contracts::domain::push_binding::push_binding_slot(&self.key_prefix,&token.tenant_id,&token.provider,&token.device_id)).arg(&token.registration_id).arg(&token.user_id).invoke_async::<bool>(&mut conn)).await.map_err(|_|unavailable())?.map_err(|_|unavailable())
    }
    async fn list_device_tokens(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
        device_id: &str,
    ) -> Result<Vec<DevicePushToken>, FlareError> {
        let device_id = device_id.trim();
        if device_id.is_empty() {
            // Terminal on purpose: there is no "all devices of the user" reading of a lookup.
            return Err(FlareError::localized(
                ErrorCode::InvalidParameter,
                "device registry lookup requires a device",
            ));
        }
        let key = device_push_token_registry_key(&self.key_prefix, tenant_id, user_id);
        let fields: Vec<String> = DEVICE_PROVIDERS
            .iter()
            .map(|provider| device_push_token_registry_field(provider, device_id))
            .collect();
        let mut conn = self.conn.clone();
        let values: Vec<String> = tokio::time::timeout(
            IO_BUDGET,
            redis::Script::new(LIST_DEVICE)
                .key(key)
                .arg(fields)
                .invoke_async(&mut conn),
        )
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?;
        let now = now_ms();
        let mut tokens = Vec::with_capacity(values.len());
        for value in values {
            let token = decode_device_token_record(&value)?;
            if token.tenant_id != tenant_id
                || token.user_id != user_id
                || token.device_id.trim() != device_id
            {
                return Err(unavailable());
            }
            if push_binding_inactive(token.last_active_ms(), now, self.inactive_days) {
                // Absent whatever the cleanup does: a failed removal is retried by the next
                // lookup and must not make this task retry for a binding it would skip anyway.
                let cutoff = push_binding_inactive_cutoff_ms(now, self.inactive_days);
                if let Err(error) = self.remove_observed(&token, Some(cutoff)).await {
                    tracing::warn!(
                        error = %error,
                        tenant_id = %token.tenant_id,
                        user_id = %token.user_id,
                        device_id = %token.device_id,
                        provider = %token.provider,
                        "inactive device push binding could not be removed"
                    );
                } else {
                    tracing::info!(
                        tenant_id = %token.tenant_id,
                        user_id = %token.user_id,
                        device_id = %token.device_id,
                        provider = %token.provider,
                        "removed inactive device push binding"
                    );
                }
                continue;
            }
            tokens.push(token);
        }
        Ok(tokens)
    }

    async fn remove_device_token(
        &self,
        _ctx: &Ctx,
        token: &DevicePushToken,
    ) -> Result<(), FlareError> {
        self.remove_observed(token, None).await
    }

    async fn was_accepted(
        &self,
        token: &DevicePushToken,
        task: &PushTaskEnvelope,
    ) -> Result<bool, FlareError> {
        let mut conn = self.conn.clone();
        tokio::time::timeout(
            IO_BUDGET,
            redis::cmd("EXISTS")
                .arg(self.receipt_key(token, task))
                .query_async(&mut conn),
        )
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())
    }

    async fn record_accepted(
        &self,
        token: &DevicePushToken,
        task: &PushTaskEnvelope,
    ) -> Result<(), FlareError> {
        let mut conn = self.conn.clone();
        tokio::time::timeout(
            IO_BUDGET,
            redis::cmd("SET")
                .arg(self.receipt_key(token, task))
                .arg("1")
                .arg("EX")
                .arg(RECEIPT_TTL_SECS)
                .query_async(&mut conn),
        )
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())
    }
}

pub fn decode_device_token_record(value: &str) -> Result<DevicePushToken, FlareError> {
    if value.len() > MAX_RECORD_BYTES {
        return Err(unavailable());
    }
    let token: DevicePushToken = serde_json::from_str(value).map_err(|_| unavailable())?;
    if !DEVICE_PROVIDERS.contains(&token.provider.as_str())
        || !token.usable_for_provider(&token.provider)
    {
        return Err(unavailable());
    }
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infrastructure::push_test_support::token;

    struct PrivateRedis {
        child: std::process::Child,
        directory: std::path::PathBuf,
    }
    impl Drop for PrivateRedis {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    #[tokio::test]
    #[ignore = "explicit private Redis binary required; never uses a shared database"]
    async fn redis_receipts_and_late_invalidation_preserve_new_registration() {
        let binary = std::env::var("FLARE_PUSH_TEST_REDIS_BIN").expect("explicit Redis binary");
        let directory = std::path::Path::new("/tmp")
            .join(format!("flare-push-registry-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&directory).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700)).unwrap();
        }
        let socket = directory.join("redis.sock");
        let child = std::process::Command::new(binary)
            .args([
                "--port",
                "0",
                "--save",
                "",
                "--appendonly",
                "no",
                "--unixsocket",
            ])
            .arg(&socket)
            .args(["--unixsocketperm", "700"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        let _guard = PrivateRedis { child, directory };
        for _ in 0..100 {
            if socket.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let url = format!("redis+unix://{}", socket.display());
        let repo = RedisDeviceTokenRepository::connect(
            &url,
            "test:registry".into(),
            flare_im_contracts::domain::push_binding::PUSH_BINDING_INACTIVE_DAYS_DEFAULT,
        )
        .await
        .unwrap();
        let mut conn = repo.conn.clone();
        let ctx = Ctx::default();
        let now = now_ms();
        let mut observed = token("device-a");
        observed.registered_at_ms = now - 1;
        let mut newer = observed.clone();
        newer.registration_id = "newer".into();
        newer.registered_at_ms = now;
        let key = device_push_token_registry_key(
            &repo.key_prefix,
            &observed.tenant_id,
            &observed.user_id,
        );
        let field = device_push_token_registry_field(&observed.provider, &observed.device_id);
        let slot = flare_im_contracts::domain::push_binding::push_binding_slot(
            &repo.key_prefix,
            &newer.tenant_id,
            &newer.provider,
            &newer.device_id,
        );
        let _: () = redis::cmd("SET").arg(&slot)
            .arg(serde_json::json!({"binding_id":newer.registration_id,"user_id":newer.user_id,"registry":key}).to_string())
            .query_async(&mut conn).await.unwrap();
        assert!(!repo.is_current(&observed).await.unwrap());
        assert!(repo.is_current(&newer).await.unwrap());
        let _: () = redis::cmd("HSET")
            .arg(&key)
            .arg(&field)
            .arg(serde_json::to_string(&newer).unwrap())
            .query_async(&mut conn)
            .await
            .unwrap();
        repo.remove_device_token(&ctx, &observed).await.unwrap();
        // Another device of the same user shares the registry hash and must stay out of reach.
        let mut other = token("device-b");
        other.registered_at_ms = now;
        let _: () = redis::cmd("HSET")
            .arg(&key)
            .arg(device_push_token_registry_field(
                &other.provider,
                &other.device_id,
            ))
            .arg(serde_json::to_string(&other).unwrap())
            .query_async(&mut conn)
            .await
            .unwrap();
        assert_eq!(
            repo.list_device_tokens(&ctx, "t1", "u1", "device-a")
                .await
                .unwrap(),
            vec![newer.clone()]
        );
        assert_eq!(
            repo.list_device_tokens(&ctx, "t1", "u1", "device-b")
                .await
                .unwrap(),
            vec![other]
        );
        assert!(
            repo.list_device_tokens(&ctx, "t1", "u1", "device-c")
                .await
                .unwrap()
                .is_empty()
        );
        let no_device = repo
            .list_device_tokens(&ctx, "t1", "u1", " ")
            .await
            .unwrap_err();
        assert!(!no_device.is_retryable());
        let mut task = PushTaskEnvelope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            message_id: "m1".into(),
            ..Default::default()
        };
        task.headers.insert("a".into(), "one".into());
        task.headers.insert("b".into(), "two".into());
        let mut reordered = task.clone();
        reordered.headers.clear();
        reordered.headers.insert("b".into(), "two".into());
        reordered.headers.insert("a".into(), "one".into());
        assert_eq!(
            repo.receipt_key(&newer, &task),
            repo.receipt_key(&newer, &reordered)
        );
        assert!(!repo.was_accepted(&newer, &task).await.unwrap());
        repo.record_accepted(&newer, &task).await.unwrap();
        assert!(repo.was_accepted(&newer, &reordered).await.unwrap());
        assert!(!repo.was_accepted(&observed, &task).await.unwrap());
        let ttl: i64 = redis::cmd("TTL")
            .arg(repo.receipt_key(&newer, &task))
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!((1..=RECEIPT_TTL_SECS as i64).contains(&ttl));
        assert!(!repo.receipt_key(&newer, &task).contains("device-a"));
        repo.remove_device_token(&ctx, &newer).await.unwrap();
        assert!(
            !repo.is_current(&newer).await.unwrap(),
            "vendor invalidation retires the exact authority generation"
        );
        assert!(
            repo.list_device_tokens(&ctx, "t1", "u1", "device-a")
                .await
                .unwrap()
                .is_empty()
        );
        // A binding not seen for the inactivity window is skipped and removed; one registered
        // even earlier but inspected recently is kept.
        const DAY: i64 = 86_400_000;
        let mut stale = token("device-stale");
        stale.registered_at_ms = now - 61 * DAY;
        let mut seen = token("device-seen");
        seen.registered_at_ms = now - 90 * DAY;
        seen.last_seen_ms = now - DAY;
        for row in [&stale, &seen] {
            let _: () = redis::cmd("HSET")
                .arg(&key)
                .arg(device_push_token_registry_field(
                    &row.provider,
                    &row.device_id,
                ))
                .arg(serde_json::to_string(row).unwrap())
                .query_async(&mut conn)
                .await
                .unwrap();
        }
        assert!(
            repo.list_device_tokens(&ctx, "t1", "u1", "device-stale")
                .await
                .unwrap()
                .is_empty()
        );
        let stale_left: bool = redis::cmd("HEXISTS")
            .arg(&key)
            .arg(device_push_token_registry_field(
                &stale.provider,
                &stale.device_id,
            ))
            .query_async(&mut conn)
            .await
            .unwrap();
        assert!(!stale_left, "the inactive binding is removed");
        assert_eq!(
            repo.list_device_tokens(&ctx, "t1", "u1", "device-seen")
                .await
                .unwrap(),
            vec![seen]
        );
        let _: () = redis::cmd("HSET")
            .arg(key)
            .arg(field)
            .arg("PRIVATE malformed")
            .query_async(&mut conn)
            .await
            .unwrap();
        let error = repo
            .list_device_tokens(&ctx, "t1", "u1", "device-a")
            .await
            .unwrap_err();
        assert!(error.is_retryable());
        assert!(!error.to_string().contains("PRIVATE"));
    }

    #[test]
    fn a_stable_idempotency_key_is_the_receipt_identity() {
        let binding = token("device-a");
        let task = PushTaskEnvelope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "device-a".into(),
            message_id: "m1".into(),
            idempotency_key: "key-1".into(),
            ..Default::default()
        };
        // A republish of the same decision: the rendered bytes moved, the key did not.
        let mut republished = task.clone();
        republished.expire_at = Some(1_800_000_000_000);
        republished.push_payload = vec![1, 2, 3];
        republished.headers.insert("trace".into(), "other".into());
        assert_eq!(
            receipt_digest(&binding, &task),
            receipt_digest(&binding, &republished)
        );
        let mut next = task.clone();
        next.idempotency_key = "key-2".into();
        assert_ne!(
            receipt_digest(&binding, &task),
            receipt_digest(&binding, &next)
        );
        // The receipt belongs to one binding generation, never to the task alone.
        let mut rebound = binding.clone();
        rebound.registration_id = "generation-next".into();
        assert_ne!(
            receipt_digest(&binding, &task),
            receipt_digest(&rebound, &task)
        );
        assert_ne!(
            receipt_digest(&binding, &task),
            receipt_digest(&token("device-b"), &task)
        );
    }

    #[test]
    fn without_a_key_the_task_content_is_the_receipt_identity() {
        use flare_proto::common::PushDisposition;
        let binding = token("device-a");
        let loud = PushTaskEnvelope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            device_id: "device-a".into(),
            message_id: "m1".into(),
            disposition: PushDisposition::Loud as i32,
            ..Default::default()
        };
        assert_eq!(
            receipt_digest(&binding, &loud),
            receipt_digest(&binding, &loud.clone())
        );
        // The recall replacement of a message keeps its message id and must still be sent.
        let mut replaced = loud.clone();
        replaced.disposition = PushDisposition::RecallReplace as i32;
        let mut refreshed = loud.clone();
        refreshed.count = 2;
        let mut rebadged = loud.clone();
        rebadged.badge = Some(0);
        for other in [&replaced, &refreshed, &rebadged] {
            assert_ne!(
                receipt_digest(&binding, &loud),
                receipt_digest(&binding, other)
            );
        }
        // A key that happens to spell a content digest is still a different identity.
        let mut keyed = loud.clone();
        keyed.idempotency_key = receipt_digest(&binding, &loud);
        assert_ne!(
            receipt_digest(&binding, &loud),
            receipt_digest(&binding, &keyed)
        );
    }
    #[test]
    fn strict_registration_and_safe_errors() {
        let record = r#"{"tenant_id":"t","user_id":"u","device_id":"d","platform":"ios","provider":"apns","token":"PRIVATE","registration_id":"r1","registered_at_ms":1}"#;
        let token = decode_device_token_record(record).expect("registration");
        assert!(token.usable_for_provider("apns"));
        assert!(!format!("{token:?}").contains("PRIVATE"));
        let error = decode_device_token_record("PRIVATE").expect_err("invalid");
        assert!(!error.to_string().contains("PRIVATE"));
        assert!(
            decode_device_token_record(&record.replace(",\"registration_id\":\"r1\"", "")).is_err()
        );
    }
}
