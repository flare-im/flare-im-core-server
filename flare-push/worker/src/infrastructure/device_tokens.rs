//! Bounded device registry and per-registration vendor-acceptance receipts.
use crate::domain::DeviceTokenRepository;
use flare_im_contracts::{
    Ctx, DevicePushToken, device_push_token_registry_field, device_push_token_registry_key,
};
use flare_proto::PushTaskEnvelope;
use flare_server_core::error::{ErrorCode, FlareError};
use redis::aio::ConnectionManager;
use sha2::{Digest, Sha256};
use std::time::Duration;

const IO_BUDGET: Duration = Duration::from_secs(3);
const RECEIPT_TTL_SECS: u64 = 7 * 24 * 3600;
const MAX_DEVICES: usize = 128;
const REMOVE_OBSERVED: &str = r#"
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local ok, row = pcall(cjson.decode, raw)
if ok and row.registration_id == ARGV[2] and row.token == ARGV[3] then
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
}

fn unavailable() -> FlareError {
    FlareError::localized(ErrorCode::ServiceUnavailable, "device registry unavailable")
}

impl RedisDeviceTokenRepository {
    pub async fn connect(redis_url: &str, key_prefix: String) -> Result<Self, FlareError> {
        let client = redis::Client::open(redis_url).map_err(|_| unavailable())?;
        let conn = tokio::time::timeout(IO_BUDGET, client.get_connection_manager())
            .await
            .map_err(|_| unavailable())?
            .map_err(|_| unavailable())?;
        Ok(Self { conn, key_prefix })
    }

    fn receipt_key(&self, token: &DevicePushToken, task: &PushTaskEnvelope) -> String {
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
        // Protobuf map order is not canonical across broker decodes.
        for value in [
            &task.tenant_id,
            &task.user_id,
            &task.message_id,
            &task.conversation_id,
        ] {
            hash.update((value.len() as u64).to_be_bytes());
            hash.update(value.as_bytes());
        }
        hash.update(task.priority.to_be_bytes());
        hash.update(task.payload_kind.to_be_bytes());
        hash.update([u8::from(task.expire_at.is_some())]);
        hash.update(task.expire_at.unwrap_or_default().to_be_bytes());
        hash.update((task.push_payload.len() as u64).to_be_bytes());
        hash.update(&task.push_payload);
        let headers: std::collections::BTreeMap<_, _> = task.headers.iter().collect();
        for (key, value) in headers {
            for part in [key, value] {
                hash.update((part.len() as u64).to_be_bytes());
                hash.update(part.as_bytes());
            }
        }
        format!(
            "{}:accepted:{}",
            self.key_prefix.trim_end_matches(':'),
            hex::encode(hash.finalize())
        )
    }
}

#[async_trait::async_trait]
impl DeviceTokenRepository for RedisDeviceTokenRepository {
    async fn is_current(&self, token: &DevicePushToken) -> Result<bool, FlareError> {
        let mut conn = self.conn.clone();
        tokio::time::timeout(IO_BUDGET,redis::Script::new("local raw=redis.call('GET',KEYS[1]);if not raw then return 0 end local row=cjson.decode(raw);return row.registry and row.binding_id==ARGV[1] and row.user_id==ARGV[2] and 1 or 0")
            .key(flare_im_contracts::domain::push_binding::push_binding_slot(&self.key_prefix,&token.tenant_id,&token.provider,&token.device_id)).arg(&token.registration_id).arg(&token.user_id).invoke_async::<bool>(&mut conn)).await.map_err(|_|unavailable())?.map_err(|_|unavailable())
    }
    async fn list_user_tokens(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<DevicePushToken>, FlareError> {
        let key = device_push_token_registry_key(&self.key_prefix, tenant_id, user_id);
        let mut conn = self.conn.clone();
        let values: Vec<String> = tokio::time::timeout(IO_BUDGET,
            redis::Script::new("if redis.call('HLEN', KEYS[1]) > tonumber(ARGV[1]) then return redis.error_reply('device limit') end local rows=redis.call('HVALS', KEYS[1]); for _,v in ipairs(rows) do if #v > 16384 then return redis.error_reply('record limit') end end return rows")
                .key(key).arg(MAX_DEVICES).invoke_async(&mut conn)).await
            .map_err(|_| unavailable())?.map_err(|_| unavailable())?;
        let mut tokens = Vec::with_capacity(values.len());
        for value in values {
            let token = decode_device_token_record(&value)?;
            if token.tenant_id != tenant_id || token.user_id != user_id {
                return Err(unavailable());
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
                .invoke_async(&mut conn),
        )
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?;
        Ok(())
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
    if value.len() > 16 * 1024 {
        return Err(unavailable());
    }
    let token: DevicePushToken = serde_json::from_str(value).map_err(|_| unavailable())?;
    if !matches!(token.provider.as_str(), "fcm" | "apns" | "getui")
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
        let repo = RedisDeviceTokenRepository::connect(&url, "test:registry".into())
            .await
            .unwrap();
        let mut conn = repo.conn.clone();
        let ctx = Ctx::default();
        let observed = token("device-a");
        let mut newer = observed.clone();
        newer.registration_id = "newer".into();
        newer.registered_at_ms = 2;
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
        assert_eq!(
            repo.list_user_tokens(&ctx, "t1", "u1").await.unwrap(),
            vec![newer.clone()]
        );
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
            repo.list_user_tokens(&ctx, "t1", "u1")
                .await
                .unwrap()
                .is_empty()
        );
        let _: () = redis::cmd("HSET")
            .arg(key)
            .arg(field)
            .arg("PRIVATE malformed")
            .query_async(&mut conn)
            .await
            .unwrap();
        let error = repo.list_user_tokens(&ctx, "t1", "u1").await.unwrap_err();
        assert!(error.is_retryable());
        assert!(!error.to_string().contains("PRIVATE"));
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
