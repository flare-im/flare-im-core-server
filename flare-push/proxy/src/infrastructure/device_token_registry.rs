//! Conditional binding, revoke-before-PUT tombstones and read-back, in one Redis authority.
use crate::config::PushProxyConfig;
use flare_im_contracts::domain::push_binding::*;
use flare_im_contracts::{
    DevicePushToken, device_push_token_registry_field, device_push_token_registry_key,
};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
const BUDGET: Duration = Duration::from_secs(3);
// A binding stores the prior account's index/command keys. These two bounded
// writes follow those authority-owned references atomically. Dragonfly must
// acquire the global transaction lock; Redis ignores the comment. Never add
// disable-atomicity: a late PUT must not survive its revocation tombstone.
const REGISTER: &str = r#"--!df flags=allow-undeclared-keys
local time=redis.call('TIME'); local now=tonumber(time[1])*1000+math.floor(tonumber(time[2])/1000)
local deadline=tonumber(ARGV[4])
if not deadline or deadline<now or deadline>now+600000 then return -3 end
local oldcmd=redis.call('GET',KEYS[3])
if oldcmd then
 local cmd=cjson.decode(oldcmd)
 if cmd.revoked then return -2 end
 if cmd.digest~=ARGV[3] then return -1 end
 local current=redis.call('GET',KEYS[1])
 if current and cjson.decode(current).binding_id==ARGV[2] then return 1 end
 return -1
end
local oldraw=redis.call('GET',KEYS[1]); local old=nil
if oldraw then old=cjson.decode(oldraw) end
local version=old and old.binding_id or ''
if version~=ARGV[1] then return -1 end
if redis.call('HEXISTS',KEYS[2],ARGV[5])==0 and redis.call('HLEN',KEYS[2])>=128 then return -4 end
if old and old.registry then
 local prior=redis.call('HGET',old.registry,old.field)
 if prior and cjson.decode(prior).registration_id==old.binding_id then redis.call('HDEL',old.registry,old.field) end
 redis.call('EXPIRE',old.command,86400)
end
local value=cjson.decode(ARGV[6]);value.registered_at_ms=now
local slot={binding_id=ARGV[2],user_id=value.user_id,registry=KEYS[2],field=ARGV[5],command=KEYS[3]}
redis.call('SET',KEYS[3],cjson.encode({digest=ARGV[3],slot=KEYS[1],registry=KEYS[2],field=ARGV[5]}))
redis.call('SET',KEYS[1],cjson.encode(slot))
redis.call('HSET',KEYS[2],ARGV[5],cjson.encode(value))
return 1
"#;
const REVOKE: &str = r#"--!df flags=allow-undeclared-keys
local raw=redis.call('GET',KEYS[1]);local cmd=nil
if raw then cmd=cjson.decode(raw) end
if not raw then
 local time=redis.call('TIME');local now=tonumber(time[1])*1000+math.floor(tonumber(time[2])/1000)
 local expired=redis.call('ZRANGEBYSCORE',KEYS[2],'-inf',now-86400000,'LIMIT',0,64)
 if #expired>0 then redis.call('ZREM',KEYS[2],unpack(expired)) end
 if redis.call('ZCARD',KEYS[2])>=4096 then return -1 end
 redis.call('ZADD',KEYS[2],now,ARGV[1]);redis.call('EXPIRE',KEYS[2],86400)
end
if cmd and cmd.slot then
 local slotraw=redis.call('GET',cmd.slot)
 if slotraw then
  local slot=cjson.decode(slotraw)
  if slot.binding_id==ARGV[1] then slot.registry=nil;redis.call('SET',cmd.slot,cjson.encode(slot),'EX',86400) end
 end
 local value=redis.call('HGET',cmd.registry,cmd.field)
 if value and cjson.decode(value).registration_id==ARGV[1] then redis.call('HDEL',cmd.registry,cmd.field) end
end
redis.call('SET',KEYS[1],'{"revoked":true}','EX',86400)
return 1
"#;
const INSPECT: &str = r#"
local raw=redis.call('GET',KEYS[1]);local version='';local own=''
if raw then local slot=cjson.decode(raw);version=slot.binding_id;if slot.registry and slot.user_id==ARGV[1] then own=version end end
local state='absent'
if ARGV[2]~='' then
 local cmd=redis.call('GET',KEYS[2]);if cmd and cjson.decode(cmd).revoked then state='revoked' elseif own==ARGV[2] then state='active' else state='notCurrent' end
elseif own~='' then state='active' end
return {version,own,state}
"#;
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindingError {
    Conflict,
    Revoked,
    Invalid,
    Limit,
    Unavailable,
}
impl std::fmt::Display for BindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Conflict => "binding version conflict",
            Self::Revoked => "binding revoked",
            Self::Invalid => "binding command expired or invalid",
            Self::Limit => "device registration limit",
            Self::Unavailable => "device registry unavailable",
        })
    }
}
impl std::error::Error for BindingError {}
pub struct BindingView {
    pub version: String,
    pub current_binding_id: String,
    pub state: String,
}
pub struct RedisDeviceTokenRegistry {
    client: redis::Client,
    key_prefix: String,
}
impl RedisDeviceTokenRegistry {
    pub fn new(config: Arc<PushProxyConfig>) -> flare_server_core::error::Result<Self> {
        Ok(Self {
            client: redis::Client::open(config.device_token_redis_url.as_str())?,
            key_prefix: config.device_token_key_prefix.clone(),
        })
    }
    pub async fn register(
        &self,
        token: &DevicePushToken,
        expected: &str,
        expires: i64,
    ) -> Result<(), BindingError> {
        if !binding_id_valid(&token.registration_id)
            || (!expected.is_empty() && !binding_id_valid(expected))
        {
            return Err(BindingError::Invalid);
        }
        let key =
            device_push_token_registry_key(&self.key_prefix, &token.tenant_id, &token.user_id);
        let field = device_push_token_registry_field(&token.provider, &token.device_id);
        let value = serde_json::to_string(token).map_err(|_| BindingError::Invalid)?;
        let digest = format!(
            "{:x}",
            Sha256::digest(format!("{value}|{expected}|{expires}"))
        );
        let result = tokio::time::timeout(BUDGET, async {
            let mut conn = self.client.get_multiplexed_tokio_connection().await?;
            redis::Script::new(REGISTER)
                .key(push_binding_slot(
                    &self.key_prefix,
                    &token.tenant_id,
                    &token.provider,
                    &token.device_id,
                ))
                .key(key)
                .key(push_binding_command(
                    &self.key_prefix,
                    &token.registration_id,
                ))
                .arg(expected)
                .arg(&token.registration_id)
                .arg(digest)
                .arg(expires)
                .arg(field)
                .arg(value)
                .invoke_async::<i64>(&mut conn)
                .await
        })
        .await
        .map_err(|_| BindingError::Unavailable)?
        .map_err(|_| BindingError::Unavailable)?;
        match result {
            1 => Ok(()),
            -1 => Err(BindingError::Conflict),
            -2 => Err(BindingError::Revoked),
            -3 => Err(BindingError::Invalid),
            -4 => Err(BindingError::Limit),
            _ => Err(BindingError::Unavailable),
        }
    }
    pub async fn revoke(&self, secret: &str) -> Result<(), BindingError> {
        let id = binding_id(secret).ok_or(BindingError::Invalid)?;
        let result = tokio::time::timeout(BUDGET, async {
            let mut conn = self.client.get_multiplexed_tokio_connection().await?;
            redis::Script::new(REVOKE)
                .key(push_binding_command(&self.key_prefix, &id))
                .key(format!(
                    "{}:unbound-revocations",
                    self.key_prefix.trim_end_matches(':')
                ))
                .arg(id)
                .invoke_async::<i64>(&mut conn)
                .await
        })
        .await
        .map_err(|_| BindingError::Unavailable)?
        .map_err(|_| BindingError::Unavailable)?;
        if result != 1 {
            return Err(BindingError::Unavailable);
        }
        Ok(())
    }
    pub async fn inspect(
        &self,
        app: &str,
        user: &str,
        provider: &str,
        device: &str,
        id: &str,
    ) -> Result<BindingView, BindingError> {
        if !id.is_empty() && !binding_id_valid(id) {
            return Err(BindingError::Invalid);
        }
        let (version, current_binding_id, state) = tokio::time::timeout(BUDGET, async {
            let mut conn = self.client.get_multiplexed_tokio_connection().await?;
            redis::Script::new(INSPECT)
                .key(push_binding_slot(&self.key_prefix, app, provider, device))
                .key(push_binding_command(&self.key_prefix, id))
                .arg(user)
                .arg(id)
                .invoke_async::<(String, String, String)>(&mut conn)
                .await
        })
        .await
        .map_err(|_| BindingError::Unavailable)?
        .map_err(|_| BindingError::Unavailable)?;
        Ok(BindingView {
            version,
            current_binding_id,
            state,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::AsyncCommands;
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
    #[ignore = "explicit private Redis binary; never an installed registry"]
    async fn conditional_binding_revoke_and_unknown_write_recovery() {
        let binary = std::env::var("FLARE_PUSH_TEST_REDIS_BIN").expect("explicit binary");
        let directory = std::path::Path::new("/tmp")
            .join(format!("flare-push-binding-{}", uuid::Uuid::new_v4()));
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
        let client = redis::Client::open(format!("redis+unix://{}", socket.display())).unwrap();
        let registry = RedisDeviceTokenRegistry {
            client: client.clone(),
            key_prefix: "binding-test".into(),
        };
        let deadline = chrono::Utc::now().timestamp_millis() + 60_000;
        let make = |owner: &str, secret: &str| DevicePushToken {
            tenant_id: "a".into(),
            user_id: owner.into(),
            device_id: "device".into(),
            platform: "ios".into(),
            provider: "apns".into(),
            token: "aabb".into(),
            registration_id: binding_id(secret).unwrap(),
            registered_at_ms: 0,
        };
        let secret_a = "a".repeat(64);
        let a = make("alice", &secret_a);
        assert!(registry.register(&a, "", deadline).await.is_ok());
        assert!(
            registry.register(&a, "", deadline).await.is_ok(),
            "same immutable request is idempotent"
        );
        let mut conflict = a.clone();
        conflict.token = "bbcc".into();
        assert_eq!(
            registry.register(&conflict, "", deadline).await,
            Err(BindingError::Conflict)
        );
        let secret_b = "b".repeat(64);
        let b = make("bob", &secret_b);
        assert!(
            registry
                .register(&b, &a.registration_id, deadline)
                .await
                .is_ok()
        );
        let mut conn = client.get_multiplexed_tokio_connection().await.unwrap();
        let old_count: usize = conn
            .hlen(device_push_token_registry_key("binding-test", "a", "alice"))
            .await
            .unwrap();
        assert_eq!(
            old_count, 0,
            "account switch atomically removes prior user's index"
        );
        registry.revoke(&secret_a).await.unwrap();
        assert_eq!(
            registry.register(&a, "", deadline).await,
            Err(BindingError::Revoked)
        );
        assert_eq!(
            registry
                .inspect("a", "bob", "apns", "device", &b.registration_id)
                .await
                .unwrap()
                .state,
            "active"
        );
        assert!(
            registry
                .inspect("a", "alice", "apns", "device", "")
                .await
                .unwrap()
                .current_binding_id
                .is_empty()
        );
        assert!(
            registry
                .inspect("other-app", "bob", "apns", "device", "")
                .await
                .unwrap()
                .version
                .is_empty()
        );
        let secret_c = "c".repeat(64);
        let c = make("bob", &secret_c);
        registry.revoke(&secret_c).await.unwrap();
        assert_eq!(
            registry.register(&c, &b.registration_id, deadline).await,
            Err(BindingError::Revoked),
            "revoke-before-late-PUT wins"
        );
        registry.revoke(&secret_b).await.unwrap();
        registry.revoke(&secret_b).await.unwrap();
        assert_eq!(
            registry
                .inspect("a", "bob", "apns", "device", &b.registration_id)
                .await
                .unwrap()
                .state,
            "revoked"
        );
        let d = make("bob", &"d".repeat(64));
        let e = make("bob", &"e".repeat(64));
        let (left, right) = tokio::join!(
            registry.register(&d, &b.registration_id, deadline),
            registry.register(&e, &b.registration_id, deadline)
        );
        assert_eq!(
            [left.is_ok(), right.is_ok()]
                .into_iter()
                .filter(|v| *v)
                .count(),
            1,
            "one conditional winner"
        );
        assert!(
            matches!(left, Ok(()) | Err(BindingError::Conflict))
                && matches!(right, Ok(()) | Err(BindingError::Conflict))
        );
        let winner = registry
            .inspect("a", "bob", "apns", "device", "")
            .await
            .unwrap();
        registry.revoke(&secret_b).await.unwrap();
        assert_eq!(
            registry
                .inspect("a", "bob", "apns", "device", "")
                .await
                .unwrap()
                .version,
            winner.version,
            "old logout cannot erase winner"
        );
        assert_eq!(
            registry
                .register(
                    &c,
                    &winner.version,
                    chrono::Utc::now().timestamp_millis() - 1
                )
                .await,
            Err(BindingError::Invalid)
        );
        // Anonymous pre-PUT cancellations cannot allocate an unbounded authority.
        let index = "binding-test:unbound-revocations";
        let mut fill = redis::cmd("ZADD");
        fill.arg(index);
        for i in 0..4096 {
            fill.arg(chrono::Utc::now().timestamp_millis())
                .arg(format!("quota-{i}"));
        }
        let _: i64 = fill.query_async(&mut conn).await.unwrap();
        assert_eq!(
            registry.revoke(&"f".repeat(64)).await,
            Err(BindingError::Unavailable)
        );
        let unknown: bool = conn
            .exists(push_binding_command(
                "binding-test",
                &binding_id(&"f".repeat(64)).unwrap(),
            ))
            .await
            .unwrap();
        assert!(
            !unknown,
            "capacity failure is not an acknowledged tombstone"
        );
        let winner_secret = if left.is_ok() {
            "d".repeat(64)
        } else {
            "e".repeat(64)
        };
        registry.revoke(&winner_secret).await.unwrap();
    }
}
