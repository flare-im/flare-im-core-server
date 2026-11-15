//! Conditional binding, revoke-before-PUT tombstones and read-back, in one Redis authority.
use crate::config::PushProxyConfig;
use flare_im_contracts::domain::push_binding::*;
use flare_im_contracts::{
    DevicePushToken, device_push_token_registry_field, device_push_token_registry_key,
};
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
const BUDGET: Duration = Duration::from_secs(3);
// A binding stores the prior account's index/command keys. These bounded
// writes follow those authority-owned references atomically. Dragonfly must
// acquire the global transaction lock; Redis ignores the comment. Never add
// disable-atomicity: a late PUT must not survive its revocation tombstone.
const UNDECLARED_KEYS: &str = "--!df flags=allow-undeclared-keys\n";
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
// The one removal of a binding: detach its slot (kept 24h as the CAS version),
// drop its registry row, and leave the command key as a 24h tombstone. Both
// revocations below end here, so a later inspect or CAS registration cannot
// tell which of them ran.
const REVOKE_BINDING_FN: &str = r#"local function revoke_binding(command_key,id,cmd)
 if cmd and cmd.slot then
  local slotraw=redis.call('GET',cmd.slot)
  if slotraw then
   local slot=cjson.decode(slotraw)
   if slot.binding_id==id then slot.registry=nil;redis.call('SET',cmd.slot,cjson.encode(slot),'EX',86400) end
  end
  local value=redis.call('HGET',cmd.registry,cmd.field)
  if value and cjson.decode(value).registration_id==id then redis.call('HDEL',cmd.registry,cmd.field) end
 end
 redis.call('SET',command_key,'{"revoked":true}','EX',86400)
end
"#;
// Client revocation: the holder of the revocation secret names one binding.
const REVOKE: &str = r#"local raw=redis.call('GET',KEYS[1]);local cmd=nil
if raw then cmd=cjson.decode(raw) end
if not raw then
 local time=redis.call('TIME');local now=tonumber(time[1])*1000+math.floor(tonumber(time[2])/1000)
 local expired=redis.call('ZRANGEBYSCORE',KEYS[2],'-inf',now-86400000,'LIMIT',0,64)
 if #expired>0 then redis.call('ZREM',KEYS[2],unpack(expired)) end
 if redis.call('ZCARD',KEYS[2])>=4096 then return -1 end
 redis.call('ZADD',KEYS[2],now,ARGV[1]);redis.call('EXPIRE',KEYS[2],86400)
end
revoke_binding(KEYS[1],ARGV[1],cmd)
return 1
"#;
// Server revocation: walk one user's registry (at most 128 rows) and revoke the
// rows of the named device (ARGV[2], '' = every device) on every provider that
// were registered at or before ARGV[3]. `registered_at_ms` is this authority's
// own clock at registration, so a binding registered after the revoking event
// survives a revocation that arrives late. The trailing HDEL only matters when a
// row has lost its command key: the row must not outlive the revocation.
const REVOKE_BINDINGS: &str = r#"local before=tonumber(ARGV[3])
if not before or before<=0 then return -1 end
local revoked=0
local rows=redis.call('HGETALL',KEYS[1])
for i=1,#rows,2 do
 local ok,value=pcall(cjson.decode,rows[i+1])
 if ok and type(value)=='table' and (ARGV[2]=='' or value.device_id==ARGV[2]) and (tonumber(value.registered_at_ms) or 0)<=before then
  local id=value.registration_id
  if type(id)=='string' and id~='' then
   local command_key=ARGV[1]..id
   local raw=redis.call('GET',command_key);local cmd=nil
   if raw then cmd=cjson.decode(raw) end
   revoke_binding(command_key,id,cmd)
  end
  redis.call('HDEL',KEYS[1],rows[i])
  revoked=revoked+1
 end
end
return revoked
"#;
/// A revocation script: the shared removal followed by its own entry point.
fn revocation_script(entry: &str) -> redis::Script {
    redis::Script::new(&format!("{UNDECLARED_KEYS}{REVOKE_BINDING_FN}{entry}"))
}
// Read-back by the binding's owner. The client does this on every return to the
// foreground, so an owner who finds its own current binding also refreshes that
// row's `last_seen_ms` (Redis time): a binding nobody has inspected for the
// inactivity window is treated as gone by the router and the worker. The row is
// reached through the slot's own registry/field, hence the undeclared keys.
const INSPECT: &str = r#"--!df flags=allow-undeclared-keys
local raw=redis.call('GET',KEYS[1]);local version='';local own='';local slot=nil
if raw then slot=cjson.decode(raw);version=slot.binding_id;if slot.registry and slot.user_id==ARGV[1] then own=version end end
local state='absent'
if ARGV[2]~='' then
 local cmd=redis.call('GET',KEYS[2]);if cmd and cjson.decode(cmd).revoked then state='revoked' elseif own==ARGV[2] then state='active' else state='notCurrent' end
elseif own~='' then state='active' end
if state=='active' and slot.field then
 local row=redis.call('HGET',slot.registry,slot.field)
 if row then
  local ok,value=pcall(cjson.decode,row)
  if ok and type(value)=='table' and value.registration_id==own then
   local time=redis.call('TIME')
   value.last_seen_ms=tonumber(time[1])*1000+math.floor(tonumber(time[2])/1000)
   redis.call('HSET',slot.registry,slot.field,cjson.encode(value))
  end
 end
end
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
/// Identity of one registration attempt under a binding id: the whole serialized
/// record (token and every binding attribute) plus the CAS version and deadline.
/// A repeat with the same content is idempotent; any changed field conflicts and
/// needs a new binding id, so a binding id always names one set of attributes and
/// per-binding receipts stay valid.
fn registration_digest(value: &str, expected: &str, expires: i64) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!("{value}|{expected}|{expires}"))
    )
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
        let digest = registration_digest(&value, expected, expires);
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
            revocation_script(REVOKE)
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
    /// Server-initiated revocation of one user's bindings in a tenant, on every
    /// provider: the named device, or every device when `device_id` is empty.
    /// Only bindings registered at or before `registered_before` (ms) go. Returns
    /// how many were revoked; none is not an error, and a repeat finds none.
    pub async fn revoke_bindings(
        &self,
        tenant_id: &str,
        user_id: &str,
        device_id: &str,
        registered_before: i64,
    ) -> Result<u32, BindingError> {
        if registered_before <= 0 {
            return Err(BindingError::Invalid);
        }
        let revoked = tokio::time::timeout(BUDGET, async {
            let mut conn = self.client.get_multiplexed_tokio_connection().await?;
            revocation_script(REVOKE_BINDINGS)
                .key(device_push_token_registry_key(
                    &self.key_prefix,
                    tenant_id,
                    user_id,
                ))
                .arg(push_binding_command_prefix(&self.key_prefix))
                .arg(device_id)
                .arg(registered_before)
                .invoke_async::<i64>(&mut conn)
                .await
        })
        .await
        .map_err(|_| BindingError::Unavailable)?
        .map_err(|_| BindingError::Unavailable)?;
        u32::try_from(revoked).map_err(|_| BindingError::Invalid)
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
    /// A throwaway Redis on a private socket, and a registry over it.
    async fn private_registry() -> (PrivateRedis, redis::Client, RedisDeviceTokenRegistry) {
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
        let guard = PrivateRedis { child, directory };
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
        (guard, client, registry)
    }
    /// Every binding attribute takes part in the attempt identity: retrying the same
    /// registration is idempotent, changing any field under the same binding id conflicts.
    #[test]
    fn every_binding_attribute_is_part_of_the_registration_digest() {
        let base = DevicePushToken {
            tenant_id: "a".into(),
            user_id: "alice".into(),
            device_id: "device".into(),
            platform: "android".into(),
            provider: "fcm".into(),
            token: "token".into(),
            registration_id: "b".repeat(64),
            app_bundle: "com.example.app".into(),
            locale: "zh-CN".into(),
            time_zone: "Asia/Shanghai".into(),
            app_version: "1.0.0".into(),
            capabilities: vec!["dataMessages".into()],
            ..Default::default()
        };
        let digest = |token: &DevicePushToken| {
            registration_digest(&serde_json::to_string(token).unwrap(), "", 1)
        };
        assert_eq!(digest(&base), digest(&base.clone()));
        let changes: [fn(&mut DevicePushToken); 6] = [
            |t| t.app_bundle = "com.example.other".into(),
            |t| t.environment = "sandbox".into(),
            |t| t.locale = "en-US".into(),
            |t| t.time_zone = "Europe/Berlin".into(),
            |t| t.app_version = "1.0.1".into(),
            |t| t.capabilities.clear(),
        ];
        for change in changes {
            let mut changed = base.clone();
            change(&mut changed);
            assert_ne!(digest(&base), digest(&changed), "{changed:?}");
        }
        // An empty capability list is left out of the record: Redis's cjson would turn
        // an empty array into `{}` when the scripts re-encode the row.
        let mut bare = base.clone();
        bare.capabilities.clear();
        assert!(
            !serde_json::to_string(&bare)
                .unwrap()
                .contains("capabilities")
        );
    }

    #[tokio::test]
    #[ignore = "explicit private Redis binary; never an installed registry"]
    async fn conditional_binding_revoke_and_unknown_write_recovery() {
        let (_guard, client, registry) = private_registry().await;
        let deadline = chrono::Utc::now().timestamp_millis() + 60_000;
        let make = |owner: &str, secret: &str| DevicePushToken {
            tenant_id: "a".into(),
            user_id: owner.into(),
            device_id: "device".into(),
            platform: "ios".into(),
            provider: "apns".into(),
            token: "aabb".into(),
            registration_id: binding_id(secret).unwrap(),
            app_bundle: "com.example.app".into(),
            environment: "sandbox".into(),
            ..Default::default()
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
        // A changed binding attribute is a different registration as well.
        let mut moved = a.clone();
        moved.environment = "production".into();
        assert_eq!(
            registry.register(&moved, "", deadline).await,
            Err(BindingError::Conflict)
        );
        // The owner's read-back stamps the row with Redis time; it starts unseen.
        let alice_rows = device_push_token_registry_key("binding-test", "a", "alice");
        let field = device_push_token_registry_field("apns", "device");
        let row = |raw: String| serde_json::from_str::<DevicePushToken>(&raw).unwrap();
        {
            let mut conn = client.get_multiplexed_tokio_connection().await.unwrap();
            let before = row(conn.hget(&alice_rows, &field).await.unwrap());
            assert_eq!(before.last_seen_ms, 0);
            assert_eq!(before.app_bundle, "com.example.app");
            assert!(before.capabilities.is_empty());
            // Another user's read-back of the same slot refreshes nothing.
            registry
                .inspect("a", "mallory", "apns", "device", "")
                .await
                .unwrap();
            let untouched = row(conn.hget(&alice_rows, &field).await.unwrap());
            assert_eq!(untouched.last_seen_ms, 0);
            let view = registry
                .inspect("a", "alice", "apns", "device", &a.registration_id)
                .await
                .unwrap();
            assert_eq!(view.state, "active");
            let seen = row(conn.hget(&alice_rows, &field).await.unwrap());
            assert!(seen.last_seen_ms >= seen.registered_at_ms);
            assert!(seen.last_seen_ms > 0);
            assert_eq!(
                DevicePushToken {
                    last_seen_ms: 0,
                    ..seen
                },
                before,
                "only the activity time changes"
            );
        }
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
    #[tokio::test]
    #[ignore = "explicit private Redis binary; never an installed registry"]
    async fn server_revocation_by_device_by_user_and_by_registration_time() {
        let (_guard, client, registry) = private_registry().await;
        let mut conn = client.get_multiplexed_tokio_connection().await.unwrap();
        let deadline = chrono::Utc::now().timestamp_millis() + 60_000;
        let make = |owner: &str, device: &str, provider: &str, secret: char| DevicePushToken {
            tenant_id: "a".into(),
            user_id: owner.into(),
            device_id: device.into(),
            platform: "ios".into(),
            provider: provider.into(),
            token: "aabb".into(),
            registration_id: binding_id(&secret.to_string().repeat(64)).unwrap(),
            app_bundle: "com.example.app".into(),
            ..Default::default()
        };
        // alice: a tablet, then a phone bound on two providers; bob: one phone.
        let tablet = make("alice", "tablet", "apns", 'c');
        let phone_apns = make("alice", "phone", "apns", 'a');
        let phone_fcm = make("alice", "phone", "fcm", 'b');
        let bob = make("bob", "bob-phone", "apns", 'd');
        for token in [&tablet, &phone_apns, &phone_fcm, &bob] {
            registry.register(token, "", deadline).await.unwrap();
        }
        let alice_rows = device_push_token_registry_key("binding-test", "a", "alice");
        let mut phone_times = Vec::new();
        for provider in ["apns", "fcm"] {
            let row: String = conn
                .hget(
                    &alice_rows,
                    device_push_token_registry_field(provider, "phone"),
                )
                .await
                .unwrap();
            phone_times.push(
                serde_json::from_str::<DevicePushToken>(&row)
                    .unwrap()
                    .registered_at_ms,
            );
        }
        let first = *phone_times.iter().min().unwrap();
        let last = *phone_times.iter().max().unwrap();
        assert!(first > 0, "registration time comes from Redis");
        let far_future = last + 3_600_000;

        // Registered one millisecond after the revoking event: kept.
        assert_eq!(
            registry
                .revoke_bindings("a", "alice", "phone", first - 1)
                .await
                .unwrap(),
            0,
            "registered after the revoking event"
        );
        assert_eq!(
            registry
                .revoke_bindings("other-tenant", "alice", "phone", far_future)
                .await
                .unwrap(),
            0
        );
        // Registered exactly at the event time: revoked. Both providers of the phone go;
        // the tablet and another user are untouched.
        assert_eq!(
            registry
                .revoke_bindings("a", "alice", "phone", last)
                .await
                .unwrap(),
            2
        );
        let left: Vec<String> = conn.hkeys(&alice_rows).await.unwrap();
        assert_eq!(
            left,
            vec![device_push_token_registry_field("apns", "tablet")]
        );
        let bob_rows: usize = conn
            .hlen(device_push_token_registry_key("binding-test", "a", "bob"))
            .await
            .unwrap();
        assert_eq!(bob_rows, 1);
        // Repeats find nothing.
        assert_eq!(
            registry
                .revoke_bindings("a", "alice", "phone", far_future)
                .await
                .unwrap(),
            0
        );

        // Same state as a client revoke: the slot keeps its version but owns nothing, the
        // binding reads as revoked, the same binding cannot come back, a CAS on the kept
        // version can.
        let view = registry
            .inspect("a", "alice", "apns", "phone", &phone_apns.registration_id)
            .await
            .unwrap();
        assert_eq!(view.version, phone_apns.registration_id);
        assert!(view.current_binding_id.is_empty());
        assert_eq!(view.state, "revoked");
        assert_eq!(
            registry.register(&phone_apns, "", deadline).await,
            Err(BindingError::Revoked)
        );
        // Logging in again registers a new binding, strictly later on the Redis clock.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let again = make("alice", "phone", "apns", 'e');
        registry
            .register(&again, &phone_apns.registration_id, deadline)
            .await
            .unwrap();
        // It survives the late revocation of everything the user had at the event time.
        assert_eq!(
            registry
                .revoke_bindings("a", "alice", "", last)
                .await
                .unwrap(),
            1,
            "only the tablet is that old"
        );
        assert_eq!(
            registry
                .inspect("a", "alice", "apns", "phone", &again.registration_id)
                .await
                .unwrap()
                .state,
            "active"
        );
        // Empty device: every remaining device of the user.
        assert_eq!(
            registry
                .revoke_bindings("a", "alice", "", far_future)
                .await
                .unwrap(),
            1
        );
        let alice_left: usize = conn.hlen(&alice_rows).await.unwrap();
        assert_eq!(alice_left, 0);
        assert_eq!(
            registry
                .revoke_bindings("a", "alice", "", far_future)
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            registry.revoke_bindings("a", "alice", "", 0).await,
            Err(BindingError::Invalid)
        );
        // A client revoke of a binding the server already revoked is still accepted.
        registry.revoke(&"a".repeat(64)).await.unwrap();
    }
}
