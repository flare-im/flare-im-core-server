//! 用户级通知设置读取：经会话服务的内部读接口 `BatchGetUserNotificationSettings`，以服务身份
//! 一批批地读，进程内缓存 60 秒。
//!
//! 设置变化不频繁，而每次离线推送都要用；旧值的代价是改了勿扰时段或预览后最多一分钟才在推送上
//! 生效，用户的其他设备照样即时收到同步。读不到时调用方用默认值放行。

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use flare_grpc_proto::conversation::BatchGetUserNotificationSettingsRequest;
use flare_grpc_proto::conversation::conversation_internal_read_service_client::ConversationInternalReadServiceClient;
use flare_im_contracts::Ctx;
use flare_im_contracts::service_names::{CONVERSATION, get_service_name};
use flare_notify_rules::UserPrefs;
use flare_server_core::error::{ErrorCode, FlareError};
use tokio::sync::Mutex;
use tonic::transport::Channel;

use crate::domain::notify_inputs::user_prefs;
use crate::domain::repository::UserNotifySettingsRepository;
use crate::infrastructure::read_request::request_with_context;

/// 一次读取的时限。路由在等它：读不回来就用默认值，不值得等更久。
const READ_TIMEOUT: Duration = Duration::from_secs(2);

/// 会话服务单次至多收这么多个用户。
const SETTINGS_BATCH: usize = 500;

/// 设置在进程内缓存多久。
pub const SETTINGS_CACHE_TTL: Duration = Duration::from_secs(60);

/// 缓存里最多放多少个（租户，用户）。
const SETTINGS_CACHE_CAPACITY: u64 = 200_000;

fn unavailable(reason: impl Into<String>) -> FlareError {
    FlareError::localized(ErrorCode::ServiceUnavailable, reason)
}

/// 会话服务接受的用户标识：非空、没有首尾空白，去重后保持原顺序。其余的不问，按默认值处理。
fn lookup_ids(user_ids: &[String]) -> Vec<String> {
    let mut seen = HashSet::with_capacity(user_ids.len());
    user_ids
        .iter()
        .filter(|id| !id.is_empty() && id.trim() == id.as_str())
        .filter(|id| seen.insert(id.as_str()))
        .cloned()
        .collect()
}

#[derive(Default)]
pub struct ConversationUserNotifySettings {
    channel: Arc<Mutex<Option<Channel>>>,
}

impl ConversationUserNotifySettings {
    pub fn new() -> Self {
        Self::default()
    }

    async fn client(&self) -> Result<ConversationInternalReadServiceClient<Channel>, FlareError> {
        let mut guard = self.channel.lock().await;
        if let Some(channel) = guard.as_ref() {
            return Ok(ConversationInternalReadServiceClient::new(channel.clone()));
        }
        let name = get_service_name(CONVERSATION);
        let fallback = flare_im_service_kit::discovery::default_static_grpc_fallback(&name);
        let channel =
            flare_im_service_kit::discovery::connect_grpc_channel_with_fallback(&name, fallback)
                .await
                .map_err(|e| unavailable(format!("connect {name}: {e}")))?;
        *guard = Some(channel.clone());
        Ok(ConversationInternalReadServiceClient::new(channel))
    }
}

#[async_trait::async_trait]
impl UserNotifySettingsRepository for ConversationUserNotifySettings {
    async fn settings_for(
        &self,
        ctx: &Ctx,
        _tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserPrefs>, FlareError> {
        let ids = lookup_ids(user_ids);
        if ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut client = self.client().await?;
        let mut out = HashMap::with_capacity(ids.len());
        for batch in ids.chunks(SETTINGS_BATCH) {
            // 租户不在请求体里：会话服务只认签名上下文里的租户。
            let request = request_with_context(
                BatchGetUserNotificationSettingsRequest {
                    user_ids: batch.to_vec(),
                },
                ctx,
            );
            let response = tokio::time::timeout(
                READ_TIMEOUT,
                client.batch_get_user_notification_settings(request),
            )
            .await
            .map_err(|_| unavailable("conversation service did not answer in time"))?
            .map_err(|status| {
                unavailable(format!(
                    "batch get user notification settings: {:?}",
                    status.code()
                ))
            })?
            .into_inner();
            for (user_id, settings) in response.settings {
                out.insert(user_id, user_prefs(&settings));
            }
        }
        Ok(out)
    }
}

/// 给设置读取加一层进程内缓存：有界、定时过期，没设置过的用户（默认值）同样缓存。
/// 读失败不进缓存。
pub struct CachedUserNotifySettings {
    inner: Arc<dyn UserNotifySettingsRepository>,
    cache: moka::future::Cache<(String, String), UserPrefs>,
}

impl CachedUserNotifySettings {
    pub fn new(inner: Arc<dyn UserNotifySettingsRepository>) -> Self {
        Self::with_ttl(inner, SETTINGS_CACHE_TTL)
    }

    pub fn with_ttl(inner: Arc<dyn UserNotifySettingsRepository>, ttl: Duration) -> Self {
        Self {
            inner,
            cache: moka::future::Cache::builder()
                .max_capacity(SETTINGS_CACHE_CAPACITY)
                .time_to_live(ttl)
                .build(),
        }
    }
}

#[async_trait::async_trait]
impl UserNotifySettingsRepository for CachedUserNotifySettings {
    async fn settings_for(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserPrefs>, FlareError> {
        let mut out = HashMap::with_capacity(user_ids.len());
        let mut misses = Vec::new();
        for user_id in lookup_ids(user_ids) {
            match self
                .cache
                .get(&(tenant_id.to_string(), user_id.clone()))
                .await
            {
                Some(prefs) => {
                    out.insert(user_id, prefs);
                }
                None => misses.push(user_id),
            }
        }
        if misses.is_empty() {
            return Ok(out);
        }
        let read = self.inner.settings_for(ctx, tenant_id, &misses).await?;
        for user_id in misses {
            let prefs = read.get(&user_id).copied().unwrap_or_default();
            self.cache
                .insert((tenant_id.to_string(), user_id.clone()), prefs)
                .await;
            out.insert(user_id, prefs);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_notify_rules::{MultiDevicePolicy, PreviewMode};
    use std::sync::Mutex as StdMutex;

    #[derive(Default)]
    struct Source {
        prefs: HashMap<String, UserPrefs>,
        reads: StdMutex<Vec<Vec<String>>>,
        down: StdMutex<bool>,
    }

    #[async_trait::async_trait]
    impl UserNotifySettingsRepository for Source {
        async fn settings_for(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            user_ids: &[String],
        ) -> Result<HashMap<String, UserPrefs>, FlareError> {
            self.reads.lock().unwrap().push(user_ids.to_vec());
            if *self.down.lock().unwrap() {
                return Err(unavailable("down"));
            }
            Ok(user_ids
                .iter()
                .filter_map(|id| self.prefs.get(id).map(|prefs| (id.clone(), *prefs)))
                .collect())
        }
    }

    fn ctx() -> Ctx {
        Arc::new(flare_server_core::context::Context::with_request_id(
            "req-1",
        ))
    }

    fn ids(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn ids_the_service_would_reject_are_not_asked() {
        assert_eq!(
            lookup_ids(&ids(&["u1", "", " u2", "u3 ", "u1", "u4"])),
            ids(&["u1", "u4"])
        );
    }

    /// 没设置过的用户同样缓存：大多数人从不改设置，不缓存默认值缓存就白加了。
    #[tokio::test]
    async fn defaults_are_cached_and_only_misses_are_read() {
        let hidden = UserPrefs {
            preview: PreviewMode::Hidden,
            multi_device: MultiDevicePolicy::Immediate,
            ..UserPrefs::default()
        };
        let source = Arc::new(Source {
            prefs: HashMap::from([("u1".to_string(), hidden)]),
            ..Default::default()
        });
        let cached = CachedUserNotifySettings::new(source.clone());

        let first = cached
            .settings_for(&ctx(), "t1", &ids(&["u1", "u2"]))
            .await
            .unwrap();
        assert_eq!(first["u1"], hidden);
        assert_eq!(first["u2"], UserPrefs::default());
        let second = cached
            .settings_for(&ctx(), "t1", &ids(&["u1", "u2", "u3"]))
            .await
            .unwrap();
        assert_eq!(second.len(), 3);
        assert_eq!(
            *source.reads.lock().unwrap(),
            vec![ids(&["u1", "u2"]), ids(&["u3"])]
        );
    }

    #[tokio::test]
    async fn a_failed_read_is_not_cached() {
        let source = Arc::new(Source::default());
        let cached = CachedUserNotifySettings::new(source.clone());
        *source.down.lock().unwrap() = true;
        assert!(
            cached
                .settings_for(&ctx(), "t1", &ids(&["u1"]))
                .await
                .is_err()
        );
        *source.down.lock().unwrap() = false;
        assert!(
            cached
                .settings_for(&ctx(), "t1", &ids(&["u1"]))
                .await
                .is_ok()
        );
        assert_eq!(source.reads.lock().unwrap().len(), 2);
    }
}
