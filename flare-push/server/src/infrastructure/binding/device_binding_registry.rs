//! 设备推送绑定读取。
//!
//! 绑定表由 Push Proxy 登记、Push Worker 投递时读取：每个（租户，用户）一张 Redis hash，
//! field 是 `provider:device_id`，吊销即删除 field。路由只需要知道哪些设备有绑定，
//! 从记录里只取登记与最近活跃两个时间，用来跳过长期不活跃的绑定；令牌留在 Worker 那一侧。

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flare_im_contracts::domain::push_binding::{
    push_binding_inactive, push_binding_last_active_ms,
};
use flare_im_contracts::{Ctx, device_push_token_registry_key};
use flare_server_core::error::{ErrorCode, FlareError};
use redis::aio::ConnectionManager;

use crate::domain::model::DeviceProfile;
use crate::domain::repository::{BoundDevices, DeviceBindingRepository};

/// 单次 Redis 往返的时限。绑定在每条消息的路由路径上，宁可这一次不推也不能把路由拖住。
const IO_BUDGET: Duration = Duration::from_secs(3);

/// 一条管线里最多读多少个用户的绑定表，理由同会话记录的分批。
const REGISTRY_READ_BATCH: usize = 500;

/// 绑定在进程内缓存多久。绑定很少变化；旧值的代价见 [`DeviceBindingRepository::bound_devices`]。
pub const BINDING_CACHE_TTL: Duration = Duration::from_secs(30);

/// 缓存里最多放多少个（租户，用户）。条目很小（几个设备标识），上限只是防止无界增长。
const BINDING_CACHE_CAPACITY: u64 = 200_000;

fn unavailable(reason: &str) -> FlareError {
    FlareError::localized(
        ErrorCode::ServiceUnavailable,
        format!("device binding registry unavailable: {reason}"),
    )
}

/// 绑定表的 field（`provider:device_id`）里的设备标识。provider 不含冒号，设备标识可能含，
/// 所以只在第一个冒号处切开。
fn device_id_of_binding_field(field: &str) -> Option<&str> {
    let (provider, device_id) = field.split_once(':')?;
    let device_id = device_id.trim();
    (!provider.trim().is_empty() && !device_id.is_empty()).then_some(device_id)
}

/// 绑定记录里路由要看的两个时间。其余字段（令牌等）不反序列化。
#[derive(serde::Deserialize)]
struct BindingActivity {
    #[serde(default)]
    registered_at_ms: i64,
    #[serde(default)]
    last_seen_ms: i64,
}

/// 这条绑定记录在 `now_ms` 时是否已经因为长期不活跃而失效。
///
/// 读不懂的记录不按失效处理：路由照常给这台设备建任务，记录有没有用由 Worker 判断，
/// 不能因为一条坏记录就悄悄不推。
fn binding_expired(record: &str, now_ms: i64, inactive_days: u32) -> bool {
    serde_json::from_str::<BindingActivity>(record).is_ok_and(|activity| {
        push_binding_inactive(
            push_binding_last_active_ms(activity.registered_at_ms, activity.last_seen_ms),
            now_ms,
            inactive_days,
        )
    })
}

/// 一张绑定表归并成去重后的设备标识。同一台设备可以同时绑定两个通道
/// （例如 FCM 与个推），它仍然是一台设备、只该收一条离线任务。长期不活跃的绑定视为不存在；
/// 删除它们是 Worker 的事，路由只读。
fn bound_devices_from_rows(
    rows: &HashMap<String, String>,
    now_ms: i64,
    inactive_days: u32,
) -> BoundDevices {
    let devices: BTreeSet<&str> = rows
        .iter()
        .filter(|(_, record)| !binding_expired(record, now_ms, inactive_days))
        .filter_map(|(field, _)| device_id_of_binding_field(field))
        .collect();
    devices.into_iter().map(str::to_string).collect()
}

/// 绑定记录里提示要看的登记信息。
#[derive(serde::Deserialize, Default)]
struct BindingProfile {
    #[serde(default)]
    platform: String,
    #[serde(default)]
    time_zone: String,
}

/// 一张绑定表按设备归并出提示用的登记信息。同一台设备的几条绑定（例如 APNs 加个推的 iOS 通道）
/// 合成一项：有一条是 APNs 或登记为 iOS 就算 iOS 设备，时区取第一条非空的。读不懂的记录照样是
/// 一台设备，只是时区未知、不按 iOS 处理。
fn device_profiles_from_rows(
    rows: &HashMap<String, String>,
    now_ms: i64,
    inactive_days: u32,
) -> Vec<DeviceProfile> {
    let mut profiles: BTreeMap<&str, DeviceProfile> = BTreeMap::new();
    for (field, record) in rows {
        if binding_expired(record, now_ms, inactive_days) {
            continue;
        }
        let Some(device_id) = device_id_of_binding_field(field) else {
            continue;
        };
        let provider = field.split_once(':').map(|(provider, _)| provider.trim());
        let profile: BindingProfile = serde_json::from_str(record).unwrap_or_default();
        let entry = profiles
            .entry(device_id)
            .or_insert_with(|| DeviceProfile::unknown(device_id));
        if provider == Some("apns") || profile.platform.trim().eq_ignore_ascii_case("ios") {
            entry.ios = true;
        }
        if entry.time_zone.is_empty() && !profile.time_zone.trim().is_empty() {
            entry.time_zone = profile.time_zone.trim().to_string();
        }
    }
    profiles.into_values().collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or_default()
}

/// 直读 Redis 绑定表，不缓存。
pub struct RedisDeviceBindingRegistry {
    conn: ConnectionManager,
    key_prefix: String,
    /// 多少天没活跃的绑定视为不存在。
    inactive_days: u32,
}

impl RedisDeviceBindingRegistry {
    /// 连接绑定表所在的 Redis。错误里不带地址：地址里可能有口令。
    pub async fn connect(
        redis_url: &str,
        key_prefix: String,
        inactive_days: u32,
    ) -> Result<Self, FlareError> {
        let client = redis::Client::open(redis_url)
            .map_err(|_| FlareError::system("invalid push server device token redis uri"))?;
        let conn = tokio::time::timeout(IO_BUDGET, ConnectionManager::new(client))
            .await
            .map_err(|_| unavailable("connect timed out"))?
            .map_err(|err| {
                FlareError::system(format!("connect push server device token redis: {err}"))
            })?;
        Ok(Self {
            conn,
            key_prefix,
            inactive_days,
        })
    }

    /// 按给定顺序读出每个用户有绑定的设备；没有绑定的用户得到空表。
    async fn read(
        &self,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<Vec<BoundDevices>, FlareError> {
        let now = now_ms();
        Ok(self
            .read_rows(tenant_id, user_ids)
            .await?
            .iter()
            .map(|rows| bound_devices_from_rows(rows, now, self.inactive_days))
            .collect())
    }

    /// 按给定顺序读出每个用户的整张绑定表。
    async fn read_rows(
        &self,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<Vec<HashMap<String, String>>, FlareError> {
        let mut conn = self.conn.clone();
        let mut out = Vec::with_capacity(user_ids.len());
        for batch in user_ids.chunks(REGISTRY_READ_BATCH) {
            let mut pipe = redis::pipe();
            for user_id in batch {
                pipe.hgetall(device_push_token_registry_key(
                    &self.key_prefix,
                    tenant_id,
                    user_id,
                ));
            }
            let reply: redis::RedisResult<Vec<HashMap<String, String>>> =
                tokio::time::timeout(IO_BUDGET, pipe.query_async(&mut conn))
                    .await
                    .map_err(|_| unavailable("read timed out"))?;
            let rows = reply.map_err(|err| unavailable(&err.to_string()))?;
            if rows.len() != batch.len() {
                return Err(unavailable("pipeline reply count mismatch"));
            }
            out.extend(rows);
        }
        Ok(out)
    }
}

#[async_trait::async_trait]
impl DeviceBindingRepository for RedisDeviceBindingRegistry {
    async fn bound_devices(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, BoundDevices>, FlareError> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let devices = self.read(tenant_id, user_ids).await?;
        Ok(user_ids
            .iter()
            .cloned()
            .zip(devices)
            .filter(|(_, devices)| !devices.is_empty())
            .collect())
    }

    async fn bound_devices_fresh(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<BoundDevices, FlareError> {
        let devices = self.read(tenant_id, &[user_id.to_string()]).await?;
        Ok(devices.into_iter().next().unwrap_or_default())
    }

    async fn device_profiles(
        &self,
        _ctx: &Ctx,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, Vec<DeviceProfile>>, FlareError> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let now = now_ms();
        let rows = self.read_rows(tenant_id, user_ids).await?;
        Ok(user_ids
            .iter()
            .cloned()
            .zip(rows)
            .map(|(user_id, rows)| {
                (
                    user_id,
                    device_profiles_from_rows(&rows, now, self.inactive_days),
                )
            })
            .filter(|(_, profiles)| !profiles.is_empty())
            .collect())
    }
}

/// 给绑定读取加一层进程内缓存：有界、定时过期，「没有绑定」同样缓存。
///
/// 没有绑定的用户（只用桌面与 Web 的人）才是大多数，不缓存空结果的话他们每条消息都要读一次
/// Redis，缓存就白加了。
pub struct CachedDeviceBindings {
    inner: Arc<dyn DeviceBindingRepository>,
    cache: moka::future::Cache<(String, String), BoundDevices>,
    /// 提示用的登记信息，另一份缓存：只有要推送的设备才问，周期与上面相同。
    profiles: moka::future::Cache<(String, String), Arc<[DeviceProfile]>>,
}

impl CachedDeviceBindings {
    pub fn new(inner: Arc<dyn DeviceBindingRepository>) -> Self {
        Self::with_ttl(inner, BINDING_CACHE_TTL)
    }

    pub fn with_ttl(inner: Arc<dyn DeviceBindingRepository>, ttl: Duration) -> Self {
        Self {
            inner,
            cache: moka::future::Cache::builder()
                .max_capacity(BINDING_CACHE_CAPACITY)
                .time_to_live(ttl)
                .build(),
            profiles: moka::future::Cache::builder()
                .max_capacity(BINDING_CACHE_CAPACITY)
                .time_to_live(ttl)
                .build(),
        }
    }
}

#[async_trait::async_trait]
impl DeviceBindingRepository for CachedDeviceBindings {
    async fn bound_devices(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, BoundDevices>, FlareError> {
        let mut out = HashMap::new();
        let mut misses = Vec::new();
        for user_id in user_ids {
            match self
                .cache
                .get(&(tenant_id.to_string(), user_id.clone()))
                .await
            {
                Some(devices) => {
                    if !devices.is_empty() {
                        out.insert(user_id.clone(), devices);
                    }
                }
                None => misses.push(user_id.clone()),
            }
        }
        if misses.is_empty() {
            return Ok(out);
        }

        let mut read = self.inner.bound_devices(ctx, tenant_id, &misses).await?;
        for user_id in misses {
            let devices = read.remove(&user_id).unwrap_or_default();
            self.cache
                .insert((tenant_id.to_string(), user_id.clone()), devices.clone())
                .await;
            if !devices.is_empty() {
                out.insert(user_id, devices);
            }
        }
        Ok(out)
    }

    async fn bound_devices_fresh(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<BoundDevices, FlareError> {
        let devices = self
            .inner
            .bound_devices_fresh(ctx, tenant_id, user_id)
            .await?;
        self.cache
            .insert(
                (tenant_id.to_string(), user_id.to_string()),
                devices.clone(),
            )
            .await;
        Ok(devices)
    }

    async fn device_profiles(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, Vec<DeviceProfile>>, FlareError> {
        let mut out = HashMap::new();
        let mut misses = Vec::new();
        for user_id in user_ids {
            match self
                .profiles
                .get(&(tenant_id.to_string(), user_id.clone()))
                .await
            {
                Some(profiles) => {
                    if !profiles.is_empty() {
                        out.insert(user_id.clone(), profiles.to_vec());
                    }
                }
                None => misses.push(user_id.clone()),
            }
        }
        if misses.is_empty() {
            return Ok(out);
        }

        let mut read = self.inner.device_profiles(ctx, tenant_id, &misses).await?;
        for user_id in misses {
            let profiles = read.remove(&user_id).unwrap_or_default();
            self.profiles
                .insert(
                    (tenant_id.to_string(), user_id.clone()),
                    Arc::from(profiles.clone()),
                )
                .await;
            if !profiles.is_empty() {
                out.insert(user_id, profiles);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    const NOW: i64 = 1_800_000_000_000;
    const DAY: i64 = 86_400_000;
    const DAYS: u32 = flare_im_contracts::domain::push_binding::PUSH_BINDING_INACTIVE_DAYS_DEFAULT;

    /// 一条刚登记、没核对过的绑定记录。
    fn record(registered_at_ms: i64, last_seen_ms: i64) -> String {
        serde_json::json!({
            "provider": "fcm",
            "token": "PRIVATE",
            "registration_id": "r1",
            "registered_at_ms": registered_at_ms,
            "last_seen_ms": last_seen_ms,
        })
        .to_string()
    }

    fn rows(values: &[(&str, String)]) -> HashMap<String, String> {
        values
            .iter()
            .map(|(field, record)| (field.to_string(), record.clone()))
            .collect()
    }

    #[test]
    fn binding_fields_yield_distinct_device_ids() {
        let fresh = record(NOW - DAY, 0);
        let devices = bound_devices_from_rows(
            &rows(&[
                ("fcm:pixel-1", fresh.clone()),
                ("getui:pixel-1", fresh.clone()),
                ("apns:iphone-1", fresh.clone()),
                ("apns:pad:with:colons", fresh),
            ]),
            NOW,
            DAYS,
        );
        assert_eq!(
            devices.to_vec(),
            vec!["iphone-1", "pad:with:colons", "pixel-1"]
        );
    }

    /// 认不出的 field 不算绑定：宁可这台设备不推，也不给一个猜出来的设备标识建任务。
    #[test]
    fn malformed_binding_fields_are_not_devices() {
        for field in ["", "apns", "apns:", ":device", " : ", "apns:  "] {
            assert_eq!(device_id_of_binding_field(field), None, "{field:?}");
        }
        let fresh = record(NOW, 0);
        assert!(
            bound_devices_from_rows(&rows(&[("apns", fresh.clone()), (":x", fresh)]), NOW, DAYS)
                .is_empty()
        );
        assert!(bound_devices_from_rows(&HashMap::new(), NOW, DAYS).is_empty());
    }

    /// 60 天没活跃的绑定视为不存在：按最近一次核对算，没核对过按登记时间算。
    /// 同一台设备另一个通道还活跃时，设备仍然有绑定。
    #[test]
    fn long_inactive_bindings_are_not_devices() {
        let devices = bound_devices_from_rows(
            &rows(&[
                ("fcm:stale-phone", record(NOW - 61 * DAY, 0)),
                ("fcm:seen-phone", record(NOW - 90 * DAY, NOW - DAY)),
                ("apns:new-phone", record(NOW - DAY, 0)),
                ("fcm:two-channels", record(NOW - 61 * DAY, NOW - 61 * DAY)),
                ("getui:two-channels", record(NOW - 2 * DAY, 0)),
            ]),
            NOW,
            DAYS,
        );
        assert_eq!(
            devices.to_vec(),
            vec!["new-phone", "seen-phone", "two-channels"]
        );
        assert!(binding_expired(&record(NOW - 60 * DAY - 1, 0), NOW, DAYS));
        assert!(!binding_expired(&record(NOW - 60 * DAY, 0), NOW, DAYS));
    }

    /// 提示用的登记信息按设备归并：APNs 或登记为 iOS 的是 iOS 设备，时区取第一条非空的。
    #[test]
    fn device_profiles_merge_the_bindings_of_one_device() {
        let with = |platform: &str, time_zone: &str| {
            serde_json::json!({
                "provider": "x",
                "platform": platform,
                "time_zone": time_zone,
                "registered_at_ms": NOW - DAY,
                "last_seen_ms": 0,
            })
            .to_string()
        };
        let profiles = device_profiles_from_rows(
            &rows(&[
                ("apns:iphone", with("", "")),
                ("getui:iphone", with("ios", "Asia/Shanghai")),
                ("fcm:pixel", with("android", "Europe/Berlin")),
                ("getui:pad", with("iOS", "")),
                ("fcm:stale", record(NOW - 61 * DAY, 0)),
                ("fcm:opaque", "not json".into()),
            ]),
            NOW,
            DAYS,
        );
        let summary: Vec<(&str, &str, bool)> = profiles
            .iter()
            .map(|p| (p.device_id.as_str(), p.time_zone.as_str(), p.ios))
            .collect();
        assert_eq!(
            summary,
            vec![
                ("iphone", "Asia/Shanghai", true),
                ("opaque", "", false),
                ("pad", "", true),
                ("pixel", "Europe/Berlin", false),
            ]
        );
    }

    /// 读不懂的记录不算过期：路由照常建任务，记录能不能用交给 Worker。
    #[test]
    fn an_unreadable_record_still_counts_as_a_binding() {
        assert!(!binding_expired("PRIVATE malformed", NOW, DAYS));
        let devices =
            bound_devices_from_rows(&rows(&[("fcm:phone", "not json".into())]), NOW, DAYS);
        assert_eq!(devices.to_vec(), vec!["phone"]);
    }

    /// 可控的绑定来源：记录每次被问到了谁，`down` 模拟 Redis 不可用。
    #[derive(Default)]
    struct Registry {
        devices: Mutex<HashMap<String, Vec<String>>>,
        batch_reads: Mutex<Vec<Vec<String>>>,
        fresh_reads: Mutex<usize>,
        down: Mutex<bool>,
    }

    impl Registry {
        fn bind(&self, user_id: &str, devices: &[&str]) {
            self.devices.lock().unwrap().insert(
                user_id.to_string(),
                devices.iter().map(|device| device.to_string()).collect(),
            );
        }

        fn of(&self, user_id: &str) -> BoundDevices {
            self.devices
                .lock()
                .unwrap()
                .get(user_id)
                .cloned()
                .unwrap_or_default()
                .into()
        }
    }

    #[async_trait::async_trait]
    impl DeviceBindingRepository for Registry {
        async fn bound_devices(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            user_ids: &[String],
        ) -> Result<HashMap<String, BoundDevices>, FlareError> {
            self.batch_reads.lock().unwrap().push(user_ids.to_vec());
            if *self.down.lock().unwrap() {
                return Err(unavailable("down"));
            }
            Ok(user_ids
                .iter()
                .map(|user_id| (user_id.clone(), self.of(user_id)))
                .filter(|(_, devices)| !devices.is_empty())
                .collect())
        }

        async fn bound_devices_fresh(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            user_id: &str,
        ) -> Result<BoundDevices, FlareError> {
            *self.fresh_reads.lock().unwrap() += 1;
            if *self.down.lock().unwrap() {
                return Err(unavailable("down"));
            }
            Ok(self.of(user_id))
        }
    }

    fn ctx() -> Ctx {
        Arc::new(flare_server_core::context::Context::with_request_id(
            "req-1",
        ))
    }

    fn users(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    /// 命中缓存的人不再读 Redis，没有绑定的人同样算命中；只有没见过的人才去读。
    #[tokio::test]
    async fn only_uncached_users_reach_the_registry_and_empty_answers_are_cached() {
        let registry = Arc::new(Registry::default());
        registry.bind("phone-user", &["phone"]);
        let cached = CachedDeviceBindings::new(registry.clone());

        let first = cached
            .bound_devices(&ctx(), "t1", &users(&["phone-user", "desktop-only"]))
            .await
            .unwrap();
        assert_eq!(first.len(), 1, "没有绑定的用户不出现在结果里");
        assert_eq!(first["phone-user"].to_vec(), vec!["phone"]);

        let second = cached
            .bound_devices(
                &ctx(),
                "t1",
                &users(&["phone-user", "desktop-only", "newcomer"]),
            )
            .await
            .unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(
            *registry.batch_reads.lock().unwrap(),
            vec![users(&["phone-user", "desktop-only"]), users(&["newcomer"])]
        );
    }

    /// 同一个用户标识在两个租户里是两个人，缓存不能串。
    #[tokio::test]
    async fn the_cache_is_scoped_by_tenant() {
        let registry = Arc::new(Registry::default());
        registry.bind("u1", &["phone"]);
        let cached = CachedDeviceBindings::new(registry.clone());

        cached
            .bound_devices(&ctx(), "t1", &users(&["u1"]))
            .await
            .unwrap();
        cached
            .bound_devices(&ctx(), "t2", &users(&["u1"]))
            .await
            .unwrap();
        assert_eq!(registry.batch_reads.lock().unwrap().len(), 2);
    }

    /// 交接直读权威数据并刷新缓存：缓存里「没有绑定」的旧答案不能让刚登记的设备丢掉补提示，
    /// 读到之后正常路由也立刻看得见这条绑定。
    #[tokio::test]
    async fn a_fresh_read_bypasses_and_then_replaces_a_stale_cached_answer() {
        let registry = Arc::new(Registry::default());
        let cached = CachedDeviceBindings::new(registry.clone());

        let before = cached
            .bound_devices(&ctx(), "t1", &users(&["u1"]))
            .await
            .unwrap();
        assert!(before.is_empty());

        registry.bind("u1", &["phone"]);
        let stale = cached
            .bound_devices(&ctx(), "t1", &users(&["u1"]))
            .await
            .unwrap();
        assert!(stale.is_empty(), "缓存周期内仍是旧答案");

        let fresh = cached
            .bound_devices_fresh(&ctx(), "t1", "u1")
            .await
            .unwrap();
        assert_eq!(fresh.to_vec(), vec!["phone"]);
        assert_eq!(*registry.fresh_reads.lock().unwrap(), 1);

        let after = cached
            .bound_devices(&ctx(), "t1", &users(&["u1"]))
            .await
            .unwrap();
        assert_eq!(after["u1"].to_vec(), vec!["phone"]);
        assert_eq!(
            registry.batch_reads.lock().unwrap().len(),
            1,
            "直读刷新了缓存，之后不必再读"
        );
    }

    #[tokio::test]
    async fn cached_answers_expire() {
        let registry = Arc::new(Registry::default());
        let cached = CachedDeviceBindings::with_ttl(registry.clone(), Duration::from_millis(20));

        cached
            .bound_devices(&ctx(), "t1", &users(&["u1"]))
            .await
            .unwrap();
        registry.bind("u1", &["phone"]);
        tokio::time::sleep(Duration::from_millis(80)).await;

        let after = cached
            .bound_devices(&ctx(), "t1", &users(&["u1"]))
            .await
            .unwrap();
        assert_eq!(after["u1"].to_vec(), vec!["phone"]);
    }

    /// 读失败不进缓存：否则 Redis 抖一下，接下来一个缓存周期里所有人都「没有绑定」。
    #[tokio::test]
    async fn a_failed_read_is_an_error_and_is_not_cached() {
        let registry = Arc::new(Registry::default());
        registry.bind("u1", &["phone"]);
        let cached = CachedDeviceBindings::new(registry.clone());

        *registry.down.lock().unwrap() = true;
        assert!(
            cached
                .bound_devices(&ctx(), "t1", &users(&["u1"]))
                .await
                .is_err()
        );
        assert!(
            cached
                .bound_devices_fresh(&ctx(), "t1", "u1")
                .await
                .is_err()
        );

        *registry.down.lock().unwrap() = false;
        let recovered = cached
            .bound_devices(&ctx(), "t1", &users(&["u1"]))
            .await
            .unwrap();
        assert_eq!(recovered["u1"].to_vec(), vec!["phone"]);
    }
}
