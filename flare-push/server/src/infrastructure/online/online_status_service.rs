use std::collections::HashMap;
use std::sync::Arc;

use flare_grpc_proto::signaling::online::online_service_client::OnlineServiceClient;
use flare_grpc_proto::signaling::online::{BatchGetUserPresenceRequest, UserPresence};
use flare_im_contracts::Ctx;
use flare_im_service_kit::discovery::{
    invalidate_static_channel_on_transport_failure, resolve_grpc_channel,
};
use flare_proto::common::{DeviceAppState, DeviceAttention};
use flare_server_core::SignedActorType;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError, Result};
use flare_server_core::grpc::middleware::signed_context::VerifiedContext;
use redis::aio::ConnectionManager;

use crate::config::PushServerConfig;
use crate::domain::model::{LinkState, UserReachability};

/// 会话记录里的应用状态（决策链用）。缺省按前台；认不出的取值为 `None`。
fn session_link_state(app_state: Option<&str>) -> Option<LinkState> {
    match app_state.map(str::trim) {
        None | Some("") => Some(LinkState::Active),
        Some(state) if state.eq_ignore_ascii_case("active") => Some(LinkState::Active),
        Some(state) if state.eq_ignore_ascii_case("standby") => Some(LinkState::Standby),
        Some(state) if state.eq_ignore_ascii_case("deferred") => Some(LinkState::Deferred),
        Some(state) if state.eq_ignore_ascii_case("parking") => Some(LinkState::Parked),
        Some(_) => None,
    }
}

/// Online 给出的应用状态（决策链用），口径与会话记录相同。
fn presence_link_state(app_state: i32) -> Option<LinkState> {
    match DeviceAppState::try_from(app_state) {
        Ok(DeviceAppState::Active | DeviceAppState::Unspecified) => Some(LinkState::Active),
        Ok(DeviceAppState::Standby) => Some(LinkState::Standby),
        Ok(DeviceAppState::Deferred) => Some(LinkState::Deferred),
        Ok(DeviceAppState::Parking) => Some(LinkState::Parked),
        Err(_) => None,
    }
}

/// 会话记录里表示「链路负责提示」的应用状态取值：前台（`active`），以及在后台、链路保留、
/// 本进程能自己发系统通知（`standby`）。只有这两种（和缺省）算活跃设备。
///
/// 其余取值的设备连接还在也不算活跃，提示归推送通道：即将被挂起的（`parking`），和在后台、
/// 链路保留、却不能自己提示的（`deferred`）。后者正是按白名单而不按「不是 parking」判定的原因：
/// 它的连接一直在，消息也照常经链路下发，若算作活跃，每条消息都得等送达确认超时再交接才推得出去。
/// 认不出的取值同样不算活跃：宁可多推一条，不能让一台没人提示的设备静悄悄。
const LINK_ALERTING_APP_STATES: [&str; 2] = ["active", "standby"];

/// 会话记录里的应用状态是否表示链路负责提示。缺省（旧客户端不上报）按前台处理。
fn link_alerts(app_state: Option<&str>) -> bool {
    match app_state.map(str::trim) {
        None | Some("") => true,
        Some(state) => LINK_ALERTING_APP_STATES
            .iter()
            .any(|alerting| state.eq_ignore_ascii_case(alerting)),
    }
}

/// Online 给出的应用状态是否表示链路负责提示。未填按前台处理，与会话记录的缺省一致；
/// 已托管、已转交推送与认不出的取值都不算。
fn presence_link_alerts(app_state: i32) -> bool {
    matches!(
        DeviceAppState::try_from(app_state),
        Ok(DeviceAppState::Active | DeviceAppState::Standby | DeviceAppState::Unspecified)
    )
}

/// 一条管线里最多读多少个用户的会话记录。回包要在内存里整份攒齐，
/// 不分批的话一次大群扇出就是几万个 HVALS 的回包同时在场。
const SESSION_READ_BATCH: usize = 500;

/// Online 的批量在线状态接口一次最多收多少个用户。
const PRESENCE_BATCH: usize = 100;

/// 会话记录里路由要看的字段；其余字段不解析。
#[derive(serde::Deserialize)]
struct SessionRecordView {
    #[serde(default)]
    device_id: String,
    #[serde(default)]
    app_state: Option<String>,
    #[serde(default)]
    attention: Option<String>,
}

/// 会话记录里的应用状态与注意力是否表示「前台且用户正在看」：应用状态是前台（缺省也算），
/// 注意力是 `attentive`。
fn session_attentive(app_state: Option<&str>, attention: Option<&str>) -> bool {
    let active = match app_state.map(str::trim) {
        None | Some("") => true,
        Some(state) => state.eq_ignore_ascii_case("active"),
    };
    active
        && attention
            .map(str::trim)
            .is_some_and(|attention| attention.eq_ignore_ascii_case("attentive"))
}

/// 由一个用户的全部会话记录得出设备可达视图。
///
/// 读不懂的记录仍然算一条会话：在线任务照发（没人收就丢弃），但认不出是哪台设备，
/// 所以不能据此压掉任何一台设备的推送。
pub(crate) fn reachability_from_session_records(records: &[Vec<u8>]) -> UserReachability {
    let mut reach = UserReachability::offline();
    for raw in records {
        match serde_json::from_slice::<SessionRecordView>(raw) {
            Ok(session) => {
                let push_owned = !link_alerts(session.app_state.as_deref());
                reach.record_session(&session.device_id, push_owned);
                if let Some(state) = session_link_state(session.app_state.as_deref()) {
                    reach.record_link_state(&session.device_id, state);
                }
                if session_attentive(session.app_state.as_deref(), session.attention.as_deref()) {
                    reach.record_attentive(&session.device_id);
                }
            }
            Err(_) => reach.record_session("", false),
        }
    }
    reach
}

/// 由 Online 的在线状态得出设备可达视图，判定规则与会话记录相同。
fn reachability_from_presence(presence: &UserPresence) -> UserReachability {
    let mut reach = UserReachability::offline();
    for device in &presence.devices {
        let push_owned = !presence_link_alerts(device.app_state);
        reach.record_session(&device.device_id, push_owned);
        if let Some(state) = presence_link_state(device.app_state) {
            reach.record_link_state(&device.device_id, state);
        }
        let active = matches!(
            DeviceAppState::try_from(device.app_state),
            Ok(DeviceAppState::Active | DeviceAppState::Unspecified)
        );
        if active && device.attention == DeviceAttention::Attentive as i32 {
            reach.record_attentive(&device.device_id);
        }
    }
    // 说在线却没给设备明细：仍是一条会话，只是认不出设备。
    if presence.is_online {
        reach.has_session = true;
    }
    reach
}

enum OnlineStatusBackend {
    /// signaling-online 的静态 gRPC 地址。通道取自 service-kit 的静态通道缓存（建连上限 2 s 覆盖
    /// 名字解析与握手、HTTP/2 探活），调用遇到传输层失败时丢弃，下一次调用重新解析、新建连接。
    Grpc {
        endpoint: String,
    },
    Redis(ConnectionManager),
}

fn require_tenant(ctx: &Ctx) -> Result<&str> {
    let tenant_id = ctx.tenant_id().ok_or_else(|| {
        ErrorBuilder::new(
            ErrorCode::PermissionDenied,
            "push online query requires tenant",
        )
        .build_error()
    })?;
    if !flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant_id)
        || flare_im_contracts::utils::normalize_tenant_id(tenant_id) != tenant_id
    {
        return Err(
            ErrorBuilder::new(ErrorCode::PermissionDenied, "non-canonical push tenant")
                .build_error(),
        );
    }
    let claims = ctx
        .get_data::<VerifiedContext>()
        .and_then(|marker| marker.claims())
        .ok_or_else(|| {
            ErrorBuilder::new(
                ErrorCode::PermissionDenied,
                "verified Push online identity required",
            )
            .build_error()
        })?;
    if claims.tenant_id != tenant_id
        || !(claims.actor_type == SignedActorType::User
            || (claims.actor_type == SignedActorType::Service
                && matches!(
                    claims.actor_id.as_str(),
                    "flare-push-server" | "flare-push-worker"
                )))
    {
        return Err(ErrorBuilder::new(
            ErrorCode::PermissionDenied,
            "Push online actor or tenant mismatch",
        )
        .build_error());
    }
    Ok(tenant_id)
}

pub struct OnlineStatusService {
    config: Arc<PushServerConfig>,
    backend: OnlineStatusBackend,
}

impl OnlineStatusService {
    pub async fn new(config: Arc<PushServerConfig>) -> Result<Self> {
        let backend = match config.online_status_backend.as_str() {
            "redis" => {
                let client = redis::Client::open(config.redis_url.clone()).map_err(|err| {
                    FlareError::system(format!(
                        "invalid push server redis uri {}: {err}",
                        config.redis_url
                    ))
                })?;
                let manager = ConnectionManager::new(client).await.map_err(|err| {
                    FlareError::system(format!("connect push server redis: {err}"))
                })?;
                OnlineStatusBackend::Redis(manager)
            }
            "grpc" => {
                // 惰性建连：signaling-online 暂时不可达时照常启动，查询按请求失败。以前启动时
                // 立即建连，对端没起来就启动失败，建好的通道也没有探活、永久复用。
                let endpoint = config.online_service_endpoint.clone();
                resolve_grpc_channel(&endpoint).await.map_err(|err| {
                    FlareError::system(format!("invalid online grpc uri {endpoint}: {err}"))
                })?;
                OnlineStatusBackend::Grpc { endpoint }
            }
            other => {
                return Err(ErrorBuilder::new(
                    ErrorCode::ConfigurationError,
                    "unsupported push server online status backend",
                )
                .param("backend", other.to_string())
                .build_error());
            }
        };
        Ok(Self { config, backend })
    }

    /// 这批用户各自的设备可达视图。没有任何会话的用户不出现在结果里。
    ///
    /// 按设备而不是按用户：以前只问「这个用户有没有会话」，电脑在线就把同一个人所有手机的
    /// 离线推送都压掉了。
    pub async fn device_reachability(
        &self,
        ctx: &Ctx,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserReachability>> {
        if user_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let tenant_id = require_tenant(ctx)?;

        match &self.backend {
            OnlineStatusBackend::Grpc { endpoint } => {
                self.grpc_device_reachability(ctx, endpoint, user_ids).await
            }
            OnlineStatusBackend::Redis(manager) => {
                self.redis_device_reachability(manager, tenant_id, user_ids)
                    .await
            }
        }
    }

    pub async fn conversation_online_user_ids(
        &self,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        match &self.backend {
            OnlineStatusBackend::Redis(manager) => {
                self.redis_conversation_online_user_ids(manager, ctx, conversation_id)
                    .await
            }
            OnlineStatusBackend::Grpc { .. } => Err(ErrorBuilder::new(
                ErrorCode::ConfigurationError,
                "conversation online index requires redis online status backend",
            )
            .param("backend", "grpc")
            .build_error()),
        }
    }

    async fn grpc_device_reachability(
        &self,
        ctx: &Ctx,
        endpoint: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserReachability>> {
        // MQ context headers are not a signing authority. Forward only a token
        // that an upstream gRPC boundary actually verified; otherwise fail closed.
        let token = ctx
            .get_data::<VerifiedContext>()
            .and_then(|marker| marker.token())
            .ok_or_else(|| {
                ErrorBuilder::new(
                    ErrorCode::PermissionDenied,
                    "verified Online query identity required",
                )
                .build_error()
            })?;
        query_online_service(endpoint, token, user_ids).await
    }

    /// 直读 Online 写的会话记录：`session:{tenant}:{user}` 是一张 session id → JSON 的表，
    /// 一个用户通常只有几条。只取值（HVALS），不关心 session id。
    async fn redis_device_reachability(
        &self,
        manager: &ConnectionManager,
        tenant_id: &str,
        user_ids: &[String],
    ) -> Result<HashMap<String, UserReachability>> {
        let mut conn = manager.clone();
        let mut out = HashMap::with_capacity(user_ids.len());
        for batch in user_ids.chunks(SESSION_READ_BATCH) {
            let mut pipe = redis::pipe();
            for user_id in batch {
                pipe.hvals(format!("session:{tenant_id}:{user_id}"));
            }
            let records: Vec<Vec<Vec<u8>>> = pipe.query_async(&mut conn).await.map_err(|err| {
                FlareError::system(format!("query redis session records batch: {err}"))
            })?;
            for (user_id, sessions) in batch.iter().zip(records) {
                let reach = reachability_from_session_records(&sessions);
                if reach.has_session {
                    out.insert(user_id.clone(), reach);
                }
            }
        }
        Ok(out)
    }

    /// 单次 SSCAN 取回的成员数。给大了等于把 SMEMBERS 的问题原样搬过来，
    /// 给小了则往返次数过多；1024 是常见折中。
    const ONLINE_SCAN_BATCH: usize = 1024;

    /// 一个会话最多取多少在线成员。超出即截断并告警——宁可少推一部分，
    /// 也不能让单个超大群把推送进程撑爆。
    const ONLINE_SCAN_HARD_CAP: usize = 200_000;

    async fn redis_conversation_online_user_ids(
        &self,
        manager: &ConnectionManager,
        ctx: &Ctx,
        conversation_id: &str,
    ) -> Result<Vec<String>> {
        let tenant_id = require_tenant(ctx)?;
        let key = format!("conv:online:{tenant_id}:{conversation_id}");
        let mut conn = manager.clone();

        // 用 SSCAN 游标分批，不用 SMEMBERS。
        //
        // SMEMBERS 会把整个集合一次性拉进 Vec：500 万人的群就是一次调用分配
        // 几百 MB，并发几条消息就是 GB 级。线上实测 push-server 因此涨到
        // 13.4GB，触发的是**全局** OOM（CONSTRAINT_NONE），把 NATS 和 Redis
        // 一起拖下水——NATS 因此重启了 34 次。
        //
        // SMEMBERS 还会阻塞 Redis 的单线程：大集合期间所有其他命令排队，
        // 表现为整个 IM 的 seq 分配、在线查询集体变慢。
        let mut user_ids: Vec<String> = Vec::new();
        let mut cursor: u64 = 0;
        let mut truncated = false;
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SSCAN")
                .arg(&key)
                .arg(cursor)
                .arg("COUNT")
                .arg(Self::ONLINE_SCAN_BATCH)
                .query_async(&mut conn)
                .await
                .map_err(|err| {
                    FlareError::system(format!("scan redis conversation online index {key}: {err}"))
                })?;
            user_ids.extend(batch.into_iter().filter(|id| !id.trim().is_empty()));
            cursor = next;
            if user_ids.len() >= Self::ONLINE_SCAN_HARD_CAP {
                truncated = true;
                break;
            }
            // SSCAN 以游标回到 0 表示遍历完成
            if cursor == 0 {
                break;
            }
        }

        user_ids.sort();
        user_ids.dedup();
        if truncated {
            user_ids.truncate(Self::ONLINE_SCAN_HARD_CAP);
            tracing::warn!(
                conversation_id = %conversation_id,
                cap = Self::ONLINE_SCAN_HARD_CAP,
                "会话在线成员超过上限，本次推送已截断"
            );
        }
        Ok(user_ids)
    }

    pub fn default_tenant_id(&self) -> &str {
        &self.config.default_tenant_id
    }
}

/// 向 signaling-online 查询设备可达视图。传输层失败（连不上、连接被断、ping 超时）时丢弃该地址的
/// 静态通道，对端回的状态不动缓存。
///
/// 用批量在线状态接口而不是 `GetOnlineStatus`：后者每个用户只回一台设备，按它判定又会退回
/// 「按用户判定在线」。
async fn query_online_service(
    endpoint: &str,
    token: &str,
    user_ids: &[String],
) -> Result<HashMap<String, UserReachability>> {
    let mut out = HashMap::with_capacity(user_ids.len());
    for batch in user_ids.chunks(PRESENCE_BATCH) {
        let channel = resolve_grpc_channel(endpoint).await.map_err(|err| {
            ErrorBuilder::new(ErrorCode::ServiceUnavailable, "online grpc channel")
                .details(err)
                .build_error()
        })?;
        let mut client = OnlineServiceClient::new(channel);
        let mut req = tonic::Request::new(BatchGetUserPresenceRequest {
            user_ids: batch.to_vec(),
        });
        flare_server_core::set_signed_context_token(&mut req, token)?;
        let resp = match client.batch_get_user_presence(req).await {
            Ok(resp) => resp.into_inner(),
            Err(status) => {
                if invalidate_static_channel_on_transport_failure(endpoint, &status) {
                    tracing::debug!(
                        endpoint,
                        code = ?status.code(),
                        "online grpc channel dropped after transport failure"
                    );
                }
                return Err(status.into());
            }
        };
        for (user_id, presence) in resp.presences {
            let reach = reachability_from_presence(&presence);
            if reach.has_session {
                out.insert(user_id, reach);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod device_reachability_tests {
    use super::*;
    use flare_grpc_proto::signaling::online::DeviceInfo;

    fn records(values: &[&str]) -> Vec<Vec<u8>> {
        values
            .iter()
            .map(|value| value.as_bytes().to_vec())
            .collect()
    }

    /// 电脑在线、手机已托管：用户有会话，但活跃设备只有电脑——手机该收离线推送。
    #[test]
    fn a_parked_phone_next_to_a_live_desktop_is_not_live() {
        let reach = reachability_from_session_records(&records(&[
            r#"{"device_id":"desktop","device_platform":"macos","gateway_id":"g1","app_state":"active"}"#,
            r#"{"device_id":"phone","device_platform":"ios","gateway_id":"g1","app_state":"parking","badge":3}"#,
        ]));
        assert!(reach.has_session);
        assert!(reach.is_live("desktop"));
        assert!(!reach.is_live("phone"));
    }

    /// 应用状态缺省按前台处理；后台、链路保留、能自己提示的设备仍由链路负责提示。
    #[test]
    fn missing_active_and_standby_app_states_are_live() {
        let reach = reachability_from_session_records(&records(&[
            r#"{"device_id":"old-client","device_platform":"web","gateway_id":"g1"}"#,
            r#"{"device_id":"null-state","app_state":null}"#,
            r#"{"device_id":"blank-state","app_state":" "}"#,
            r#"{"device_id":"desktop","app_state":"active"}"#,
            r#"{"device_id":"android","device_platform":"android","gateway_id":"g1","app_state":"standby","attention":"idle","state_since_ms":1}"#,
            r#"{"device_id":"shouting","app_state":" Standby "}"#,
        ]));
        for live in [
            "old-client",
            "null-state",
            "blank-state",
            "desktop",
            "android",
            "shouting",
        ] {
            assert!(reach.is_live(live), "{live}");
        }
    }

    /// 后台、链路保留、却不能自己提示的手机（`deferred`）：连接还在，所以用户有会话、在线任务
    /// 照发；但它不是活跃设备，路由时就该给它发离线任务，不必等送达确认超时再交接。
    #[test]
    fn a_deferred_phone_keeps_its_session_but_is_not_live() {
        let reach = reachability_from_session_records(&records(&[
            r#"{"device_id":"phone","device_platform":"ios","gateway_id":"g1","app_state":"deferred"}"#,
        ]));
        assert!(reach.has_session);
        assert!(!reach.is_live("phone"));
        assert!(reach.live_devices.is_empty());
    }

    /// 只有 `active` 与 `standby` 算活跃：已托管、已转交推送、以及认不出的取值都不算，
    /// 大小写与首尾空白不影响判定。
    #[test]
    fn only_active_and_standby_app_states_are_live() {
        for state in [
            "parking",
            " PARKING ",
            "deferred",
            "Deferred",
            "suspended",
            "0",
        ] {
            let record = format!(r#"{{"device_id":"phone","app_state":"{state}"}}"#);
            let reach = reachability_from_session_records(&records(&[record.as_str()]));
            assert!(reach.has_session, "{state}");
            assert!(!reach.is_live("phone"), "{state}");
        }
    }

    /// 同一台设备的两条会话（重连交叠）里只要有一条由链路负责提示，它就是活跃的。
    #[test]
    fn one_link_alerting_session_makes_a_device_live() {
        let reach = reachability_from_session_records(&records(&[
            r#"{"device_id":"phone","app_state":"deferred"}"#,
            r#"{"device_id":"phone","app_state":"active"}"#,
        ]));
        assert!(reach.is_live("phone"));
    }

    /// 读不懂的记录、没有设备标识的记录：在线任务照发，但不压任何设备的推送。
    #[test]
    fn unreadable_or_anonymous_sessions_keep_the_user_online_without_a_live_device() {
        for raw in [
            "not json",
            r#"{"gateway_id":"g1"}"#,
            r#"{"device_id":null}"#,
            r#"{"device_id":"phone","app_state":7}"#,
        ] {
            let reach = reachability_from_session_records(&records(&[raw]));
            assert!(reach.has_session, "{raw}");
            assert!(reach.live_devices.is_empty(), "{raw}");
        }
        assert_eq!(
            reachability_from_session_records(&[]),
            UserReachability::offline()
        );
    }

    #[test]
    fn online_presence_maps_app_state_per_device() {
        let device = |device_id: &str, state: DeviceAppState| DeviceInfo {
            device_id: device_id.to_string(),
            app_state: state as i32,
            ..Default::default()
        };
        let reach = reachability_from_presence(&UserPresence {
            user_id: "u1".into(),
            is_online: true,
            devices: vec![
                device("desktop", DeviceAppState::Active),
                device("tablet", DeviceAppState::Standby),
                device("legacy", DeviceAppState::Unspecified),
                device("phone", DeviceAppState::Parking),
                device("background-phone", DeviceAppState::Deferred),
                DeviceInfo {
                    device_id: "from-the-future".to_string(),
                    app_state: 99,
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
        assert!(reach.has_session);
        for live in ["desktop", "tablet", "legacy"] {
            assert!(reach.is_live(live), "{live}");
        }
        for push_owned in ["phone", "background-phone", "from-the-future"] {
            assert!(!reach.is_live(push_owned), "{push_owned}");
        }

        // 在线却没有设备明细：仍算有会话，不压任何设备。
        let bare = reachability_from_presence(&UserPresence {
            is_online: true,
            ..Default::default()
        });
        assert!(bare.has_session && bare.live_devices.is_empty());
        assert_eq!(
            reachability_from_presence(&UserPresence::default()),
            UserReachability::offline()
        );
    }

    /// 前台且注意力为 `attentive` 的设备才算「正在看」；后台、空闲或没报注意力的都不算。
    #[test]
    fn attentive_devices_are_active_and_attentive() {
        let reach = reachability_from_session_records(&records(&[
            r#"{"device_id":"desktop","app_state":"active","attention":"attentive"}"#,
            r#"{"device_id":"legacy","attention":"Attentive"}"#,
            r#"{"device_id":"idle-laptop","app_state":"active","attention":"idle"}"#,
            r#"{"device_id":"no-attention","app_state":"active"}"#,
            r#"{"device_id":"standby","app_state":"standby","attention":"attentive"}"#,
            r#"{"device_id":"phone","app_state":"parking","attention":"attentive"}"#,
        ]));
        let mut attentive: Vec<&str> = reach.attentive_devices.iter().map(String::as_str).collect();
        attentive.sort_unstable();
        assert_eq!(attentive, vec!["desktop", "legacy"]);
        assert!(reach.other_device_attentive("phone"));

        let presence = reachability_from_presence(&UserPresence {
            user_id: "u1".into(),
            is_online: true,
            devices: vec![
                DeviceInfo {
                    device_id: "desktop".into(),
                    app_state: DeviceAppState::Active as i32,
                    attention: DeviceAttention::Attentive as i32,
                    ..Default::default()
                },
                DeviceInfo {
                    device_id: "tablet".into(),
                    app_state: DeviceAppState::Standby as i32,
                    attention: DeviceAttention::Attentive as i32,
                    ..Default::default()
                },
            ],
            ..Default::default()
        });
        assert!(presence.other_device_attentive("phone"));
        assert!(!presence.other_device_attentive("desktop"));
    }

    /// 按设备判定靠的是读出每条会话记录；退回 HLEN 就又变成「有会话即在线」。
    /// 断言源码是因为这条性质从返回类型上看不出来：HLEN 加一个布尔同样能拼出这个结构。
    #[test]
    fn redis_backend_reads_session_records_not_their_count() {
        let src = include_str!("online_status_service.rs");
        let body = src
            .split("async fn redis_device_reachability")
            .nth(1)
            .expect("函数应存在");
        let body = &body[..body.find("\n    /// 单次 SSCAN").unwrap_or(body.len())];
        assert!(body.contains(".hvals("), "必须读出会话记录本身");
        assert!(!body.contains(".hlen("), "不得退回按会话条数判定在线");
    }
}

#[cfg(test)]
mod online_index_scan_tests {
    use super::*;

    /// 会话在线索引必须用游标分批读，不能整集合一次性进内存。
    ///
    /// SMEMBERS 对 500 万人的群一次分配几百 MB，并发几条消息就是 GB 级：
    /// 线上实测 push-server 涨到 13.4GB 触发全局 OOM，把 NATS 一起拖死
    /// （NATS 因此重启 34 次）。它还会阻塞 Redis 单线程，拖慢全局 seq 分配。
    ///
    /// 断言源码而不是行为，是因为这条性质无法从返回值上观察到——
    /// 换回 SMEMBERS 结果完全一样，只有内存曲线会爆。
    #[test]
    fn conversation_online_index_uses_cursor_scan_not_smembers() {
        let src = include_str!("online_status_service.rs");
        let body = src
            .split("async fn redis_conversation_online_user_ids")
            .nth(1)
            .expect("函数应存在");
        let body = &body[..body.find("\n    pub fn ").unwrap_or(body.len())];

        assert!(
            body.contains("\"SSCAN\""),
            "会话在线索引必须用 SSCAN 游标分批读取"
        );
        assert!(
            !body.contains("\"SMEMBERS\""),
            "不得用 SMEMBERS：整集合一次性进内存，大群会直接把推送进程撑爆"
        );
    }

    /// 必须有硬上限：光有分批而没有上限，超大群照样能把内存堆满，
    /// 只是从"一次分配几百 MB"变成"分很多次堆到几百 MB"。
    #[test]
    #[allow(clippy::assertions_on_constants)] // 这条测试就是要钉住常量的量级
    fn scan_has_a_hard_cap_and_batches_are_bounded() {
        assert!(
            OnlineStatusService::ONLINE_SCAN_HARD_CAP > 0
                && OnlineStatusService::ONLINE_SCAN_HARD_CAP <= 1_000_000,
            "上限要存在且在合理量级"
        );
        assert!(
            OnlineStatusService::ONLINE_SCAN_BATCH > 0
                && OnlineStatusService::ONLINE_SCAN_BATCH <= 10_000,
            "单批过大等于把 SMEMBERS 的问题原样搬过来"
        );
        assert!(
            OnlineStatusService::ONLINE_SCAN_BATCH < OnlineStatusService::ONLINE_SCAN_HARD_CAP,
            "单批不该超过总上限"
        );
    }
}

#[cfg(test)]
mod online_grpc_channel_tests {
    use super::*;
    use flare_im_service_kit::discovery::is_static_channel_cached;

    fn grpc_backend_config(endpoint: &str) -> PushServerConfig {
        PushServerConfig {
            online_status_backend: "grpc".into(),
            online_service_endpoint: endpoint.to_string(),
            ..PushServerConfig::for_tests()
        }
    }

    /// gRPC 在线状态后端走 service-kit 的静态通道（建连上限、HTTP/2 探活），启动时不连下游，
    /// 传输层失败后丢弃该地址的通道。以前启动时立即建连（对端没起来就启动失败），建好的通道
    /// 没有探活、永久复用。
    #[tokio::test]
    async fn grpc_backend_uses_the_static_channel_and_drops_it_after_transport_failure() {
        // 一个刚关掉的回环端口：连上会被拒绝。
        let closed = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let endpoint = format!("http://{closed}");
        OnlineStatusService::new(Arc::new(grpc_backend_config(&endpoint)))
            .await
            .unwrap_or_else(|_| panic!("unreachable online service must not fail startup"));
        assert!(
            is_static_channel_cached(&endpoint),
            "online grpc backend must use the service-kit static channel"
        );

        let result = query_online_service(&endpoint, "token", &["u1".to_string()]).await;
        assert!(result.is_err(), "nothing listens on a closed port");
        assert!(
            !is_static_channel_cached(&endpoint),
            "transport failure must drop the cached static channel"
        );
    }

    #[tokio::test]
    async fn an_invalid_online_endpoint_is_a_startup_error() {
        assert!(
            OnlineStatusService::new(Arc::new(grpc_backend_config("http://bad host:1")))
                .await
                .is_err()
        );
    }
}
