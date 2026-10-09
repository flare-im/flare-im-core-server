//! ADM-01 管理员只读视图与踢线命令的 HTTP 契约，以及 proto → HTTP 的映射。
//!
//! 租户永远来自已验签的 Staff 上下文：请求 DTO 不带 `tenant_id`，下游（Conversation、
//! Online、Capability）也只认令牌里的租户。时间字段统一为 Unix 毫秒，枚举统一为小写
//! 蛇形字符串，未知取值落到 `unspecified`，不把 proto 的整数编号暴露给调用方。

use std::collections::HashMap;

use flare_grpc_proto::capability::{
    CapabilityDescriptor, GetTenantRuntimeResponse, ListCapabilitiesResponse,
    ListRegisteredPluginsResponse, ListTenantCapabilitySwitchesResponse,
    RegisterPluginEndpointRequest, RegisteredPluginInstance, TenantCapabilitySwitch,
};
use flare_grpc_proto::control::{TenantQuota, TenantStatus};
use flare_grpc_proto::conversation::{
    AdminListConversationsResponse, AdminListParticipantsResponse,
};
use flare_grpc_proto::signaling::online::{DeviceInfo, UserPresence};
use flare_proto::common::{
    ConversationDetail, ConversationLifecycleState, ConversationParticipant,
    ConversationVisibility, DevicePriority,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// 会话分页：缺省 50，上限 200（与 `AdminListConversationsRequest` 的服务端上限一致）。
pub const DEFAULT_CONVERSATION_PAGE: i32 = 50;
pub const MAX_CONVERSATION_PAGE: i32 = 200;
/// 成员分页：缺省 200，上限 500（与 `AdminListParticipantsRequest` 的服务端上限一致）。
pub const DEFAULT_PARTICIPANT_PAGE: i32 = 200;
pub const MAX_PARTICIPANT_PAGE: i32 = 500;

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct AdminConversationListQuery {
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<i32>,
    #[serde(default)]
    pub include_deleted: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct AdminParticipantListQuery {
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub limit: Option<i32>,
    #[serde(default)]
    pub include_removed: Option<bool>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct AdminPluginListQuery {
    /// 只列承接该能力的实例；缺省列出租户下全部实例。
    #[serde(default)]
    pub capability_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminConversationHttp {
    pub conversation_id: String,
    pub conversation_type: String,
    pub channel_id: String,
    pub display_name: String,
    pub avatar_url: String,
    pub description: String,
    /// `private` / `tenant` / `public` / `unspecified`
    pub visibility: String,
    /// `active` / `suspended` / `archived` / `deleted` / `unspecified`
    pub lifecycle_state: String,
    /// 当前有效成员数。
    pub member_count: i32,
    /// Unix 毫秒。
    pub created_at: i64,
    /// Unix 毫秒。
    pub updated_at: i64,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminConversationPageHttp {
    pub conversations: Vec<AdminConversationHttp>,
    /// 下一页游标；`has_more=false` 时为 `null`。
    pub next_cursor: Option<String>,
    pub has_more: bool,
    /// 本页实际使用的页大小。
    pub limit: i32,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminConversationDetailHttp {
    pub conversation: AdminConversationHttp,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminParticipantHttp {
    pub user_id: String,
    pub roles: Vec<String>,
    pub muted: bool,
    pub pinned: bool,
    pub mention_only: bool,
    /// 该成员可见的历史下限（只能看到 `conversation_seq` 大于它的消息）；0 = 不限。
    pub visible_from_seq: i64,
    /// Unix 毫秒。
    pub joined_at: i64,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminParticipantPageHttp {
    pub conversation_id: String,
    pub participants: Vec<AdminParticipantHttp>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
    /// 与 `include_removed` 同口径的成员总数。
    pub member_count: i32,
    pub limit: i32,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminDeviceHttp {
    pub device_id: String,
    pub platform: String,
    pub model: String,
    pub os_version: String,
    /// `low` / `normal` / `high` / `critical` / `unspecified`
    pub priority: String,
    pub token_version: i64,
    /// Unix 毫秒；未上报时为 `null`。
    pub last_active_at: Option<i64>,
    pub gateway_id: String,
    pub server_id: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminUserDevicesHttp {
    pub user_id: String,
    pub devices: Vec<AdminDeviceHttp>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminUserPresenceHttp {
    pub user_id: String,
    pub online: bool,
    /// Unix 毫秒；从未上线过时为 `null`。
    pub last_seen_at: Option<i64>,
    pub devices: Vec<AdminDeviceHttp>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminUserKickHttp {
    pub user_id: String,
    /// 本次踢下线的设备。
    pub kicked: Vec<String>,
    /// 列出之后、踢之前已经离线的设备（Online 回 NOT_FOUND 或 success=false）。
    pub already_offline: Vec<String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminDeviceKickHttp {
    pub user_id: String,
    pub device_id: String,
    pub kicked: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminTenantKickHttp {
    /// 令牌租户。
    pub tenant_id: String,
    /// 本次踢出的 (user, device) 数。
    pub kicked: u32,
}

#[derive(Debug, Clone, Default, Serialize, ToSchema)]
pub struct AdminTenantQuotaHttp {
    /// 0 = 不限。以下同。
    pub max_users: u32,
    pub max_groups: u32,
    pub max_group_members: u32,
    pub send_qps: u32,
    pub sync_pull_qps: u32,
    pub max_online_devices_per_user: u32,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminTenantRuntimeHttp {
    /// 令牌租户（规范形式）。
    pub tenant_id: String,
    /// Capability 本地是否持有该租户的投影行；`false` 时其余字段均为零值。
    pub projected: bool,
    pub name: String,
    /// `active` / `suspended` / `deleting` / `unspecified`
    pub status: String,
    pub quota: AdminTenantQuotaHttp,
    /// 控制面透传的设置（JSON 对象文本），Core 不解释。
    pub settings_json: String,
    /// 本地持有的投影版本；0 = 预置行，未收到过投影。
    pub version: u64,
    /// Unix 毫秒；未投影过为 `null`。
    pub projected_at: Option<i64>,
    /// Unix 毫秒。
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminCapabilitySwitchHttp {
    pub capability_id: String,
    /// `false` = 装过但停用；列表里没有 = 从未设置。
    pub enabled: bool,
    /// Unix 毫秒。
    pub updated_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminTenantCapabilitiesHttp {
    pub tenant_id: String,
    /// 按 `capability_id` 升序。
    pub switches: Vec<AdminCapabilitySwitchHttp>,
}

/// 能力目录项：本部署登记的能力描述符（与 proto `CapabilityDescriptor` 同字段）。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminCapabilityDescriptorHttp {
    pub capability_id: String,
    pub plugin_id: String,
    pub version: String,
    pub scope: String,
    pub visibility: String,
    pub permissions: Vec<String>,
    pub message_types: Vec<String>,
    pub timeout_ms: u64,
    pub description: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminCapabilityCatalogHttp {
    /// 按 `capability_id` 升序。
    pub capabilities: Vec<AdminCapabilityDescriptorHttp>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct AdminTenantCapabilitySwitchHttpRequest {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminCapabilitySwitchUpdateHttp {
    pub tenant_id: String,
    pub capability_id: String,
    pub enabled: bool,
    /// Capability 的确认文案。
    pub message: String,
}

/// 登记插件实例：`tenant_id` 与 `request_id` 不由调用方给，取自 Staff 上下文。
#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
pub struct AdminPluginRegisterHttpRequest {
    pub plugin_id: String,
    pub capability_id: String,
    pub grpc_authority: String,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub plugin_version: String,
    #[serde(default)]
    pub api_version: String,
    #[serde(default)]
    pub manifest_sha256: String,
    #[serde(default)]
    pub declared_operations: Vec<String>,
    /// `tenant` / `per_user`；留空沿用 Capability 的旧语义。
    #[serde(default)]
    pub seat_model: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminPluginRegisterHttp {
    pub tenant_id: String,
    pub plugin_id: String,
    pub accepted: bool,
    pub message: String,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct AdminPluginDeregisterHttpRequest {
    pub plugin_id: String,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminPluginDeregisterHttp {
    pub tenant_id: String,
    pub plugin_id: String,
    pub accepted: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminPluginHttp {
    pub plugin_id: String,
    pub capability_id: String,
    pub grpc_authority: String,
    pub plugin_version: String,
    pub api_version: String,
    pub manifest_sha256: String,
    pub declared_operations: Vec<String>,
    /// 未携带 `declared_operations` 的插件：能用，但声明边界无法强制。
    pub unverified: bool,
    pub seat_model: String,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminPluginsHttp {
    pub tenant_id: String,
    /// 按 `plugin_id` 升序。
    pub plugins: Vec<AdminPluginHttp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminPageLimitError {
    pub code: &'static str,
    pub message: String,
}

/// 请求体字段校验失败：固定原因码与说明，不回显取值。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminArgError {
    pub code: &'static str,
    pub message: String,
}

/// 插件标识：1 到 128 个 ASCII 字母、数字或 `_ - . :`。与 Capability 登记时的规则
/// （`flare-capability` 的 `is_plugin_identifier`）相同，这里提前拒绝，不让不合规的值进审计日志或转发。
fn is_plugin_identifier(value: &str) -> bool {
    (1..=128).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}

/// 登记插件的 proto 请求：三个必填字段去空白后非空；`plugin_id`、`capability_id` 与
/// `declared_operations` 的每一项须是插件标识（[`is_plugin_identifier`]），错误里不带取值；其余原样。
/// 租户与请求号来自 Staff 上下文。
pub fn register_plugin_request(
    tenant_id: &str,
    request_id: &str,
    request: AdminPluginRegisterHttpRequest,
) -> Result<RegisterPluginEndpointRequest, AdminArgError> {
    let required = |value: &str, name: &str| -> Result<String, AdminArgError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(AdminArgError {
                code: "ADMIN_PLUGIN_FIELD_REQUIRED",
                message: format!("{name} is required"),
            });
        }
        Ok(value.to_string())
    };
    let invalid = |name: &str| AdminArgError {
        code: "ADMIN_PLUGIN_FIELD_INVALID",
        message: format!("{name} must be 1 to 128 ASCII letters, digits or _ - . :"),
    };
    let identifier = |value: &str, name: &str| -> Result<String, AdminArgError> {
        let value = required(value, name)?;
        if is_plugin_identifier(&value) {
            Ok(value)
        } else {
            Err(invalid(name))
        }
    };
    let plugin_id = identifier(&request.plugin_id, "plugin_id")?;
    let capability_id = identifier(&request.capability_id, "capability_id")?;
    let grpc_authority = required(&request.grpc_authority, "grpc_authority")?;
    if !request
        .declared_operations
        .iter()
        .all(|op| is_plugin_identifier(op))
    {
        return Err(invalid("each declared_operations entry"));
    }
    Ok(RegisterPluginEndpointRequest {
        tenant_id: tenant_id.to_string(),
        plugin_id,
        capability_id,
        grpc_authority,
        labels: request.labels,
        request_id: request_id.to_string(),
        plugin_version: request.plugin_version,
        api_version: request.api_version,
        manifest_sha256: request.manifest_sha256,
        declared_operations: request.declared_operations,
        seat_model: request.seat_model,
    })
}

fn capability_descriptor_http(descriptor: CapabilityDescriptor) -> AdminCapabilityDescriptorHttp {
    AdminCapabilityDescriptorHttp {
        capability_id: descriptor.capability_id,
        plugin_id: descriptor.plugin_id,
        version: descriptor.version,
        scope: descriptor.scope,
        visibility: descriptor.visibility,
        permissions: descriptor.permissions,
        message_types: descriptor.message_types,
        timeout_ms: descriptor.timeout_ms,
        description: descriptor.description,
    }
}

/// 目录顺序由 Capability 的注册表决定，不稳定；这里按 `capability_id` 排好。
pub fn capability_catalog_http(response: ListCapabilitiesResponse) -> AdminCapabilityCatalogHttp {
    let mut capabilities: Vec<_> = response
        .capabilities
        .into_iter()
        .map(capability_descriptor_http)
        .collect();
    capabilities.sort_by(|a, b| a.capability_id.cmp(&b.capability_id));
    AdminCapabilityCatalogHttp { capabilities }
}

/// 页大小：缺省取 `default`，小于 1 拒绝，大于 `max` 截到 `max`（与消息查询接口一致）。
pub fn page_limit(
    requested: Option<i32>,
    default: i32,
    max: i32,
) -> Result<i32, AdminPageLimitError> {
    match requested {
        None => Ok(default),
        Some(limit) if limit < 1 => Err(AdminPageLimitError {
            code: "ADMIN_PAGE_LIMIT_INVALID",
            message: format!("limit must be between 1 and {max}"),
        }),
        Some(limit) => Ok(limit.min(max)),
    }
}

fn non_empty(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn timestamp_ms(seconds: i64, nanos: i32) -> i64 {
    seconds
        .saturating_mul(1000)
        .saturating_add(i64::from(nanos) / 1_000_000)
}

macro_rules! optional_timestamp_ms {
    ($value:expr) => {
        $value.map(|ts| timestamp_ms(ts.seconds, ts.nanos))
    };
}

pub fn visibility_label(value: i32) -> &'static str {
    match ConversationVisibility::try_from(value) {
        Ok(ConversationVisibility::Private) => "private",
        Ok(ConversationVisibility::Tenant) => "tenant",
        Ok(ConversationVisibility::Public) => "public",
        _ => "unspecified",
    }
}

pub fn lifecycle_label(value: i32) -> &'static str {
    match ConversationLifecycleState::try_from(value) {
        Ok(ConversationLifecycleState::ConversationLifecycleActive) => "active",
        Ok(ConversationLifecycleState::ConversationLifecycleSuspended) => "suspended",
        Ok(ConversationLifecycleState::ConversationLifecycleArchived) => "archived",
        Ok(ConversationLifecycleState::ConversationLifecycleDeleted) => "deleted",
        _ => "unspecified",
    }
}

pub fn device_priority_label(value: i32) -> &'static str {
    match DevicePriority::try_from(value) {
        Ok(DevicePriority::Low) => "low",
        Ok(DevicePriority::Normal) => "normal",
        Ok(DevicePriority::High) => "high",
        Ok(DevicePriority::Critical) => "critical",
        _ => "unspecified",
    }
}

pub fn tenant_status_label(value: i32) -> &'static str {
    match TenantStatus::try_from(value) {
        Ok(TenantStatus::Active) => "active",
        Ok(TenantStatus::Suspended) => "suspended",
        Ok(TenantStatus::Deleting) => "deleting",
        _ => "unspecified",
    }
}

pub fn conversation_http(detail: ConversationDetail) -> AdminConversationHttp {
    AdminConversationHttp {
        conversation_id: detail.conversation_id,
        conversation_type: detail.conversation_type,
        channel_id: detail.channel_id,
        display_name: detail.display_name,
        avatar_url: detail.avatar_url,
        description: detail.description,
        visibility: visibility_label(detail.visibility).to_string(),
        lifecycle_state: lifecycle_label(detail.lifecycle_state).to_string(),
        member_count: detail.member_count,
        created_at: detail.created_at,
        updated_at: detail.updated_at,
        attributes: detail.attributes,
    }
}

pub fn conversation_page_http(
    response: AdminListConversationsResponse,
    limit: i32,
) -> AdminConversationPageHttp {
    AdminConversationPageHttp {
        conversations: response
            .conversations
            .into_iter()
            .map(conversation_http)
            .collect(),
        next_cursor: non_empty(response.next_cursor).filter(|_| response.has_more),
        has_more: response.has_more,
        limit,
    }
}

pub fn participant_http(participant: ConversationParticipant) -> AdminParticipantHttp {
    AdminParticipantHttp {
        user_id: participant.user_id,
        roles: participant.roles,
        muted: participant.muted,
        pinned: participant.pinned,
        mention_only: participant.mention_only,
        visible_from_seq: participant.visible_from_seq,
        joined_at: participant.joined_at,
        attributes: participant.attributes,
    }
}

pub fn participant_page_http(
    response: AdminListParticipantsResponse,
    limit: i32,
) -> AdminParticipantPageHttp {
    AdminParticipantPageHttp {
        conversation_id: response.conversation_id,
        participants: response
            .participants
            .into_iter()
            .map(participant_http)
            .collect(),
        next_cursor: non_empty(response.next_cursor).filter(|_| response.has_more),
        has_more: response.has_more,
        member_count: response.member_count,
        limit,
    }
}

pub fn device_http(device: DeviceInfo) -> AdminDeviceHttp {
    AdminDeviceHttp {
        device_id: device.device_id,
        platform: device.platform,
        model: device.model,
        os_version: device.os_version,
        priority: device_priority_label(device.priority).to_string(),
        token_version: device.token_version,
        last_active_at: optional_timestamp_ms!(device.last_active_time),
        gateway_id: device.gateway_id,
        server_id: device.server_id,
    }
}

pub fn user_devices_http(user_id: String, devices: Vec<DeviceInfo>) -> AdminUserDevicesHttp {
    AdminUserDevicesHttp {
        user_id,
        devices: devices.into_iter().map(device_http).collect(),
    }
}

/// Online 对从未上线的用户可能不回 `presence`：按离线处理，而不是 500。
pub fn user_presence_http(
    user_id: String,
    presence: Option<UserPresence>,
) -> AdminUserPresenceHttp {
    let presence = presence.unwrap_or_default();
    AdminUserPresenceHttp {
        user_id,
        online: presence.is_online,
        last_seen_at: optional_timestamp_ms!(presence.last_seen),
        devices: presence.devices.into_iter().map(device_http).collect(),
    }
}

fn quota_http(quota: Option<TenantQuota>) -> AdminTenantQuotaHttp {
    let quota = quota.unwrap_or_default();
    let core = quota.core.unwrap_or_default();
    AdminTenantQuotaHttp {
        max_users: quota.max_users,
        max_groups: quota.max_groups,
        max_group_members: quota.max_group_members,
        send_qps: core.send_qps,
        sync_pull_qps: core.sync_pull_qps,
        max_online_devices_per_user: core.max_online_devices_per_user,
    }
}

pub fn tenant_runtime_http(response: GetTenantRuntimeResponse) -> AdminTenantRuntimeHttp {
    AdminTenantRuntimeHttp {
        tenant_id: response.tenant_id,
        projected: response.projected,
        name: response.name,
        status: tenant_status_label(response.status).to_string(),
        quota: quota_http(response.quota),
        settings_json: response.settings_json,
        version: response.version,
        projected_at: optional_timestamp_ms!(response.projected_at),
        updated_at: optional_timestamp_ms!(response.updated_at),
    }
}

fn capability_switch_http(switch: TenantCapabilitySwitch) -> AdminCapabilitySwitchHttp {
    AdminCapabilitySwitchHttp {
        capability_id: switch.capability_id,
        enabled: switch.enabled,
        updated_at: optional_timestamp_ms!(switch.updated_at),
    }
}

pub fn tenant_capabilities_http(
    response: ListTenantCapabilitySwitchesResponse,
) -> AdminTenantCapabilitiesHttp {
    AdminTenantCapabilitiesHttp {
        tenant_id: response.tenant_id,
        switches: response
            .switches
            .into_iter()
            .map(capability_switch_http)
            .collect(),
    }
}

fn plugin_http(instance: RegisteredPluginInstance) -> AdminPluginHttp {
    AdminPluginHttp {
        plugin_id: instance.plugin_id,
        capability_id: instance.capability_id,
        grpc_authority: instance.grpc_authority,
        plugin_version: instance.plugin_version,
        api_version: instance.api_version,
        manifest_sha256: instance.manifest_sha256,
        declared_operations: instance.declared_operations,
        unverified: instance.unverified,
        seat_model: instance.seat_model,
        labels: instance.labels,
    }
}

/// Capability 的插件簿是并发散列表，返回顺序不稳定；这里按 `plugin_id` 排好。
pub fn plugins_http(
    tenant_id: String,
    response: ListRegisteredPluginsResponse,
) -> AdminPluginsHttp {
    let mut plugins: Vec<_> = response.instances.into_iter().map(plugin_http).collect();
    plugins.sort_by(|a, b| a.plugin_id.cmp(&b.plugin_id));
    AdminPluginsHttp { tenant_id, plugins }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_grpc_proto::control::CoreQuota;
    use std::time::{Duration, UNIX_EPOCH};

    #[test]
    fn page_limit_defaults_caps_and_rejects_non_positive() {
        assert_eq!(page_limit(None, 50, 200), Ok(50));
        assert_eq!(page_limit(Some(1), 50, 200), Ok(1));
        assert_eq!(page_limit(Some(200), 50, 200), Ok(200));
        assert_eq!(page_limit(Some(10_000), 50, 200), Ok(200));
        for bad in [0, -1, i32::MIN] {
            assert_eq!(
                page_limit(Some(bad), 50, 200).unwrap_err().code,
                "ADMIN_PAGE_LIMIT_INVALID"
            );
        }
    }

    #[test]
    fn enum_labels_never_leak_proto_numbers() {
        assert_eq!(
            visibility_label(ConversationVisibility::Tenant as i32),
            "tenant"
        );
        assert_eq!(visibility_label(99), "unspecified");
        assert_eq!(
            lifecycle_label(ConversationLifecycleState::ConversationLifecycleDeleted as i32),
            "deleted"
        );
        assert_eq!(lifecycle_label(-3), "unspecified");
        assert_eq!(device_priority_label(DevicePriority::High as i32), "high");
        assert_eq!(
            tenant_status_label(TenantStatus::Suspended as i32),
            "suspended"
        );
        assert_eq!(tenant_status_label(0), "unspecified");
    }

    #[test]
    fn next_cursor_is_only_reported_while_more_pages_exist() {
        let page = conversation_page_http(
            AdminListConversationsResponse {
                conversations: vec![],
                next_cursor: "c1:stale".into(),
                has_more: false,
            },
            50,
        );
        assert_eq!(page.next_cursor, None);
        let page = conversation_page_http(
            AdminListConversationsResponse {
                conversations: vec![],
                next_cursor: "c1:next".into(),
                has_more: true,
            },
            50,
        );
        assert_eq!(page.next_cursor.as_deref(), Some("c1:next"));
    }

    #[test]
    fn tenant_runtime_flattens_quota_and_converts_timestamps_to_millis() {
        let runtime = tenant_runtime_http(GetTenantRuntimeResponse {
            tenant_id: "tenant-a".into(),
            projected: true,
            name: "Tenant A".into(),
            status: TenantStatus::Active as i32,
            quota: Some(TenantQuota {
                max_users: 10,
                max_groups: 2,
                max_group_members: 500,
                core: Some(CoreQuota {
                    send_qps: 30,
                    sync_pull_qps: 40,
                    max_online_devices_per_user: 3,
                }),
            }),
            settings_json: "{}".into(),
            version: 7,
            // SystemTime → prost Timestamp，不必直接依赖 prost-types。
            projected_at: Some((UNIX_EPOCH + Duration::new(1_700_000_000, 250_000_000)).into()),
            updated_at: None,
        });
        assert_eq!(runtime.status, "active");
        assert_eq!(runtime.quota.send_qps, 30);
        assert_eq!(runtime.quota.max_online_devices_per_user, 3);
        assert_eq!(runtime.projected_at, Some(1_700_000_000_250));
        assert_eq!(runtime.updated_at, None);

        let unprojected = tenant_runtime_http(GetTenantRuntimeResponse {
            tenant_id: "tenant-b".into(),
            ..Default::default()
        });
        assert!(!unprojected.projected);
        assert_eq!(unprojected.status, "unspecified");
        assert_eq!(unprojected.quota.max_users, 0);
    }

    #[test]
    fn missing_presence_reads_as_offline() {
        let presence = user_presence_http("u1".into(), None);
        assert!(!presence.online);
        assert!(presence.devices.is_empty());
        assert_eq!(presence.last_seen_at, None);
    }

    #[test]
    fn plugin_registration_takes_tenant_and_request_id_from_the_staff_context() {
        let proto = register_plugin_request(
            "tenant-a",
            "req-9",
            AdminPluginRegisterHttpRequest {
                plugin_id: " plugin-a ".into(),
                capability_id: "rtc.call".into(),
                grpc_authority: "http://sfu:9".into(),
                declared_operations: vec!["rtc.call".into()],
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(proto.tenant_id, "tenant-a");
        assert_eq!(proto.request_id, "req-9");
        assert_eq!(proto.plugin_id, "plugin-a");
        for (plugin_id, capability_id, authority) in [
            ("", "rtc.call", "http://sfu:9"),
            ("plugin-a", " ", "http://sfu:9"),
            ("plugin-a", "rtc.call", ""),
        ] {
            let error = register_plugin_request(
                "tenant-a",
                "req-9",
                AdminPluginRegisterHttpRequest {
                    plugin_id: plugin_id.into(),
                    capability_id: capability_id.into(),
                    grpc_authority: authority.into(),
                    ..Default::default()
                },
            )
            .unwrap_err();
            assert_eq!(error.code, "ADMIN_PLUGIN_FIELD_REQUIRED");
        }
    }

    #[test]
    fn plugin_registration_requires_identifiers() {
        let request = |plugin_id: &str, capability_id: &str, operations: &[&str]| {
            AdminPluginRegisterHttpRequest {
                plugin_id: plugin_id.into(),
                capability_id: capability_id.into(),
                grpc_authority: "http://sfu:9".into(),
                declared_operations: operations.iter().map(|op| op.to_string()).collect(),
                ..Default::default()
            }
        };
        let accepted = register_plugin_request(
            "tenant-a",
            "req-9",
            request(
                "flare-moments",
                "social.moments.feed",
                &["social.moments.feed", "ns:op_1-2"],
            ),
        )
        .unwrap();
        assert_eq!(accepted.capability_id, "social.moments.feed");
        let long = "p".repeat(129);
        for (invalid, field) in [
            (
                request("p1\nWARN forged line", "rtc.call", &[]),
                "plugin_id",
            ),
            (request("plugin a", "rtc.call", &[]), "plugin_id"),
            (request(&long, "rtc.call", &[]), "plugin_id"),
            (
                request("plugin-a", "rtc.call\r\nWARN x", &[]),
                "capability_id",
            ),
            (request("plugin-a", "rtc/call", &[]), "capability_id"),
            (request("插件", "rtc.call", &[]), "plugin_id"),
            (
                request("plugin-a", "rtc.call", &["rtc.call", "rtc.call\nWARN x"]),
                "declared_operations",
            ),
            (
                request("plugin-a", "rtc.call", &["rtc.call", ""]),
                "declared_operations",
            ),
        ] {
            let error = register_plugin_request("tenant-a", "req-9", invalid).unwrap_err();
            assert_eq!(error.code, "ADMIN_PLUGIN_FIELD_INVALID");
            assert!(error.message.contains(field), "{}", error.message);
            assert!(!error.message.contains("WARN"), "{}", error.message);
        }
    }

    #[test]
    fn the_capability_catalog_is_sorted_by_capability_id() {
        let catalog = capability_catalog_http(ListCapabilitiesResponse {
            capabilities: ["rtc.call", "ai.summary"]
                .into_iter()
                .map(|id| CapabilityDescriptor {
                    capability_id: id.into(),
                    ..Default::default()
                })
                .collect(),
        });
        let ids: Vec<_> = catalog
            .capabilities
            .iter()
            .map(|c| c.capability_id.as_str())
            .collect();
        assert_eq!(ids, ["ai.summary", "rtc.call"]);
    }

    #[test]
    fn plugins_are_sorted_by_plugin_id() {
        let listed = plugins_http(
            "tenant-a".into(),
            ListRegisteredPluginsResponse {
                instances: ["p2", "p1", "p3"]
                    .into_iter()
                    .map(|id| RegisteredPluginInstance {
                        plugin_id: id.into(),
                        ..Default::default()
                    })
                    .collect(),
            },
        );
        let ids: Vec<_> = listed
            .plugins
            .iter()
            .map(|p| p.plugin_id.as_str())
            .collect();
        assert_eq!(ids, ["p1", "p2", "p3"]);
    }
}
