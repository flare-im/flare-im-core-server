//! Hook 治理路由（`/hooks/**`）的 HTTP 契约，以及与 `CapabilityService.Administer` 载荷
//! （`flare.capability.v1.*HookConfig*` 消息）的双向映射。
//!
//! 租户永远来自已验签的 Staff 上下文：创建与列表请求的 `tenant_id` 由网关填入，调用方给的
//! 一律忽略；按 hook_id 的操作由 Capability 先核对该配置的租户。时间统一为 Unix 毫秒；枚举
//! `HookPoint` 用 proto 的名字（如 `HOOK_POINT_MESSAGE_PRE_SEND`），未知名字拒绝。webhook
//! 的签名密钥只进不出：读回的传输配置不含 `secret`，只说明是否已配置。

use std::collections::HashMap;

use base64::Engine as _;
use flare_grpc_proto::capability::{
    CreateHookConfigRequest, DeleteHookConfigResponse, GetHookStatisticsRequest, HookConfig,
    HookExecution, HookExtensionBag, HookPoint, HookRetryPolicy, HookSelector, HookStatistics,
    HookTransport, ListHookConfigsRequest, ListHookConfigsResponse, QueryHookExecutionsRequest,
    QueryHookExecutionsResponse, SetHookStatusRequest, SetHookStatusResponse,
    UpdateHookConfigRequest,
};
use flare_proto::common::{Pagination, TimeRange};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// 列表与执行记录的页大小：缺省 100，上限 1000（Capability 同样截到 1000）。
pub const DEFAULT_HOOK_PAGE: i32 = 100;
pub const MAX_HOOK_PAGE: i32 = 1000;

/// `Administer` 的 operation 名。
pub mod operation {
    pub const CREATE: &str = "flare.extension.v1.hook_config.create";
    pub const GET: &str = "flare.extension.v1.hook_config.get";
    pub const UPDATE: &str = "flare.extension.v1.hook_config.update";
    pub const LIST: &str = "flare.extension.v1.hook_config.list";
    pub const DELETE: &str = "flare.extension.v1.hook_config.delete";
    pub const SET_STATUS: &str = "flare.extension.v1.hook_config.set_status";
    pub const GET_STATISTICS: &str = "flare.extension.v1.hook_config.get_statistics";
    pub const QUERY_EXECUTIONS: &str = "flare.extension.v1.hook_config.query_executions";
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminHookArgError {
    pub code: &'static str,
    pub message: String,
}

fn invalid(code: &'static str, message: impl Into<String>) -> AdminHookArgError {
    AdminHookArgError {
        code,
        message: message.into(),
    }
}

/// 传输配置。读回时 `secret` 不出网关：`secret_configured` 说明 Capability 持有的配置里有无签名密钥。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct AdminHookTransportHttp {
    /// `grpc` / `webhook` / `local`
    #[serde(rename = "type", default)]
    pub transport_type: String,
    #[serde(default)]
    pub service_name: String,
    #[serde(default)]
    pub endpoint: String,
    #[serde(default)]
    pub registry_type: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub load_balance: String,
    /// 只在创建与更新时接受；读回时省略。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<String>,
    /// 读回时：Capability 持有的配置是否带签名密钥。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_configured: Option<bool>,
    #[serde(default)]
    pub headers: HashMap<String, String>,
    #[serde(default)]
    pub target: String,
    /// 毫秒，0 = Capability 缺省（5000）。
    #[serde(default)]
    pub timeout_ms: i32,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct AdminHookSelectorHttp {
    #[serde(default)]
    pub tenants: Vec<String>,
    #[serde(default)]
    pub conversation_types: Vec<String>,
    #[serde(default)]
    pub message_types: Vec<String>,
    #[serde(default)]
    pub business_types: Vec<String>,
    /// `HookPoint` 名字，如 `HOOK_POINT_MESSAGE_PRE_SEND`。
    #[serde(default)]
    pub hook_points: Vec<String>,
    #[serde(default)]
    pub app_ids: Vec<String>,
    #[serde(default)]
    pub conversation_ids: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct AdminHookRetryPolicyHttp {
    #[serde(default)]
    pub max_retries: i32,
    #[serde(default)]
    pub retry_interval_ms: i32,
    /// `exponential` / `linear`
    #[serde(default)]
    pub backoff_strategy: String,
}

/// `google.protobuf.Any`：类型地址与 base64 编码的原始字节。
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct AdminProtoAnyHttp {
    pub type_url: String,
    pub value_base64: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct AdminHookExtensionBagHttp {
    #[serde(default)]
    pub contract_version: u32,
    #[serde(default)]
    pub attributes: HashMap<String, String>,
    #[serde(default)]
    pub extensions: HashMap<String, AdminProtoAnyHttp>,
}

/// 一条 Hook 配置（读回形状）。
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminHookHttp {
    pub hook_id: String,
    pub name: String,
    pub hook_type: String,
    pub tenant_id: String,
    pub priority: i32,
    pub enabled: bool,
    pub transport: Option<AdminHookTransportHttp>,
    pub selector: Option<AdminHookSelectorHttp>,
    pub retry_policy: Option<AdminHookRetryPolicyHttp>,
    /// Unix 毫秒。
    pub created_at: Option<i64>,
    pub updated_at: Option<i64>,
    pub hook_points: Vec<String>,
    pub config_extensions: Option<AdminHookExtensionBagHttp>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminHookDetailHttp {
    pub hook: AdminHookHttp,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminHooksPageHttp {
    pub hooks: Vec<AdminHookHttp>,
    /// 本页实际使用的页大小。
    pub limit: i32,
    pub has_more: bool,
    /// 过滤后的总数。
    pub total: i64,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct AdminHookListQuery {
    #[serde(default)]
    pub hook_type: Option<String>,
    #[serde(default)]
    pub enabled_only: Option<bool>,
    #[serde(default)]
    pub limit: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct AdminHookCreateHttpRequest {
    pub name: String,
    pub hook_type: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub transport: Option<AdminHookTransportHttp>,
    #[serde(default)]
    pub selector: Option<AdminHookSelectorHttp>,
    #[serde(default)]
    pub retry_policy: Option<AdminHookRetryPolicyHttp>,
    #[serde(default)]
    pub hook_points: Vec<String>,
    #[serde(default)]
    pub config_extensions: Option<AdminHookExtensionBagHttp>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct AdminHookUpdateHttpRequest {
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub priority: i32,
    #[serde(default)]
    pub transport: Option<AdminHookTransportHttp>,
    #[serde(default)]
    pub selector: Option<AdminHookSelectorHttp>,
    #[serde(default)]
    pub retry_policy: Option<AdminHookRetryPolicyHttp>,
    #[serde(default)]
    pub hook_points: Vec<String>,
    #[serde(default)]
    pub config_extensions: Option<AdminHookExtensionBagHttp>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct AdminHookStatusHttpRequest {
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminHookStatusHttp {
    pub hook_id: String,
    pub enabled: bool,
    pub updated: bool,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminHookDeleteHttp {
    pub hook_id: String,
    pub deleted: bool,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct AdminHookStatisticsQuery {
    /// Unix 毫秒；两者都缺省时 Capability 取最近 24 小时。
    #[serde(default)]
    pub start_time: Option<i64>,
    #[serde(default)]
    pub end_time: Option<i64>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminHookStatisticsHttp {
    pub hook_id: String,
    pub total_executions: i64,
    pub success_count: i64,
    pub failure_count: i64,
    pub avg_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub rate_limit_count: i64,
    pub circuit_break_count: i64,
    pub error_count_by_code: HashMap<String, i64>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, ToSchema)]
pub struct AdminHookExecutionsQuery {
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub success_only: Option<bool>,
    /// Unix 毫秒。
    #[serde(default)]
    pub start_time: Option<i64>,
    #[serde(default)]
    pub end_time: Option<i64>,
    #[serde(default)]
    pub limit: Option<i32>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminHookExecutionHttp {
    pub execution_id: String,
    pub hook_id: String,
    pub message_id: String,
    pub success: bool,
    pub latency_ms: i32,
    pub error_code: String,
    pub error_message: String,
    /// Unix 毫秒。
    pub executed_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct AdminHookExecutionsPageHttp {
    pub hook_id: String,
    pub executions: Vec<AdminHookExecutionHttp>,
    pub limit: i32,
    pub has_more: bool,
}

fn timestamp_ms(ts: &prost_types::Timestamp) -> i64 {
    ts.seconds
        .saturating_mul(1000)
        .saturating_add(i64::from(ts.nanos) / 1_000_000)
}

/// 页大小：缺省 100，小于 1 拒绝，大于 1000 截到 1000。
pub fn hook_page_limit(requested: Option<i32>) -> Result<i32, AdminHookArgError> {
    match requested {
        None => Ok(DEFAULT_HOOK_PAGE),
        Some(limit) if limit < 1 => Err(invalid(
            "ADMIN_PAGE_LIMIT_INVALID",
            format!("limit must be between 1 and {MAX_HOOK_PAGE}"),
        )),
        Some(limit) => Ok(limit.min(MAX_HOOK_PAGE)),
    }
}

fn hook_points_from_http(names: &[String]) -> Result<Vec<i32>, AdminHookArgError> {
    names
        .iter()
        .map(|name| {
            HookPoint::from_str_name(name)
                .map(|point| point as i32)
                .ok_or_else(|| {
                    invalid(
                        "ADMIN_HOOK_POINT_INVALID",
                        format!("unknown hook point {name}"),
                    )
                })
        })
        .collect()
}

fn hook_points_to_http(points: &[i32]) -> Vec<String> {
    points
        .iter()
        .map(|point| {
            HookPoint::try_from(*point)
                .map(|point| point.as_str_name().to_string())
                .unwrap_or_else(|_| format!("HOOK_POINT_{point}"))
        })
        .collect()
}

fn transport_from_http(transport: AdminHookTransportHttp) -> HookTransport {
    HookTransport {
        r#type: transport.transport_type,
        service_name: transport.service_name,
        endpoint: transport.endpoint,
        registry_type: transport.registry_type,
        namespace: transport.namespace,
        load_balance: transport.load_balance,
        secret: transport.secret.unwrap_or_default(),
        headers: transport.headers,
        target: transport.target,
        timeout_ms: transport.timeout_ms,
        metadata: transport.metadata,
    }
}

fn transport_to_http(transport: HookTransport) -> AdminHookTransportHttp {
    AdminHookTransportHttp {
        transport_type: transport.r#type,
        service_name: transport.service_name,
        endpoint: transport.endpoint,
        registry_type: transport.registry_type,
        namespace: transport.namespace,
        load_balance: transport.load_balance,
        secret: None,
        secret_configured: Some(!transport.secret.is_empty()),
        headers: transport.headers,
        target: transport.target,
        timeout_ms: transport.timeout_ms,
        metadata: transport.metadata,
    }
}

fn selector_from_http(selector: AdminHookSelectorHttp) -> Result<HookSelector, AdminHookArgError> {
    Ok(HookSelector {
        tenants: selector.tenants,
        conversation_types: selector.conversation_types,
        message_types: selector.message_types,
        business_types: selector.business_types,
        hook_points: hook_points_from_http(&selector.hook_points)?,
        app_ids: selector.app_ids,
        conversation_ids: selector.conversation_ids,
    })
}

fn selector_to_http(selector: HookSelector) -> AdminHookSelectorHttp {
    AdminHookSelectorHttp {
        tenants: selector.tenants,
        conversation_types: selector.conversation_types,
        message_types: selector.message_types,
        business_types: selector.business_types,
        hook_points: hook_points_to_http(&selector.hook_points),
        app_ids: selector.app_ids,
        conversation_ids: selector.conversation_ids,
    }
}

fn retry_policy_from_http(policy: AdminHookRetryPolicyHttp) -> HookRetryPolicy {
    HookRetryPolicy {
        max_retries: policy.max_retries,
        retry_interval_ms: policy.retry_interval_ms,
        backoff_strategy: policy.backoff_strategy,
    }
}

fn retry_policy_to_http(policy: HookRetryPolicy) -> AdminHookRetryPolicyHttp {
    AdminHookRetryPolicyHttp {
        max_retries: policy.max_retries,
        retry_interval_ms: policy.retry_interval_ms,
        backoff_strategy: policy.backoff_strategy,
    }
}

fn extensions_from_http(
    bag: AdminHookExtensionBagHttp,
) -> Result<HookExtensionBag, AdminHookArgError> {
    let mut extensions = HashMap::new();
    for (key, any) in bag.extensions {
        let value = base64::engine::general_purpose::STANDARD
            .decode(any.value_base64.as_bytes())
            .map_err(|_| {
                invalid(
                    "ADMIN_HOOK_EXTENSION_INVALID",
                    format!("extension {key} is not base64"),
                )
            })?;
        extensions.insert(
            key,
            prost_types::Any {
                type_url: any.type_url,
                value,
            },
        );
    }
    Ok(HookExtensionBag {
        contract_version: bag.contract_version,
        attributes: bag.attributes,
        extensions,
    })
}

fn extensions_to_http(bag: HookExtensionBag) -> AdminHookExtensionBagHttp {
    AdminHookExtensionBagHttp {
        contract_version: bag.contract_version,
        attributes: bag.attributes,
        extensions: bag
            .extensions
            .into_iter()
            .map(|(key, any)| {
                (
                    key,
                    AdminProtoAnyHttp {
                        type_url: any.type_url,
                        value_base64: base64::engine::general_purpose::STANDARD.encode(any.value),
                    },
                )
            })
            .collect(),
    }
}

pub fn hook_http(config: HookConfig) -> AdminHookHttp {
    AdminHookHttp {
        hook_id: config.hook_id,
        name: config.name,
        hook_type: config.hook_type,
        tenant_id: config.tenant_id,
        priority: config.priority,
        enabled: config.enabled,
        transport: config.transport.map(transport_to_http),
        selector: config.selector.map(selector_to_http),
        retry_policy: config.retry_policy.map(retry_policy_to_http),
        created_at: config.created_at.as_ref().map(timestamp_ms),
        updated_at: config.updated_at.as_ref().map(timestamp_ms),
        hook_points: hook_points_to_http(&config.hook_points),
        config_extensions: config.config_extensions.map(extensions_to_http),
    }
}

pub fn build_create_request(
    tenant_id: &str,
    request: AdminHookCreateHttpRequest,
) -> Result<CreateHookConfigRequest, AdminHookArgError> {
    if request.name.trim().is_empty() {
        return Err(invalid("ADMIN_HOOK_NAME_REQUIRED", "name is required"));
    }
    if request.hook_type.trim().is_empty() {
        return Err(invalid("ADMIN_HOOK_TYPE_REQUIRED", "hook_type is required"));
    }
    Ok(CreateHookConfigRequest {
        name: request.name,
        hook_type: request.hook_type,
        tenant_id: tenant_id.to_string(),
        priority: request.priority,
        transport: request.transport.map(transport_from_http),
        selector: request.selector.map(selector_from_http).transpose()?,
        retry_policy: request.retry_policy.map(retry_policy_from_http),
        hook_points: hook_points_from_http(&request.hook_points)?,
        config_extensions: request
            .config_extensions
            .map(extensions_from_http)
            .transpose()?,
    })
}

pub fn build_update_request(
    hook_id: &str,
    request: AdminHookUpdateHttpRequest,
) -> Result<UpdateHookConfigRequest, AdminHookArgError> {
    Ok(UpdateHookConfigRequest {
        hook_id: hook_id.to_string(),
        name: request.name,
        priority: request.priority,
        transport: request.transport.map(transport_from_http),
        selector: request.selector.map(selector_from_http).transpose()?,
        retry_policy: request.retry_policy.map(retry_policy_from_http),
        hook_points: hook_points_from_http(&request.hook_points)?,
        config_extensions: request
            .config_extensions
            .map(extensions_from_http)
            .transpose()?,
    })
}

pub fn build_list_request(
    tenant_id: &str,
    query: &AdminHookListQuery,
) -> Result<(ListHookConfigsRequest, i32), AdminHookArgError> {
    let limit = hook_page_limit(query.limit)?;
    Ok((
        ListHookConfigsRequest {
            tenant_id: tenant_id.to_string(),
            hook_type: query.hook_type.clone().unwrap_or_default(),
            enabled_only: query.enabled_only.unwrap_or(false),
            pagination: Some(Pagination {
                limit,
                ..Default::default()
            }),
        },
        limit,
    ))
}

pub fn hooks_page_http(response: ListHookConfigsResponse, limit: i32) -> AdminHooksPageHttp {
    let (has_more, total) = response
        .pagination
        .as_ref()
        .map(|pagination| (pagination.has_more, pagination.total_size))
        .unwrap_or((false, response.configs.len() as i64));
    AdminHooksPageHttp {
        hooks: response.configs.into_iter().map(hook_http).collect(),
        limit,
        has_more,
        total,
    }
}

pub fn build_status_request(
    hook_id: &str,
    request: AdminHookStatusHttpRequest,
) -> SetHookStatusRequest {
    SetHookStatusRequest {
        hook_id: hook_id.to_string(),
        enabled: request.enabled,
    }
}

pub fn status_http(
    hook_id: String,
    enabled: bool,
    response: SetHookStatusResponse,
) -> AdminHookStatusHttp {
    AdminHookStatusHttp {
        hook_id,
        enabled,
        updated: response.success,
    }
}

pub fn delete_http(hook_id: String, response: DeleteHookConfigResponse) -> AdminHookDeleteHttp {
    AdminHookDeleteHttp {
        hook_id,
        deleted: response.success,
    }
}

fn time_range(
    start: Option<i64>,
    end: Option<i64>,
) -> Result<Option<TimeRange>, AdminHookArgError> {
    if let (Some(start), Some(end)) = (start, end)
        && end < start
    {
        return Err(invalid(
            "ADMIN_HOOK_TIME_RANGE_INVALID",
            "end_time must not be earlier than start_time",
        ));
    }
    Ok((start.is_some() || end.is_some()).then_some(TimeRange {
        start_time: start,
        end_time: end,
    }))
}

pub fn build_statistics_request(
    hook_id: &str,
    query: &AdminHookStatisticsQuery,
) -> Result<GetHookStatisticsRequest, AdminHookArgError> {
    Ok(GetHookStatisticsRequest {
        hook_id: hook_id.to_string(),
        time_range: time_range(query.start_time, query.end_time)?,
    })
}

pub fn statistics_http(
    hook_id: String,
    statistics: Option<HookStatistics>,
) -> AdminHookStatisticsHttp {
    let statistics = statistics.unwrap_or_default();
    AdminHookStatisticsHttp {
        hook_id,
        total_executions: statistics.total_executions,
        success_count: statistics.success_count,
        failure_count: statistics.failure_count,
        avg_latency_ms: statistics.avg_latency_ms,
        p99_latency_ms: statistics.p99_latency_ms,
        rate_limit_count: statistics.rate_limit_count,
        circuit_break_count: statistics.circuit_break_count,
        error_count_by_code: statistics.error_count_by_code,
    }
}

pub fn build_executions_request(
    hook_id: &str,
    query: &AdminHookExecutionsQuery,
) -> Result<(QueryHookExecutionsRequest, i32), AdminHookArgError> {
    let limit = hook_page_limit(query.limit)?;
    Ok((
        QueryHookExecutionsRequest {
            hook_id: hook_id.to_string(),
            message_id: query.message_id.clone().unwrap_or_default(),
            success_only: query.success_only.unwrap_or(false),
            time_range: time_range(query.start_time, query.end_time)?,
            pagination: Some(Pagination {
                limit,
                ..Default::default()
            }),
        },
        limit,
    ))
}

fn execution_http(execution: HookExecution) -> AdminHookExecutionHttp {
    AdminHookExecutionHttp {
        execution_id: execution.execution_id,
        hook_id: execution.hook_id,
        message_id: execution.message_id,
        success: execution.success,
        latency_ms: execution.latency_ms,
        error_code: execution.error_code,
        error_message: execution.error_message,
        executed_at: execution.executed_at.as_ref().map(timestamp_ms),
    }
}

pub fn executions_page_http(
    hook_id: String,
    response: QueryHookExecutionsResponse,
    limit: i32,
) -> AdminHookExecutionsPageHttp {
    let has_more = response
        .pagination
        .as_ref()
        .is_some_and(|pagination| pagination.has_more);
    AdminHookExecutionsPageHttp {
        hook_id,
        executions: response
            .executions
            .into_iter()
            .map(execution_http)
            .collect(),
        limit,
        has_more,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_requests_take_the_tenant_from_the_caller_and_validate_names() {
        let request = AdminHookCreateHttpRequest {
            name: "dlp".into(),
            hook_type: "message_pre_send".into(),
            priority: 10,
            transport: Some(AdminHookTransportHttp {
                transport_type: "webhook".into(),
                endpoint: "https://hooks.example/dlp".into(),
                secret: Some("s3cret".into()),
                ..Default::default()
            }),
            selector: Some(AdminHookSelectorHttp {
                hook_points: vec!["HOOK_POINT_MESSAGE_PRE_SEND".into()],
                ..Default::default()
            }),
            retry_policy: None,
            hook_points: vec!["HOOK_POINT_MESSAGE_PRE_SEND".into()],
            config_extensions: Some(AdminHookExtensionBagHttp {
                contract_version: 1,
                attributes: HashMap::from([("k".to_string(), "v".to_string())]),
                extensions: HashMap::from([(
                    "x".to_string(),
                    AdminProtoAnyHttp {
                        type_url: "type.googleapis.com/google.protobuf.Empty".into(),
                        value_base64: "AQI=".into(),
                    },
                )]),
            }),
        };
        let proto = build_create_request("tenant-a", request).unwrap();
        assert_eq!(proto.tenant_id, "tenant-a");
        assert_eq!(proto.hook_points, vec![HookPoint::MessagePreSend as i32]);
        assert_eq!(proto.transport.as_ref().unwrap().secret, "s3cret");
        assert_eq!(
            proto.config_extensions.as_ref().unwrap().extensions["x"].value,
            vec![1, 2]
        );

        let unnamed = AdminHookCreateHttpRequest {
            name: " ".into(),
            hook_type: "message_pre_send".into(),
            priority: 0,
            transport: None,
            selector: None,
            retry_policy: None,
            hook_points: Vec::new(),
            config_extensions: None,
        };
        assert_eq!(
            build_create_request("tenant-a", unnamed).unwrap_err().code,
            "ADMIN_HOOK_NAME_REQUIRED"
        );
        let bad_point = AdminHookCreateHttpRequest {
            name: "dlp".into(),
            hook_type: "message_pre_send".into(),
            priority: 0,
            transport: None,
            selector: None,
            retry_policy: None,
            hook_points: vec!["pre_send".into()],
            config_extensions: None,
        };
        assert_eq!(
            build_create_request("tenant-a", bad_point)
                .unwrap_err()
                .code,
            "ADMIN_HOOK_POINT_INVALID"
        );
    }

    #[test]
    fn read_back_hooks_hide_the_secret_and_use_millis() {
        let hook = hook_http(HookConfig {
            hook_id: "7".into(),
            name: "dlp".into(),
            hook_type: "message_pre_send".into(),
            tenant_id: "tenant-a".into(),
            priority: 10,
            enabled: true,
            transport: Some(HookTransport {
                r#type: "webhook".into(),
                secret: "s3cret".into(),
                ..Default::default()
            }),
            selector: None,
            retry_policy: None,
            created_at: Some(prost_types::Timestamp {
                seconds: 1_700_000_000,
                nanos: 250_000_000,
            }),
            updated_at: None,
            hook_points: vec![HookPoint::MessagePreSend as i32, 999],
            config_extensions: None,
        });
        let transport = hook.transport.unwrap();
        assert_eq!(transport.secret, None);
        assert_eq!(transport.secret_configured, Some(true));
        assert_eq!(hook.created_at, Some(1_700_000_000_250));
        assert_eq!(
            hook.hook_points,
            vec!["HOOK_POINT_MESSAGE_PRE_SEND", "HOOK_POINT_999"]
        );
        let json = serde_json::to_string(&transport).unwrap();
        assert!(!json.contains("s3cret"));
        assert!(!json.contains("\"secret\""));
    }

    #[test]
    fn paging_and_time_ranges_are_bounded() {
        assert_eq!(hook_page_limit(None), Ok(100));
        assert_eq!(hook_page_limit(Some(5000)), Ok(1000));
        assert_eq!(
            hook_page_limit(Some(0)).unwrap_err().code,
            "ADMIN_PAGE_LIMIT_INVALID"
        );
        let (list, limit) = build_list_request(
            "tenant-a",
            &AdminHookListQuery {
                hook_type: Some("message_pre_send".into()),
                enabled_only: Some(true),
                limit: Some(20),
            },
        )
        .unwrap();
        assert_eq!(limit, 20);
        assert_eq!(list.tenant_id, "tenant-a");
        assert!(list.enabled_only);
        assert_eq!(list.pagination.unwrap().limit, 20);
        assert_eq!(
            build_statistics_request(
                "7",
                &AdminHookStatisticsQuery {
                    start_time: Some(20),
                    end_time: Some(10),
                }
            )
            .unwrap_err()
            .code,
            "ADMIN_HOOK_TIME_RANGE_INVALID"
        );
        let statistics =
            build_statistics_request("7", &AdminHookStatisticsQuery::default()).unwrap();
        assert!(statistics.time_range.is_none());
        let (executions, _) = build_executions_request(
            "7",
            &AdminHookExecutionsQuery {
                start_time: Some(10),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(
            executions.time_range.unwrap(),
            TimeRange {
                start_time: Some(10),
                end_time: None
            }
        );
    }
}
