use axum::http::HeaderMap;
use flare_core_base::context::keys;
use flare_server_core::grpc::middleware::signed_context::SIGNED_CONTEXT_HEADER;
use serde::Serialize;

pub const APP_ID_HEADER: &str = "x-app-id";
pub const AUDIT_REASON_HEADER: &str = "x-audit-reason";
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
/// `x-audit-reason` 的长度上限（字节，按解码后的 UTF-8 计）。
pub const MAX_AUDIT_REASON_BYTES: usize = 512;
/// `x-audit-reason` 原始头值的长度上限（字节）：百分号编码后一个字节至多占三个字符。
pub const MAX_AUDIT_REASON_HEADER_BYTES: usize = 3 * MAX_AUDIT_REASON_BYTES;
/// `x-request-id` 与 `idempotency-key` 的长度上限（字节，按原始头值计）。
pub const MAX_CORRELATION_ID_BYTES: usize = 128;

/// `x-audit-reason` 不合规的两种情形，各对应一个 400 原因。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditReasonError {
    /// 重复的头、非可见 ASCII 的原始值、坏的百分号编码（`%` 后不是两位十六进制）或解码后
    /// 不是 UTF-8：`ADMIN_AUDIT_REASON_INVALID`。
    Invalid,
    /// 原始值超过 [`MAX_AUDIT_REASON_HEADER_BYTES`]，或解码后超过 [`MAX_AUDIT_REASON_BYTES`]：
    /// `ADMIN_AUDIT_HEADER_TOO_LONG`。
    TooLong,
}

/// 操作员的审计原因。线上格式是百分号编码的 UTF-8（控制台用 `encodeURIComponent` 编码，
/// 头值因此是可见 ASCII，中文等非 ASCII 原因也能经浏览器与 Social 网关原样到达）；不含 `%`
/// 的纯 ASCII 原因解码后不变。规则与 Social 网关 admin 分区一致：恰好一个头，原始值至多
/// [`MAX_AUDIT_REASON_HEADER_BYTES`] 字节，解码后是合法 UTF-8 且至多 [`MAX_AUDIT_REASON_BYTES`]
/// 字节。去掉首尾空白后为空与没带一样，返回 `None`；否则返回去掉首尾空白的解码值，审计日志
/// 与下游（Online 的踢线原因）用的都是它。
pub fn audit_reason(headers: &HeaderMap) -> Result<Option<String>, AuditReasonError> {
    let mut values = headers.get_all(AUDIT_REASON_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(AuditReasonError::Invalid);
    }
    if value.len() > MAX_AUDIT_REASON_HEADER_BYTES {
        return Err(AuditReasonError::TooLong);
    }
    let raw = value.to_str().map_err(|_| AuditReasonError::Invalid)?;
    let decoded = percent_decode(raw).ok_or(AuditReasonError::Invalid)?;
    let decoded = String::from_utf8(decoded).map_err(|_| AuditReasonError::Invalid)?;
    if decoded.len() > MAX_AUDIT_REASON_BYTES {
        return Err(AuditReasonError::TooLong);
    }
    let reason = decoded.trim();
    Ok((!reason.is_empty()).then(|| reason.to_owned()))
}

/// 百分号解码：`%XX`（十六进制，大小写均可）还原为一个字节，其余字符原样保留（`+` 不是
/// 空格，与 `encodeURIComponent` 对应）；`%` 后不足两位十六进制时为 `None`。
fn percent_decode(raw: &str) -> Option<Vec<u8>> {
    let bytes = raw.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let hex = bytes.get(index + 1..index + 3)?;
            let hex = std::str::from_utf8(hex).ok()?;
            if !hex.bytes().all(|b| b.is_ascii_hexdigit()) {
                return None;
            }
            decoded.push(u8::from_str_radix(hex, 16).ok()?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    Some(decoded)
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct AdminCapabilitiesResponse {
    pub service: String,
    pub boundary: String,
    pub required_scopes: Vec<String>,
    pub required_headers: AdminRequiredHeaders,
    pub endpoints: Vec<AdminEndpointDescriptor>,
    pub organization_policy: AdminOrganizationPolicyDescriptor,
    pub data_residency_policy: AdminDataResidencyPolicyDescriptor,
    pub retention_legal_policy: AdminRetentionLegalPolicyDescriptor,
}

/// The headers the admin boundary reads. Identity is the signed staff context only; the
/// three audit headers are required on writes (reason plus one of the two correlation keys).
#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct AdminRequiredHeaders {
    pub staff_context_header: String,
    pub audit_reason_header: String,
    pub request_id_header: String,
    pub idempotency_key_header: String,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct AdminEndpointDescriptor {
    pub method: String,
    pub path: String,
    pub scope: String,
    pub write: bool,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EnterprisePolicyStatus {
    CoreGuardrailsAvailable,
    ExternalPolicyRequired,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct AdminOrganizationPolicyDescriptor {
    pub status: EnterprisePolicyStatus,
    pub authority: EnterprisePolicyAuthority,
    pub required_role_sources: Vec<OrganizationRoleSource>,
    pub protected_operations: Vec<EnterpriseProtectedOperation>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct AdminDataResidencyPolicyDescriptor {
    pub status: EnterprisePolicyStatus,
    pub authority: EnterprisePolicyAuthority,
    pub tenant_routing_key: String,
    pub protected_operations: Vec<EnterpriseProtectedOperation>,
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct AdminRetentionLegalPolicyDescriptor {
    pub status: EnterprisePolicyStatus,
    pub authority: EnterprisePolicyAuthority,
    pub enforcement_anchors: Vec<RetentionEnforcementAnchor>,
    pub protected_operations: Vec<EnterpriseProtectedOperation>,
}

// 变体同以 Provider 结尾是刻意的：它们是对外 API 契约名，
// 经 serde 序列化并进入 OpenAPI schema，改名等于破坏已发布接口。
#[allow(clippy::enum_variant_names)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EnterprisePolicyAuthority {
    BusinessAdminIdentityProvider,
    TenantRegionPolicyProvider,
    EnterpriseRetentionPolicyProvider,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum OrganizationRoleSource {
    OperatorIdentityLifecycle,
    BusinessRolesAndApprovalFlows,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum EnterpriseProtectedOperation {
    AdminMessageExport,
    CapabilityPolicyMutation,
    TenantPolicyMutation,
    MessageStorageQuery,
    MessageExport,
    MediaObjectAccess,
    MessageRecall,
    RetentionPurge,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RetentionEnforcementAnchor {
    MessageRetentionEvents,
    ComplianceHooks,
    CapabilityAuditLog,
}

pub fn build_admin_capabilities() -> AdminCapabilitiesResponse {
    AdminCapabilitiesResponse {
        service: "flare-admin-gateway".to_string(),
        boundary: "internal_admin_api_only_no_admin_console".to_string(),
        required_scopes: vec![
            "admin_gateway:admin".to_string(),
            "admin_gateway:admin:*".to_string(),
        ],
        required_headers: AdminRequiredHeaders {
            staff_context_header: SIGNED_CONTEXT_HEADER.to_string(),
            audit_reason_header: AUDIT_REASON_HEADER.to_string(),
            request_id_header: keys::REQUEST_ID.to_string(),
            idempotency_key_header: IDEMPOTENCY_KEY_HEADER.to_string(),
        },
        endpoints: vec![
            admin_endpoint("/api/v1/admin/auth/check"),
            admin_endpoint("/api/v1/admin/capabilities"),
            admin_endpoint("/api/v1/admin/gateway/health"),
            admin_endpoint("/api/v1/admin/gateway/upstreams"),
            admin_endpoint("/api/v1/admin/gateway/routes"),
            admin_endpoint("/api/v1/admin/gateway/config"),
            admin_endpoint("/api/v1/admin/medias/bucket"),
            AdminEndpointDescriptor {
                method: "POST".to_string(),
                path: "/api/v1/admin/medias/cleanup-orphaned-assets".to_string(),
                scope: "admin_gateway:admin".to_string(),
                write: true,
                status: "available".to_string(),
            },
            AdminEndpointDescriptor {
                method: "GET".to_string(),
                path: "/api/v1/admin/medias/objects".to_string(),
                scope: "admin_gateway:admin".to_string(),
                write: false,
                status: "available".to_string(),
            },
            AdminEndpointDescriptor {
                method: "POST".to_string(),
                path: "/api/v1/admin/medias/object-acl".to_string(),
                scope: "admin_gateway:admin".to_string(),
                write: true,
                status: "available".to_string(),
            },
            AdminEndpointDescriptor {
                method: "POST".to_string(),
                path: "/api/v1/admin/messages/query".to_string(),
                scope: "admin_gateway:admin".to_string(),
                write: false,
                status: "available".to_string(),
            },
            admin_endpoint("/api/v1/admin/messages/{message_id}"),
            admin_endpoint("/api/v1/admin/messages/{message_id}/events"),
            AdminEndpointDescriptor {
                method: "POST".to_string(),
                path: "/api/v1/admin/messages/write-ledger/query".to_string(),
                scope: "admin_gateway:admin".to_string(),
                write: false,
                status: "available".to_string(),
            },
            AdminEndpointDescriptor {
                method: "POST".to_string(),
                path: "/api/v1/admin/messages/export".to_string(),
                scope: "admin_gateway:admin".to_string(),
                write: true,
                status: "available".to_string(),
            },
            // ADM-01：租户只取自 Staff 上下文，下游（Conversation / Online / Capability）按签名再验一次。
            admin_endpoint("/api/v1/admin/conversations"),
            admin_endpoint("/api/v1/admin/conversations/{conversation_id}"),
            admin_endpoint("/api/v1/admin/conversations/{conversation_id}/participants"),
            admin_endpoint("/api/v1/admin/users/{user_id}/devices"),
            admin_endpoint("/api/v1/admin/users/{user_id}/presence"),
            admin_write_endpoint("POST", "/api/v1/admin/users/{user_id}/kick"),
            admin_write_endpoint(
                "POST",
                "/api/v1/admin/users/{user_id}/devices/{device_id}/kick",
            ),
            admin_write_endpoint("POST", "/api/v1/admin/tenant/kick-all"),
            admin_endpoint("/api/v1/admin/tenant/runtime"),
            admin_endpoint("/api/v1/admin/capabilities/catalog"),
            admin_endpoint("/api/v1/admin/capabilities/tenant"),
            admin_write_endpoint("PUT", "/api/v1/admin/capabilities/tenant/{capability_id}"),
            admin_endpoint("/api/v1/admin/plugins"),
            admin_write_endpoint("POST", "/api/v1/admin/plugins/register"),
            admin_write_endpoint("POST", "/api/v1/admin/plugins/deregister"),
            admin_endpoint("/api/v1/admin/hooks"),
            admin_write_endpoint("POST", "/api/v1/admin/hooks"),
            admin_endpoint("/api/v1/admin/hooks/{hook_id}"),
            admin_write_endpoint("PUT", "/api/v1/admin/hooks/{hook_id}"),
            admin_write_endpoint("DELETE", "/api/v1/admin/hooks/{hook_id}"),
            admin_write_endpoint("PUT", "/api/v1/admin/hooks/{hook_id}/status"),
            admin_endpoint("/api/v1/admin/hooks/{hook_id}/statistics"),
            admin_endpoint("/api/v1/admin/hooks/{hook_id}/executions"),
            // Plugin admin pages: permission `plugin:<plugin_id>:admin`; the query POST is a read.
            AdminEndpointDescriptor {
                method: "POST".to_string(),
                path: "/api/v1/admin/plugin-admin/{plugin_id}/query".to_string(),
                scope: "admin_gateway:admin".to_string(),
                write: false,
                status: "available".to_string(),
            },
            admin_write_endpoint("POST", "/api/v1/admin/plugin-admin/{plugin_id}/dispatch"),
        ],
        organization_policy: AdminOrganizationPolicyDescriptor {
            status: EnterprisePolicyStatus::ExternalPolicyRequired,
            authority: EnterprisePolicyAuthority::BusinessAdminIdentityProvider,
            required_role_sources: vec![
                OrganizationRoleSource::OperatorIdentityLifecycle,
                OrganizationRoleSource::BusinessRolesAndApprovalFlows,
            ],
            protected_operations: vec![
                EnterpriseProtectedOperation::AdminMessageExport,
                EnterpriseProtectedOperation::CapabilityPolicyMutation,
                EnterpriseProtectedOperation::TenantPolicyMutation,
            ],
        },
        data_residency_policy: AdminDataResidencyPolicyDescriptor {
            status: EnterprisePolicyStatus::ExternalPolicyRequired,
            authority: EnterprisePolicyAuthority::TenantRegionPolicyProvider,
            tenant_routing_key: keys::TENANT_ID.to_string(),
            protected_operations: vec![
                EnterpriseProtectedOperation::MessageStorageQuery,
                EnterpriseProtectedOperation::MessageExport,
                EnterpriseProtectedOperation::MediaObjectAccess,
            ],
        },
        retention_legal_policy: AdminRetentionLegalPolicyDescriptor {
            status: EnterprisePolicyStatus::CoreGuardrailsAvailable,
            authority: EnterprisePolicyAuthority::EnterpriseRetentionPolicyProvider,
            enforcement_anchors: vec![
                RetentionEnforcementAnchor::MessageRetentionEvents,
                RetentionEnforcementAnchor::ComplianceHooks,
                RetentionEnforcementAnchor::CapabilityAuditLog,
            ],
            protected_operations: vec![
                EnterpriseProtectedOperation::MessageRecall,
                EnterpriseProtectedOperation::MessageExport,
                EnterpriseProtectedOperation::RetentionPurge,
            ],
        },
    }
}

fn admin_endpoint(path: &str) -> AdminEndpointDescriptor {
    AdminEndpointDescriptor {
        method: "GET".to_string(),
        path: path.to_string(),
        scope: "admin_gateway:admin".to_string(),
        write: false,
        status: "available".to_string(),
    }
}

/// 写操作：中间件要求 `x-audit-reason`，以及 `idempotency-key` 或 `x-request-id` 之一；写路由的
/// 集合与写标记来自共享路由表 `core_admin::ROUTES`。
fn admin_write_endpoint(method: &str, path: &str) -> AdminEndpointDescriptor {
    AdminEndpointDescriptor {
        method: method.to_string(),
        path: path.to_string(),
        scope: "admin_gateway:admin".to_string(),
        write: true,
        status: "available".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn reason_of(values: &[&[u8]]) -> Result<Option<String>, AuditReasonError> {
        let mut headers = HeaderMap::new();
        for value in values {
            headers.append(AUDIT_REASON_HEADER, HeaderValue::from_bytes(value).unwrap());
        }
        audit_reason(&headers)
    }

    #[test]
    fn the_audit_reason_is_percent_encoded_utf8() {
        assert_eq!(reason_of(&[]), Ok(None));
        // encodeURIComponent("清理违规 spam") and plain ASCII, which passes unchanged.
        assert_eq!(
            reason_of(&[b"%E6%B8%85%E7%90%86%E8%BF%9D%E8%A7%84%20spam"]),
            Ok(Some("清理违规 spam".to_owned()))
        );
        assert_eq!(
            reason_of(&[b"incident-42 containment"]),
            Ok(Some("incident-42 containment".to_owned()))
        );
        // Lower-case hex, `+` kept as `+`, surrounding blanks trimmed.
        assert_eq!(
            reason_of(&[b"%20caf%c3%a9+1%20"]),
            Ok(Some("café+1".to_owned()))
        );
        // Blank once decoded is the same as no reason.
        assert_eq!(reason_of(&[b"%20%09%20"]), Ok(None));
    }

    #[test]
    fn malformed_repeated_and_over_long_reasons_are_refused() {
        for malformed in [
            &b"%ZZ"[..],
            b"50%",
            b"%4",
            b"%E6%B8",
            b"%FF%FE",
            b"caf\xc3\xa9",
        ] {
            assert_eq!(
                reason_of(&[malformed]),
                Err(AuditReasonError::Invalid),
                "{malformed:?}"
            );
        }
        assert_eq!(
            reason_of(&[b"first", b"second"]),
            Err(AuditReasonError::Invalid)
        );

        // 512 decoded bytes pass, 513 do not, both within the 1536-byte raw bound.
        let at_limit = "%E4%B8%AD".repeat(170) + "ab";
        assert!(at_limit.len() <= MAX_AUDIT_REASON_HEADER_BYTES);
        assert_eq!(
            reason_of(&[at_limit.as_bytes()]).unwrap().unwrap().len(),
            512
        );
        let over = "%E4%B8%AD".repeat(170) + "abc";
        assert!(over.len() <= MAX_AUDIT_REASON_HEADER_BYTES);
        assert_eq!(
            reason_of(&[over.as_bytes()]),
            Err(AuditReasonError::TooLong)
        );
        // A raw value over 1536 bytes is refused before it is decoded.
        let raw_over = "%41".repeat(MAX_AUDIT_REASON_HEADER_BYTES / 3) + "A";
        assert_eq!(
            reason_of(&[raw_over.as_bytes()]),
            Err(AuditReasonError::TooLong)
        );
    }
}
