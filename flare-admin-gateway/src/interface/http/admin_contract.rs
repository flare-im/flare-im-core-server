use flare_core_base::context::keys;
use serde::Serialize;

pub const APP_ID_HEADER: &str = "x-app-id";
pub const AUDIT_REASON_HEADER: &str = "x-audit-reason";
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";
/// `x-audit-reason` 的长度上限（字节，按原始头值计）。
pub const MAX_AUDIT_REASON_BYTES: usize = 512;
/// `x-request-id` 与 `idempotency-key` 的长度上限（字节，按原始头值计）。
pub const MAX_CORRELATION_ID_BYTES: usize = 128;

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

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct AdminRequiredHeaders {
    pub tenant_header: String,
    pub actor_header: String,
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
            tenant_header: keys::TENANT_ID.to_string(),
            actor_header: keys::ACTOR_ID.to_string(),
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
                status: "not_implemented".to_string(),
            },
            AdminEndpointDescriptor {
                method: "POST".to_string(),
                path: "/api/v1/admin/medias/object-acl".to_string(),
                scope: "admin_gateway:admin".to_string(),
                write: true,
                status: "not_implemented".to_string(),
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
            // ADM-01：租户只取自 admin 令牌，下游（Conversation / Online / Capability）按令牌再验一次。
            admin_endpoint("/api/v1/admin/conversations"),
            admin_endpoint("/api/v1/admin/conversations/{conversation_id}"),
            admin_endpoint("/api/v1/admin/conversations/{conversation_id}/participants"),
            admin_endpoint("/api/v1/admin/users/{user_id}/devices"),
            admin_endpoint("/api/v1/admin/users/{user_id}/presence"),
            admin_write_endpoint("/api/v1/admin/users/{user_id}/kick"),
            admin_write_endpoint("/api/v1/admin/users/{user_id}/devices/{device_id}/kick"),
            admin_write_endpoint("/api/v1/admin/tenant/kick-all"),
            admin_endpoint("/api/v1/admin/tenant/runtime"),
            admin_endpoint("/api/v1/admin/capabilities/tenant"),
            admin_endpoint("/api/v1/admin/plugins"),
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

/// 写操作：中间件要求 `x-audit-reason`、与令牌主体一致的 `x-actor-id`，以及
/// `idempotency-key` 或 `x-request-id` 之一。
fn admin_write_endpoint(path: &str) -> AdminEndpointDescriptor {
    AdminEndpointDescriptor {
        method: "POST".to_string(),
        path: path.to_string(),
        scope: "admin_gateway:admin".to_string(),
        write: true,
        status: "available".to_string(),
    }
}
