use flare_core_base::context::keys;
use serde::Serialize;

pub const APP_ID_HEADER: &str = "x-app-id";
pub const AUDIT_REASON_HEADER: &str = "x-audit-reason";
pub const IDEMPOTENCY_KEY_HEADER: &str = "idempotency-key";

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
pub struct AdminCapabilitiesResponse {
    pub service: String,
    pub boundary: String,
    pub required_scopes: Vec<String>,
    pub required_headers: AdminRequiredHeaders,
    pub endpoints: Vec<AdminEndpointDescriptor>,
    pub business_owned: Vec<String>,
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

pub fn build_admin_capabilities() -> AdminCapabilitiesResponse {
    AdminCapabilitiesResponse {
        service: "flare-admin-gateway".to_string(),
        boundary: "internal_admin_api_only_no_admin_console".to_string(),
        required_scopes: vec![
            "admin_gateway:admin".to_string(),
            "admin_gateway:admin:*".to_string(),
            "core_gateway:admin".to_string(),
            "core_gateway:admin:*".to_string(),
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
        ],
        business_owned: vec![
            "admin_console_and_backend".to_string(),
            "business_roles_and_approval_flows".to_string(),
            "operator_identity_lifecycle".to_string(),
        ],
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
