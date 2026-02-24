//! # 租户上下文中间件
//!
//! 提供租户上下文提取和验证功能。

use anyhow::Result;

use tracing::debug;

use crate::interface::middleware::auth::TokenClaims;
use flare_server_core::error::{ErrorBuilder, ErrorCode, FlareError};

/// 租户上下文中间件
pub struct TenantMiddleware;

impl TenantMiddleware {
    /// 从Token Claims提取租户上下文
    pub fn extract_from_claims(claims: &TokenClaims) -> TenantContext {
        TenantContext {
            tenant_id: claims.tenant_id.clone(),
            business_type: claims.business_type.clone(),
            environment: claims.environment.clone(),
            organization_id: claims.organization_id.clone(),
            ..Default::default()
        }
    }
    
    /// 从请求Metadata提取租户上下文（备用方法）
    pub fn extract_from_metadata(metadata: &tonic::metadata::MetadataMap) -> Option<TenantContext> {
        // 实现从Metadata提取租户上下文
        // 支持x-tenant-id header等
        let tenant_id = metadata.get("x-tenant-id")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
            
        if let Some(id) = tenant_id {
            let business_type = metadata.get("x-business-type")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
                .unwrap_or_default();
                
            let environment = metadata.get("x-environment")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
                .unwrap_or_default();
                
            let organization_id = metadata.get("x-organization-id")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.to_string())
                .unwrap_or_default();
            
            Some(TenantContext {
                tenant_id: id,
                business_type,
                environment,
                organization_id,
                ..Default::default()
            })
        } else {
            None
        }
    }
    
    /// 验证租户上下文
    pub async fn validate_tenant_context(
        tenant_context: &TenantContext,
        tenant_repository: Option<&dyn TenantRepository>,
    ) -> Result<()> {
        // 如果提供了tenant_repository，验证租户是否存在和启用
        if let Some(repo) = tenant_repository {
            if !repo.tenant_exists(&tenant_context.tenant_id).await.unwrap_or(false) {
                return Err(ErrorBuilder::new(
                    ErrorCode::InvalidTenant,
                    "Tenant does not exist",
                )
                .build_error());
            }
            
            if !repo.is_tenant_enabled(&tenant_context.tenant_id).await.unwrap_or(false) {
                return Err(ErrorBuilder::new(
                    ErrorCode::InvalidTenant,
                    "Tenant is disabled",
                )
                .build_error());
            }
        }
        
        debug!("Tenant context validated");
        Ok(())
    }
}

/// 租户仓储接口（用于验证租户）
#[async_trait::async_trait]
pub trait TenantRepository: Send + Sync {
    /// 检查租户是否存在
    async fn tenant_exists(&self, tenant_id: &str) -> Result<bool>;
    
    /// 检查租户是否启用
    async fn is_tenant_enabled(&self, tenant_id: &str) -> Result<bool>;
}