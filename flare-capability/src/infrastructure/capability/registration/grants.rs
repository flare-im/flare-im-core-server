//! 内存能力策略：用户授权、租户开关（无 DB 时的默认实现）。

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;

use crate::domain::capability::{
    CapabilityError, CapabilityPolicyBackend, Result, UserCapabilityGrant,
};

/// 与旧 `InMemoryCapabilityPolicyChecker` 等价的最小子集
pub struct InMemoryCapabilityGrants {
    global_enabled: AtomicBool,
    tenant_capability_switches: DashMap<(String, String), bool>,
    user_capability_grants: DashMap<(String, String, String), UserCapabilityGrant>,
}

impl Default for InMemoryCapabilityGrants {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryCapabilityGrants {
    pub fn new() -> Self {
        Self {
            global_enabled: AtomicBool::new(true),
            tenant_capability_switches: DashMap::new(),
            user_capability_grants: DashMap::new(),
        }
    }

    pub fn set_global_enabled(&self, enabled: bool) {
        self.global_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn set_tenant_capability(&self, tenant_id: &str, capability_id: &str, enabled: bool) {
        self.tenant_capability_switches
            .insert((tenant_id.to_string(), capability_id.to_string()), enabled);
    }

    pub fn grant_user_capability(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
        plan_code: Option<String>,
        source: Option<String>,
    ) {
        let grant = UserCapabilityGrant {
            tenant_id: tenant_id.to_string(),
            user_id: user_id.to_string(),
            capability_id: capability_id.to_string(),
            granted_at: Utc::now(),
            expires_at,
            plan_code,
            source,
        };
        self.user_capability_grants.insert(
            (
                tenant_id.to_string(),
                user_id.to_string(),
                capability_id.to_string(),
            ),
            grant,
        );
    }

    pub fn revoke_user_capability(&self, tenant_id: &str, user_id: &str, capability_id: &str) {
        self.user_capability_grants.remove(&(
            tenant_id.to_string(),
            user_id.to_string(),
            capability_id.to_string(),
        ));
    }

    pub fn list_user_capabilities(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Vec<UserCapabilityGrant> {
        self.user_capability_grants
            .iter()
            .filter(|e| {
                let (t, u, _) = e.key();
                t == tenant_id && u == user_id
            })
            .map(|e| e.value().clone())
            .collect()
    }

    fn has_active_capability_grant(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> bool {
        let now = Utc::now();
        let try_tenant_user = |t: &str, uid: &str| -> bool {
            if let Some(grant) = self.user_capability_grants.get(&(
                t.to_string(),
                uid.to_string(),
                capability_id.to_string(),
            )) && grant.is_active(now)
            {
                return true;
            }
            if let Some((namespace, _)) = capability_id.split_once('.') {
                let wildcard = format!("{namespace}.*");
                if let Some(grant) =
                    self.user_capability_grants
                        .get(&(t.to_string(), uid.to_string(), wildcard))
                    && grant.is_active(now)
                {
                    return true;
                }
            }
            false
        };
        let tenants: &[&str] = if tenant_id == "0" || tenant_id == "default" {
            &["0", "default"]
        } else {
            &[tenant_id]
        };
        for t in tenants {
            if try_tenant_user(t, user_id) || try_tenant_user(t, "*") {
                return true;
            }
        }
        false
    }

    /// dispatch 前校验（tenant 开关 + 用户授权）
    pub fn check_dispatch_allowed(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> Result<()> {
        if !self.global_enabled.load(Ordering::Relaxed) {
            return Err(CapabilityError::PolicyDenied(
                "global capability switch is disabled".into(),
            ));
        }
        if let Some(enabled) = self
            .tenant_capability_switches
            .get(&(tenant_id.to_string(), capability_id.to_string()))
            && !*enabled
        {
            return Err(CapabilityError::PolicyDenied(format!(
                "capability {capability_id} disabled for tenant {tenant_id}"
            )));
        }
        if !self.has_active_capability_grant(tenant_id, user_id, capability_id) {
            return Err(CapabilityError::PolicyDenied(
                "user capability grant missing or expired".into(),
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl CapabilityPolicyBackend for InMemoryCapabilityGrants {
    async fn ensure_dispatch_allowed(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> Result<()> {
        self.check_dispatch_allowed(tenant_id, user_id, capability_id)
    }

    async fn list_user_grants(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Result<Vec<UserCapabilityGrant>> {
        Ok(self.list_user_capabilities(tenant_id, user_id))
    }

    async fn grant_user_capability(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
        expires_at: Option<chrono::DateTime<chrono::Utc>>,
        plan_code: Option<String>,
        source: Option<String>,
    ) -> Result<()> {
        InMemoryCapabilityGrants::grant_user_capability(
            self,
            tenant_id,
            user_id,
            capability_id,
            expires_at,
            plan_code,
            source,
        );
        Ok(())
    }

    async fn revoke_user_capability(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> Result<()> {
        InMemoryCapabilityGrants::revoke_user_capability(self, tenant_id, user_id, capability_id);
        Ok(())
    }

    async fn set_tenant_capability(
        &self,
        tenant_id: &str,
        capability_id: &str,
        enabled: bool,
    ) -> Result<()> {
        InMemoryCapabilityGrants::set_tenant_capability(self, tenant_id, capability_id, enabled);
        Ok(())
    }
}
