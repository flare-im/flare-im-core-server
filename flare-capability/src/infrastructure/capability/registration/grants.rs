//! 内存能力策略：用户授权、租户开关（无 DB 时的默认实现）。

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use flare_im_contracts::utils::normalize_tenant_id;

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
        let tenant_id = normalize_tenant_id(tenant_id);
        self.tenant_capability_switches
            .insert((tenant_id, capability_id.to_string()), enabled);
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
        let tenant_id = normalize_tenant_id(tenant_id);
        let grant = UserCapabilityGrant {
            tenant_id: tenant_id.clone(),
            user_id: user_id.to_string(),
            capability_id: capability_id.to_string(),
            granted_at: Utc::now(),
            expires_at,
            plan_code,
            source,
        };
        self.user_capability_grants.insert(
            (tenant_id, user_id.to_string(), capability_id.to_string()),
            grant,
        );
    }

    pub fn revoke_user_capability(&self, tenant_id: &str, user_id: &str, capability_id: &str) {
        let tenant_id = normalize_tenant_id(tenant_id);
        self.user_capability_grants.remove(&(
            tenant_id,
            user_id.to_string(),
            capability_id.to_string(),
        ));
    }

    pub fn list_user_capabilities(
        &self,
        tenant_id: &str,
        user_id: &str,
    ) -> Vec<UserCapabilityGrant> {
        let tenant_id = normalize_tenant_id(tenant_id);
        self.user_capability_grants
            .iter()
            .filter(|e| {
                let (t, u, _) = e.key();
                t == &tenant_id && u == user_id
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
        let tenant_id = normalize_tenant_id(tenant_id);
        if try_tenant_user(&tenant_id, user_id) || try_tenant_user(&tenant_id, "*") {
            return true;
        }
        false
    }

    /// 租户级校验：只看「装没装」。
    ///
    /// 与按人那条的关键差别：**开关缺失即拒**。旧语义里「不设开关」意味着放行，
    /// 那是为了不打断既有部署；而租户模型是新增的，从第一天就该表达
    /// 「没装 = 不能用」，否则「安装」这个动作就没有意义。
    pub fn check_tenant_enabled(&self, tenant_id: &str, capability_id: &str) -> Result<()> {
        let tenant_id = normalize_tenant_id(tenant_id);
        if !self.global_enabled.load(Ordering::Relaxed) {
            return Err(CapabilityError::PolicyDenied(
                "global capability switch is disabled".into(),
            ));
        }
        match self
            .tenant_capability_switches
            .get(&(tenant_id.clone(), capability_id.to_string()))
            .map(|v| *v)
        {
            Some(true) => Ok(()),
            Some(false) => Err(CapabilityError::PolicyDenied(format!(
                "capability {capability_id} is disabled for tenant {tenant_id}"
            ))),
            None => Err(CapabilityError::PolicyDenied(format!(
                "capability {capability_id} is not installed for tenant {tenant_id}"
            ))),
        }
    }

    /// dispatch 前校验（tenant 开关 + 用户授权）
    pub fn check_dispatch_allowed(
        &self,
        tenant_id: &str,
        user_id: &str,
        capability_id: &str,
    ) -> Result<()> {
        let tenant_id = normalize_tenant_id(tenant_id);
        if !self.global_enabled.load(Ordering::Relaxed) {
            return Err(CapabilityError::PolicyDenied(
                "global capability switch is disabled".into(),
            ));
        }
        if let Some(enabled) = self
            .tenant_capability_switches
            .get(&(tenant_id.clone(), capability_id.to_string()))
            && !*enabled
        {
            return Err(CapabilityError::PolicyDenied(format!(
                "capability {capability_id} disabled for tenant {tenant_id}"
            )));
        }
        if !self.has_active_capability_grant(&tenant_id, user_id, capability_id) {
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

    async fn ensure_tenant_capability_enabled(
        &self,
        tenant_id: &str,
        capability_id: &str,
    ) -> Result<()> {
        self.check_tenant_enabled(tenant_id, capability_id)
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

#[cfg(test)]
mod seat_model_tests {
    use super::InMemoryCapabilityGrants;

    const CAP: &str = "social.moments.feed";

    /// 租户模型：**开关缺失即拒**。
    ///
    /// 这是与旧语义最关键的差别。旧语义里「不设开关」意味着放行——那是为了
    /// 不打断既有部署；而租户模型的整个意义就是「安装」这个动作要有效力，
    /// 不设即未安装。
    #[test]
    fn tenant_scope_denies_when_never_installed() {
        let g = InMemoryCapabilityGrants::new();
        assert!(g.check_tenant_enabled("0", CAP).is_err(), "没装就该拒");
    }

    #[test]
    fn tenant_scope_allows_after_install() {
        let g = InMemoryCapabilityGrants::new();
        g.set_tenant_capability("0", CAP, true);
        assert!(g.check_tenant_enabled("0", CAP).is_ok(), "装了就该放行");
    }

    /// 停用与从未安装都拒，但**理由不同** —— 运维排查时这两者的动作完全不一样：
    /// 前者去问谁停用了，后者去装。
    #[test]
    fn disabled_and_never_installed_report_different_reasons() {
        let g = InMemoryCapabilityGrants::new();
        let never = g.check_tenant_enabled("0", CAP).unwrap_err().to_string();
        g.set_tenant_capability("0", CAP, false);
        let disabled = g.check_tenant_enabled("0", CAP).unwrap_err().to_string();

        assert!(never.contains("not installed"), "实际: {never}");
        assert!(disabled.contains("disabled"), "实际: {disabled}");
        assert_ne!(never, disabled, "两种拒绝必须能区分");
    }

    /// 租户模型下**不看用户授权** —— 装了就全员可用。
    #[test]
    fn tenant_scope_ignores_user_grants() {
        let g = InMemoryCapabilityGrants::new();
        g.set_tenant_capability("0", CAP, true);
        // 谁都没授权，但租户装了 —— 任意用户都该放行。
        assert!(g.check_tenant_enabled("0", CAP).is_ok());
        // 而按人那条在同样状态下仍然拒 —— 两条语义确实是分开的。
        assert!(g.check_dispatch_allowed("0", "anyone", CAP).is_err());
    }

    /// 全局开关高于一切：关掉后连装过的租户也拒。
    #[test]
    fn global_switch_overrides_tenant_install() {
        let g = InMemoryCapabilityGrants::new();
        g.set_tenant_capability("0", CAP, true);
        g.set_global_enabled(false);
        assert!(g.check_tenant_enabled("0", CAP).is_err());
    }
}
