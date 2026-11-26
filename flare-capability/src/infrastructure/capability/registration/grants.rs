//! 内存能力策略：租户开关（无 DB 时的默认实现）。

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use dashmap::DashMap;
use flare_im_core_contracts::utils::normalize_tenant_id;

use crate::domain::capability::{
    CapabilityError, CapabilityPolicyBackend, Result, TenantCapabilitySwitch,
};

/// 与旧 `InMemoryCapabilityPolicyChecker` 等价的最小子集
pub struct InMemoryCapabilityGrants {
    global_enabled: AtomicBool,
    tenant_capability_switches: DashMap<(String, String), bool>,
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

    /// 租户级校验：只看「装没装」。
    ///
    /// **开关缺失即拒**：「没装 = 不能用」，否则「安装」这个动作就没有意义。
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

    /// 紧急停用：全局开关关闭，或该能力的租户开关存在且为关闭，或覆盖它的 `<前缀>.*` 租户开关
    /// （如 `example.poll.*` 停用整个插件）存在且为关闭。缺开关不算停用（插件操作按
    /// 安装投影授权，见 `CapabilityPolicyBackend::ensure_not_suspended`）。
    pub fn check_not_suspended(&self, tenant_id: &str, capability_id: &str) -> Result<()> {
        let tenant_id = normalize_tenant_id(tenant_id);
        if !self.global_enabled.load(Ordering::Relaxed) {
            return Err(CapabilityError::PolicyDenied(
                "global capability switch is disabled".into(),
            ));
        }
        let stopped = self.tenant_capability_switches.iter().any(|entry| {
            let ((tenant, key), enabled) = entry.pair();
            !*enabled
                && *tenant == tenant_id
                && (key == capability_id
                    || key
                        .strip_suffix('*')
                        .filter(|prefix| prefix.len() > 1 && prefix.ends_with('.'))
                        .is_some_and(|prefix| capability_id.starts_with(prefix)))
        });
        if stopped {
            return Err(CapabilityError::PolicyDenied(format!(
                "capability {capability_id} disabled for tenant {tenant_id}"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl CapabilityPolicyBackend for InMemoryCapabilityGrants {
    async fn ensure_not_suspended(&self, tenant_id: &str, capability_id: &str) -> Result<()> {
        self.check_not_suspended(tenant_id, capability_id)
    }

    async fn ensure_tenant_capability_enabled(
        &self,
        tenant_id: &str,
        capability_id: &str,
    ) -> Result<()> {
        self.check_tenant_enabled(tenant_id, capability_id)
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

    async fn list_tenant_switches(&self, tenant_id: &str) -> Result<Vec<TenantCapabilitySwitch>> {
        let tenant_id = normalize_tenant_id(tenant_id);
        let mut switches: Vec<TenantCapabilitySwitch> = self
            .tenant_capability_switches
            .iter()
            .filter(|entry| entry.key().0 == tenant_id)
            .map(|entry| TenantCapabilitySwitch {
                tenant_id: tenant_id.clone(),
                capability_id: entry.key().1.clone(),
                enabled: *entry.value(),
                updated_at: None,
            })
            .collect();
        switches.sort_by(|a, b| a.capability_id.cmp(&b.capability_id));
        Ok(switches)
    }
}

#[cfg(test)]
mod tenant_switch_tests {
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

    /// 全局开关高于一切：关掉后连装过的租户也拒。
    #[test]
    fn global_switch_overrides_tenant_install() {
        let g = InMemoryCapabilityGrants::new();
        g.set_tenant_capability("0", CAP, true);
        g.set_global_enabled(false);
        assert!(g.check_tenant_enabled("0", CAP).is_err());
    }

    /// One `<pluginId>.*` switch that is off stops every operation of that plugin and
    /// nothing else; an operation switch that is on does not lift it; the global switch stops all.
    #[test]
    fn a_plugin_prefix_switch_stops_all_its_operations() {
        let g = InMemoryCapabilityGrants::new();
        assert!(g.check_not_suspended("app-a", "example.poll.vote").is_ok());
        g.set_tenant_capability("app-a", "example.poll.*", false);
        g.set_tenant_capability("app-a", "example.poll.vote", true);
        for stopped in [
            "example.poll.vote",
            "example.poll.create",
            "example.poll.a.b",
        ] {
            assert!(
                g.check_not_suspended("app-a", stopped).is_err(),
                "{stopped}"
            );
        }
        for open in [
            "example.pollster.vote",
            "example.poll",
            "example.other.vote",
            "vendorx.do",
        ] {
            assert!(g.check_not_suspended("app-a", open).is_ok(), "{open}");
        }
        assert!(g.check_not_suspended("app-b", "example.poll.vote").is_ok());
        // A bare `*` or `.*` is no prefix switch.
        g.set_tenant_capability("app-b", ".*", false);
        g.set_tenant_capability("app-b", "*", false);
        assert!(g.check_not_suspended("app-b", "example.poll.vote").is_ok());
        g.set_tenant_capability("app-a", "example.poll.*", true);
        assert!(
            g.check_not_suspended("app-a", "example.poll.create")
                .is_ok()
        );
        g.set_global_enabled(false);
        assert!(
            g.check_not_suspended("app-a", "example.poll.create")
                .is_err()
        );
    }
}
