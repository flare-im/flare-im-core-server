//! # Hook 配置快照（按租户分桶）
//!
//! 全局配置（配置文件、配置中心、`tenant_id IS NULL` 的数据库行）对所有租户生效；各租户经
//! `CapabilityService/Administer` 登记的行只放在该租户自己的桶里。执行某个租户的事件时，取
//! 全局配置加上该租户的桶，再去掉选择器限定了别的租户的 Hook。
//!
//! 全局 Hook 的名字在同类型内为平台保留：租户桶里与全局 Hook 同类型同名的条目不装（告警），
//! 全局 Hook 照常执行。否则租户管理员登记一条同名的空操作 Hook，就能让平台的审核、审计或
//! 合规 Hook 对本租户失效。Administer 也拒绝登记这样的名字。

use std::collections::HashMap;

use tracing::{error, warn};

use crate::domain::model::HookConfig;
use crate::infrastructure::config::loader::{ConfigMerger, ConfigValidator, LoadedHookConfig};
use flare_server_core::error::Result;

/// 一次加载得到的 Hook 配置：全局一份，各租户各一份。
#[derive(Debug, Clone, Default)]
pub struct HookConfigSnapshot {
    global: HookConfig,
    tenants: HashMap<String, HookConfig>,
}

impl HookConfigSnapshot {
    /// 租户桶里与全局 Hook 同类型同名的条目会被去掉（见模块说明）。
    pub fn new(global: HookConfig, tenants: HashMap<String, HookConfig>) -> Self {
        let tenants = tenants
            .into_iter()
            .filter_map(|(tenant, mut config)| {
                drop_reserved_names(&tenant, &mut config, &global);
                (!config.is_empty()).then_some((tenant, config))
            })
            .collect();
        Self { global, tenants }
    }

    /// 对所有租户生效的配置。
    pub fn global(&self) -> &HookConfig {
        &self.global
    }

    /// 该租户自己登记的配置（不含全局配置）。
    pub fn tenant(&self, tenant_id: &str) -> Option<&HookConfig> {
        self.tenants.get(tenant_id)
    }

    /// 有自己配置的租户数。
    pub fn tenant_count(&self) -> usize {
        self.tenants.len()
    }

    /// `hook_type` 类型里名为 `name` 的全局 Hook 存在（该名字为平台保留，租户不能登记）。
    pub fn is_global_hook(&self, hook_type: &str, name: &str) -> bool {
        self.global.has_hook(hook_type, name)
    }

    /// `tenant_id` 的事件实际要执行的配置（见模块说明）：全局 Hook 一条不少。
    pub fn for_tenant(&self, tenant_id: &str) -> HookConfig {
        let mut merged = self.global.clone();
        if let Some(own) = self.tenants.get(tenant_id) {
            merged.extend_keeping_existing(own);
        }
        merged.retain_tenant(tenant_id);
        merged
    }

    /// 由各配置源最近一次的加载结果合成快照（`loaded` 按优先级从低到高）。
    ///
    /// 全局配置校验不过时返回错误，调用方保留原快照；某个租户的配置校验不过时只影响该租户：
    /// 沿用 `previous` 中该租户的配置（没有就不装），其他租户照常更新。
    pub fn build(
        loaded: &[Option<LoadedHookConfig>],
        previous: &HookConfigSnapshot,
    ) -> Result<Self> {
        let global = ConfigMerger::merge(
            loaded
                .iter()
                .flatten()
                .map(|source| source.global.clone())
                .collect(),
        );
        ConfigValidator::validate(&global)?;

        let mut layers: HashMap<&str, Vec<HookConfig>> = HashMap::new();
        for source in loaded.iter().flatten() {
            for (tenant, config) in &source.tenants {
                layers
                    .entry(tenant.as_str())
                    .or_default()
                    .push(config.clone());
            }
        }

        let mut tenants = HashMap::with_capacity(layers.len());
        for (tenant, configs) in layers {
            let merged = ConfigMerger::merge(configs);
            match ConfigValidator::validate(&merged) {
                Ok(()) => {
                    tenants.insert(tenant.to_owned(), merged);
                }
                Err(error) => {
                    error!(
                        tenant_id = %tenant,
                        %error,
                        "invalid tenant hook config; keeping that tenant's previous hooks"
                    );
                    if let Some(kept) = previous.tenant(tenant) {
                        tenants.insert(tenant.to_owned(), kept.clone());
                    }
                }
            }
        }

        // 全局配置可能刚加了与某个租户已有条目同名的 Hook，沿用的上一版也要重新筛。
        Ok(Self::new(global, tenants))
    }
}

/// 去掉 `config`（租户 `tenant` 的桶）里与全局 Hook 同类型同名的条目，每条告警一次。
fn drop_reserved_names(tenant: &str, config: &mut HookConfig, global: &HookConfig) {
    for (hook_type, name) in config.remove_reserved(global) {
        warn!(
            tenant_id = %tenant,
            hook_type,
            hook_name = %name,
            "tenant hook reuses the name of a global hook; the global hook keeps running and the tenant hook is not loaded"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::{HookConfigItem, HookSelectorConfig, HookTransportConfig};

    fn hook(name: &str, endpoint: &str, tenants: &[&str]) -> HookConfigItem {
        HookConfigItem {
            name: name.into(),
            version: None,
            description: None,
            enabled: true,
            priority: 900,
            group: None,
            timeout_ms: 2000,
            max_retries: 0,
            error_policy: "fail_fast".into(),
            require_success: true,
            selector: HookSelectorConfig {
                tenants: tenants.iter().map(|tenant| tenant.to_string()).collect(),
                ..Default::default()
            },
            transport: HookTransportConfig::Webhook {
                endpoint: endpoint.into(),
                secret: None,
                headers: HashMap::new(),
            },
            metadata: HashMap::new(),
        }
    }

    fn pre_send(hooks: Vec<HookConfigItem>) -> HookConfig {
        HookConfig {
            pre_send: hooks,
            ..Default::default()
        }
    }

    fn endpoints(config: &HookConfig) -> Vec<String> {
        let mut endpoints: Vec<String> = config
            .pre_send
            .iter()
            .map(|hook| match &hook.transport {
                HookTransportConfig::Webhook { endpoint, .. } => endpoint.clone(),
                other => panic!("unexpected transport {other:?}"),
            })
            .collect();
        endpoints.sort();
        endpoints
    }

    fn two_tenants() -> HookConfigSnapshot {
        HookConfigSnapshot::new(
            pre_send(vec![hook("audit", "http://global", &[])]),
            HashMap::from([
                (
                    "a".to_string(),
                    pre_send(vec![hook("wh-a", "http://a", &[])]),
                ),
                (
                    "b".to_string(),
                    pre_send(vec![hook("wh-b", "http://b", &[])]),
                ),
            ]),
        )
    }

    #[test]
    fn a_tenants_hooks_run_only_for_that_tenant_and_global_ones_for_all() {
        let snapshot = two_tenants();
        assert_eq!(
            endpoints(&snapshot.for_tenant("a")),
            ["http://a", "http://global"]
        );
        assert_eq!(
            endpoints(&snapshot.for_tenant("b")),
            ["http://b", "http://global"]
        );
        assert_eq!(endpoints(&snapshot.for_tenant("c")), ["http://global"]);
    }

    /// 租户登记一条与全局 Hook 同名的（例如指向空操作端点），全局 Hook 对该租户照常执行，
    /// 租户的那条不装；租户其余的 Hook 不受影响。
    #[test]
    fn a_tenant_hook_with_a_global_hooks_name_cannot_replace_it() {
        let snapshot = HookConfigSnapshot::new(
            pre_send(vec![hook("audit", "http://global", &[])]),
            HashMap::from([(
                "a".to_string(),
                pre_send(vec![
                    hook("audit", "http://a-noop", &[]),
                    hook("wh-a", "http://a", &[]),
                ]),
            )]),
        );
        assert_eq!(
            endpoints(&snapshot.for_tenant("a")),
            ["http://a", "http://global"]
        );
        assert_eq!(endpoints(&snapshot.for_tenant("b")), ["http://global"]);
        assert!(snapshot.is_global_hook("pre_send", "audit"));
        assert!(!snapshot.is_global_hook("post_send", "audit"));
        assert!(!snapshot.is_global_hook("pre_send", "wh-a"));
    }

    /// 同名只在同类型内保留：别的类型可以用这个名字。
    #[test]
    fn a_global_name_is_reserved_only_within_its_hook_type() {
        let snapshot = HookConfigSnapshot::new(
            pre_send(vec![hook("audit", "http://global", &[])]),
            HashMap::from([(
                "a".to_string(),
                HookConfig {
                    post_send: vec![hook("audit", "http://a-post", &[])],
                    ..Default::default()
                },
            )]),
        );
        let config = snapshot.for_tenant("a");
        assert_eq!(endpoints(&config), ["http://global"]);
        assert_eq!(config.post_send.len(), 1);
    }

    /// 全局配置后来加了与租户已有条目同名的 Hook：重载后全局的生效，租户那条（含沿用的
    /// 上一版）不再装。
    #[test]
    fn a_reload_drops_tenant_hooks_that_a_new_global_hook_reserves() {
        let previous = two_tenants();
        let mut broken = hook("wh-b2", "http://b2", &[]);
        broken.timeout_ms = 0;
        let loaded = vec![Some(LoadedHookConfig {
            global: pre_send(vec![
                hook("audit", "http://global", &[]),
                hook("wh-a", "http://global-wh-a", &[]),
                hook("wh-b", "http://global-wh-b", &[]),
            ]),
            tenants: HashMap::from([
                (
                    "a".to_string(),
                    pre_send(vec![hook("wh-a", "http://a", &[])]),
                ),
                // b 这次的行无效，沿用上一版（wh-b），而 wh-b 已被全局占用。
                ("b".to_string(), pre_send(vec![broken])),
            ]),
            failed_tenants: Default::default(),
        })];
        let snapshot = HookConfigSnapshot::build(&loaded, &previous).unwrap();
        let all_global = ["http://global", "http://global-wh-a", "http://global-wh-b"];
        assert_eq!(endpoints(&snapshot.for_tenant("a")), all_global);
        assert_eq!(endpoints(&snapshot.for_tenant("b")), all_global);
        assert_eq!(snapshot.tenant_count(), 0);
    }

    /// 选择器列了租户的 Hook 只作用于所列租户：全局配置里也一样，租户桶里写别的租户也不生效。
    #[test]
    fn selector_tenants_limit_where_a_hook_runs() {
        let snapshot = HookConfigSnapshot::new(
            pre_send(vec![
                hook("only-a", "http://global-only-a", &["a"]),
                hook("all", "http://global", &[]),
            ]),
            HashMap::from([(
                "b".to_string(),
                pre_send(vec![hook("names-a", "http://b-names-a", &["a"])]),
            )]),
        );
        assert_eq!(
            endpoints(&snapshot.for_tenant("a")),
            ["http://global", "http://global-only-a"]
        );
        assert_eq!(endpoints(&snapshot.for_tenant("b")), ["http://global"]);
    }

    #[test]
    fn an_invalid_tenant_config_keeps_that_tenants_previous_hooks_only() {
        let previous = two_tenants();
        let mut broken = hook("wh-a2", "http://a2", &[]);
        broken.timeout_ms = 0;
        let loaded = vec![Some(LoadedHookConfig {
            global: pre_send(vec![hook("audit", "http://global", &[])]),
            tenants: HashMap::from([
                ("a".to_string(), pre_send(vec![broken])),
                (
                    "b".to_string(),
                    pre_send(vec![hook("wh-b2", "http://b2", &[])]),
                ),
            ]),
            failed_tenants: Default::default(),
        })];
        let snapshot = HookConfigSnapshot::build(&loaded, &previous).unwrap();
        assert_eq!(
            endpoints(&snapshot.for_tenant("a")),
            ["http://a", "http://global"]
        );
        assert_eq!(
            endpoints(&snapshot.for_tenant("b")),
            ["http://b2", "http://global"]
        );
    }

    #[test]
    fn an_invalid_global_config_is_an_error() {
        let mut broken = hook("audit", "http://global", &[]);
        broken.priority = 5000;
        let loaded = vec![Some(LoadedHookConfig::global(pre_send(vec![broken])))];
        assert!(HookConfigSnapshot::build(&loaded, &HookConfigSnapshot::default()).is_err());
    }

    /// 某个源这次没加载成功（槽位为 `None`）就不贡献配置；上层负责沿用它上一次的结果。
    #[test]
    fn later_sources_override_earlier_ones_by_name() {
        let loaded = vec![
            Some(LoadedHookConfig::global(pre_send(vec![hook(
                "audit",
                "http://file",
                &[],
            )]))),
            None,
            Some(LoadedHookConfig::global(pre_send(vec![hook(
                "audit",
                "http://db",
                &[],
            )]))),
        ];
        let snapshot = HookConfigSnapshot::build(&loaded, &HookConfigSnapshot::default()).unwrap();
        assert_eq!(endpoints(snapshot.global()), ["http://db"]);
    }
}
