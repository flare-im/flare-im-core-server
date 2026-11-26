//! 调用链注册表：配置放在原子指针后面，发送时一次读取加一次按应用查表。
//!
//! 链按来源排（应用 → 平台 → 部署），同来源按 `order`。三段在换指针时合成每个应用的完整链，发送路径
//! 不排序也不拼接；没有自己 Hook 的应用都走同一条共享链。

use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;

use crate::Ctx;

use super::selector::HookSelector;
use super::types::{HookMetadata, HookSource, PreSendHook};

/// 调用链上的一条 Hook。
pub struct PreSendPlan {
    pub(crate) metadata: HookMetadata,
    pub(crate) selector: HookSelector,
    pub(crate) handler: Arc<dyn PreSendHook>,
}

impl PreSendPlan {
    pub fn new(
        metadata: HookMetadata,
        selector: HookSelector,
        handler: Arc<dyn PreSendHook>,
    ) -> Self {
        Self {
            metadata,
            selector,
            handler,
        }
    }

    pub fn metadata(&self) -> &HookMetadata {
        &self.metadata
    }
}

/// 一条完整的调用链。
pub type HookChain = Arc<[Arc<PreSendPlan>]>;

type Plans = Vec<Arc<PreSendPlan>>;

/// 数据库来源的一个范围。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HookScope {
    /// `tenant_id` 为空的行。
    Platform,
    /// 一个应用自己的行。
    Application(String),
}

#[derive(Clone, Default)]
struct Segments {
    deployment: Plans,
    platform: Plans,
    applications: HashMap<String, Plans>,
}

struct HookChains {
    segments: Segments,
    /// 平台 → 部署：没有自己 Hook 的应用走这条。
    shared: HookChain,
    /// 应用 → 平台 → 部署。
    merged: HashMap<String, HookChain>,
}

impl HookChains {
    fn build(segments: Segments) -> Self {
        let tail = || {
            segments
                .platform
                .iter()
                .chain(&segments.deployment)
                .cloned()
        };
        let shared: HookChain = tail().collect();
        let merged = segments
            .applications
            .iter()
            .map(|(app, plans)| {
                let chain: HookChain = plans.iter().cloned().chain(tail()).collect();
                (app.clone(), chain)
            })
            .collect();
        Self {
            segments,
            shared,
            merged,
        }
    }
}

fn by_order(mut plans: Plans) -> Plans {
    plans.sort_by_key(|plan| plan.metadata.order);
    plans
}

/// Hook 注册表。
pub struct HookRegistry {
    chains: ArcSwap<HookChains>,
}

impl Default for HookRegistry {
    fn default() -> Self {
        Self {
            chains: ArcSwap::from_pointee(HookChains::build(Segments::default())),
        }
    }
}

impl HookRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn update(&self, change: impl Fn(&mut Segments)) {
        self.chains.rcu(|current| {
            let mut segments = current.segments.clone();
            change(&mut segments);
            Arc::new(HookChains::build(segments))
        });
    }

    /// 注册一条部署 Hook（部署文件，或进程内代码）。
    pub fn register_pre_send(
        &self,
        metadata: HookMetadata,
        selector: HookSelector,
        handler: Arc<dyn PreSendHook>,
    ) {
        let plan = Arc::new(PreSendPlan::new(
            metadata.with_source(HookSource::Deployment),
            selector,
            handler,
        ));
        self.update(|segments| {
            let mut plans = segments.deployment.clone();
            plans.push(Arc::clone(&plan));
            segments.deployment = by_order(plans);
        });
    }

    /// 整体换掉数据库来源的两段（启动时的全量加载）。
    pub fn replace_stored(&self, stored: HashMap<HookScope, Vec<PreSendPlan>>) {
        let mut platform = Plans::new();
        let mut applications = HashMap::new();
        for (scope, plans) in stored {
            let plans = by_order(plans.into_iter().map(Arc::new).collect());
            match scope {
                HookScope::Platform => platform = plans,
                HookScope::Application(app) => {
                    applications.insert(app, plans);
                }
            }
        }
        self.update(|segments| {
            segments.platform = platform.clone();
            segments.applications = applications.clone();
        });
    }

    /// 换掉变化的范围（热加载）。范围没有行时值为空，这个范围的 Hook 就没了。
    pub fn replace_scopes(&self, changed: HashMap<HookScope, Vec<PreSendPlan>>) {
        let changed: Vec<(HookScope, Plans)> = changed
            .into_iter()
            .map(|(scope, plans)| (scope, by_order(plans.into_iter().map(Arc::new).collect())))
            .collect();
        self.update(|segments| {
            for (scope, plans) in &changed {
                match scope {
                    HookScope::Platform => segments.platform = plans.clone(),
                    HookScope::Application(app) if plans.is_empty() => {
                        segments.applications.remove(app);
                    }
                    HookScope::Application(app) => {
                        segments.applications.insert(app.clone(), plans.clone());
                    }
                }
            }
        });
    }

    /// 一个应用的完整调用链：一次读取加一次查表。缺租户时只有共享链。
    pub fn chain_for(&self, tenant_id: Option<&str>) -> HookChain {
        let chains = self.chains.load();
        tenant_id
            .filter(|tenant| !tenant.trim().is_empty())
            .and_then(|tenant| chains.merged.get(tenant))
            .unwrap_or(&chains.shared)
            .clone()
    }

    /// 这个上下文实际会执行哪些 Hook，按执行顺序。
    pub fn plan_pre_send(&self, ctx: &Ctx) -> Vec<Arc<PreSendPlan>> {
        self.chain_for(ctx.tenant_id())
            .iter()
            .filter(|plan| plan.selector.matches(ctx))
            .cloned()
            .collect()
    }
}
