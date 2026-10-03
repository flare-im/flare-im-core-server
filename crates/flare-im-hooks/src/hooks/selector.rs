use std::collections::HashSet;

use crate::Ctx;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum MatchRule {
    #[default]
    Any,
    Exact {
        values: HashSet<String>,
    },
}

impl MatchRule {
    pub fn any() -> Self {
        MatchRule::Any
    }

    pub fn of<I, T>(values: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        MatchRule::Exact {
            values: values.into_iter().map(Into::into).collect(),
        }
    }

    pub fn matches(&self, value: Option<&str>) -> bool {
        match self {
            MatchRule::Any => true,
            MatchRule::Exact { values } => value.map(|val| values.contains(val)).unwrap_or(false),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HookSelector {
    #[serde(default)]
    pub tenants: MatchRule,
    #[serde(default)]
    pub conversation_types: MatchRule,
    #[serde(default)]
    pub message_types: MatchRule,
}

impl HookSelector {
    /// 上下文缺租户时只有不限租户（`Any`）的 Hook 被选中：按租户列出的 Hook（含只列 "0" 的）
    /// 不再把缺租户当成租户 "0" 执行（W12-T）。发送路径在执行 Hook 之前已拒绝缺租户的消息。
    pub fn matches(&self, ctx: &Ctx) -> bool {
        use crate::hooks::hook_context_data::get_hook_context_data;

        let tenant_id = ctx
            .tenant_id()
            .filter(|tenant_id| !tenant_id.trim().is_empty());
        let hook_data = get_hook_context_data(ctx);

        self.tenants.matches(tenant_id)
            && self
                .conversation_types
                .matches(hook_data.and_then(|d| d.conversation_type.as_deref()))
            && self
                .message_types
                .matches(hook_data.and_then(|d| d.message_type.as_deref()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{HookSelector, MatchRule};
    use crate::Ctx;
    use flare_server_core::context::Context;

    fn ctx(tenant: Option<&str>) -> Ctx {
        let ctx = Context::with_request_id("selector-test");
        Arc::new(match tenant {
            Some(tenant) => ctx.with_tenant_id(tenant),
            None => ctx,
        })
    }

    /// 缺租户不按租户 "0" 匹配：只列 "0" 的 Hook 不被选中，不限租户的照常；显式的 "0" 照常匹配。
    #[test]
    fn a_missing_tenant_is_not_selected_as_tenant_zero() {
        let zero_only = HookSelector {
            tenants: MatchRule::of(["0"]),
            ..HookSelector::default()
        };
        assert!(zero_only.matches(&ctx(Some("0"))));
        assert!(!zero_only.matches(&ctx(Some("acme"))));
        assert!(!zero_only.matches(&ctx(None)));
        assert!(!zero_only.matches(&ctx(Some("  "))));

        let any = HookSelector::default();
        assert!(any.matches(&ctx(None)));
        assert!(any.matches(&ctx(Some("acme"))));
    }
}
