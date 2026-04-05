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
    pub fn matches(&self, ctx: &Ctx) -> bool {
        use crate::hooks::hook_context_data::get_hook_context_data;

        let tenant_id = ctx.tenant_id().unwrap_or("0").to_string();
        let hook_data = get_hook_context_data(ctx);

        self.tenants.matches(Some(tenant_id.as_str()))
            && self
                .conversation_types
                .matches(hook_data.and_then(|d| d.conversation_type.as_deref()))
            && self
                .message_types
                .matches(hook_data.and_then(|d| d.message_type.as_deref()))
    }
}
