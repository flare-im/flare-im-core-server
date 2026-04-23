use std::collections::HashSet;

use flare_im_core::Ctx;
use flare_proto::common::{Event, Message};

/// 扩展路由规则：按租户、事件类型、消息类型控制扩展是否执行。
#[derive(Clone, Debug, Default)]
pub struct ExtensionRouting {
    tenant_allowlist: HashSet<String>,
    hook_message_type_allowlist: HashSet<i32>,
    plugin_event_type_allowlist: HashSet<i32>,
}

impl ExtensionRouting {
    pub fn new(
        tenant_allowlist: Vec<String>,
        hook_message_type_allowlist: Vec<i32>,
        plugin_event_type_allowlist: Vec<i32>,
    ) -> Self {
        Self {
            tenant_allowlist: tenant_allowlist.into_iter().collect(),
            hook_message_type_allowlist: hook_message_type_allowlist.into_iter().collect(),
            plugin_event_type_allowlist: plugin_event_type_allowlist.into_iter().collect(),
        }
    }

    pub fn allows_hook_for_message(&self, ctx: &Ctx, message: &Message) -> bool {
        self.allows_tenant(ctx)
            && (self.hook_message_type_allowlist.is_empty()
                || self
                    .hook_message_type_allowlist
                    .contains(&message.message_type))
    }

    pub fn allows_hook_for_message_type(&self, ctx: &Ctx, message_type: i32) -> bool {
        self.allows_tenant(ctx)
            && (self.hook_message_type_allowlist.is_empty()
                || self.hook_message_type_allowlist.contains(&message_type))
    }

    pub fn allows_plugin_for_event(&self, ctx: &Ctx, event: &Event) -> bool {
        self.allows_tenant(ctx)
            && (self.plugin_event_type_allowlist.is_empty()
                || self.plugin_event_type_allowlist.contains(&event.r#type))
    }

    fn allows_tenant(&self, ctx: &Ctx) -> bool {
        if self.tenant_allowlist.is_empty() {
            return true;
        }
        let tenant_id = ctx.tenant_id().unwrap_or("0");
        self.tenant_allowlist.contains(tenant_id)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::ExtensionRouting;
    use flare_im_core::Ctx;
    use flare_proto::common::{Event, Message};
    use flare_server_core::Context;

    fn test_ctx(tenant_id: &str) -> Ctx {
        Arc::new(
            Context::with_request_id("trace-routing-test")
                .with_user_id("user-routing-test")
                .with_tenant_id(tenant_id),
        )
    }

    #[test]
    fn allows_all_when_no_allowlists() {
        let routing = ExtensionRouting::new(vec![], vec![], vec![]);
        let ctx = test_ctx("tenant-a");
        let msg = Message {
            message_type: 100,
            ..Default::default()
        };
        let evt = Event {
            r#type: 200,
            ..Default::default()
        };
        assert!(routing.allows_hook_for_message(&ctx, &msg));
        assert!(routing.allows_hook_for_message_type(&ctx, 100));
        assert!(routing.allows_plugin_for_event(&ctx, &evt));
    }

    #[test]
    fn tenant_filter_blocks_hook_and_plugin() {
        let routing = ExtensionRouting::new(vec!["tenant-b".to_string()], vec![], vec![]);
        let ctx = test_ctx("tenant-a");
        let msg = Message {
            message_type: 100,
            ..Default::default()
        };
        let evt = Event {
            r#type: 200,
            ..Default::default()
        };
        assert!(!routing.allows_hook_for_message(&ctx, &msg));
        assert!(!routing.allows_hook_for_message_type(&ctx, 100));
        assert!(!routing.allows_plugin_for_event(&ctx, &evt));
    }

    #[test]
    fn message_and_event_type_allowlists_work() {
        let routing = ExtensionRouting::new(vec![], vec![7], vec![9]);
        let ctx = test_ctx("tenant-any");
        let allowed_msg = Message {
            message_type: 7,
            ..Default::default()
        };
        let blocked_msg = Message {
            message_type: 8,
            ..Default::default()
        };
        let allowed_evt = Event {
            r#type: 9,
            ..Default::default()
        };
        let blocked_evt = Event {
            r#type: 10,
            ..Default::default()
        };
        assert!(routing.allows_hook_for_message(&ctx, &allowed_msg));
        assert!(!routing.allows_hook_for_message(&ctx, &blocked_msg));
        assert!(routing.allows_plugin_for_event(&ctx, &allowed_evt));
        assert!(!routing.allows_plugin_for_event(&ctx, &blocked_evt));
    }
}
