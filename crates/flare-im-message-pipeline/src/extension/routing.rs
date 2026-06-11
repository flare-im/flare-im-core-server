use std::collections::HashSet;

use flare_im_contracts::Ctx;
use flare_proto::common::Message;

/// 扩展路由规则：按租户、消息类型控制 Hook 是否执行。
#[derive(Clone, Debug, Default)]
pub struct ExtensionRouting {
    tenant_allowlist: HashSet<String>,
    hook_message_type_allowlist: HashSet<i32>,
}

impl ExtensionRouting {
    pub fn new(tenant_allowlist: Vec<String>, hook_message_type_allowlist: Vec<i32>) -> Self {
        Self {
            tenant_allowlist: tenant_allowlist.into_iter().collect(),
            hook_message_type_allowlist: hook_message_type_allowlist.into_iter().collect(),
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
    use flare_im_contracts::Ctx;
    use flare_proto::common::Message;
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
        let routing = ExtensionRouting::new(vec![], vec![]);
        let ctx = test_ctx("tenant-a");
        let msg = Message {
            message_type: 100,
            ..Default::default()
        };
        assert!(routing.allows_hook_for_message(&ctx, &msg));
        assert!(routing.allows_hook_for_message_type(&ctx, 100));
    }

    #[test]
    fn tenant_filter_blocks_hook() {
        let routing = ExtensionRouting::new(vec!["tenant-b".to_string()], vec![]);
        let ctx = test_ctx("tenant-a");
        let msg = Message {
            message_type: 100,
            ..Default::default()
        };
        assert!(!routing.allows_hook_for_message(&ctx, &msg));
        assert!(!routing.allows_hook_for_message_type(&ctx, 100));
    }

    #[test]
    fn message_type_allowlist_works() {
        let routing = ExtensionRouting::new(vec![], vec![7]);
        let ctx = test_ctx("tenant-any");
        let allowed_msg = Message {
            message_type: 7,
            ..Default::default()
        };
        let blocked_msg = Message {
            message_type: 8,
            ..Default::default()
        };
        assert!(routing.allows_hook_for_message(&ctx, &allowed_msg));
        assert!(!routing.allows_hook_for_message(&ctx, &blocked_msg));
    }
}
