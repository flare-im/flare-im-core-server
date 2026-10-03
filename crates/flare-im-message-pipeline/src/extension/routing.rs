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

    /// 租户白名单。上下文缺租户时放行到 Hook 执行，由 `HookExecutionService` 以「缺租户」拒绝发送：
    /// 既不按租户 "0" 查白名单，也不能因此跳过必需的 Hook（W12-T）。
    fn allows_tenant(&self, ctx: &Ctx) -> bool {
        if self.tenant_allowlist.is_empty() {
            return true;
        }
        match ctx
            .tenant_id()
            .filter(|tenant_id| !tenant_id.trim().is_empty())
        {
            Some(tenant_id) => self.tenant_allowlist.contains(tenant_id),
            None => true,
        }
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

    /// 缺租户不按租户 "0" 查白名单：白名单里有 "0" 也不因此放行，白名单里没有也不因此跳过
    /// 必需的 Hook；交给 Hook 执行以「缺租户」拒绝。显式的租户 "0" 照常按白名单判定。
    #[test]
    fn missing_tenant_is_not_matched_as_tenant_zero() {
        let msg = Message {
            message_type: 100,
            ..Default::default()
        };
        let no_tenant: Ctx = Arc::new(Context::with_request_id("trace-routing-test"));
        let blank_tenant = test_ctx("  ");

        let zero_only = ExtensionRouting::new(vec!["0".to_string()], vec![]);
        assert!(zero_only.allows_hook_for_message(&test_ctx("0"), &msg));
        assert!(!zero_only.allows_hook_for_message(&test_ctx("tenant-a"), &msg));

        let other_only = ExtensionRouting::new(vec!["tenant-b".to_string()], vec![]);
        assert!(!other_only.allows_hook_for_message(&test_ctx("0"), &msg));
        for ctx in [&no_tenant, &blank_tenant] {
            assert!(other_only.allows_hook_for_message(ctx, &msg));
            assert!(other_only.allows_hook_for_message_type(ctx, 100));
        }
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
