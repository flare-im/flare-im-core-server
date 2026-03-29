//! 领域服务与上下文
//!
//! 当前仅保留 RouteContext（流控/路由上下文），供 MessageRoutingHandler / EventRoutingHandler 使用。
//! 与 flare_im_core 的 ConversationId / UserId 对齐，便于跨 BC 一致。

use flare_im_core::{ConversationId, UserId};

/// 路由上下文值对象（流控、追踪等）
#[derive(Debug, Clone, Default)]
pub struct RouteContext {
    pub svid: String,
    pub conversation_id: Option<ConversationId>,
    pub user_id: Option<UserId>,
    pub tenant_id: Option<String>,
    pub client_geo: Option<String>,
    pub login_gateway: Option<String>,
}
