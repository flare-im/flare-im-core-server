//! 内建：单聊接收者解析。

use async_trait::async_trait;
use flare_core_base::context::Ctx;

use crate::domain::capability::{
    CapabilityError, ConversationKind, RecipientResolveRequest, RecipientResolveResult,
    RecipientResolver, ResolveTrigger, Result,
};

pub struct DirectConversationRecipientResolver {
    id: String,
}

impl DirectConversationRecipientResolver {
    pub fn new() -> Self {
        Self {
            id: "builtin.direct_recipient".into(),
        }
    }
}

#[async_trait]
impl RecipientResolver for DirectConversationRecipientResolver {
    fn id(&self) -> &str {
        &self.id
    }

    fn supports(&self, kind: &ConversationKind, trigger: ResolveTrigger) -> bool {
        matches!(kind, ConversationKind::Direct)
            && matches!(
                trigger,
                ResolveTrigger::MessageDelivery | ResolveTrigger::RtcInvite
            )
    }

    async fn resolve(
        &self,
        _ctx: &Ctx,
        req: &RecipientResolveRequest,
    ) -> Result<RecipientResolveResult> {
        let peer = req
            .direct_peer_user_id
            .clone()
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| {
                CapabilityError::NotRegistered(
                    "direct_peer_user_id missing for DirectConversationRecipientResolver".into(),
                )
            })?;
        Ok(RecipientResolveResult {
            recipient_user_ids: vec![peer],
            ext: serde_json::Value::Null,
        })
    }
}
