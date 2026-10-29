//! Host reads remain behind domain-owned APIs. No plugin can supply a tenant,
//! actor, credential or backend URL in a resource request.
use crate::domain::capability::Result;
use flare_im_capability_core::plugin::resource::ResourceRequest;
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Clone)]
pub struct ResourceActor {
    pub tenant_id: String,
    pub user_id: String,
    pub conversation_id: String,
    pub request_id: String,
}

#[async_trait::async_trait]
pub trait ResourceAccessBackend: Send + Sync {
    async fn read(&self, actor: &ResourceActor, request: &ResourceRequest) -> Result<Value>;

    /// The members (not left; hiding the conversation does not matter) among `user_ids` of
    /// `conversation_id`, read as Capability itself for `tenant_id` rather than as a user. The
    /// lookup is scoped to that tenant: a conversation of another tenant has no members.
    async fn tenant_members(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        request_id: &str,
        user_ids: &[String],
    ) -> Result<BTreeSet<String>>;
}

#[async_trait::async_trait]
pub trait ResourceCallBudget: Send + Sync {
    /// Atomic across replicas, using the signed invocation identity, never a
    /// body-provided idempotency key. False is a definitive budget rejection.
    async fn reserve(
        &self,
        tenant: &str,
        plugin: &str,
        actor: &str,
        request: &str,
        expires_at_ms: i64,
    ) -> Result<bool>;
}
