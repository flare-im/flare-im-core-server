//! Whether a user is a member of a conversation, read before a user's event is sequenced so that
//! only members react, pin, mark, read, edit or recall in it.
use flare_im_contracts::Ctx;
use flare_server_core::error::Result;

/// How recent a membership answer has to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipRead {
    /// An answer from the last few seconds will do: for the frequent events whose effect is small
    /// and short-lived (read receipts, reactions), where a member removed a moment ago acting once
    /// more costs nothing lasting.
    Recent,
    /// Asked of the conversation service now: for everything that changes what other members see
    /// or keeps it (edit, recall, delete, pin, mark, custom events, and any event type added
    /// later), so a member removed from the group cannot do any of it afterwards.
    Current,
}

#[async_trait::async_trait]
pub trait ConversationMembershipRepository: Send + Sync {
    /// Whether `user_id` is a member of `conversation_id` in `tenant_id`: a participant who has
    /// not left. A member who hid the conversation (removed it from their list) is still a member:
    /// hiding is how the list shows it, and their read receipts and reactions must still go
    /// through. Implementations look up this one user, never the member list, so the cost does
    /// not grow with the group. An error is an outage and is answered retryable.
    async fn is_member(
        &self,
        ctx: &Ctx,
        tenant_id: &str,
        conversation_id: &str,
        user_id: &str,
        read: MembershipRead,
    ) -> Result<bool>;
}
