//! Business-neutral IM contracts shared by Flare IM services.

pub mod abstractions;
pub mod constants;
pub mod domain;
pub mod event;
pub mod instrumentation;
pub mod message;
pub mod service_names;
pub mod utils;
pub mod wal;

pub use flare_core_base::context::Ctx;
pub use flare_core_messaging::eventbus::{EventEnvelope, TopicEventBus};

pub use abstractions::storage_payload::{EXTRA_KEY_SYNC, EXTRA_KEY_TAGS, StorageMessagePayload};
pub use domain::{
    ClientMessageId, ConnectionEvent, ConnectionId, ConversationId, ConversationSyncSlice,
    DeleteType, DeviceId, DevicePushToken, EventMeta, GatewayId, MarkType, MessageCommand,
    MessageCommandHandler, MessageId, MultiDeviceSyncResult, OperationResult, ReactionAction,
    SendAckResult, SendMessageCommand, Seq, SyncQueryHandler, SyncResult, UserId,
    device_push_token_registry_field, device_push_token_registry_key,
};
pub use instrumentation::{
    BusinessProbeDelivery, BusinessProbeEvent, BusinessProbeKind, BusinessProbeSink,
    NoopBusinessProbeSink, SharedBusinessProbeSink,
};
pub use message::{
    Attachment, Message as MessageDomain, RetentionTransitionError, message_from_proto,
    message_into_proto, message_to_proto,
};
pub use service_names::{get_service_name, service_name_env_var, validate_service_name};
pub use utils::{
    context_from_mq_metadata, context_to_mq_metadata, extract_context_opt,
    extract_session_id_from_context, require_context, require_request_id_from_context,
    require_tenant_id_from_context, require_user_id_from_context,
};
pub use wal::wal_pending_index_key;
