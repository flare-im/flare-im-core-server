mod media_client;
mod message_client;

pub use media_client::{GrpcClients, MediaServiceClientWrapper};
pub use message_client::{MessageActionServiceClientWrapper, MessageSendServiceClientWrapper};
