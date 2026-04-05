mod media_client;
mod message_client;

pub use media_client::{MediaServiceClientWrapper, GrpcClients};
pub use message_client::{MessageSendServiceClientWrapper, MessageActionServiceClientWrapper};
