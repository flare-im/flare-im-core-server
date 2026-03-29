//! gRPC 接口层：协议适配，将请求转为 application 命令，响应由领域结果映射为 proto。

mod message_action_grpc;
mod message_send_grpc;

pub use message_action_grpc::MessageActionGrpcHandler;
pub use message_send_grpc::MessageSendGrpcHandler;
