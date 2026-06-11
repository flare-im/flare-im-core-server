//! gRPC 接口层：协议适配，将请求转为 application 命令，响应由领域结果映射为 proto。

mod send_handler;

pub use send_handler::MessageSendGrpcHandler;
