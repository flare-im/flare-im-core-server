//! gRPC：`PushService` 接收请求并写入 MQ（由 Push Server 消费）。

mod push_handler;

pub use push_handler::PushServiceHandler;
