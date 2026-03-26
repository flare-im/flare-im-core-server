//! gRPC 接口层：`MediaService` 适配，请求/响应与 application 命令、查询互转。

mod media_handler;

pub use media_handler::MediaGrpcHandler;
