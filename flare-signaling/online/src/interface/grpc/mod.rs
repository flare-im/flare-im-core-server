//! gRPC：`OnlineService` 协议适配，入参/出参与应用层命令、查询互转。

mod online_handler;

pub use online_handler::OnlineHandler;
