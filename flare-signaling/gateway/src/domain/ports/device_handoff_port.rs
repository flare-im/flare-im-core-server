//! 设备交接上报端口
//!
//! 一台设备的新消息提示从「这条链路负责」转为「推送通道负责」时，网关把这件事连同还没确认的
//! 条目交给推送侧。基础设施实现见
//! [`crate::infrastructure::ports::PushProxyHandoffReporter`]。

use flare_proto::common::DeviceHandoff;

pub trait IDeviceHandoffPort: Send + Sync {
    /// 提交一次交接。调用方在下行写出路径和连接读循环里，这里不能等下游：实现方排队后由后台
    /// 任务发送，排不进去的要计数并留日志，不能悄悄丢。
    fn report(&self, handoff: DeviceHandoff);
}
