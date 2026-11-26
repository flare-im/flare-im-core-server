//! Hook 点目录。Hook 只做同步拦截；要拦截别的动作，就在那个命令入口加一个点位，运行时和管理面都
//! 不用改。事后观察归事件。

/// `hook_configs.hook_type` 与执行记录里的类型名。
pub const PRE_SEND_HOOK_TYPE: &str = "pre_send";

/// 目录里的一个点位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HookPointInfo {
    /// 对外的点位名。
    pub id: &'static str,
    /// 存储用的类型名。
    pub hook_type: &'static str,
    pub summary: &'static str,
}

/// 全部点位。
pub const HOOK_POINTS: [HookPointInfo; 1] = [HookPointInfo {
    id: "message.pre_send",
    hook_type: PRE_SEND_HOOK_TYPE,
    summary: "消息进入持久化之前；可放行、拒绝或改写",
}];
