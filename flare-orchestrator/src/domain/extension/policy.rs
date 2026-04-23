/// 扩展执行失败策略。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtensionFailureMode {
    /// 失败开放：记录并降级，不中断主流程。
    FailOpen,
    /// 失败关闭：返回错误，中断主流程。
    FailClosed,
}

impl ExtensionFailureMode {
    pub fn from_fail_open(enabled: bool) -> Self {
        if enabled {
            Self::FailOpen
        } else {
            Self::FailClosed
        }
    }
}

/// 单个扩展阶段的运行策略（超时/重试）。
#[derive(Clone, Copy, Debug)]
pub struct ExtensionRuntimePolicy {
    /// 单次执行超时（毫秒）。
    pub timeout_ms: u64,
    /// 失败后重试次数（不含首次执行）。
    pub retry: u32,
}

impl ExtensionRuntimePolicy {
    pub const fn new(timeout_ms: u64, retry: u32) -> Self {
        Self { timeout_ms, retry }
    }

    pub fn attempts(self) -> u32 {
        self.retry.saturating_add(1).max(1)
    }
}

/// 扩展策略集合（当前覆盖 CallSignal enrich 与 PostSend hook）。
#[derive(Clone, Copy, Debug)]
pub struct ExtensionPolicy {
    pub call_signal_enrich_failure_mode: ExtensionFailureMode,
    pub post_send_hook_failure_mode: ExtensionFailureMode,
    pub pre_send: ExtensionRuntimePolicy,
    pub post_send: ExtensionRuntimePolicy,
    pub event_enrich: ExtensionRuntimePolicy,
}

impl ExtensionPolicy {
    pub fn new(call_signal_fail_open: bool, post_send_fail_open: bool) -> Self {
        Self {
            call_signal_enrich_failure_mode: ExtensionFailureMode::from_fail_open(
                call_signal_fail_open,
            ),
            post_send_hook_failure_mode: ExtensionFailureMode::from_fail_open(post_send_fail_open),
            pre_send: ExtensionRuntimePolicy::new(1500, 0),
            post_send: ExtensionRuntimePolicy::new(1200, 0),
            event_enrich: ExtensionRuntimePolicy::new(1800, 1),
        }
    }

    pub fn with_runtime(
        mut self,
        pre_send: ExtensionRuntimePolicy,
        post_send: ExtensionRuntimePolicy,
        event_enrich: ExtensionRuntimePolicy,
    ) -> Self {
        self.pre_send = pre_send;
        self.post_send = post_send;
        self.event_enrich = event_enrich;
        self
    }
}
