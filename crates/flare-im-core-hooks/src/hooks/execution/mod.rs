//! Hook 执行证据：采样、有界缓冲、批量写入与指标。消息链路上只有一次加锁入队，写库在后台；发送不等
//! 执行记录。

pub mod buffer;
pub mod guard;
pub mod metrics;
pub mod record;
pub mod settings;
pub mod writer;

pub use buffer::{
    HookExecutionBuffer, HookExecutionBufferSettings, MetricsOnlyHookExecutions, SampleChooser,
    sample_every,
};
pub use guard::{HookCall, HookExecutionGuard, HookExecutionObserver, HookIdentity};
pub use metrics::{DropReason, HookExecutionMetrics};
pub use record::{
    HOOK_BUDGET_EXHAUSTED_CODE, HOOK_CALL_FAILED_CODE, HOOK_CIRCUIT_OPEN_CODE, HOOK_REJECTED_CODE,
    HOOK_REWRITE_INVALID_CODE, HOOK_TIMEOUT_CODE, HookExecutionClass, HookExecutionOutcome,
    HookExecutionRecord, MAX_RECORDED_FIELD_CHARS, bounded, execution_hook_id,
};
pub use settings::{HookExecutionConfig, HookRuntimeSettings};
pub use writer::{HookExecutionSink, run_writer};
