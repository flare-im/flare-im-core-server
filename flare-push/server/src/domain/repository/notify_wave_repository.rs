//! 会话波次状态的存储（Port）。
//!
//! 规则在领域模型里（[`crate::domain::model::WaveState`]），这里只负责「一次读出、比较版本后一次写回」。
//! 波次的到期条目与闸门的尾部刷新、推迟提示在同一个到期队列里，由
//! [`super::NotifyGateStore::claim_due`] 一起领取。

use flare_server_core::error::FlareError;

use crate::domain::model::{WaveKey, WaveState};

/// 读出的一个会话的波次状态。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WaveSnapshot {
    pub state: WaveState,
    /// 比较后写入用的版本；不存在的状态是 0。
    pub version: u64,
    /// 累加期间最新一条消息的正文（协议 `Message` 的编码）；没有累加、或正文太大没存时为空。
    pub message: Option<Vec<u8>>,
}

/// 最新一条消息正文的改动。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaveMessageWrite {
    Keep,
    Set(Vec<u8>),
    Clear,
}

/// 一次比较后写入：存储里的版本等于 `expect` 才写，写入的各项一起生效。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaveCommit {
    pub key: WaveKey,
    pub expect: u64,
    pub state: WaveState,
    pub message: WaveMessageWrite,
    /// 状态保留多久（毫秒）。
    pub ttl_ms: i64,
    /// 登记（或改登记）这个会话的波次到期时刻。
    pub due_ms: Option<i64>,
}

#[async_trait::async_trait]
pub trait WaveStore: Send + Sync {
    /// 读出一个会话的波次状态。不存在的是 `default()`。
    async fn load_wave(&self, key: &WaveKey) -> Result<WaveSnapshot, FlareError>;

    /// 比较后写入。写入了返回 `true`；版本对不上什么都不写，返回 `false`，调用方重读重算。
    /// 存储不可用是错误。
    async fn commit_wave(&self, commit: &WaveCommit) -> Result<bool, FlareError>;
}
