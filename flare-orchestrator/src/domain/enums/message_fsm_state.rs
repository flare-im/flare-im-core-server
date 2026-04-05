//! 消息 FSM 状态枚举
//!
//! 定义消息的生命周期状态，用于状态判断和转换验证

use serde::{Deserialize, Serialize};
use std::fmt;

/// 消息 FSM 状态枚举
///
/// 管理消息的客观生命周期状态：
/// - INIT: 服务端构建中（客户端不可见）
/// - SENT: 已发送（正常态）
/// - EDITED: 已被编辑（可多次进入）
/// - RECALLED: 已撤回（终态）
/// - DELETED_HARD: 已硬删除（终态）
/// - DELETED_SOFT: 已软删除（用户维度）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MessageFsmState {
    /// 服务端构建中（客户端不可见）
    Init,
    /// 已发送（正常态）
    Sent,
    /// 已被编辑（可多次进入）
    Edited,
    /// 已撤回（终态）
    Recalled,
    /// 已硬删除（终态）
    DeletedHard,
    /// 已软删除（用户维度的删除，对当前用户不可见但仍存在于数据库中）
    DeletedSoft,
}

impl MessageFsmState {
    /// 转换为数据库存储的字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            MessageFsmState::Init => "INIT",
            MessageFsmState::Sent => "SENT",
            MessageFsmState::Edited => "EDITED",
            MessageFsmState::Recalled => "RECALLED",
            MessageFsmState::DeletedHard => "DELETED_HARD",
            MessageFsmState::DeletedSoft => "DELETED_SOFT",
        }
    }

    /// 从数据库字符串解析
    pub fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "INIT" => Ok(MessageFsmState::Init),
            "SENT" => Ok(MessageFsmState::Sent),
            "EDITED" => Ok(MessageFsmState::Edited),
            "RECALLED" => Ok(MessageFsmState::Recalled),
            "DELETED_HARD" => Ok(MessageFsmState::DeletedHard),
            "DELETED_SOFT" => Ok(MessageFsmState::DeletedSoft),
            _ => Err(format!("Invalid message FSM state: {}", s)),
        }
    }

    /// 是否为终态（不可再变更）
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            MessageFsmState::Recalled | MessageFsmState::DeletedHard
        )
    }

    /// 是否可以编辑
    pub fn can_edit(&self) -> bool {
        matches!(self, MessageFsmState::Sent | MessageFsmState::Edited)
    }

    /// 是否可以撤回
    pub fn can_recall(&self) -> bool {
        matches!(self, MessageFsmState::Sent | MessageFsmState::Edited)
    }

    /// 是否可以硬删除
    pub fn can_delete_hard(&self) -> bool {
        matches!(self, MessageFsmState::Sent | MessageFsmState::Edited)
    }

    /// 是否可以软删除
    pub fn can_delete_soft(&self) -> bool {
        matches!(
            self,
            MessageFsmState::Sent | MessageFsmState::Edited | MessageFsmState::Recalled
        )
    }
}

impl Default for MessageFsmState {
    fn default() -> Self {
        MessageFsmState::Sent
    }
}

impl fmt::Display for MessageFsmState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
