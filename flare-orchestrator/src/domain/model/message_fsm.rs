//! 消息FSM（有限状态机）领域模型
//!
//! 根据 FSM 设计文档，消息状态机管理消息的客观生命周期状态：
//! - INIT: 服务端构建中（客户端不可见）
//! - SENT: 已发送（正常态）
//! - EDITED: 已被编辑（可多次进入）
//! - RECALLED: 已撤回（终态）
//! - DELETED_HARD: 已硬删除（终态）

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

/// 消息FSM状态枚举
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

impl fmt::Display for MessageFsmState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// 编辑历史记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EditHistoryEntry {
    /// 编辑版本号（从1开始，每次编辑递增）
    pub edit_version: i32,
    /// 编辑后的内容（编码后的内容）
    pub content_encoded: Vec<u8>,
    /// 编辑时间
    pub edited_at: DateTime<Utc>,
    /// 编辑者ID
    pub editor_id: String,
    /// 编辑原因（可选）
    pub reason: Option<String>,
}

/// 消息聚合根（Message Aggregate）
///
/// 负责维护消息的业务不变式和FSM状态迁移
#[derive(Debug, Clone)]
pub struct Message {
    /// 服务端消息ID（全局唯一）
    pub server_id: String,
    /// 会话ID
    pub conversation_id: String,
    /// 发送者ID
    pub sender_id: String,
    /// 会话频道 ID：单聊=对方 user_id，群聊=群 ID（与 common/message.proto 一致）
    pub channel_id: String,
    /// 消息内容（二进制）
    pub content: Vec<u8>,
    /// 消息时间戳
    pub timestamp: DateTime<Utc>,
    /// FSM状态
    pub fsm_state: MessageFsmState,
    /// FSM状态变更时间
    pub fsm_state_changed_at: DateTime<Utc>,
    /// 编辑版本号
    pub edit_version: i32,
    /// 编辑历史
    pub edit_history: Vec<EditHistoryEntry>,
    /// 扩展字段（元数据）
    pub extra: std::collections::HashMap<String, String>,
    /// 更新时间
    pub updated_at: DateTime<Utc>,
}

impl Message {
    /// 创建新消息（初始状态为INIT）
    pub fn new(
        server_id: String,
        conversation_id: String,
        sender_id: String,
        channel_id: String,
        content: Vec<u8>,
        timestamp: DateTime<Utc>,
    ) -> Self {
        let now = Utc::now();
        Self {
            server_id,
            conversation_id,
            sender_id,
            channel_id,
            content,
            timestamp,
            fsm_state: MessageFsmState::Init,
            fsm_state_changed_at: now,
            edit_version: 0,
            edit_history: Vec::new(),
            extra: std::collections::HashMap::new(),
            updated_at: now,
        }
    }

    /// 标记为已发送（INIT -> SENT）
    pub fn mark_as_sent(&mut self) -> Result<(), String> {
        if self.fsm_state != MessageFsmState::Init {
            return Err(format!(
                "Cannot mark as sent: current state is {}",
                self.fsm_state
            ));
        }
        self.fsm_state = MessageFsmState::Sent;
        self.fsm_state_changed_at = Utc::now();
        self.updated_at = Utc::now();
        Ok(())
    }

    /// 编辑消息（SENT/EDITED -> EDITED）
    pub fn edit(
        &mut self,
        new_content: Vec<u8>,
        editor_id: String,
        reason: Option<String>,
    ) -> Result<(), String> {
        if !self.fsm_state.can_edit() {
            return Err(format!(
                "Cannot edit message: current state is {}",
                self.fsm_state
            ));
        }

        // 保存编辑历史
        self.edit_version += 1;
        let edit_entry = EditHistoryEntry {
            edit_version: self.edit_version,
            content_encoded: self.content.clone(), // 保存旧内容
            edited_at: Utc::now(),
            editor_id: editor_id.clone(),
            reason,
        };
        self.edit_history.push(edit_entry);

        // 更新内容和状态
        self.content = new_content;
        self.fsm_state = MessageFsmState::Edited;
        self.fsm_state_changed_at = Utc::now();
        self.updated_at = Utc::now();

        Ok(())
    }

    /// 撤回消息（SENT/EDITED -> RECALLED）
    pub fn recall(&mut self, _reason: Option<String>) -> Result<(), String> {
        if !self.fsm_state.can_recall() {
            return Err(format!(
                "Cannot recall message: current state is {}",
                self.fsm_state
            ));
        }

        self.fsm_state = MessageFsmState::Recalled;
        self.fsm_state_changed_at = Utc::now();
        self.updated_at = Utc::now();

        Ok(())
    }

    /// 硬删除消息（SENT/EDITED -> DELETED_HARD）
    pub fn delete_hard(&mut self) -> Result<(), String> {
        if !self.fsm_state.can_delete_hard() {
            return Err(format!(
                "Cannot delete message: current state is {}",
                self.fsm_state
            ));
        }

        self.fsm_state = MessageFsmState::DeletedHard;
        self.fsm_state_changed_at = Utc::now();
        self.updated_at = Utc::now();

        Ok(())
    }

    /// 软删除消息（任何非硬删除/撤回状态 -> DELETED_SOFT）
    pub fn delete_soft(&mut self) -> Result<(), String> {
        if !self.fsm_state.can_delete_soft() {
            return Err(format!(
                "Cannot soft delete message: current state is {}",
                self.fsm_state
            ));
        }

        self.fsm_state = MessageFsmState::DeletedSoft;
        self.fsm_state_changed_at = Utc::now();
        self.updated_at = Utc::now();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_fsm_state_transitions() {
        let mut msg = Message::new(
            "msg-1".to_string(),
            "conv-1".to_string(),
            "user-1".to_string(),
            "user-2".to_string(),
            b"Hello".to_vec(),
            Utc::now(),
        );

        // INIT -> SENT
        assert_eq!(msg.fsm_state, MessageFsmState::Init);
        msg.mark_as_sent().unwrap();
        assert_eq!(msg.fsm_state, MessageFsmState::Sent);

        // SENT -> EDITED
        msg.edit(b"Hello World".to_vec(), "user-1".to_string(), None)
            .unwrap();
        assert_eq!(msg.fsm_state, MessageFsmState::Edited);
        assert_eq!(msg.edit_version, 1);

        // EDITED -> EDITED (再次编辑)
        msg.edit(b"Hello World!".to_vec(), "user-1".to_string(), None)
            .unwrap();
        assert_eq!(msg.fsm_state, MessageFsmState::Edited);
        assert_eq!(msg.edit_version, 2);

        // EDITED -> RECALLED
        msg.recall(None).unwrap();
        assert_eq!(msg.fsm_state, MessageFsmState::Recalled);

        // RECALLED 后不能再编辑
        assert!(
            msg.edit(b"New".to_vec(), "user-1".to_string(), None)
                .is_err()
        );
    }

    #[test]
    fn test_message_fsm_soft_delete() {
        let mut msg = Message::new(
            "msg-1".to_string(),
            "conv-1".to_string(),
            "user-1".to_string(),
            "user-2".to_string(),
            b"Hello".to_vec(),
            Utc::now(),
        );

        // INIT -> SENT
        msg.mark_as_sent().unwrap();
        assert_eq!(msg.fsm_state, MessageFsmState::Sent);

        // SENT -> SOFT_DELETED
        msg.delete_soft().unwrap();
        assert_eq!(msg.fsm_state, MessageFsmState::DeletedSoft);

        // SOFT_DELETED 不是终态，理论上可以恢复（但目前没有实现恢复功能）
        // 但不能进行其他操作如编辑、撤回等
        assert!(
            msg.edit(b"New".to_vec(), "user-1".to_string(), None)
                .is_err()
        );
        assert!(msg.recall(None).is_err());
    }

    #[test]
    fn test_message_fsm_state_validation() {
        let mut msg = Message::new(
            "msg-1".to_string(),
            "conv-1".to_string(),
            "user-1".to_string(),
            "user-2".to_string(),
            b"Hello".to_vec(),
            Utc::now(),
        );

        // INIT 状态不能直接编辑
        assert!(
            msg.edit(b"New".to_vec(), "user-1".to_string(), None)
                .is_err()
        );

        // INIT -> SENT
        msg.mark_as_sent().unwrap();

        // SENT -> RECALLED
        msg.recall(None).unwrap();
        assert_eq!(msg.fsm_state, MessageFsmState::Recalled);

        // RECALLED 状态下可以软删除
        msg.delete_soft().unwrap();
        assert_eq!(msg.fsm_state, MessageFsmState::DeletedSoft);

        // RECALLED 是终态，不能再撤回或编辑，但可以软删除
        let mut msg2 = Message::new(
            "msg-2".to_string(),
            "conv-1".to_string(),
            "user-1".to_string(),
            "user-2".to_string(),
            b"Hello".to_vec(),
            Utc::now(),
        );
        msg2.mark_as_sent().unwrap();
        msg2.recall(None).unwrap();
        assert!(
            msg2.edit(b"New".to_vec(), "user-1".to_string(), None)
                .is_err()
        );
        assert!(msg2.recall(None).is_err());
    }
}
