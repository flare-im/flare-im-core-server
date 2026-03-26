//! 操作分类器
//!
//! 基于 EventType 判断操作是否需要编排、委托、Kafka 或推送。
//! 与 common/event.proto 的 EventType 对齐。

use flare_proto::common::EventType;

/// 操作分类器
pub struct OperationClassifier;

impl OperationClassifier {
    /// 判断操作是否需要编排（权限、状态、事件、Hook）
    pub fn requires_orchestration(operation_type: i32) -> bool {
        matches!(
            EventType::try_from(operation_type).ok(),
            Some(EventType::EventMessageRecall) | Some(EventType::EventMessageEdit)
        )
    }

    /// 判断操作是否可直接委托给 Storage
    pub fn can_delegate_directly(operation_type: i32) -> bool {
        matches!(
            EventType::try_from(operation_type).ok(),
            Some(EventType::EventReadReceipt)
                | Some(EventType::EventReaction)
                | Some(EventType::EventPin)
                | Some(EventType::EventUnpin)
                | Some(EventType::EventMark)
                | Some(EventType::EventMessageDelete)
                | Some(EventType::EventUnmark)
        )
    }

    /// 判断操作是否需要创建新消息
    pub fn requires_message_creation(_operation_type: i32) -> bool {
        false
    }

    /// 获取操作类别
    pub fn get_operation_category(operation_type: i32) -> OperationCategory {
        if Self::requires_orchestration(operation_type) {
            OperationCategory::Complex
        } else if Self::can_delegate_directly(operation_type) {
            OperationCategory::Simple
        } else {
            OperationCategory::Unknown
        }
    }

    /// 判断操作是否需要 Kafka 发布（Event 已统一发往 operation topic）
    pub fn requires_kafka(operation_type: i32) -> bool {
        match EventType::try_from(operation_type) {
            Ok(EventType::EventMessageRecall) | Ok(EventType::EventMessageEdit) => true,
            Ok(EventType::EventMessageDelete)
            | Ok(EventType::EventReadReceipt)
            | Ok(EventType::EventReaction)
            | Ok(EventType::EventPin)
            | Ok(EventType::EventUnpin) => true,
            Ok(EventType::EventMark) => false,
            Ok(EventType::EventUnmark) => true,
            _ => false,
        }
    }

    /// 判断操作是否需要推送通知
    pub fn requires_push_notification(operation_type: i32) -> bool {
        match EventType::try_from(operation_type) {
            Ok(EventType::EventMessageRecall)
            | Ok(EventType::EventMessageEdit)
            | Ok(EventType::EventMessageDelete)
            | Ok(EventType::EventReadReceipt)
            | Ok(EventType::EventReaction)
            | Ok(EventType::EventPin)
            | Ok(EventType::EventUnpin) => true,
            Ok(EventType::EventMark) | Ok(EventType::EventUnmark) => false,
            _ => false,
        }
    }

    /// 判断撤回操作是否为仅自己撤回（不需要 Kafka）
    pub fn is_recall_self_only(metadata: &std::collections::HashMap<String, String>) -> bool {
        metadata.get("scope").map(|s| s.as_str()) == Some("self")
    }
}

/// 操作类别
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationCategory {
    Simple,
    Complex,
    Unknown,
}
