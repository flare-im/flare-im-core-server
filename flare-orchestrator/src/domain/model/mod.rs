pub mod message_kind;
pub mod message_submission;

pub use message_kind::{MessageProfile, notification_persistent};
pub use message_submission::{MessageDefaults, MessageSubmission};

/// 会话类型枚举（与 proto ConversationType、数据库 conversation_type INT 对齐）
///
/// 枚举值与 CID TypePrefix 一致：
/// - SINGLE = 1 (CID 前缀 1)
/// - GROUP = 2 (CID 前缀 2)
/// - AI = 3 (CID 前缀 3)
/// - SYSTEM = 4 (CID 前缀 4)
/// - CUSTOMER = 5 (CID 前缀 5)
/// - TEMP = 6 (CID 前缀 6)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConversationType {
    Unspecified = 0,
    Single = 1,
    Group = 2,
    Ai = 3,
    System = 4,
    Customer = 5,
    Temp = 6,
}

impl ConversationType {
    /// 从 proto 枚举值转换
    pub fn from_proto(value: i32) -> Self {
        use flare_proto::common::ConversationType as ProtoConversationType;
        match ProtoConversationType::try_from(value).ok() {
            Some(ProtoConversationType::Single) => Self::Single,
            Some(ProtoConversationType::Group) => Self::Group,
            Some(ProtoConversationType::Ai) => Self::Ai,
            Some(ProtoConversationType::System) => Self::System,
            Some(ProtoConversationType::Customer) => Self::Customer,
            Some(ProtoConversationType::Temp) => Self::Temp,
            _ => Self::Unspecified,
        }
    }

    /// 转换为 proto 枚举值
    pub fn as_proto(&self) -> i32 {
        use flare_proto::common::ConversationType as ProtoConversationType;
        match self {
            ConversationType::Unspecified => ProtoConversationType::Unspecified as i32,
            ConversationType::Single => ProtoConversationType::Single as i32,
            ConversationType::Group => ProtoConversationType::Group as i32,
            ConversationType::Ai => ProtoConversationType::Ai as i32,
            ConversationType::System => ProtoConversationType::System as i32,
            ConversationType::Customer => ProtoConversationType::Customer as i32,
            ConversationType::Temp => ProtoConversationType::Temp as i32,
        }
    }

    /// 从数据库 INT 值转换
    pub fn from_int(value: i32) -> Self {
        match value {
            1 => Self::Single,
            2 => Self::Group,
            3 => Self::Ai,
            4 => Self::System,
            5 => Self::Customer,
            6 => Self::Temp,
            _ => Self::Unspecified,
        }
    }

    /// 转换为数据库 INT 值
    pub fn as_int(&self) -> i32 {
        *self as i32
    }
}
