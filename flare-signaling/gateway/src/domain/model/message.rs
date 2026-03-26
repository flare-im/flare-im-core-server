//! 消息值对象
//!
//! 封装消息数据,提供消息的基本信息和内容访问。

/// 消息值对象
///
/// 封装消息的基本信息和内容,作为消息传递的值对象。
/// 值对象是不可变的,通过属性值标识。
#[derive(Debug, Clone)]
pub struct MessageWrapper {
    /// 消息ID
    pub message_id: String,
    /// 连接ID
    pub connection_id: String,
    /// 用户ID
    pub user_id: String,
    /// 消息内容(原始字节)
    pub content: Vec<u8>,
    /// 消息类型
    pub message_type: i32,
    /// 发送时间戳
    pub timestamp: i64,
}

impl MessageWrapper {
    /// 创建新的消息包装器
    ///
    /// # 参数
    /// - `message_id`: 消息唯一标识符
    /// - `connection_id`: 连接ID
    /// - `user_id`: 用户ID
    /// - `content`: 消息内容(原始字节)
    /// - `message_type`: 消息类型
    /// - `timestamp`: 发送时间戳(毫秒)
    pub fn new(
        message_id: String,
        connection_id: String,
        user_id: String,
        content: Vec<u8>,
        message_type: i32,
        timestamp: i64,
    ) -> Self {
        Self {
            message_id,
            connection_id,
            user_id,
            content,
            message_type,
            timestamp,
        }
    }

    /// 创建当前时间戳的消息包装器
    ///
    /// 使用当前时间作为时间戳。
    pub fn new_with_current_time(
        message_id: String,
        connection_id: String,
        user_id: String,
        content: Vec<u8>,
        message_type: i32,
    ) -> Self {
        let timestamp = chrono::Utc::now().timestamp_millis();
        Self::new(
            message_id,
            connection_id,
            user_id,
            content,
            message_type,
            timestamp,
        )
    }

    /// 获取消息大小(字节)
    pub fn size(&self) -> usize {
        self.content.len()
    }

    /// 检查消息是否为空
    pub fn is_empty(&self) -> bool {
        self.content.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_message_wrapper() {
        let message = MessageWrapper::new(
            "msg123".to_string(),
            "conn123".to_string(),
            "user123".to_string(),
            b"hello world".to_vec(),
            1,
            1234567890,
        );

        assert_eq!(message.message_id, "msg123");
        assert_eq!(message.connection_id, "conn123");
        assert_eq!(message.user_id, "user123");
        assert_eq!(message.content, b"hello world");
        assert_eq!(message.message_type, 1);
        assert_eq!(message.timestamp, 1234567890);
    }

    #[test]
    fn test_new_message_wrapper_with_current_time() {
        let message = MessageWrapper::new_with_current_time(
            "msg123".to_string(),
            "conn123".to_string(),
            "user123".to_string(),
            b"hello world".to_vec(),
            1,
        );

        assert_eq!(message.message_id, "msg123");
        assert_eq!(message.connection_id, "conn123");
        assert_eq!(message.user_id, "user123");
        assert_eq!(message.content, b"hello world");
        assert_eq!(message.message_type, 1);
        assert!(message.timestamp > 0);
    }

    #[test]
    fn test_message_size() {
        let message = MessageWrapper::new(
            "msg123".to_string(),
            "conn123".to_string(),
            "user123".to_string(),
            b"hello world".to_vec(),
            1,
            1234567890,
        );

        assert_eq!(message.size(), 11);
    }

    #[test]
    fn test_message_is_empty() {
        let empty_message = MessageWrapper::new(
            "msg123".to_string(),
            "conn123".to_string(),
            "user123".to_string(),
            vec![],
            1,
            1234567890,
        );
        assert!(empty_message.is_empty());

        let non_empty_message = MessageWrapper::new(
            "msg123".to_string(),
            "conn123".to_string(),
            "user123".to_string(),
            b"hello".to_vec(),
            1,
            1234567890,
        );
        assert!(!non_empty_message.is_empty());
    }

    #[test]
    fn test_message_wrapper_clone() {
        let message = MessageWrapper::new(
            "msg123".to_string(),
            "conn123".to_string(),
            "user123".to_string(),
            b"hello world".to_vec(),
            1,
            1234567890,
        );

        let cloned = message.clone();
        assert_eq!(cloned.message_id, message.message_id);
        assert_eq!(cloned.connection_id, message.connection_id);
        assert_eq!(cloned.user_id, message.user_id);
        assert_eq!(cloned.content, message.content);
    }
}
