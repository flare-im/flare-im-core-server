//! Redis 消息热缓存的键约定。
//!
//! storage-writer 落库后写入（单条消息 + 按 seq 排序的会话尾部索引），storage-reader
//! 按 seq 同步时优先读取；会话服务物理清除历史之后要按同一组键失效。三方各自拼字符串
//! 会在改格式时悄悄错开——失效打不中，读侧就继续返回已经删掉的消息——所以集中在这里。

/// 单条消息缓存（base64 编码的 protobuf `Message`）。
pub fn message_cache_key(tenant_id: &str, conversation_id: &str, message_id: &str) -> String {
    format!("cache:msg:{tenant_id}:{conversation_id}:{message_id}")
}

/// 会话尾部索引：ZSET，member = 消息 server_id，score = 会话 seq。
pub fn message_tail_key(tenant_id: &str, conversation_id: &str) -> String {
    format!("cache:tail:{tenant_id}:{conversation_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_tenant_and_conversation_scoped() {
        assert_eq!(
            message_cache_key("tenant-a", "conv-a", "msg-a"),
            "cache:msg:tenant-a:conv-a:msg-a"
        );
        assert_eq!(
            message_tail_key("tenant-a", "conv-a"),
            "cache:tail:tenant-a:conv-a"
        );
    }
}
