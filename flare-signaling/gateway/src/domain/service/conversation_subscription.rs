//! 会话级在线订阅注册表（网关本节点）——统一读扩散的地基。
//!
//! `conversation_id → 本节点订阅该会话的在线连接集合`。一条会话级 publish 命中本节点时，
//! 只需 O(本节点在线成员) 扇出，**与群总人数无关**（10 万群同档）。
//!
//! 语义：
//! - 成员在登录/进入会话时 [`join`]，离开会话/断开时 [`leave`]；断线按 connection_id 全清 [`remove_connection`]。
//! - 幂等：重复 join/leave 安全。
//! - 反向索引 `connection_id → conversations` 让断线清扫为 O(该连接订阅的会话数)，不扫全表。
//! - 纯内存、本节点局部；跨节点由"会话 publish 命中所有有订阅者的节点"达成（上层 MQ 主题/广播）。
//!
//! [`join`]: ConversationSubscriptionRegistry::join
//! [`leave`]: ConversationSubscriptionRegistry::leave
//! [`remove_connection`]: ConversationSubscriptionRegistry::remove_connection

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

/// 网关本节点的会话→在线连接订阅表。
#[derive(Default)]
pub struct ConversationSubscriptionRegistry {
    /// conversation_id → 本节点订阅的连接集合。
    by_conversation: RwLock<HashMap<String, HashSet<String>>>,
    /// connection_id → 其订阅的会话集合（断线清扫反向索引）。
    by_connection: RwLock<HashMap<String, HashSet<String>>>,
}

impl ConversationSubscriptionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 连接订阅会话（幂等）。
    pub fn join(&self, conversation_id: &str, connection_id: &str) {
        if conversation_id.is_empty() || connection_id.is_empty() {
            return;
        }
        self.by_conversation
            .write()
            .expect("conversation subscription lock poisoned")
            .entry(conversation_id.to_string())
            .or_default()
            .insert(connection_id.to_string());
        self.by_connection
            .write()
            .expect("connection index lock poisoned")
            .entry(connection_id.to_string())
            .or_default()
            .insert(conversation_id.to_string());
    }

    /// 连接退订会话（幂等）。集合空则回收键，避免无界增长。
    pub fn leave(&self, conversation_id: &str, connection_id: &str) {
        {
            let mut conv = self
                .by_conversation
                .write()
                .expect("conversation subscription lock poisoned");
            if let Some(set) = conv.get_mut(conversation_id) {
                set.remove(connection_id);
                if set.is_empty() {
                    conv.remove(conversation_id);
                }
            }
        }
        {
            let mut byc = self
                .by_connection
                .write()
                .expect("connection index lock poisoned");
            if let Some(set) = byc.get_mut(connection_id) {
                set.remove(conversation_id);
                if set.is_empty() {
                    byc.remove(connection_id);
                }
            }
        }
    }

    /// 断线：清除该连接的所有会话订阅（O(该连接订阅的会话数)）。
    pub fn remove_connection(&self, connection_id: &str) {
        let conversations = {
            let mut byc = self
                .by_connection
                .write()
                .expect("connection index lock poisoned");
            byc.remove(connection_id).unwrap_or_default()
        };
        if conversations.is_empty() {
            return;
        }
        let mut conv = self
            .by_conversation
            .write()
            .expect("conversation subscription lock poisoned");
        for cid in conversations {
            if let Some(set) = conv.get_mut(&cid) {
                set.remove(connection_id);
                if set.is_empty() {
                    conv.remove(&cid);
                }
            }
        }
    }

    /// 本节点订阅该会话的在线连接（读扩散扇出目标）。
    pub fn local_subscribers(&self, conversation_id: &str) -> Vec<String> {
        self.by_conversation
            .read()
            .expect("conversation subscription lock poisoned")
            .get(conversation_id)
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 本节点该会话的在线订阅数（用于"无订阅则跳过扇出"快判）。
    pub fn local_subscriber_count(&self, conversation_id: &str) -> usize {
        self.by_conversation
            .read()
            .expect("conversation subscription lock poisoned")
            .get(conversation_id)
            .map(HashSet::len)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_is_idempotent_and_listed() {
        let r = ConversationSubscriptionRegistry::new();
        r.join("c1", "conn-a");
        r.join("c1", "conn-a");
        r.join("c1", "conn-b");
        let mut subs = r.local_subscribers("c1");
        subs.sort();
        assert_eq!(subs, vec!["conn-a".to_string(), "conn-b".to_string()]);
        assert_eq!(r.local_subscriber_count("c1"), 2);
    }

    #[test]
    fn leave_removes_and_reclaims_empty_keys() {
        let r = ConversationSubscriptionRegistry::new();
        r.join("c1", "conn-a");
        r.leave("c1", "conn-a");
        assert!(r.local_subscribers("c1").is_empty());
        assert_eq!(r.local_subscriber_count("c1"), 0);
        // 退订空会话/未知连接安全幂等。
        r.leave("c1", "conn-a");
        r.leave("nope", "conn-x");
    }

    #[test]
    fn remove_connection_sweeps_all_conversations() {
        let r = ConversationSubscriptionRegistry::new();
        r.join("c1", "conn-a");
        r.join("c2", "conn-a");
        r.join("c1", "conn-b");
        r.remove_connection("conn-a");
        assert_eq!(r.local_subscribers("c1"), vec!["conn-b".to_string()]);
        assert!(r.local_subscribers("c2").is_empty());
        // 断线未知连接安全。
        r.remove_connection("ghost");
    }

    #[test]
    fn empty_inputs_are_noops() {
        let r = ConversationSubscriptionRegistry::new();
        r.join("", "conn-a");
        r.join("c1", "");
        assert_eq!(r.local_subscriber_count("c1"), 0);
    }
}
