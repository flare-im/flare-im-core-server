//! 会话级在线订阅注册表（网关本节点）——统一读扩散的地基。
//!
//! `(tenant_id, conversation_id) → 本节点订阅该会话的在线连接集合`。一条会话级 publish 命中本节点时，
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
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ConversationKey(String, String);

#[derive(Default)]
struct SubscriptionState {
    by_conversation: HashMap<ConversationKey, HashSet<String>>,
    by_connection: HashMap<String, HashSet<ConversationKey>>,
}

/// 网关本节点的会话→在线连接订阅表。
#[derive(Default)]
pub struct ConversationSubscriptionRegistry {
    /// 两个索引在同一把锁下原子更新，断线不会落在 join 两次写入的中间态。
    state: RwLock<SubscriptionState>,
}

/// 长驻服务路径不 panic：锁中毒（持锁线程 panic）时恢复内层数据继续服务——
/// 订阅表操作均为幂等集合增删，部分状态可被后续 join/leave/断线清扫自愈。
fn write_recovered(lock: &RwLock<SubscriptionState>) -> RwLockWriteGuard<'_, SubscriptionState> {
    lock.write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn read_recovered(lock: &RwLock<SubscriptionState>) -> RwLockReadGuard<'_, SubscriptionState> {
    lock.read().unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl ConversationSubscriptionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// 连接订阅会话（幂等）。
    pub fn join(&self, tenant_id: &str, conversation_id: &str, connection_id: &str) {
        if !canonical_tenant(tenant_id) || conversation_id.is_empty() || connection_id.is_empty() {
            return;
        }
        let key = ConversationKey(tenant_id.to_string(), conversation_id.to_string());
        let mut state = write_recovered(&self.state);
        state
            .by_conversation
            .entry(key.clone())
            .or_default()
            .insert(connection_id.to_string());
        state
            .by_connection
            .entry(connection_id.to_string())
            .or_default()
            .insert(key);
    }

    /// 连接退订会话（幂等）。集合空则回收键，避免无界增长。
    pub fn leave(&self, tenant_id: &str, conversation_id: &str, connection_id: &str) {
        if !canonical_tenant(tenant_id) {
            return;
        }
        let key = ConversationKey(tenant_id.to_string(), conversation_id.to_string());
        let mut state = write_recovered(&self.state);
        if let Some(set) = state.by_conversation.get_mut(&key) {
            set.remove(connection_id);
            if set.is_empty() {
                state.by_conversation.remove(&key);
            }
        }
        if let Some(set) = state.by_connection.get_mut(connection_id) {
            set.remove(&key);
            if set.is_empty() {
                state.by_connection.remove(connection_id);
            }
        }
    }

    /// 断线：清除该连接的所有会话订阅（O(该连接订阅的会话数)）。
    pub fn remove_connection(&self, connection_id: &str) {
        let mut state = write_recovered(&self.state);
        let conversations = state
            .by_connection
            .remove(connection_id)
            .unwrap_or_default();
        if conversations.is_empty() {
            // 断连时该连接在反向索引里没有记录。正常路径下不该出现——
            // 它意味着 join 只写了 by_conversation 没写 by_connection，
            // 或者 remove 被调了两次。任其静默会让 by_conversation 里
            // 留下永不清除的陈旧 connection_id：每次投递都要遍历它们，
            // 且订阅数随重连单调增长。
            tracing::debug!(
                connection_id = %connection_id,
                "断连清理：该连接没有订阅记录（可能已清理过）"
            );
            return;
        }
        tracing::info!(
            connection_id = %connection_id,
            conversations = conversations.len(),
            "断连清理：移除该连接的会话订阅"
        );
        for cid in conversations {
            if let Some(set) = state.by_conversation.get_mut(&cid) {
                set.remove(connection_id);
                if set.is_empty() {
                    state.by_conversation.remove(&cid);
                }
            }
        }
    }

    /// 本节点订阅该会话的在线连接（读扩散扇出目标）。
    pub fn local_subscribers(&self, tenant_id: &str, conversation_id: &str) -> Vec<String> {
        if !canonical_tenant(tenant_id) {
            return Vec::new();
        }
        read_recovered(&self.state)
            .by_conversation
            .get(&ConversationKey(
                tenant_id.to_string(),
                conversation_id.to_string(),
            ))
            .map(|set| set.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// 本节点该会话的在线订阅数（用于"无订阅则跳过扇出"快判）。
    pub fn local_subscriber_count(&self, tenant_id: &str, conversation_id: &str) -> usize {
        if !canonical_tenant(tenant_id) {
            return 0;
        }
        read_recovered(&self.state)
            .by_conversation
            .get(&ConversationKey(
                tenant_id.to_string(),
                conversation_id.to_string(),
            ))
            .map(HashSet::len)
            .unwrap_or(0)
    }
}

fn canonical_tenant(tenant_id: &str) -> bool {
    flare_im_contracts::domain::tenant::is_valid_tenant_id(tenant_id)
        && flare_im_contracts::utils::normalize_tenant_id(tenant_id) == tenant_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_is_idempotent_and_listed() {
        let r = ConversationSubscriptionRegistry::new();
        r.join("t1", "c1", "conn-a");
        r.join("t1", "c1", "conn-a");
        r.join("t1", "c1", "conn-b");
        let mut subs = r.local_subscribers("t1", "c1");
        subs.sort();
        assert_eq!(subs, vec!["conn-a".to_string(), "conn-b".to_string()]);
        assert_eq!(r.local_subscriber_count("t1", "c1"), 2);
    }

    #[test]
    fn leave_removes_and_reclaims_empty_keys() {
        let r = ConversationSubscriptionRegistry::new();
        r.join("t1", "c1", "conn-a");
        r.leave("t1", "c1", "conn-a");
        assert!(r.local_subscribers("t1", "c1").is_empty());
        assert_eq!(r.local_subscriber_count("t1", "c1"), 0);
        // 退订空会话/未知连接安全幂等。
        r.leave("t1", "c1", "conn-a");
        r.leave("t1", "nope", "conn-x");
    }

    #[test]
    fn remove_connection_sweeps_all_conversations() {
        let r = ConversationSubscriptionRegistry::new();
        r.join("t1", "c1", "conn-a");
        r.join("t1", "c2", "conn-a");
        r.join("t1", "c1", "conn-b");
        r.remove_connection("conn-a");
        assert_eq!(r.local_subscribers("t1", "c1"), vec!["conn-b".to_string()]);
        assert!(r.local_subscribers("t1", "c2").is_empty());
        // 断线未知连接安全。
        r.remove_connection("ghost");
    }

    #[test]
    fn empty_inputs_are_noops() {
        let r = ConversationSubscriptionRegistry::new();
        r.join("t1", "", "conn-a");
        r.join("t1", "c1", "");
        r.join("", "c1", "conn-a");
        r.join("default", "c1", "conn-a");
        r.join("invalid:tenant", "c1", "conn-a");
        assert_eq!(r.local_subscriber_count("t1", "c1"), 0);
    }

    #[test]
    fn same_conversation_id_is_isolated_by_tenant_and_reverse_cleanup() {
        let r = ConversationSubscriptionRegistry::new();
        r.join("t1", "same", "conn-a");
        r.join("t2", "same", "conn-b");
        assert_eq!(r.local_subscribers("t1", "same"), ["conn-a"]);
        assert_eq!(r.local_subscribers("t2", "same"), ["conn-b"]);
        r.remove_connection("conn-a");
        assert!(r.local_subscribers("t1", "same").is_empty());
        assert_eq!(r.local_subscribers("t2", "same"), ["conn-b"]);
    }
}
