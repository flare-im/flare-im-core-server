//! 推送决策链：一条消息在推送各环节留下的记录，用来回答「这条消息为什么没提示」。
//!
//! 路由、交接、决策、闸门、建任务（push-server）、厂商（push-worker）、到达回执（push-proxy）各写
//! 自己那一环；修正（已读清除、撤回替换）也记在被修正的那条消息上。记录在推送 Redis 里留 24 小时：
//!
//! - `push:trace:{tenant}:{user}:{trace_key}`：LIST，每项一条记录的紧凑 JSON，只留最近 128 条；
//! - `push:trace:idx:{tenant}:{user}`：ZSET，成员是 `trace_key`，分数是第一条记录的时刻，只留最新
//!   200 个。
//!
//! 这里只有纯模型：键、记录与编解码。写入（有界队列、后台批量写）在 service-kit 的
//! `push_trace`，读取在 push-proxy。记录里没有任何推送令牌、凭据或消息内容。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// 记录留多久（秒）。
pub const PUSH_TRACE_TTL_SECS: u64 = 86_400;

/// 一条消息至多留多少条记录（最近的）。大群波次、反复重投也不会让一个键无界增长。
pub const PUSH_TRACE_MAX_ENTRIES: usize = 128;

/// 一个用户的索引至多留多少条消息（最新的）。
pub const PUSH_TRACE_INDEX_LIMIT: usize = 200;

/// 每个写入方都带上的字段：这条记录所属的会话。读取方取第一条记录的它作为整条链的会话。
pub const PUSH_TRACE_CONVERSATION_FIELD: &str = "conv";

/// 推送的环节。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TraceStage {
    /// push-server 路由：设备当时的状态与这一路的来由。字段 `state`、`path`。
    Route,
    /// push-server 处理设备交接。字段 `cause`、`pending`。
    Handoff,
    /// push-server 的提示决策。字段 `class`、`decision`、`reason`、`delay_ms`。
    Decide,
    /// push-server 的防打扰闸门。字段 `result`、`due_ms`、`count`。
    Gate,
    /// push-server 发出的离线任务。字段 `disposition`、`collapse`、`lane`、`badge`。
    Task,
    /// push-worker 调用厂商。字段 `provider`、`outcome`、`latency_ms`、`app_version`、`code`。
    Vendor,
    /// push-proxy 收到的到达回执。字段 `provider`、`path`、`received_at_ms`（设备时钟）、
    /// `app_version`、`disposition`；记录的时刻是服务端收到的时刻。
    Receipt,
    /// push-server 对已发通知的修正。字段 `kind`。
    Correction,
}

impl TraceStage {
    pub const ALL: [TraceStage; 8] = [
        TraceStage::Route,
        TraceStage::Handoff,
        TraceStage::Decide,
        TraceStage::Gate,
        TraceStage::Task,
        TraceStage::Vendor,
        TraceStage::Receipt,
        TraceStage::Correction,
    ];

    /// 编码与展示用的名字。
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Route => "route",
            Self::Handoff => "handoff",
            Self::Decide => "decide",
            Self::Gate => "gate",
            Self::Task => "task",
            Self::Vendor => "vendor",
            Self::Receipt => "receipt",
            Self::Correction => "correction",
        }
    }

    /// 按名字解析；认不出的名字（更新的写入方加的环节）为 `None`。
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|stage| stage.as_str() == name)
    }
}

impl std::fmt::Display for TraceStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 决策链里的一条记录。字段的取值都是字符串，没有的字段表示不知道。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushTraceEntry {
    /// 写下这条记录的时刻（服务端时钟，Unix 毫秒）。
    pub at_ms: i64,
    pub stage: TraceStage,
    /// 所涉及的设备；与具体设备无关（例如用户没有任何推送绑定）时为空。
    pub device_id: String,
    pub fields: BTreeMap<String, String>,
}

/// 编码时借用记录，不复制字段。
#[derive(Serialize)]
struct WireRef<'a> {
    at: i64,
    st: &'a str,
    dev: &'a str,
    f: &'a BTreeMap<String, String>,
}

#[derive(Deserialize)]
struct Wire {
    at: i64,
    st: String,
    #[serde(default)]
    dev: String,
    #[serde(default)]
    f: BTreeMap<String, String>,
}

impl PushTraceEntry {
    pub fn new(at_ms: i64, stage: TraceStage, device_id: impl Into<String>) -> Self {
        Self {
            at_ms,
            stage,
            device_id: device_id.into(),
            fields: BTreeMap::new(),
        }
    }

    /// 加一个字段（同名的覆盖）。
    pub fn with(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    /// 加上所属的会话（[`PUSH_TRACE_CONVERSATION_FIELD`]）。
    pub fn with_conversation(self, conversation_id: impl Into<String>) -> Self {
        self.with(PUSH_TRACE_CONVERSATION_FIELD, conversation_id)
    }

    /// 记录里的会话；没写时为 `None`。
    pub fn conversation_id(&self) -> Option<&str> {
        self.fields
            .get(PUSH_TRACE_CONVERSATION_FIELD)
            .map(String::as_str)
    }

    /// 紧凑 JSON：`{"at":..,"st":"..","dev":"..","f":{..}}`。
    pub fn encode(&self) -> String {
        serde_json::to_string(&WireRef {
            at: self.at_ms,
            st: self.stage.as_str(),
            dev: &self.device_id,
            f: &self.fields,
        })
        .unwrap_or_default()
    }

    /// 解码一条记录。读不懂的 JSON 与认不出的环节为 `None`，读取方跳过它。
    pub fn decode(raw: &str) -> Option<Self> {
        let wire: Wire = serde_json::from_str(raw).ok()?;
        Some(Self {
            at_ms: wire.at,
            stage: TraceStage::parse(&wire.st)?,
            device_id: wire.dev,
            fields: wire.f,
        })
    }
}

/// 一条消息的决策链的键：消息 ID；没有消息 ID 时（补提示读不回消息、只有会话与 seq）是
/// `"{conversation_id}#{seq}"`。各环节对同一条消息必须算出同一个键。
pub fn trace_key(message_id: &str, conversation_id: &str, seq: u64) -> String {
    let message_id = message_id.trim();
    if message_id.is_empty() {
        format!("{}#{seq}", conversation_id.trim())
    } else {
        message_id.to_string()
    }
}

/// 一条消息的记录列表：`push:trace:{tenant}:{user}:{trace_key}`。
pub fn trace_list_key(tenant_id: &str, user_id: &str, trace_key: &str) -> String {
    format!("push:trace:{tenant_id}:{user_id}:{trace_key}")
}

/// 一个用户的消息索引：`push:trace:idx:{tenant}:{user}`。
pub fn trace_index_key(tenant_id: &str, user_id: &str) -> String {
    format!("push:trace:idx:{tenant_id}:{user_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_is_the_message_id_or_conversation_and_seq() {
        assert_eq!(trace_key("m-9", "c-1", 7), "m-9");
        assert_eq!(trace_key(" m-9 ", "c-1", 7), "m-9");
        assert_eq!(trace_key("", "c-1", 7), "c-1#7");
        assert_eq!(trace_key("  ", "c-1", 0), "c-1#0");
    }

    #[test]
    fn redis_keys_follow_the_documented_layout() {
        assert_eq!(
            trace_list_key("t-1", "u-1", "m-9"),
            "push:trace:t-1:u-1:m-9"
        );
        assert_eq!(trace_index_key("t-1", "u-1"), "push:trace:idx:t-1:u-1");
    }

    #[test]
    fn an_entry_encodes_compactly_and_decodes_back() {
        let entry = PushTraceEntry::new(1_731_400_000_000, TraceStage::Route, "d-1")
            .with_conversation("c-1")
            .with("state", "parked")
            .with("path", "direct");
        let raw = entry.encode();
        assert_eq!(
            raw,
            r#"{"at":1731400000000,"st":"route","dev":"d-1","f":{"conv":"c-1","path":"direct","state":"parked"}}"#
        );
        let decoded = PushTraceEntry::decode(&raw).expect("decodes");
        assert_eq!(decoded, entry);
        assert_eq!(decoded.conversation_id(), Some("c-1"));
    }

    #[test]
    fn missing_device_and_fields_decode_as_empty() {
        let decoded = PushTraceEntry::decode(r#"{"at":5,"st":"receipt"}"#).expect("decodes");
        assert_eq!(decoded.at_ms, 5);
        assert_eq!(decoded.stage, TraceStage::Receipt);
        assert!(decoded.device_id.is_empty());
        assert!(decoded.fields.is_empty());
        assert_eq!(decoded.conversation_id(), None);
    }

    #[test]
    fn unknown_stages_and_broken_json_are_skipped() {
        assert!(PushTraceEntry::decode(r#"{"at":5,"st":"teleport","dev":"d"}"#).is_none());
        assert!(PushTraceEntry::decode("not json").is_none());
        assert!(PushTraceEntry::decode(r#"{"st":"route"}"#).is_none());
    }

    #[test]
    fn every_stage_round_trips_by_name() {
        for stage in TraceStage::ALL {
            assert_eq!(TraceStage::parse(stage.as_str()), Some(stage));
            assert_eq!(stage.to_string(), stage.as_str());
        }
        assert_eq!(TraceStage::parse("Route"), None);
    }
}
