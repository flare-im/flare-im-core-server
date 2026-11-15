//! 热会话波次：群会话的离线推送按会话限频枚举接收者。
//!
//! 群消息不物化收件人，离线推送要枚举成员；每条消息枚举一次的话，活跃群的成本随消息量线性增长。
//! 波次把枚举改成按会话限频：
//!
//! - 会话空闲时来的第一条当场枚举，没有额外延迟，同时开始一个间隔。
//! - 间隔进行中来的消息只在会话级累加：条数、最高的提示等级、最新一条、这期间已经直接推过的人；
//!   间隔结束时由到期队列触发，枚举一次。
//! - 间隔按 [`WAVE_LADDER_MS`] 自适应：间隔里来过消息就升一档，安静过一个完整间隔就降一档。
//! - 被点名的成员不等波次，由路由直接按人推送；紧急与 @所有人 结束当前间隔、当场枚举。
//! - 超过成员上限的群里，@所有人 是一次全员波次，按会话限频；超出配额按普通消息处理。
//!
//! 这里只有纯计算：时刻由调用方传入，状态由调用方读出、比较版本后写回。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::GateStoreOrigin;

/// 波次间隔的阶梯（毫秒）。
pub const WAVE_LADDER_MS: [i64; 4] = [3_000, 10_000, 30_000, 120_000];

/// 一个间隔里最多为多少人记「已经直接推过的条数」。再多的人不记，至多让他们的条数多算几条。
pub const WAVE_EXCLUDED_CAP: usize = 256;

/// 超过成员上限的群里，@所有人 全员波次的缺省配额：每个会话每 10 分钟至多一次。
pub const DEFAULT_ALL_MENTION_INTERVAL_MS: i64 = 10 * 60 * 1000;

/// 波次状态比它要管的最长时间（间隔、@所有人 配额）多留多久。过期只会让档位回到最低、配额重新可用。
const WAVE_STATE_TTL_MARGIN_MS: i64 = 10 * 60 * 1000;

fn top_level() -> u8 {
    (WAVE_LADDER_MS.len() - 1) as u8
}

/// 某一档的间隔（毫秒）。档位越界按最高档。
pub fn wave_interval_ms(level: u8) -> i64 {
    WAVE_LADDER_MS[usize::from(level.min(top_level()))]
}

fn level_up(level: u8) -> u8 {
    level.saturating_add(1).min(top_level())
}

/// 从 `level` 那一档的间隔开始算，安静了 `quiet_ms` 之后的档位：每安静过当前档一个完整间隔降一档。
fn level_after_quiet(level: u8, quiet_ms: i64) -> u8 {
    let mut level = level.min(top_level());
    let mut rest = quiet_ms;
    while level > 0 {
        let step = wave_interval_ms(level);
        if rest < step {
            break;
        }
        rest -= step;
        level -= 1;
    }
    level
}

/// 波次状态在存储里保留多久。
pub fn wave_state_ttl_ms(all_mention_interval_ms: i64) -> i64 {
    all_mention_interval_ms
        .max(wave_interval_ms(top_level()))
        .saturating_add(WAVE_STATE_TTL_MARGIN_MS)
}

/// 一个会话的波次所属：租户与会话。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WaveKey {
    pub tenant_id: String,
    pub conversation_id: String,
}

impl WaveKey {
    pub fn new(tenant_id: impl Into<String>, conversation_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            conversation_id: conversation_id.into(),
        }
    }
}

/// 到期队列里领取出来的一个波次。
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WaveDue {
    pub due_ms: i64,
    pub key: WaveKey,
    /// 登记在哪个状态存储里，到期时回到同一个存储处理。
    pub origin: GateStoreOrigin,
}

/// 波次记下的一条消息。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaveMessage {
    /// 服务端消息 ID。
    #[serde(rename = "m", default)]
    pub message_id: String,
    /// 会话 seq。
    #[serde(rename = "s", default)]
    pub seq: u64,
    /// 发送者。
    #[serde(rename = "f", default)]
    pub sender_id: String,
}

impl WaveMessage {
    /// `other` 是否不比这条旧。seq 为 0 时分不出先后，按新的算。
    fn superseded_by(&self, other: &WaveMessage) -> bool {
        other.seq == 0 || other.seq >= self.seq
    }
}

/// 这条消息怎么进波次。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaveTrigger {
    /// 普通消息：会话空闲时当场枚举，间隔进行中只累加。
    Normal,
    /// 紧急：不等间隔，当场枚举。
    Now,
    /// @所有人，成员不超过上限：当场枚举，人人按点名提示。
    AllMembers,
    /// @所有人，超过成员上限：全员波次，每个会话每 `interval_ms` 至多一次；超出配额按普通消息处理。
    AllEveryone { interval_ms: i64 },
}

/// 一次枚举里 @所有人 的范围。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MentionAll {
    /// 没有 @所有人，或超出了配额。
    #[default]
    No,
    /// 成员不超过上限：枚举出的全体成员都按点名提示。
    Members,
    /// 超过成员上限：分页流式枚举全体成员，人人按点名提示。
    Everyone,
}

/// 一条消息进波次的输入。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaveArrival<'a> {
    pub message: WaveMessage,
    /// 对没被点名的人的提示等级（协议 `AlertClass` 的取值）。
    pub class: i32,
    /// 消息的提及解析不出（富文本等）：「只接收@我」的人照常提示。
    pub mentions_unknown: bool,
    /// 这条消息已经直接按人推过的成员（被点名的人）。
    pub direct: &'a [String],
    pub trigger: WaveTrigger,
}

/// 一次枚举要推的内容。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WaveBatch {
    /// 这一批代表的消息条数。
    pub count: u32,
    /// 这一批里最高的提示等级（协议取值）。
    pub class: i32,
    /// 通知展示的那条：一般是这一批里最新的一条；@所有人 与紧急当场枚举时是触发它的那条。
    pub newest: WaveMessage,
    pub mentions_unknown: bool,
    /// 这一批里已经推过、或不该推给本人的条数：被点名直接推过的、自己发的。
    pub excluded: BTreeMap<String, u32>,
    pub mention_all: MentionAll,
}

impl WaveBatch {
    /// 只有这一条消息的批次：没有波次状态可用时当场枚举。超过成员上限的 @所有人 拿不到配额，
    /// 按普通消息处理：状态用不上时宁可少推，也不每条都全员扇出。
    pub fn single(arrival: &WaveArrival<'_>) -> Self {
        let mention_all = match arrival.trigger {
            WaveTrigger::AllMembers => MentionAll::Members,
            _ => MentionAll::No,
        };
        let mut excluded = BTreeMap::new();
        exclude(&mut excluded, &arrival.message.sender_id, 1);
        if mention_all == MentionAll::No {
            for user_id in arrival.direct {
                exclude(&mut excluded, user_id, 1);
            }
        }
        Self {
            count: 1,
            class: arrival.class,
            newest: arrival.message.clone(),
            mentions_unknown: arrival.mentions_unknown,
            excluded,
            mention_all,
        }
    }

    /// 这个人在这一批里要按几条推；0 表示不推。
    pub fn times_for(&self, user_id: &str) -> u32 {
        self.count
            .saturating_sub(self.excluded.get(user_id).copied().unwrap_or(0))
    }

    /// 全员波次里不是订阅者的人只为这条 @所有人 推一条；发它的人自己不推。
    pub fn times_for_non_subscriber(&self, user_id: &str) -> u32 {
        u32::from(user_id != self.newest.sender_id)
    }
}

/// 记一个人在这一批里已经推过的条数，人数到上限后不再记新的人。
fn exclude(excluded: &mut BTreeMap<String, u32>, user_id: &str, times: u32) {
    if user_id.is_empty() {
        return;
    }
    if let Some(held) = excluded.get_mut(user_id) {
        *held = held.saturating_add(times);
    } else if excluded.len() < WAVE_EXCLUDED_CAP {
        excluded.insert(user_id.to_string(), times);
    }
}

/// 一条消息进波次的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Arrived {
    /// 现在枚举。`newest_is_arrival` 为真时这一批展示的就是刚到的这条。
    Flush {
        batch: WaveBatch,
        newest_is_arrival: bool,
    },
    /// 累加进正在进行的间隔，到 `due_ms` 时枚举。`first` 表示这是这个间隔里第一条累加的消息。
    Held {
        due_ms: i64,
        first: bool,
        newest_is_arrival: bool,
    },
}

/// 波次到期的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaveDueOutcome {
    /// 枚举这一批。
    Flush(WaveBatch),
    /// 还没到（实例之间时钟有偏差）：按这个时刻重新登记。
    Reschedule(i64),
    /// 没有累加的消息：已经被后来的消息当场枚举过，或是重复的到期条目。
    Nothing,
}

/// 一个会话的波次状态，存在推送 Redis 里（JSON，与版本号、最新一条消息的正文分开存）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WaveState {
    /// 当前间隔的档位。
    #[serde(rename = "l", default)]
    pub level: u8,
    /// 当前间隔结束的时刻（毫秒）；从没枚举过为 0。
    #[serde(rename = "u", default)]
    pub open_until_ms: i64,
    /// 间隔里累加的条数。
    #[serde(rename = "n", default)]
    pub pending: u32,
    /// 累加的消息里最高的提示等级（协议取值）。
    #[serde(rename = "a", default)]
    pub class: i32,
    /// 累加的消息里最新的一条。
    #[serde(rename = "w", default)]
    pub newest: WaveMessage,
    #[serde(rename = "q", default)]
    pub mentions_unknown: bool,
    /// 累加期间已经直接推过的条数，按人。
    #[serde(rename = "x", default)]
    pub excluded: BTreeMap<String, u32>,
    /// 上一次全员 @所有人 波次的时刻（毫秒）。
    #[serde(rename = "e", default)]
    pub all_everyone_at_ms: i64,
}

impl WaveState {
    /// 解码。解不出就当作没有状态：档位回到最低，配额重新可用。
    pub fn decode(bytes: Option<&[u8]>) -> Self {
        bytes
            .and_then(|bytes| serde_json::from_slice(bytes).ok())
            .unwrap_or_default()
    }

    pub fn encode(&self) -> Vec<u8> {
        // 只含整数、字符串与映射，序列化不会失败；万一失败就写空状态。
        serde_json::to_vec(self).unwrap_or_default()
    }

    /// 一条消息到了：当场枚举，或累加进正在进行的间隔。
    pub fn arrive(&mut self, now_ms: i64, arrival: &WaveArrival<'_>) -> Arrived {
        let mention_all = match arrival.trigger {
            WaveTrigger::AllMembers => MentionAll::Members,
            WaveTrigger::AllEveryone { interval_ms }
                if self.all_everyone_at_ms <= 0
                    || now_ms.saturating_sub(self.all_everyone_at_ms) >= interval_ms =>
            {
                MentionAll::Everyone
            }
            _ => MentionAll::No,
        };
        let forced = arrival.trigger == WaveTrigger::Now || mention_all != MentionAll::No;
        let idle = now_ms >= self.open_until_ms;

        // 先把这条算进累加里：当场枚举时它和间隔里攒下的一起出去。
        let newest_is_arrival =
            forced || self.pending == 0 || self.newest.superseded_by(&arrival.message);
        self.pending = self.pending.saturating_add(1);
        self.class = self.class.max(arrival.class);
        if newest_is_arrival {
            self.newest = arrival.message.clone();
        }
        self.mentions_unknown |= arrival.mentions_unknown;
        exclude(&mut self.excluded, &arrival.message.sender_id, 1);
        // 人人按点名提示时，被点名的人不另外直接推，不能从这一批里减掉。
        if mention_all == MentionAll::No {
            for user_id in arrival.direct {
                exclude(&mut self.excluded, user_id, 1);
            }
        }

        if !idle && !forced {
            return Arrived::Held {
                due_ms: self.open_until_ms,
                first: self.pending == 1,
                newest_is_arrival,
            };
        }

        self.level = if !idle || self.pending > 1 {
            // 间隔还没结束就要枚举，或间隔已过而攒下的还没被到期循环枚举：消息来得比间隔快。
            level_up(self.level)
        } else if self.open_until_ms > 0 {
            let started = self.open_until_ms - wave_interval_ms(self.level);
            level_after_quiet(self.level, now_ms.saturating_sub(started))
        } else {
            0
        };
        if mention_all == MentionAll::Everyone {
            self.all_everyone_at_ms = now_ms;
        }
        let batch = self.take(mention_all);
        self.open_until_ms = now_ms.saturating_add(wave_interval_ms(self.level));
        Arrived::Flush {
            batch,
            newest_is_arrival,
        }
    }

    /// 间隔结束：有累加就枚举一次，并开始下一个更长的间隔。
    pub fn due(&mut self, now_ms: i64) -> WaveDueOutcome {
        if self.pending == 0 {
            return WaveDueOutcome::Nothing;
        }
        if now_ms < self.open_until_ms {
            return WaveDueOutcome::Reschedule(self.open_until_ms);
        }
        self.level = level_up(self.level);
        let batch = self.take(MentionAll::No);
        self.open_until_ms = now_ms.saturating_add(wave_interval_ms(self.level));
        WaveDueOutcome::Flush(batch)
    }

    fn take(&mut self, mention_all: MentionAll) -> WaveBatch {
        let batch = WaveBatch {
            count: self.pending.max(1),
            class: self.class,
            newest: std::mem::take(&mut self.newest),
            mentions_unknown: self.mentions_unknown,
            excluded: std::mem::take(&mut self.excluded),
            mention_all,
        };
        self.pending = 0;
        self.class = 0;
        self.mentions_unknown = false;
        batch
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(seq: u64, sender: &str) -> WaveMessage {
        WaveMessage {
            message_id: format!("m-{seq}"),
            seq,
            sender_id: sender.to_string(),
        }
    }

    fn arrival<'a>(
        seq: u64,
        sender: &str,
        direct: &'a [String],
        trigger: WaveTrigger,
    ) -> WaveArrival<'a> {
        WaveArrival {
            message: message(seq, sender),
            class: 3,
            mentions_unknown: false,
            direct,
            trigger,
        }
    }

    fn flushed(arrived: Arrived) -> WaveBatch {
        match arrived {
            Arrived::Flush { batch, .. } => batch,
            other => panic!("expected a flush, got {other:?}"),
        }
    }

    /// 空闲会话的第一条当场枚举；间隔里的只累加，到期时一起枚举一次，条数是全部。
    #[test]
    fn the_first_message_goes_now_and_a_burst_waits_for_the_interval() {
        let mut state = WaveState::default();
        let batch = flushed(state.arrive(0, &arrival(1, "bob", &[], WaveTrigger::Normal)));
        assert_eq!((batch.count, batch.newest.seq), (1, 1));
        assert_eq!(state.open_until_ms, 3_000);

        assert_eq!(
            state.arrive(100, &arrival(2, "bob", &[], WaveTrigger::Normal)),
            Arrived::Held {
                due_ms: 3_000,
                first: true,
                newest_is_arrival: true
            }
        );
        assert!(matches!(
            state.arrive(200, &arrival(3, "carol", &[], WaveTrigger::Normal)),
            Arrived::Held { first: false, .. }
        ));
        assert_eq!(state.due(2_999), WaveDueOutcome::Reschedule(3_000));
        let WaveDueOutcome::Flush(batch) = state.due(3_000) else {
            panic!("due flushes");
        };
        assert_eq!((batch.count, batch.newest.seq), (2, 3));
        assert_eq!(batch.times_for("bob"), 1, "bob 发了其中一条");
        assert_eq!(batch.times_for("dave"), 2);
        assert_eq!(
            state.due(3_000),
            WaveDueOutcome::Nothing,
            "重复的到期条目什么都不做"
        );
    }

    /// 消息一直来，间隔逐档升到 120 秒；安静下来后逐档回落。
    #[test]
    fn the_interval_steps_up_under_load_and_down_after_quiet() {
        let mut state = WaveState::default();
        let mut now = 0;
        let mut seq = 0;
        let mut intervals = Vec::new();
        state.arrive(now, &arrival(0, "bob", &[], WaveTrigger::Normal));
        for _ in 0..5 {
            intervals.push(state.open_until_ms - now);
            seq += 1;
            state.arrive(now + 1, &arrival(seq, "bob", &[], WaveTrigger::Normal));
            now = state.open_until_ms;
            assert!(matches!(state.due(now), WaveDueOutcome::Flush(_)));
        }
        assert_eq!(intervals, vec![3_000, 10_000, 30_000, 120_000, 120_000]);

        // 最高档的间隔安静过去，又过了 10 秒：降一档。
        let quiet_start = now;
        let batch = flushed(state.arrive(
            quiet_start + 130_000,
            &arrival(seq + 1, "bob", &[], WaveTrigger::Normal),
        ));
        assert_eq!(batch.count, 1);
        assert_eq!(state.level, 2);
        // 很久没有消息：回到最低档。
        let now = state.open_until_ms + 600_000;
        flushed(state.arrive(now, &arrival(seq + 2, "bob", &[], WaveTrigger::Normal)));
        assert_eq!(state.level, 0);
    }

    /// 被点名直接推过的人与发送者本人从批次里减掉；人人按点名时被点名的人不减。
    #[test]
    fn direct_mentions_and_senders_are_not_counted_twice() {
        let mut state = WaveState::default();
        let named = vec!["carol".to_string()];
        flushed(state.arrive(0, &arrival(1, "bob", &[], WaveTrigger::Normal)));
        state.arrive(10, &arrival(2, "bob", &named, WaveTrigger::Normal));
        state.arrive(20, &arrival(3, "dave", &[], WaveTrigger::Normal));
        let WaveDueOutcome::Flush(batch) = state.due(3_000) else {
            panic!("due flushes");
        };
        assert_eq!(batch.count, 2);
        assert_eq!(batch.times_for("carol"), 1);
        assert_eq!(batch.times_for("bob"), 1);
        assert_eq!(batch.times_for("dave"), 1);
        assert_eq!(batch.times_for("erin"), 2);

        let batch =
            flushed(state.arrive(3_100, &arrival(4, "bob", &named, WaveTrigger::AllMembers)));
        assert_eq!(batch.mention_all, MentionAll::Members);
        assert_eq!(
            batch.times_for("carol"),
            1,
            "人人按点名时被点名的人也在这一批里"
        );
        assert_eq!(batch.times_for("bob"), 0);
    }

    /// 紧急与成员不超过上限的 @所有人 不等间隔，展示的是触发它的那条。
    #[test]
    fn urgent_and_mention_all_flush_the_running_interval() {
        let mut state = WaveState::default();
        flushed(state.arrive(0, &arrival(1, "bob", &[], WaveTrigger::Normal)));
        state.arrive(10, &arrival(5, "bob", &[], WaveTrigger::Normal));
        let Arrived::Flush {
            batch,
            newest_is_arrival,
        } = state.arrive(20, &arrival(4, "carol", &[], WaveTrigger::Now))
        else {
            panic!("urgent flushes now");
        };
        assert!(newest_is_arrival);
        assert_eq!((batch.count, batch.newest.seq), (2, 4));
        assert_eq!(state.level, 1, "间隔里又来了消息，升一档");
        assert_eq!(state.due(state.open_until_ms), WaveDueOutcome::Nothing);
    }

    /// 超过成员上限的 @所有人：配额内是全员波次，配额外按普通消息累加。
    #[test]
    fn mention_all_in_a_large_group_is_rate_limited() {
        let mut state = WaveState::default();
        let all = WaveTrigger::AllEveryone {
            interval_ms: 600_000,
        };
        let batch = flushed(state.arrive(1_000, &arrival(1, "bob", &[], all)));
        assert_eq!(batch.mention_all, MentionAll::Everyone);
        assert_eq!(batch.times_for_non_subscriber("carol"), 1);
        assert_eq!(batch.times_for_non_subscriber("bob"), 0);

        assert!(matches!(
            state.arrive(2_000, &arrival(2, "bob", &[], all)),
            Arrived::Held { .. }
        ));
        let WaveDueOutcome::Flush(batch) = state.due(state.open_until_ms) else {
            panic!("due flushes");
        };
        assert_eq!(batch.mention_all, MentionAll::No);

        let later = 1_000 + 600_000;
        let batch = flushed(state.arrive(later, &arrival(3, "bob", &[], all)));
        assert_eq!(
            batch.mention_all,
            MentionAll::Everyone,
            "间隔过了，配额恢复"
        );
    }

    #[test]
    fn the_state_round_trips_and_garbage_is_no_state() {
        let mut state = WaveState::default();
        state.arrive(0, &arrival(1, "bob", &[], WaveTrigger::Normal));
        state.arrive(
            10,
            &arrival(2, "bob", &["c".to_string()], WaveTrigger::Normal),
        );
        assert_eq!(WaveState::decode(Some(state.encode().as_slice())), state);
        assert_eq!(
            WaveState::decode(Some(&b"not json"[..])),
            WaveState::default()
        );
        assert_eq!(WaveState::decode(None), WaveState::default());
        assert!(
            wave_state_ttl_ms(DEFAULT_ALL_MENTION_INTERVAL_MS) > DEFAULT_ALL_MENTION_INTERVAL_MS
        );
    }

    #[test]
    fn the_excluded_list_is_bounded() {
        let mut excluded = BTreeMap::new();
        for index in 0..(WAVE_EXCLUDED_CAP + 10) {
            exclude(&mut excluded, &format!("u{index}"), 1);
        }
        exclude(&mut excluded, "u0", 2);
        assert_eq!(excluded.len(), WAVE_EXCLUDED_CAP);
        assert_eq!(excluded["u0"], 3);
        exclude(&mut excluded, "", 1);
        assert_eq!(excluded.len(), WAVE_EXCLUDED_CAP);
    }

    /// 没有波次状态可用时只推这一条；超过上限的 @所有人 拿不到配额，不全员扇出。
    #[test]
    fn a_single_batch_never_widens_to_everyone() {
        let named = vec!["carol".to_string()];
        let all = WaveTrigger::AllEveryone {
            interval_ms: 600_000,
        };
        let batch = WaveBatch::single(&arrival(7, "bob", &named, all));
        assert_eq!(batch.mention_all, MentionAll::No);
        assert_eq!(batch.times_for("carol"), 0);
        assert_eq!(batch.times_for("bob"), 0);
        assert_eq!(batch.times_for("dave"), 1);
        let batch = WaveBatch::single(&arrival(7, "bob", &named, WaveTrigger::AllMembers));
        assert_eq!(batch.mention_all, MentionAll::Members);
        assert_eq!(batch.times_for("carol"), 1);
    }
}
