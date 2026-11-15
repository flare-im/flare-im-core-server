//! 闸门状态存在推送专用的 Redis 里。
//!
//! 规则不在这里：调用方读出状态、在进程里跑规则，再用一段很小的 Lua 脚本比较版本后写回。
//! 脚本只比较值的第 2..9 字节（规则编码里的版本号），比较通过才把槽位、设备、到期登记、展示索引
//! 与角标表一起写下；比较不过什么都不写，由调用方重读重算。这样规则只有 Rust 一份实现，
//! 而一次判定仍然是「一次读、一次脚本」两次往返。
//!
//! 群会话的波次状态也在这里：一个会话一个 hash，同样读出后在进程里算、比较版本后写回，到期条目
//! 与尾部刷新、推迟提示在同一个到期队列里。
//!
//! 部署是单实例 Redis 或 Dragonfly（与设备绑定表同一类实例），脚本碰到的键都在参数里声明。

use std::collections::HashMap;
use std::time::Duration;

use flare_server_core::error::{ErrorCode, FlareError};
use redis::aio::ConnectionManager;

use super::keys::{
    GateKeys, badge_field, due_member, parse_due_member, parse_shown_member, parse_wave_due_member,
    shown_member, wave_due_member,
};
use crate::domain::model::{
    BadgeValue, DelayedAlert, DeviceRecord, DueClaim, DueEntry, DueKind, GateDevice, GateSlotKey,
    GateSnapshot, GateStoreOrigin, SlotMessage, SlotRecord, WaveDue, WaveKey, WaveState, kind_code,
    kind_of_code, raw_version, tier_code, tier_of_code,
};
use crate::domain::repository::{
    BadgeWrite, CommitOutcome, DueChange, GateCommit, NotifyGateStore, WaveCommit,
    WaveMessageWrite, WaveSnapshot, WaveStore,
};

/// 单次 Redis 往返的时限。闸门在每条离线推送的路径上，超时按存储不可用处理，改用进程内闸门。
const IO_BUDGET: Duration = Duration::from_millis(800);

/// 角标表最多记多少个会话的贡献；超出的会话直接加进基线，读了也减不掉，打开应用时校准。
pub const BADGE_CONVERSATION_CAP: u32 = 500;

/// 角标表保留多久。设备一周没有任何推送，基线早已过时，等它下次转入后台重新上报。
pub const BADGE_TTL_MS: i64 = 7 * 24 * 60 * 60 * 1000;

fn unavailable(reason: impl Into<String>) -> FlareError {
    FlareError::localized(
        ErrorCode::ServiceUnavailable,
        format!("push gate store unavailable: {}", reason.into()),
    )
}

/// 角标表的改动，三段脚本共用。
///
/// 表里：`b` 基线，`s` 各会话贡献之和，`m` 采用的基线时刻，`c:{会话摘要}` 各会话的贡献。
/// 新基线比采用过的新就整表重置；`add` 给会话加 `amount`（会话数到上限后加进基线），`rem` 去掉
/// 会话的全部贡献，`dec` 减一；返回绝对值与这次是否真的改了。
macro_rules! badge_lua {
    () => {
        r#"
local function apply_badge(key, op, field, cap, base, base_at, ttl, amount)
  if base ~= '' then
    local adopted = tonumber(redis.call('HGET', key, 'm') or '0') or 0
    if (tonumber(base_at) or 0) > adopted then
      redis.call('DEL', key)
      redis.call('HSET', key, 'b', base, 's', 0, 'm', base_at)
    end
  end
  local changed = 0
  if op == 'add' then
    local n = tonumber(amount) or 1
    if n < 1 then n = 1 end
    if redis.call('HEXISTS', key, field) == 1 or redis.call('HLEN', key) < (tonumber(cap) or 0) + 3 then
      redis.call('HINCRBY', key, field, n)
      redis.call('HINCRBY', key, 's', n)
    else
      redis.call('HINCRBY', key, 'b', n)
    end
    changed = 1
  elseif op == 'rem' or op == 'dec' then
    local held = tonumber(redis.call('HGET', key, field) or '0') or 0
    if held > 0 then
      local take = held
      if op == 'dec' then take = 1 end
      if take >= held then
        redis.call('HDEL', key, field)
      else
        redis.call('HINCRBY', key, field, -take)
      end
      redis.call('HINCRBY', key, 's', -take)
      changed = 1
    end
  end
  local total = (tonumber(redis.call('HGET', key, 'b') or '0') or 0)
    + (tonumber(redis.call('HGET', key, 's') or '0') or 0)
  if redis.call('EXISTS', key) == 1 then
    redis.call('PEXPIRE', key, ttl)
  end
  if total < 0 then total = 0 end
  return total, changed
end
"#
    };
}

// 比较后写入。
//
// KEYS: 1 槽位，2 设备，3 到期分片，4 展示索引，5 角标表
// ARGV: 1 比较槽位（'1' 比较），2 槽位期望版本（8 字节），3 槽位新值（空串不写），4 槽位保留毫秒，
//       5 比较设备，6 设备期望版本，7 设备新值，8 设备保留毫秒，
//       9 到期操作（'add'、'rem' 或空），10 到期时刻，11 到期成员，
//       12 展示成员（空串不写），13 展示索引保留毫秒，
//       14 角标操作（'add'、'rem'、'dec'、'get' 或空），15 角标字段，16 会话上限，
//       17 新基线条数（空串表示没有），18 新基线时刻，19 角标保留毫秒，20 'add' 加几条
// 返回 {写入 1 / 冲突 0, 角标（没有改角标为 -1）, 角标是否改了}
const COMMIT_LUA: &str = concat!(
    badge_lua!(),
    r#"
local ZERO = string.rep(string.char(0), 8)
local function version_of(value)
  if not value or string.len(value) < 9 then return ZERO end
  return string.sub(value, 2, 9)
end
if ARGV[1] == '1' and version_of(redis.call('GET', KEYS[1])) ~= ARGV[2] then return {0, -1, 0} end
if ARGV[5] == '1' and version_of(redis.call('GET', KEYS[2])) ~= ARGV[6] then return {0, -1, 0} end
if ARGV[3] ~= '' then redis.call('SET', KEYS[1], ARGV[3], 'PX', ARGV[4]) end
if ARGV[7] ~= '' then redis.call('SET', KEYS[2], ARGV[7], 'PX', ARGV[8]) end
if ARGV[9] == 'add' then
  redis.call('ZADD', KEYS[3], ARGV[10], ARGV[11])
elseif ARGV[9] == 'rem' then
  redis.call('ZREM', KEYS[3], ARGV[11])
end
if ARGV[12] ~= '' then
  redis.call('SADD', KEYS[4], ARGV[12])
  redis.call('PEXPIRE', KEYS[4], ARGV[13])
end
if ARGV[14] == '' then return {1, -1, 0} end
local total, changed = apply_badge(KEYS[5], ARGV[14], ARGV[15], ARGV[16], ARGV[17], ARGV[18], ARGV[19], ARGV[20])
return {1, total, changed}
"#
);

// 记下一条推迟的提示。没有记录就新建并登记到期；有就累加：条数加上这一批的条数、最新一条换成
// seq 更大的、档位取高的。到期登记用 NX：已登记的到期时刻不动，被领走后补登回原时刻。
//
// KEYS: 1 推迟记录，2 到期分片，3 角标表
// ARGV: 1 到期成员，2 到期时刻，3 记录保留毫秒，4 会话，5 会话类型，6 档位，7 消息，8 seq，
//       9 提示等级，10 当前时刻，11 角标操作，12 角标字段，13 会话上限，14 新基线条数，
//       15 新基线时刻，16 角标保留毫秒，17 这一批的条数（也是 'add' 加几条）
// 返回 {角标（没有改角标为 -1）, 角标是否改了}
const DEFER_LUA: &str = concat!(
    badge_lua!(),
    r#"
local key = KEYS[1]
if redis.call('EXISTS', key) == 0 then
  redis.call('HSET', key, 'c', ARGV[4], 'k', ARGV[5], 't', ARGV[6], 'n', 0, 'at', ARGV[10], 'due', ARGV[2], 's', -1)
end
local due = redis.call('HGET', key, 'due') or ARGV[2]
redis.call('ZADD', KEYS[2], 'NX', due, ARGV[1])
if (tonumber(ARGV[6]) or 0) > (tonumber(redis.call('HGET', key, 't') or '0') or 0) then
  redis.call('HSET', key, 't', ARGV[6])
end
if (tonumber(ARGV[8]) or 0) >= (tonumber(redis.call('HGET', key, 's') or '-1') or -1) then
  redis.call('HSET', key, 'm', ARGV[7], 's', ARGV[8], 'a', ARGV[9])
end
local count = tonumber(ARGV[17]) or 1
if count < 1 then count = 1 end
redis.call('HINCRBY', key, 'n', count)
redis.call('PEXPIRE', key, ARGV[3])
if ARGV[11] == '' then return {-1, 0} end
local total, changed = apply_badge(KEYS[3], ARGV[11], ARGV[12], ARGV[13], ARGV[14], ARGV[15], ARGV[16], ARGV[17])
return {total, changed}
"#
);

// 取走推迟记录。KEYS: 1 推迟记录。
const TAKE_DELAYED_LUA: &str = r#"
local fields = redis.call('HGETALL', KEYS[1])
if #fields > 0 then redis.call('DEL', KEYS[1]) end
return fields
"#;

// 会话被读：推迟记录里最新一条不晚于已读位置就取消。
// KEYS: 1 推迟记录，2 到期分片；ARGV: 1 已读 seq（0 表示不知道，一律取消），2 到期成员
const CANCEL_DELAYED_LUA: &str = r#"
if redis.call('EXISTS', KEYS[1]) == 0 then return 0 end
local seq = tonumber(redis.call('HGET', KEYS[1], 's') or '-1') or -1
local read = tonumber(ARGV[1]) or 0
if read == 0 or read >= seq then
  redis.call('DEL', KEYS[1])
  redis.call('ZREM', KEYS[2], ARGV[2])
  return 1
end
return 0
"#;

// 领取到期条目，领取即移除。KEYS: 1 到期分片；ARGV: 1 当前时刻，2 至多几条。
const CLAIM_DUE_LUA: &str = r#"
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'WITHSCORES', 'LIMIT', 0, ARGV[2])
for i = 1, #due, 2 do redis.call('ZREM', KEYS[1], due[i]) end
return due
"#;

// 单独改一次角标表。KEYS: 1 角标表；ARGV: 1 操作，2 字段，3 会话上限，4 新基线条数，5 新基线时刻，
// 6 保留毫秒，7 'add' 加几条。返回 {角标, 是否改了}
const BADGE_LUA: &str = concat!(
    badge_lua!(),
    r#"
local total, changed = apply_badge(KEYS[1], ARGV[1], ARGV[2], ARGV[3], ARGV[4], ARGV[5], ARGV[6], ARGV[7])
return {total, changed}
"#
);

// 会话波次的比较后写入。版本是十进制文本，不存在或解不出按 0（与 [`wave_version`] 一致）。
//
// KEYS: 1 波次，2 到期分片
// ARGV: 1 期望版本，2 新版本，3 状态，4 正文操作（'keep'、'set'、'del'），5 正文，6 保留毫秒，
//       7 到期时刻（空串不登记），8 到期成员
// 返回 写入 1 / 冲突 0
const COMMIT_WAVE_LUA: &str = r#"
local current = tonumber(redis.call('HGET', KEYS[1], 'v') or '0') or 0
if current ~= (tonumber(ARGV[1]) or 0) then return 0 end
redis.call('HSET', KEYS[1], 'v', ARGV[2], 's', ARGV[3])
if ARGV[4] == 'set' then
  redis.call('HSET', KEYS[1], 'm', ARGV[5])
elseif ARGV[4] == 'del' then
  redis.call('HDEL', KEYS[1], 'm')
end
redis.call('PEXPIRE', KEYS[1], ARGV[6])
if ARGV[7] ~= '' then redis.call('ZADD', KEYS[2], ARGV[7], ARGV[8]) end
return 1
"#;

fn badge_op(write: Option<&BadgeWrite>) -> &'static str {
    use crate::domain::model::BadgeChange;
    match write.map(|write| write.change) {
        None => "",
        Some(BadgeChange::Add(_)) => "add",
        Some(BadgeChange::Remove) => "rem",
        Some(BadgeChange::Decrement) => "dec",
        Some(BadgeChange::Get) => "get",
    }
}

/// `add` 加几条；其余操作不看它。
fn badge_amount(write: Option<&BadgeWrite>) -> u32 {
    use crate::domain::model::BadgeChange;
    match write.map(|write| write.change) {
        Some(BadgeChange::Add(times)) => times.max(1),
        _ => 1,
    }
}

/// 波次的版本。不存在或解不出按 0，与写回脚本的口径一致：坏值也能被盖掉。
fn wave_version(raw: Option<&[u8]>) -> u64 {
    raw.and_then(|raw| std::str::from_utf8(raw).ok())
        .and_then(|text| text.trim().parse().ok())
        .unwrap_or(0)
}

/// 新基线的两个参数；没有新基线时条数为空串。
fn badge_base_args(write: Option<&BadgeWrite>) -> (String, i64) {
    match write.and_then(|write| write.base) {
        Some(base) => (base.count.to_string(), base.at_ms),
        None => (String::new(), 0),
    }
}

fn badge_value(total: i64, changed: i64) -> Option<BadgeValue> {
    (total >= 0).then(|| BadgeValue {
        count: u32::try_from(total).unwrap_or(u32::MAX),
        changed: changed != 0,
    })
}

/// 保留时间至少 1 毫秒：`PX 0` 会被拒绝。
fn ttl_arg(ttl_ms: i64) -> i64 {
    ttl_ms.max(1)
}

/// 推迟记录的 hash 换回领域对象。缺字段或解不出时为 `None`：宁可丢掉一次推迟的提示，
/// 也不按猜出来的会话去推。
fn delayed_from_fields(
    key: &GateSlotKey,
    fields: &HashMap<String, String>,
) -> Option<DelayedAlert> {
    let text = |name: &str| fields.get(name).map(String::as_str).unwrap_or_default();
    let number = |name: &str| text(name).parse::<i64>().ok();
    let conversation_id = text("c");
    if conversation_id != key.conversation_id {
        return None;
    }
    Some(DelayedAlert {
        key: key.clone(),
        kind: kind_of_code(u8::try_from(number("k").unwrap_or(1)).unwrap_or(1)),
        tier: tier_of_code(u8::try_from(number("t").unwrap_or(1)).unwrap_or(1)),
        latest: SlotMessage {
            message_id: text("m").to_string(),
            seq: u64::try_from(number("s").unwrap_or(0)).unwrap_or(0),
            alert_class: i32::try_from(number("a").unwrap_or(0)).unwrap_or(0),
        },
        count: u32::try_from(number("n").unwrap_or(1)).unwrap_or(1).max(1),
        created_ms: number("at").unwrap_or(0),
        due_ms: number("due").unwrap_or(0),
    })
}

pub struct RedisGateStore {
    conn: ConnectionManager,
    keys: GateKeys,
    commit: redis::Script,
    defer: redis::Script,
    take_delayed: redis::Script,
    cancel_delayed: redis::Script,
    claim_due: redis::Script,
    badge: redis::Script,
    commit_wave: redis::Script,
}

impl RedisGateStore {
    /// 连接推送 Redis。错误里不带地址：地址里可能有口令。
    pub async fn connect(redis_url: &str, keys: GateKeys) -> Result<Self, FlareError> {
        let client = redis::Client::open(redis_url)
            .map_err(|_| FlareError::system("invalid push server gate redis uri"))?;
        let conn = tokio::time::timeout(Duration::from_secs(3), ConnectionManager::new(client))
            .await
            .map_err(|_| unavailable("connect timed out"))?
            .map_err(|err| FlareError::system(format!("connect push server gate redis: {err}")))?;
        Ok(Self {
            conn,
            keys,
            commit: redis::Script::new(COMMIT_LUA),
            defer: redis::Script::new(DEFER_LUA),
            take_delayed: redis::Script::new(TAKE_DELAYED_LUA),
            cancel_delayed: redis::Script::new(CANCEL_DELAYED_LUA),
            claim_due: redis::Script::new(CLAIM_DUE_LUA),
            badge: redis::Script::new(BADGE_LUA),
            commit_wave: redis::Script::new(COMMIT_WAVE_LUA),
        })
    }

    async fn run<T, F>(&self, what: &str, call: F) -> Result<T, FlareError>
    where
        F: std::future::Future<Output = redis::RedisResult<T>>,
    {
        tokio::time::timeout(IO_BUDGET, call)
            .await
            .map_err(|_| unavailable(format!("{what} timed out")))?
            .map_err(|err| unavailable(format!("{what}: {err}")))
    }
}

#[async_trait::async_trait]
impl NotifyGateStore for RedisGateStore {
    async fn load(&self, key: &GateSlotKey) -> Result<GateSnapshot, FlareError> {
        let mut conn = self.conn.clone();
        let mut cmd = redis::cmd("MGET");
        cmd.arg(self.keys.slot(key))
            .arg(self.keys.device(&key.device));
        let values: Vec<Option<Vec<u8>>> = self.run("load", cmd.query_async(&mut conn)).await?;
        let slot = values.first().and_then(|value| value.as_deref());
        let device = values.get(1).and_then(|value| value.as_deref());
        Ok(GateSnapshot {
            slot: SlotRecord::decode(slot),
            slot_version: raw_version(slot),
            device: DeviceRecord::decode(device),
            device_version: raw_version(device),
        })
    }

    async fn commit(&self, commit: &GateCommit) -> Result<CommitOutcome, FlareError> {
        let key = &commit.key;
        let shown_key = match commit.shown_message_id.as_deref() {
            Some(message_id) => {
                self.keys
                    .shown(&key.device.tenant_id, &key.conversation_id, message_id)
            }
            // 没有要记的展示时这个键不会被碰到，只是脚本要求每个键都声明。
            None => self.keys.slot(key),
        };
        let mut invocation = self.commit.prepare_invoke();
        invocation
            .key(self.keys.slot(key))
            .key(self.keys.device(&key.device))
            .key(self.keys.due_shard(&key.device))
            .key(shown_key)
            .key(self.keys.badge(&key.device));
        for (write, ttl_ms) in [
            (commit.slot.as_ref(), commit.slot_ttl_ms),
            (commit.device.as_ref(), commit.device_ttl_ms),
        ] {
            match write {
                Some(write) => {
                    invocation
                        .arg("1")
                        .arg(&write.expect[..])
                        .arg(write.value.as_deref().unwrap_or_default())
                        .arg(ttl_arg(ttl_ms));
                }
                None => {
                    invocation.arg("0").arg("").arg("").arg(ttl_arg(ttl_ms));
                }
            }
        }
        let (due_op, due_at) = match commit.due {
            DueChange::None => ("", 0),
            DueChange::Schedule(due_ms) => ("add", due_ms),
            DueChange::Cancel => ("rem", 0),
        };
        invocation
            .arg(due_op)
            .arg(due_at)
            .arg(due_member(DueKind::Tail, key))
            .arg(if commit.shown_message_id.is_some() {
                shown_member(&key.device)
            } else {
                String::new()
            })
            .arg(ttl_arg(commit.slot_ttl_ms));
        let badge = commit.badge.as_ref();
        let (base_count, base_at) = badge_base_args(badge);
        invocation
            .arg(badge_op(badge))
            .arg(badge_field(&key.device.tenant_id, &key.conversation_id))
            .arg(BADGE_CONVERSATION_CAP)
            .arg(base_count)
            .arg(base_at)
            .arg(BADGE_TTL_MS)
            .arg(badge_amount(badge));
        let mut conn = self.conn.clone();
        let (applied, total, changed): (i64, i64, i64) = self
            .run("commit", invocation.invoke_async(&mut conn))
            .await?;
        Ok(if applied == 1 {
            CommitOutcome::Applied {
                badge: badge_value(total, changed),
            }
        } else {
            CommitOutcome::Conflict
        })
    }

    async fn defer_alert(
        &self,
        alert: &DelayedAlert,
        ttl_ms: i64,
        badge: Option<BadgeWrite>,
    ) -> Result<Option<BadgeValue>, FlareError> {
        let key = &alert.key;
        let (base_count, base_at) = badge_base_args(badge.as_ref());
        let mut invocation = self.defer.prepare_invoke();
        invocation
            .key(self.keys.delay(key))
            .key(self.keys.due_shard(&key.device))
            .key(self.keys.badge(&key.device))
            .arg(due_member(DueKind::Delay, key))
            .arg(alert.due_ms)
            .arg(ttl_arg(ttl_ms))
            .arg(&key.conversation_id)
            .arg(u32::from(kind_code(alert.kind)))
            .arg(u32::from(tier_code(alert.tier)))
            .arg(&alert.latest.message_id)
            .arg(alert.latest.seq)
            .arg(alert.latest.alert_class)
            .arg(alert.created_ms)
            .arg(badge_op(badge.as_ref()))
            .arg(badge_field(&key.device.tenant_id, &key.conversation_id))
            .arg(BADGE_CONVERSATION_CAP)
            .arg(base_count)
            .arg(base_at)
            .arg(BADGE_TTL_MS)
            .arg(alert.count.max(1));
        let mut conn = self.conn.clone();
        let (total, changed): (i64, i64) = self
            .run("defer alert", invocation.invoke_async(&mut conn))
            .await?;
        Ok(badge_value(total, changed))
    }

    async fn take_delayed(&self, key: &GateSlotKey) -> Result<Option<DelayedAlert>, FlareError> {
        let mut conn = self.conn.clone();
        let mut invocation = self.take_delayed.prepare_invoke();
        invocation.key(self.keys.delay(key));
        let fields: Vec<String> = self
            .run("take delayed alert", invocation.invoke_async(&mut conn))
            .await?;
        if fields.is_empty() {
            return Ok(None);
        }
        let fields: HashMap<String, String> = fields
            .chunks(2)
            .filter_map(|pair| match pair {
                [name, value] => Some((name.clone(), value.clone())),
                _ => None,
            })
            .collect();
        Ok(delayed_from_fields(key, &fields))
    }

    async fn cancel_delayed(&self, key: &GateSlotKey, read_seq: u64) -> Result<bool, FlareError> {
        let mut conn = self.conn.clone();
        let mut invocation = self.cancel_delayed.prepare_invoke();
        invocation
            .key(self.keys.delay(key))
            .key(self.keys.due_shard(&key.device))
            .arg(read_seq)
            .arg(due_member(DueKind::Delay, key));
        let cancelled: i64 = self
            .run("cancel delayed alert", invocation.invoke_async(&mut conn))
            .await?;
        Ok(cancelled == 1)
    }

    async fn claim_due(&self, now_ms: i64, limit: usize) -> Result<Vec<DueClaim>, FlareError> {
        let mut out = Vec::new();
        for shard in self.keys.due_shard_keys() {
            let mut conn = self.conn.clone();
            let mut invocation = self.claim_due.prepare_invoke();
            invocation.key(&shard).arg(now_ms).arg(limit.max(1));
            let claimed: Vec<String> = self
                .run("claim due entries", invocation.invoke_async(&mut conn))
                .await?;
            for pair in claimed.chunks(2) {
                let [member, score] = pair else {
                    continue;
                };
                let due_ms = score
                    .parse::<f64>()
                    .map(|score| score as i64)
                    .unwrap_or(now_ms);
                if let Some((kind, key)) = parse_due_member(member) {
                    out.push(DueClaim::Slot(DueEntry {
                        due_ms,
                        key,
                        kind,
                        origin: GateStoreOrigin::Primary,
                    }));
                } else if let Some(key) = parse_wave_due_member(member) {
                    out.push(DueClaim::Wave(WaveDue {
                        due_ms,
                        key,
                        origin: GateStoreOrigin::Primary,
                    }));
                } else {
                    tracing::warn!(shard = %shard, "dropping an unreadable due entry");
                }
            }
        }
        out.sort();
        Ok(out)
    }

    async fn shown_devices(
        &self,
        tenant_id: &str,
        conversation_id: &str,
        message_id: &str,
    ) -> Result<Vec<GateDevice>, FlareError> {
        let mut conn = self.conn.clone();
        let mut cmd = redis::cmd("SMEMBERS");
        cmd.arg(self.keys.shown(tenant_id, conversation_id, message_id));
        let members: Vec<String> = self
            .run("shown devices", cmd.query_async(&mut conn))
            .await?;
        Ok(members
            .iter()
            .filter_map(|member| parse_shown_member(tenant_id, member))
            .collect())
    }

    async fn badge(
        &self,
        device: &GateDevice,
        conversation_id: &str,
        badge: BadgeWrite,
    ) -> Result<BadgeValue, FlareError> {
        let (base_count, base_at) = badge_base_args(Some(&badge));
        let mut invocation = self.badge.prepare_invoke();
        invocation
            .key(self.keys.badge(device))
            .arg(badge_op(Some(&badge)))
            .arg(badge_field(&device.tenant_id, conversation_id))
            .arg(BADGE_CONVERSATION_CAP)
            .arg(base_count)
            .arg(base_at)
            .arg(BADGE_TTL_MS)
            .arg(badge_amount(Some(&badge)));
        let mut conn = self.conn.clone();
        let (total, changed): (i64, i64) = self
            .run("badge", invocation.invoke_async(&mut conn))
            .await?;
        Ok(badge_value(total, changed).unwrap_or(BadgeValue {
            count: 0,
            changed: false,
        }))
    }
}

#[async_trait::async_trait]
impl WaveStore for RedisGateStore {
    async fn load_wave(&self, key: &WaveKey) -> Result<WaveSnapshot, FlareError> {
        let mut conn = self.conn.clone();
        let mut cmd = redis::cmd("HMGET");
        cmd.arg(self.keys.wave(key)).arg("v").arg("s").arg("m");
        let values: Vec<Option<Vec<u8>>> =
            self.run("load wave", cmd.query_async(&mut conn)).await?;
        let field = |index: usize| values.get(index).and_then(|value| value.as_deref());
        Ok(WaveSnapshot {
            state: WaveState::decode(field(1)),
            version: wave_version(field(0)),
            message: field(2)
                .filter(|bytes| !bytes.is_empty())
                .map(<[u8]>::to_vec),
        })
    }

    async fn commit_wave(&self, commit: &WaveCommit) -> Result<bool, FlareError> {
        let key = &commit.key;
        let (message_op, message): (&str, &[u8]) = match &commit.message {
            WaveMessageWrite::Keep => ("keep", &[]),
            WaveMessageWrite::Set(bytes) => ("set", bytes.as_slice()),
            WaveMessageWrite::Clear => ("del", &[]),
        };
        let mut invocation = self.commit_wave.prepare_invoke();
        invocation
            .key(self.keys.wave(key))
            .key(self.keys.wave_due_shard(key))
            .arg(commit.expect)
            .arg(commit.expect.wrapping_add(1))
            .arg(commit.state.encode())
            .arg(message_op)
            .arg(message)
            .arg(ttl_arg(commit.ttl_ms))
            .arg(
                commit
                    .due_ms
                    .map(|due_ms| due_ms.to_string())
                    .unwrap_or_default(),
            )
            .arg(wave_due_member(key));
        let mut conn = self.conn.clone();
        let applied: i64 = self
            .run("commit wave", invocation.invoke_async(&mut conn))
            .await?;
        Ok(applied == 1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::BadgeBase;
    use crate::domain::model::BadgeChange;
    use flare_notify_rules::{ConversationKind, Tier};

    #[test]
    fn delayed_records_read_back_or_are_dropped() {
        let key = GateSlotKey::new(GateDevice::new("t1", "u1", "phone"), "conv-1");
        let fields: HashMap<String, String> = [
            ("c", "conv-1"),
            ("k", "0"),
            ("t", "2"),
            ("n", "3"),
            ("at", "1000"),
            ("due", "16000"),
            ("m", "m-9"),
            ("s", "9"),
            ("a", "4"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        let alert = delayed_from_fields(&key, &fields).expect("reads back");
        assert_eq!(alert.kind, ConversationKind::Direct);
        assert_eq!(alert.tier, Tier::Priority);
        assert_eq!(alert.count, 3);
        assert_eq!((alert.created_ms, alert.due_ms), (1000, 16000));
        assert_eq!(alert.latest.message_id, "m-9");
        assert_eq!((alert.latest.seq, alert.latest.alert_class), (9, 4));

        let mut other = fields.clone();
        other.insert("c".into(), "another".into());
        assert_eq!(delayed_from_fields(&key, &other), None, "会话对不上就不推");
    }

    #[test]
    fn badge_arguments_and_values() {
        let write = BadgeWrite {
            change: BadgeChange::Decrement,
            base: Some(BadgeBase {
                count: 4,
                at_ms: 77,
            }),
        };
        assert_eq!(badge_op(Some(&write)), "dec");
        assert_eq!(badge_op(None), "");
        assert_eq!(badge_amount(Some(&write)), 1);
        let batch = BadgeWrite {
            change: BadgeChange::Add(4),
            base: None,
        };
        assert_eq!(badge_op(Some(&batch)), "add");
        assert_eq!(badge_amount(Some(&batch)), 4);
        assert_eq!(
            badge_amount(Some(&BadgeWrite {
                change: BadgeChange::Add(0),
                base: None
            })),
            1
        );
        assert_eq!(badge_base_args(Some(&write)), ("4".to_string(), 77));
        assert_eq!(badge_base_args(None), (String::new(), 0));
        assert_eq!(badge_value(-1, 0), None);
        assert_eq!(
            badge_value(5, 1),
            Some(BadgeValue {
                count: 5,
                changed: true
            })
        );
        assert_eq!(ttl_arg(0), 1);
    }

    /// 比较只看版本字节：脚本里取的是第 2..9 字节，与 [`raw_version`] 取的第 1..9 字节（0 起）
    /// 是同一段。
    #[test]
    fn the_script_compares_the_same_bytes_as_raw_version() {
        assert!(COMMIT_LUA.contains("string.sub(value, 2, 9)"));
        assert!(COMMIT_LUA.contains("string.len(value) < 9"));
        assert!(COMMIT_LUA.contains("local function apply_badge"));
        assert!(DEFER_LUA.contains("'NX'"));
    }

    /// 波次版本按数字比较：不存在、解不出都是 0，脚本与读出的口径相同。
    #[test]
    fn wave_versions_read_like_the_script_compares_them() {
        assert_eq!(wave_version(None), 0);
        assert_eq!(wave_version(Some(&b"garbage"[..])), 0);
        assert_eq!(wave_version(Some(&b"42"[..])), 42);
        assert!(COMMIT_WAVE_LUA.contains("tonumber(redis.call('HGET', KEYS[1], 'v') or '0') or 0"));
        assert!(COMMIT_WAVE_LUA.contains("'ZADD', KEYS[2]"));
    }
}
