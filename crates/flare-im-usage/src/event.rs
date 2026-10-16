//! The `flare.usage.window.v1` event: what one instance counted for one tenant in one window.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::SUBJECT_PREFIX;
use crate::recorder::TenantWindow;

/// Event name in the `event` field; the stream also carries `flare.usage.snapshot.v1`.
pub const WINDOW_EVENT: &str = "flare.usage.window.v1";

/// Active-user hashes per event; a larger batch continues in further parts.
pub(crate) const MAX_USER_HASHES_PER_EVENT: usize = 1_000;

/// One window event as published (JSON). The consumer keys window rows by
/// `(instance, meter, dims, app, window_start_ms)` and active users by
/// `(app, day, user_hash)`; the message id is `{instance}:{app}:{window_start_ms}:{part}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    pub event: String,
    /// Service that counted, e.g. `flare-message-ingest`.
    pub producer: String,
    /// Process run of the producer; unique per start.
    pub instance: String,
    /// IM tenant (application) id.
    pub app: String,
    /// Window start, Unix milliseconds, aligned to `window_secs`.
    pub window_start_ms: i64,
    pub window_secs: u64,
    /// 0 for the event with the meters; further parts only continue `active_users`.
    pub part: u32,
    pub meters: Vec<MeterEntry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_users: Option<ActiveUsersEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeterEntry {
    pub meter: String,
    /// Dimension name to value; empty for meters without dimensions.
    #[serde(default)]
    pub dims: BTreeMap<String, String>,
    pub value: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveUsersEntry {
    /// UTC day, `yyyymmdd`.
    pub day: u32,
    /// [`user_hash`] values, 16 lowercase hex digits each.
    pub user_hashes: Vec<String>,
}

/// An encoded event ready to publish; kept verbatim in the backlog so a retry is identical.
#[derive(Debug, Clone)]
pub(crate) struct OutgoingEvent {
    pub(crate) subject: String,
    pub(crate) message_id: String,
    pub(crate) app: String,
    pub(crate) payload: Vec<u8>,
}

/// Identity of the counting process, stamped on every event.
pub(crate) struct Origin<'a> {
    pub(crate) producer: &'a str,
    pub(crate) instance: &'a str,
    pub(crate) window_start_ms: i64,
    pub(crate) window_secs: u64,
}

/// The events of one tenant window: part 0 carries the meters and the first batch of active
/// users, further parts the remaining batches (one day per part).
pub(crate) fn tenant_events(
    origin: &Origin<'_>,
    app: &str,
    window: TenantWindow,
) -> Vec<UsageEvent> {
    let meters: Vec<MeterEntry> = window
        .meters
        .into_iter()
        .map(|reading| MeterEntry {
            meter: reading.meter.to_owned(),
            dims: reading
                .dims
                .into_iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
            value: reading.value,
        })
        .collect();
    let mut batches = window.active_users.into_iter().flat_map(|(day, hashes)| {
        hashes
            .chunks(MAX_USER_HASHES_PER_EVENT)
            .map(|chunk| ActiveUsersEntry {
                day,
                user_hashes: chunk.iter().map(|hash| format!("{hash:016x}")).collect(),
            })
            .collect::<Vec<_>>()
    });

    let mut events = Vec::new();
    let mut part = 0u32;
    let first_batch = batches.next();
    if !meters.is_empty() || first_batch.is_some() {
        events.push(event(origin, app, part, meters, first_batch));
        part += 1;
    }
    for batch in batches {
        events.push(event(origin, app, part, Vec::new(), Some(batch)));
        part += 1;
    }
    events
}

fn event(
    origin: &Origin<'_>,
    app: &str,
    part: u32,
    meters: Vec<MeterEntry>,
    active_users: Option<ActiveUsersEntry>,
) -> UsageEvent {
    UsageEvent {
        event: WINDOW_EVENT.to_owned(),
        producer: origin.producer.to_owned(),
        instance: origin.instance.to_owned(),
        app: app.to_owned(),
        window_start_ms: origin.window_start_ms,
        window_secs: origin.window_secs,
        part,
        meters,
        active_users,
    }
}

impl UsageEvent {
    /// `{instance}:{app}:{window_start_ms}:{part}`: the JetStream message id and the
    /// consumer's idempotency key for the whole event.
    pub fn message_id(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.instance, self.app, self.window_start_ms, self.part
        )
    }

    /// `flare.usage.v1.<app>`.
    pub fn subject(&self) -> String {
        format!("{SUBJECT_PREFIX}.{}", self.app)
    }

    pub(crate) fn encode(&self) -> serde_json::Result<OutgoingEvent> {
        Ok(OutgoingEvent {
            subject: self.subject(),
            message_id: self.message_id(),
            app: self.app.clone(),
            payload: serde_json::to_vec(self)?,
        })
    }
}

/// Stable 64-bit hash of a user within a tenant: the first eight bytes of
/// `SHA-256(tenant || 0x1f || user_id)`. The same user hashes alike on every instance, so the
/// consumer can count distinct users without receiving user ids.
pub fn user_hash(tenant: &str, user_id: &str) -> u64 {
    let mut hasher = Sha256::new();
    hasher.update(tenant.as_bytes());
    hasher.update([0x1f]);
    hasher.update(user_id.as_bytes());
    let digest = hasher.finalize();
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&digest[..8]);
    u64::from_be_bytes(bytes)
}

pub(crate) fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// UTC calendar day of a Unix millisecond timestamp as `yyyymmdd`.
pub(crate) fn day_of_ms(ms: i64) -> u32 {
    use chrono::Datelike;
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|at| {
            let date = at.date_naive();
            u32::try_from(date.year()).unwrap_or(0) * 10_000 + date.month() * 100 + date.day()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::recorder::MeterReading;

    fn origin() -> Origin<'static> {
        Origin {
            producer: "flare-message-ingest",
            instance: "flare-message-ingest-01TEST",
            window_start_ms: 1_760_140_800_000,
            window_secs: 60,
        }
    }

    #[test]
    fn the_window_event_has_the_documented_shape() {
        let window = TenantWindow {
            meters: vec![MeterReading {
                meter: "messages.accepted",
                dims: vec![("category", "normal")],
                value: 7,
            }],
            active_users: BTreeMap::from([(20251011, vec![0xab, user_hash("acme", "u1")])]),
        };
        let events = tenant_events(&origin(), "acme", window);
        assert_eq!(events.len(), 1);
        let json: serde_json::Value = serde_json::to_value(&events[0]).unwrap();
        let mut keys: Vec<_> = json.as_object().unwrap().keys().cloned().collect();
        keys.sort();
        assert_eq!(
            keys,
            [
                "active_users",
                "app",
                "event",
                "instance",
                "meters",
                "part",
                "producer",
                "window_secs",
                "window_start_ms"
            ]
        );
        assert_eq!(json["event"], "flare.usage.window.v1");
        assert_eq!(
            json["meters"][0],
            serde_json::json!({"meter": "messages.accepted", "dims": {"category": "normal"}, "value": 7})
        );
        assert_eq!(json["active_users"]["day"], 20251011);
        assert_eq!(json["active_users"]["user_hashes"][0], "00000000000000ab");
        assert_eq!(
            events[0].message_id(),
            "flare-message-ingest-01TEST:acme:1760140800000:0"
        );
        assert_eq!(events[0].subject(), "flare.usage.v1.acme");
        let decoded: UsageEvent =
            serde_json::from_slice(&events[0].encode().unwrap().payload).unwrap();
        assert_eq!(decoded, events[0]);
    }

    #[test]
    fn large_active_user_batches_continue_in_numbered_parts_per_day() {
        let hashes: Vec<u64> = (0..(MAX_USER_HASHES_PER_EVENT as u64 + 1)).collect();
        let window = TenantWindow {
            meters: Vec::new(),
            active_users: BTreeMap::from([(20251010, vec![1]), (20251011, hashes)]),
        };
        let events = tenant_events(&origin(), "acme", window);
        let parts: Vec<_> = events
            .iter()
            .map(|event| {
                let users = event.active_users.as_ref().unwrap();
                (event.part, users.day, users.user_hashes.len())
            })
            .collect();
        assert_eq!(
            parts,
            [
                (0, 20251010, 1),
                (1, 20251011, MAX_USER_HASHES_PER_EVENT),
                (2, 20251011, 1)
            ]
        );
        let ids: std::collections::HashSet<_> = events.iter().map(UsageEvent::message_id).collect();
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn user_hashes_are_stable_and_scoped_by_tenant() {
        assert_eq!(user_hash("acme", "u1"), user_hash("acme", "u1"));
        assert_ne!(user_hash("acme", "u1"), user_hash("beta", "u1"));
        // The separator keeps ("ab", "c") and ("a", "bc") apart.
        assert_ne!(user_hash("ab", "c"), user_hash("a", "bc"));
    }

    #[test]
    fn days_are_utc_calendar_days() {
        assert_eq!(day_of_ms(0), 19700101);
        // 2025-10-11T23:59:59.999Z and the next millisecond.
        assert_eq!(day_of_ms(1_760_227_199_999), 20251011);
        assert_eq!(day_of_ms(1_760_227_200_000), 20251012);
    }
}
