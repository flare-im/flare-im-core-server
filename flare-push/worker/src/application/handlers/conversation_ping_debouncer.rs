use std::collections::HashMap;
use std::time::{Duration, Instant};

use flare_grpc_proto::access_gateway::PushEventRequest;
use flare_proto::common::EventEnvelopeDeliveryMode;
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PingDebounceKey {
    tenant_id: String,
    user_id: String,
    conversation_id: String,
}

impl PingDebounceKey {
    pub fn new(
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        conversation_id: impl Into<String>,
    ) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            conversation_id: conversation_id.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PingDebounceDecision {
    SendNow,
    ScheduleAfter(Duration),
    Suppressed,
}

pub struct ConversationPingDebouncer {
    window: Duration,
    state: Mutex<HashMap<PingDebounceKey, DebounceEntry>>,
}

struct DebounceEntry {
    last_sent: Instant,
    scheduled: bool,
    pending: Option<PushEventRequest>,
}

impl ConversationPingDebouncer {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            state: Mutex::new(HashMap::new()),
        }
    }

    pub async fn observe(
        &self,
        key: PingDebounceKey,
        pending: PushEventRequest,
    ) -> PingDebounceDecision {
        if self.window.is_zero() {
            return PingDebounceDecision::SendNow;
        }

        let now = Instant::now();
        let mut state = self.state.lock().await;
        let Some(entry) = state.get_mut(&key) else {
            state.insert(
                key,
                DebounceEntry {
                    last_sent: now,
                    scheduled: false,
                    pending: None,
                },
            );
            return PingDebounceDecision::SendNow;
        };

        let elapsed = now.duration_since(entry.last_sent);
        if elapsed >= self.window {
            entry.last_sent = now;
            entry.scheduled = false;
            entry.pending = None;
            return PingDebounceDecision::SendNow;
        }

        entry.pending = Some(match entry.pending.take() {
            Some(existing) => merge_pending_ping(existing, pending),
            None => pending,
        });
        if entry.scheduled {
            PingDebounceDecision::Suppressed
        } else {
            entry.scheduled = true;
            PingDebounceDecision::ScheduleAfter(self.window - elapsed)
        }
    }

    pub async fn take_pending(&self, key: &PingDebounceKey) -> Option<PushEventRequest> {
        let mut state = self.state.lock().await;
        let entry = state.get_mut(key)?;
        entry.scheduled = false;
        let pending = entry.pending.take();
        if pending.is_some() {
            entry.last_sent = Instant::now();
        }
        pending
    }
}

fn merge_pending_ping(
    mut existing: PushEventRequest,
    incoming: PushEventRequest,
) -> PushEventRequest {
    existing.max_conversation_seq = existing
        .max_conversation_seq
        .max(incoming.max_conversation_seq);
    for user_id in incoming.user_ids {
        if !existing.user_ids.contains(&user_id) {
            existing.user_ids.push(user_id);
        }
    }
    existing.options = incoming.options.or(existing.options);
    existing.events.clear();
    existing.delivery_mode = EventEnvelopeDeliveryMode::Ping as i32;
    existing.inline_events_truncated = true;
    existing
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ping(seq: u64) -> PushEventRequest {
        PushEventRequest {
            user_ids: vec!["u1".to_string()],
            events: vec![],
            options: None,
            conversation_id: "c1".to_string(),
            max_conversation_seq: seq,
            delivery_mode: EventEnvelopeDeliveryMode::Ping as i32,
            inline_events_truncated: true,
        }
    }

    #[tokio::test]
    async fn first_ping_sends_and_second_schedules_trailing_ping() {
        let debouncer = ConversationPingDebouncer::new(Duration::from_secs(1));
        let key = PingDebounceKey::new("t1", "u1", "c1");

        assert_eq!(
            debouncer.observe(key.clone(), ping(1)).await,
            PingDebounceDecision::SendNow
        );
        assert!(matches!(
            debouncer.observe(key.clone(), ping(2)).await,
            PingDebounceDecision::ScheduleAfter(_)
        ));
        assert_eq!(
            debouncer.observe(key.clone(), ping(3)).await,
            PingDebounceDecision::Suppressed
        );

        let pending = debouncer
            .take_pending(&key)
            .await
            .expect("trailing ping should be pending");
        assert!(pending.events.is_empty());
        assert_eq!(pending.max_conversation_seq, 3);
        assert_eq!(
            pending.delivery_mode,
            EventEnvelopeDeliveryMode::Ping as i32
        );
    }
}
