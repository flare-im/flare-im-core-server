use flare_proto::common::MessageReactionState;
use serde_json::Value;

/// Only the server-owned JSON array is authoritative; a submitted string is not state.
pub fn reaction_states_from_extra(extra: &Value) -> Vec<MessageReactionState> {
    extra
        .get("serverReactions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|state| {
            Some(MessageReactionState {
                user_id: state.get("userId")?.as_str()?.to_owned(),
                emoji: state.get("emoji")?.as_str()?.to_owned(),
                event_seq: state.get("eventSeq")?.as_u64()?,
                active: state.get("active")?.as_bool()?,
            })
        })
        .filter(|state| state.event_seq > 0 && !state.user_id.is_empty() && !state.emoji.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reaction_clocks_require_typed_server_state_and_keep_removal_tombstones() {
        let state = MessageReactionState {
            user_id: "u".into(),
            emoji: "👍".into(),
            event_seq: 20,
            active: false,
        };
        let extra = serde_json::json!({"serverReactions": [{"userId": "u", "emoji": "👍", "eventSeq": 20, "active": false}]});
        assert_eq!(reaction_states_from_extra(&extra), [state]);
        assert!(
            reaction_states_from_extra(
                &serde_json::json!({"serverReactions": extra["serverReactions"].to_string()})
            )
            .is_empty()
        );
    }
}
