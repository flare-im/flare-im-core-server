//! 插件实体共享状态事件应用：侧表 `message_plugin_states` 按版本单调覆盖，事件照常追加事件流。
//!
//! 事件已由编排器分配了会话序号并扇出，所以不论侧表写没写，这个序号上都追加事件（事件流不留洞）。
//! 侧表只接受已绑定到该插件实体、且在本会话内的消息：编排器入口是内部接口，这一步挡住把状态挂到
//! 任意消息上的事件。

use crate::domain::model::{Event, PluginStatePayload};
use crate::domain::repository::{ArchiveStoreRepository, EventStreamRepository, PluginStateWrite};
use flare_im_capability_core::plugin::state::SharedState;
use flare_server_core::error::{ErrorCode, Result, map_infra_error};
use flare_server_core::flare_err;

use super::{EventContext, append_event_and_stream};

pub async fn apply_plugin_state<A, E>(
    ctx: &EventContext<'_, A, E>,
    event: &Event,
    state: &PluginStatePayload,
) -> Result<()>
where
    A: ArchiveStoreRepository + Send + Sync,
    E: EventStreamRepository + Send + Sync,
{
    if state.server_msg_id.is_empty()
        || state.plugin_id.is_empty()
        || SharedState::from_parts(&state.entity, state.version, &state.values_json).is_err()
    {
        return Err(flare_err!(
            ErrorCode::InvalidParameter,
            "invalid plugin state event"
        ));
    }
    let write = ctx
        .repo
        .upsert_message_plugin_state(ctx.ctx, ctx.tenant_id, ctx.conversation_id, state)
        .await
        .map_err(|e| map_infra_error(e, ErrorCode::DatabaseError, "Database operation failed"))?;
    match write {
        PluginStateWrite::Applied => {}
        PluginStateWrite::Unchanged => tracing::debug!(
            tenant_id = %ctx.tenant_id,
            message_id = %state.server_msg_id,
            version = state.version,
            "plugin state is not newer than the stored one; kept"
        ),
        PluginStateWrite::Unbound => tracing::warn!(
            tenant_id = %ctx.tenant_id,
            conversation_id = %ctx.conversation_id,
            message_id = %state.server_msg_id,
            plugin_id = %state.plugin_id,
            entity = %state.entity,
            "plugin state names a message not bound to this plugin entity, or a recalled one; not stored"
        ),
    }
    append_event_and_stream(ctx, &state.server_msg_id, event).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::{EventPayload, EventType};
    use crate::domain::repository::StoredEventMatch;
    use flare_im_contracts::Ctx;
    use flare_im_contracts::utils::Context;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct Recorded {
        writes: Mutex<Vec<(String, String, u64)>>,
        history: Mutex<usize>,
        stream: Mutex<usize>,
    }
    struct Archive(Arc<Recorded>, PluginStateWrite);
    impl ArchiveStoreRepository for Archive {
        async fn store_archive(
            &self,
            _ctx: &Ctx,
            _message: &crate::domain::model::Message,
        ) -> Result<()> {
            Ok(())
        }
        async fn upsert_message_plugin_state(
            &self,
            _ctx: &Ctx,
            tenant_id: &str,
            conversation_id: &str,
            state: &PluginStatePayload,
        ) -> Result<PluginStateWrite> {
            self.0.writes.lock().unwrap().push((
                tenant_id.into(),
                conversation_id.into(),
                state.version,
            ));
            Ok(self.1)
        }
        async fn append_event(
            &self,
            _ctx: &Ctx,
            _tenant_id: &str,
            _message_id: &str,
            _event: &Event,
        ) -> Result<()> {
            *self.0.history.lock().unwrap() += 1;
            Ok(())
        }
    }
    struct Stream(Arc<Recorded>);
    impl EventStreamRepository for Stream {
        async fn append_event_to_stream(&self, _ctx: &Ctx, _event: &Event) -> Result<()> {
            *self.0.stream.lock().unwrap() += 1;
            Ok(())
        }
        async fn match_stored_event(&self, _ctx: &Ctx, _event: &Event) -> Result<StoredEventMatch> {
            Ok(StoredEventMatch::Absent)
        }
    }

    fn state(version: u64, values_json: &str) -> PluginStatePayload {
        PluginStatePayload {
            server_msg_id: "m-1".into(),
            plugin_id: "example.poll".into(),
            entity: "poll-1".into(),
            version,
            values_json: values_json.into(),
        }
    }

    async fn apply(
        write: PluginStateWrite,
        state: PluginStatePayload,
    ) -> (Result<()>, Arc<Recorded>) {
        let recorded = Arc::new(Recorded::default());
        let archive = Archive(recorded.clone(), write);
        let stream = Stream(recorded.clone());
        let ctx: Ctx = Arc::new(Context::with_request_id("r").with_tenant_id("app-a"));
        let event_ctx = EventContext {
            repo: &archive,
            stream: Some(&stream),
            ctx: &ctx,
            tenant_id: "app-a",
            conversation_id: "2-group",
        };
        let event = Event {
            tenant_id: "app-a".into(),
            conversation_id: "2-group".into(),
            seq: 7,
            r#type: EventType::PluginState,
            created_at: None,
            operator_id: String::new(),
            event_seq: None,
            request_id: None,
            payload: Some(EventPayload::PluginState(state.clone())),
        };
        (
            apply_plugin_state(&event_ctx, &event, &state).await,
            recorded,
        )
    }

    #[tokio::test]
    async fn every_state_event_is_appended_whether_or_not_it_changes_the_side_table() {
        for write in [
            PluginStateWrite::Applied,
            PluginStateWrite::Unchanged,
            PluginStateWrite::Unbound,
        ] {
            let (result, recorded) = apply(write, state(3, r#"{"counts":[1,2]}"#)).await;
            result.unwrap();
            assert_eq!(
                recorded.writes.lock().unwrap().as_slice(),
                [("app-a".to_string(), "2-group".to_string(), 3)]
            );
            assert_eq!(*recorded.history.lock().unwrap(), 1, "{write:?}");
            assert_eq!(*recorded.stream.lock().unwrap(), 1, "{write:?}");
        }
    }

    #[tokio::test]
    async fn a_malformed_state_is_refused_before_any_write() {
        for bad in [
            state(0, "{}"),
            state(1, "[1]"),
            state(1, r#"{"nested":{"x":1}}"#),
            PluginStatePayload {
                entity: "a b".into(),
                ..state(1, "{}")
            },
        ] {
            let (result, recorded) = apply(PluginStateWrite::Applied, bad).await;
            assert!(result.is_err());
            assert!(recorded.writes.lock().unwrap().is_empty());
            assert_eq!(*recorded.stream.lock().unwrap(), 0);
        }
    }
}
