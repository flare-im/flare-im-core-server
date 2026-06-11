use flare_call::domain::{CallSession, CallSessionEvent, CallSessionState};

#[test]
fn start_call_creates_initiating_session_and_started_event() {
    let (session, event) = CallSession::start("conversation-1".into(), "tenant-1".into());

    assert_eq!(session.tenant_id, "tenant-1");
    assert_eq!(session.conversation_id, "conversation-1");
    assert_eq!(session.state, CallSessionState::Initiating);
    assert!(session.sfu_room_id.is_none());
    assert!(session.capability_instance_id.is_none());

    match event {
        CallSessionEvent::Started {
            id,
            conversation_id,
            tenant_id,
            ..
        } => {
            assert_eq!(id, session.id);
            assert_eq!(conversation_id, "conversation-1");
            assert_eq!(tenant_id, "tenant-1");
        }
        other => panic!("unexpected event: {other:?}"),
    }
}

#[test]
fn media_room_binding_does_not_change_business_lifecycle_state() {
    let (mut session, _) = CallSession::start("conversation-1".into(), "tenant-1".into());

    let event = session.bind_room("room-1".into(), "capability-1".into());

    assert_eq!(session.state, CallSessionState::Initiating);
    assert_eq!(session.sfu_room_id.as_deref(), Some("room-1"));
    assert_eq!(
        session.capability_instance_id.as_deref(),
        Some("capability-1")
    );
    assert!(matches!(event, CallSessionEvent::RoomBound { .. }));
}

#[test]
fn terminal_commands_move_session_to_ended_or_failed() {
    let (mut rejected, _) = CallSession::start("conversation-1".into(), "tenant-1".into());
    rejected
        .reject("user-1".into(), Some("busy".into()))
        .expect("reject");
    assert_eq!(rejected.state, CallSessionState::Ended);

    let (mut cancelled, _) = CallSession::start("conversation-1".into(), "tenant-1".into());
    cancelled.cancel("user-1".into()).expect("cancel");
    assert_eq!(cancelled.state, CallSessionState::Ended);

    let (mut failed, _) = CallSession::start("conversation-1".into(), "tenant-1".into());
    failed.fail("sfu unavailable".into());
    assert_eq!(failed.state, CallSessionState::Failed);
}
