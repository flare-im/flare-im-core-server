//! No user impersonation: server fixes source/sender/type and a platform.* event namespace.
use super::*;
use flare_grpc_proto::message::{
    SendSystemMessageRequest, message_send_service_client::MessageSendServiceClient,
};
use flare_im_service_kit::platform_send::SYSTEM_MESSAGE_SCOPE;
use flare_proto::common::{
    ConversationType, Message, MessageContent, MessageSource, MessageStatus, MessageType,
    SystemContent, message_content::Content,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Input {
    client_msg_id: uuid::Uuid,
    kind: String,
    body: String,
}
impl Input {
    fn valid(&self) -> bool {
        self.client_msg_id.get_version_num() == 4
            && !self.kind.is_empty()
            && self.kind.len() <= 64
            && self
                .kind
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            && !self.body.trim().is_empty()
            && self.body.len() <= 8192
    }
}
fn group_channel(conversation: &str) -> Option<&str> {
    let channel = conversation.strip_prefix("group:")?;
    (!channel.is_empty()
        && channel.len() <= 128
        && channel
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-')))
    .then_some(channel)
}
pub(super) async fn send(
    State(state): State<Arc<PlatformState>>,
    headers: HeaderMap,
    Extension(SourceIp(ip)): Extension<SourceIp>,
    path: Result<Path<(String, String)>, axum::extract::rejection::PathRejection>,
    axum::extract::RawQuery(query): axum::extract::RawQuery,
    body: Result<Json<Input>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let (Ok(Path((app, conversation))), Ok(Json(input))) = (path, body) else {
        return invalid();
    };
    let Some(channel) = group_channel(&conversation) else {
        return invalid();
    };
    if query.is_some() || !identifier(&app, true) || !input.valid() {
        return invalid();
    }
    let key = match authorize(&state, &headers, &app, ip, SYSTEM_MESSAGE_SCOPE).await {
        Ok(key) => key,
        Err(r) => return *r,
    };
    let ticket = match state.breaker.try_admit() {
        Ok(t) => t,
        Err(_) => return unavailable(),
    };
    let client_id = input.client_msg_id.to_string();
    let origin_header = flare_im_service_kit::platform_send::ORIGIN_CONTEXT_HEADER;
    let request_id = if let Some(token) = headers.get(origin_header) {
        let claims = state
            .origin_verifier
            .as_ref()
            .and_then(|v| token.to_str().ok().and_then(|s| v.verify(s).ok()));
        let Some(claims) = claims.filter(|c| {
            c.actor_type == SignedActorType::Service
                && c.kid == flare_server_core::auth::kid::GATEWAY
                && c.tenant_id == app
                && c.actor_id == key.service_account_id
                && c.credential_id.as_deref() == Some(key.key_id.as_str())
                && c.roles == [SYSTEM_MESSAGE_SCOPE]
                && headers.get_all(origin_header).iter().count() == 1
        }) else {
            return unavailable();
        };
        claims.request_id
    } else {
        uuid::Uuid::new_v4().to_string()
    };
    let identity = SignedContextIdentity::new(
        SignedActorType::Service,
        &key.service_account_id,
        &app,
        &request_id,
    )
    .with_credential_id(key.key_id)
    .with_roles(vec![SYSTEM_MESSAGE_SCOPE.into()]);
    let token = match state.signer.sign(&identity) {
        Ok(t) => t,
        Err(_) => return unavailable(),
    };
    let message = Message {
        client_msg_id: client_id.clone(),
        conversation_id: conversation.clone(),
        channel_id: channel.into(),
        sender_id: key.service_account_id,
        source: MessageSource::System as i32,
        message_type: MessageType::System as i32,
        conversation_type: ConversationType::Group as i32,
        created_at: chrono::Utc::now().timestamp_millis(),
        status: MessageStatus::Sent as i32,
        content: Some(MessageContent {
            content: Some(Content::System(SystemContent {
                event_kind: format!("platform.{}", input.kind),
                body: input.body,
                attributes: Default::default(),
                payload: vec![],
            })),
        }),
        ..Default::default()
    };
    let mut request = tonic::Request::new(SendSystemMessageRequest {
        conversation_id: conversation,
        message: Some(message),
        system_message_type: format!("platform.{}", input.kind),
    });
    if flare_server_core::set_signed_context_token(&mut request, &token).is_err() {
        return unavailable();
    }
    request.set_timeout(RPC_TIMEOUT);
    let result = timeout(
        RPC_TIMEOUT,
        MessageSendServiceClient::new(state.ingest.clone()).send_system_message(request),
    )
    .await;
    match result {
        Ok(Ok(reply))
            if reply.get_ref().success
                && !reply.get_ref().message_id.is_empty()
                && reply.get_ref().message_id.len() <= 128
                && !reply.get_ref().message_id.chars().any(char::is_control) =>
        {
            ticket.settle(Outcome::Success);
            Json(serde_json::json!({"data":{"accepted":true,"message_id":reply.into_inner().message_id,"client_msg_id":client_id}})).into_response()
        }
        Ok(Err(e))
            if matches!(
                e.code(),
                tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
            ) =>
        {
            ticket.settle(Outcome::Success);
            failure(StatusCode::FORBIDDEN, "API_KEY_FORBIDDEN")
        }
        Ok(Err(e)) if e.code() == tonic::Code::NotFound => {
            ticket.settle(Outcome::Success);
            failure(StatusCode::NOT_FOUND, "NOT_FOUND")
        }
        Ok(Err(e)) if e.code() == tonic::Code::InvalidArgument => {
            ticket.settle(Outcome::Success);
            invalid()
        }
        _ => {
            ticket.settle(Outcome::Failure);
            unavailable()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn input_is_closed_bounded_and_cannot_declare_user_or_social_event_identity() {
        let valid = serde_json::json!({"client_msg_id":uuid::Uuid::new_v4(),"kind":"maintenance","body":"scheduled maintenance"});
        assert!(
            serde_json::from_value::<Input>(valid.clone())
                .unwrap()
                .valid()
        );
        for field in [
            "sender_id",
            "tenant_id",
            "message_type",
            "attributes",
            "source",
        ] {
            let mut v = valid.clone();
            v[field] = serde_json::json!("forged");
            assert!(serde_json::from_value::<Input>(v).is_err());
        }
        for kind in ["social.group.created", "", " a", "a/b"] {
            let mut v = valid.clone();
            v["kind"] = kind.into();
            assert!(!serde_json::from_value::<Input>(v).unwrap().valid());
        }
        assert_eq!(group_channel("group:g1"), Some("g1"));
        assert!(group_channel("single:a:b").is_none());
    }
}
