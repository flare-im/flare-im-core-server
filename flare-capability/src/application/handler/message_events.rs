//! Business-neutral observations of the durable, accepted main message stream.
//! Delivery is at least once; plugins deduplicate the stable event id. A partial
//! fanout retries the record, so successful subscribers can see it again.
use crate::domain::{capability::CapabilityPolicyBackend, repository::PluginAuthorizationReader};
use flare_grpc_proto::capability::{
    GenericRequest, extension_plugin_client::ExtensionPluginClient,
};
use flare_im_core_capability_core::plugin::{
    event::*,
    invocation::{PLUGIN_CALL_HEADER, PluginCallSigner},
    runtime::digest_bytes,
};
use flare_proto::common::{
    ConversationType, MessageSource, MessageType, MqEnvelope, MqPayloadKind, message_content,
    mq_envelope,
};
use flare_server_core::mq::consumer::{ConsumerError, Message, MessageHandler, MessageResult};
use futures_util::StreamExt;
use std::{sync::Arc, time::Duration};

pub struct MessageEventHandler {
    authorization: Arc<dyn PluginAuthorizationReader>,
    policy: Arc<dyn CapabilityPolicyBackend>,
    signer: Arc<PluginCallSigner>,
}
impl MessageEventHandler {
    pub fn new(
        authorization: Arc<dyn PluginAuthorizationReader>,
        policy: Arc<dyn CapabilityPolicyBackend>,
        signer: Arc<PluginCallSigner>,
    ) -> Self {
        Self {
            authorization,
            policy,
            signer,
        }
    }
    async fn deliver(&self, event: PluginMessageEvent) -> Result<(), ConsumerError> {
        let now = chrono::Utc::now().timestamp_millis();
        let directory = self
            .authorization
            .event_directory(&event.app)
            .await
            .map_err(|_| ConsumerError::Handler("plugin event directory unavailable".into()))?;
        let mut result = if directory.unavailable_grants.is_empty() {
            Ok(())
        } else {
            Err(ConsumerError::Handler(
                "plugin event subscription release temporarily unavailable".into(),
            ))
        };
        if directory.subscriptions.is_empty() {
            return result;
        }
        let snapshot = directory.routes.ok_or_else(|| {
            ConsumerError::Handler("plugin event routing projection unavailable".into())
        })?;
        snapshot
            .validate(now)
            .map_err(|_| ConsumerError::Handler("plugin event projection stale".into()))?;
        if snapshot.tenant_id != event.app {
            return Err(ConsumerError::DeadLetter(
                "plugin event tenant mismatch".into(),
            ));
        }
        let mut pending = futures_util::stream::FuturesUnordered::new();
        for (grant, operation) in &directory.subscriptions {
            if let Some((plugin, subscription)) = snapshot
                .plugins
                .iter()
                .find(|p| &p.grant.grant_id == grant)
                .and_then(|plugin| {
                    plugin
                        .manifest()
                        .event_subscriptions
                        .iter()
                        .find(|s| &s.operation == operation && s.event_type == event.event_type)
                        .map(|subscription| (plugin, subscription))
                })
            {
                pending.push(self.deliver_to(
                    &event,
                    plugin,
                    subscription,
                    snapshot.valid_until_ms,
                ));
                if pending.len() >= 8
                    && let Some(current) = pending.next().await
                {
                    result = result.and(current);
                }
            } else {
                result = Err(ConsumerError::Handler(
                    "plugin event subscription temporarily unroutable".into(),
                ));
            }
        }
        // Every subscriber is attempted even when another subscriber is down.
        while let Some(current) = pending.next().await {
            result = result.and(current);
        }
        result
    }
    async fn deliver_to(
        &self,
        event: &PluginMessageEvent,
        plugin: &flare_im_core_capability_core::plugin::runtime::ResolvedPlugin,
        subscription: &PluginEventSubscription,
        lease: i64,
    ) -> Result<(), ConsumerError> {
        let now = chrono::Utc::now().timestamp_millis();
        if now >= lease {
            return Err(ConsumerError::Handler(
                "plugin event projection expired during fanout".into(),
            ));
        }
        if plugin
            .authorize_event(
                &event.event_type,
                &subscription.operation,
                &event.data.actor_id,
                now,
            )
            .is_err()
        {
            return Err(ConsumerError::Handler(
                "plugin event desired subscription is not currently authorized".into(),
            ));
        }
        match self
            .policy
            .ensure_not_suspended(&event.app, &subscription.operation)
            .await
        {
            Ok(()) => {}
            Err(crate::domain::capability::CapabilityError::PolicyDenied(_)) => return Ok(()),
            Err(_) => {
                return Err(ConsumerError::Handler(
                    "plugin event policy storage unavailable".into(),
                ));
            }
        }
        let mut delivered = event.clone();
        let active_since_ms = self
            .authorization
            .event_active_since_ms(
                &event.app,
                &plugin.grant.grant_id,
                plugin.grant.revision,
                &plugin.configuration_digest,
            )
            .await
            .map_err(|_| ConsumerError::Handler("plugin event activation unavailable".into()))?
            .ok_or_else(|| ConsumerError::Handler("plugin event activation not proven".into()))?;
        if event.time < active_since_ms {
            return Ok(());
        }
        if !subscription.include_body {
            delivered.data.text = None;
        }
        let bytes = serde_json::to_vec(&delivered)
            .map_err(|_| ConsumerError::DeadLetter("plugin event encoding".into()))?;
        let op = plugin
            .manifest()
            .operation(&subscription.operation)
            .ok_or_else(|| ConsumerError::DeadLetter("plugin event operation missing".into()))?;
        let timeout = Duration::from_millis(u64::from(op.timeout_ms).min(3000));
        let claims = PluginEventClaims {
            tenant_id: event.app.clone(),
            plugin_id: plugin.grant.plugin_id.clone(),
            plugin_version: plugin.grant.plugin_version.clone(),
            manifest_digest: plugin.grant.manifest_digest.clone(),
            deployment_id: plugin.deployment.deployment_id.clone(),
            grant_id: plugin.grant.grant_id.clone(),
            grant_revision: plugin.grant.revision,
            configuration_digest: plugin.configuration_digest.clone(),
            enabled_features: plugin
                .features
                .iter()
                .filter(|(_, enabled)| **enabled)
                .map(|(id, _)| id.clone())
                .collect(),
            active_since_ms,
            operation: subscription.operation.clone(),
            event_id: event.id.clone(),
            request_digest: digest_bytes(&bytes),
            actor_id: event.data.actor_id.clone(),
            conversation_id: event.data.conversation_id.clone(),
            issued_at_ms: now,
            expires_at_ms: now
                .saturating_add(30_000)
                .min(plugin.grant.valid_until_ms)
                .min(lease),
        };
        let token = self
            .signer
            .sign_event(&claims, now)
            .map_err(|_| ConsumerError::Handler("plugin event lease expired".into()))?;
        tokio::time::timeout(timeout, async {
            let mut client =
                ExtensionPluginClient::connect(plugin.deployment.grpc_authority.clone())
                    .await
                    .map_err(|_| {
                        ConsumerError::Handler("plugin event endpoint unavailable".into())
                    })?
                    .max_encoding_message_size(MAX_EVENT_BYTES + 4096)
                    .max_decoding_message_size(8192);
            let mut request = tonic::Request::new(GenericRequest {
                operation: subscription.operation.clone(),
                request_id: event.id.clone(),
                payload: Some(prost_types::Any {
                    type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
                    value: bytes,
                }),
                ..Default::default()
            });
            request.metadata_mut().insert(
                PLUGIN_CALL_HEADER,
                token
                    .parse()
                    .map_err(|_| ConsumerError::DeadLetter("plugin event ticket".into()))?,
            );
            let response = client
                .call(request)
                .await
                .map_err(|_| ConsumerError::Handler("plugin event call failed".into()))?
                .into_inner();
            if !response.ok || response.request_id != event.id {
                return Err(ConsumerError::Handler(
                    "plugin event not acknowledged".into(),
                ));
            }
            Ok(())
        })
        .await
        .map_err(|_| ConsumerError::Handler("plugin event deadline".into()))?
    }
}

/// Only original member text messages. Tenant and actor come from ingest's MQ context;
/// neither message attributes nor plugin JSON can establish them.
pub fn decode_message_event(
    message: &Message,
) -> Result<Option<PluginMessageEvent>, ConsumerError> {
    let envelope = message
        .decode_protobuf::<MqEnvelope>()
        .map_err(|_| ConsumerError::DeadLetter("invalid main envelope".into()))?;
    if envelope.payload_kind != MqPayloadKind::Message as i32 || envelope.push_only {
        return Ok(None);
    }
    let Some(mq_envelope::Payload::Message(raw)) = envelope.payload else {
        return Ok(None);
    };
    if raw.source != MessageSource::User as i32 || raw.message_type != MessageType::Text as i32 {
        return Ok(None);
    }
    let Some(message_content::Content::Text(text)) = raw.content.and_then(|c| c.content) else {
        return Ok(None);
    };
    let mut headers = message.context.headers.clone();
    for (key, value) in envelope.headers {
        headers.insert(key, value);
    }
    let context = flare_im_core_contracts::context_from_mq_metadata(&headers);
    let app = context
        .tenant_id()
        .filter(|id| !id.is_empty() && *id != "*")
        .ok_or_else(|| ConsumerError::DeadLetter("message event tenant missing".into()))?;
    if context.user_id() != Some(raw.sender_id.as_str())
        || envelope.conversation_id != raw.conversation_id
    {
        return Err(ConsumerError::DeadLetter(
            "message event actor or conversation mismatch".into(),
        ));
    }
    let kind = match ConversationType::try_from(raw.conversation_type) {
        Ok(ConversationType::Group) => "group",
        Ok(ConversationType::Single) => "single",
        _ => return Ok(None),
    };
    let identity = format!("{app}\0{}", raw.server_id);
    let event = PluginMessageEvent {
        id: format!(
            "message:{}",
            digest_bytes(identity.as_bytes()).trim_start_matches("sha256:")
        ),
        event_type: MESSAGE_CREATED.into(),
        schema_version: 1,
        app: app.into(),
        time: raw.created_at,
        data: MessageCreatedData {
            server_message_id: raw.server_id,
            conversation_id: raw.conversation_id,
            actor_id: raw.sender_id,
            conversation_type: kind.into(),
            text: Some(text.text),
        },
    };
    event
        .validate()
        .map_err(|_| ConsumerError::DeadLetter("invalid message event".into()))?;
    Ok(Some(event))
}
#[async_trait::async_trait]
impl MessageHandler for MessageEventHandler {
    async fn handle(&self, message: Message) -> Result<MessageResult, ConsumerError> {
        if let Some(event) = decode_message_event(&message)? {
            self.deliver(event).await?;
        }
        Ok(MessageResult::Ack)
    }
    fn name(&self) -> &str {
        "plugin-message-events"
    }
}

#[cfg(test)]
#[path = "message_events_tests.rs"]
mod tests;
