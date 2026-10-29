//! Tenant notices: subscribe to Redis `{namespace}:notice` (next to the kick channel
//! `{namespace}:kick`) and forward each notice to this node's authenticated connections of its
//! tenant as `DataPacket.capability` (`flare_im_service_kit::tenant_notice`).
//!
//! Capability publishes a notice after it commits a newer plugin authorization projection.
//! Forwarding is best effort with bounded concurrency: a client that misses one refreshes its
//! catalog on reconnect and when it returns to the foreground. Scanning the node's connections is
//! O(connections on this node), acceptable for an event that happens on plugin install, enable,
//! disable or configuration change only.

use std::sync::Arc;
use std::time::Duration;

use flare_core::server::connection::ConnectionManagerTrait;
use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_im_service_kit::tenant_notice::{MAX_TENANT_NOTICE_BYTES, TenantNotice, notice_channel};
use flare_proto::common::{CapabilityPacket, DataPacket, data_packet};
use futures::StreamExt;
use prost::Message as _;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::domain::ports::IPushPort;

/// Notices forwarded at the same time; the subscription loop waits for a slot past it.
const MAX_CONCURRENT_NOTICES: usize = 8;
/// Connections per downlink fan-out call.
const CONNECTION_CHUNK: usize = 500;
/// Fan-out calls of one notice in flight.
const CHUNK_CONCURRENCY: usize = 4;

/// Start the background subscription (best effort, reconnects after a failure).
pub fn spawn_notice_subscriber(
    redis_url: String,
    namespace: String,
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    push_port: Arc<dyn IPushPort>,
) {
    let channel = notice_channel(&namespace);
    let forwarder = Arc::new(NoticeForwarder {
        connection_manager,
        push_port,
    });
    let permits = Arc::new(Semaphore::new(MAX_CONCURRENT_NOTICES));
    tokio::spawn(async move {
        loop {
            if let Err(err) = run_subscriber(&redis_url, &channel, &forwarder, &permits).await {
                warn!(%err, channel = %channel, "notice subscriber error; reconnecting in 3s");
            }
            tokio::time::sleep(Duration::from_secs(3)).await;
        }
    });
    info!("tenant notice subscriber spawned");
}

async fn run_subscriber(
    redis_url: &str,
    channel: &str,
    forwarder: &Arc<NoticeForwarder>,
    permits: &Arc<Semaphore>,
) -> flare_server_core::error::Result<()> {
    let client = redis::Client::open(redis_url)?;
    let mut pubsub = client.get_async_pubsub().await?;
    pubsub.subscribe(channel).await?;
    info!(channel = %channel, "subscribed to tenant notice channel");
    let mut stream = pubsub.on_message();
    while let Some(msg) = stream.next().await {
        let Ok(payload) = msg.get_payload::<String>() else {
            warn!("bad tenant notice payload; ignoring");
            continue;
        };
        let Some((tenant_id, packet)) = parse_notice(&payload) else {
            warn!("malformed or unknown tenant notice ignored");
            continue;
        };
        let Ok(permit) = permits.clone().acquire_owned().await else {
            continue;
        };
        let forwarder = forwarder.clone();
        tokio::spawn(async move {
            let _permit = permit;
            forwarder.forward(&tenant_id, packet).await;
        });
    }
    Ok(())
}

/// The tenant and downlink packet of a notice this gateway forwards; `None` for anything else.
fn parse_notice(payload: &str) -> Option<(String, CapabilityPacket)> {
    if payload.len() > MAX_TENANT_NOTICE_BYTES {
        return None;
    }
    let notice: TenantNotice = serde_json::from_str(payload).ok()?;
    if !is_valid_tenant_id(&notice.tenant_id)
        || flare_im_contracts::utils::normalize_tenant_id(&notice.tenant_id) != notice.tenant_id
        || notice.version.is_empty()
        || !notice.version.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let packet = notice.catalog_packet()?;
    Some((notice.tenant_id, packet))
}

struct NoticeForwarder {
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    push_port: Arc<dyn IPushPort>,
}

impl NoticeForwarder {
    /// This node's authenticated connections of `tenant_id`.
    async fn tenant_connections(&self, tenant_id: &str) -> Vec<String> {
        let mut targets = Vec::new();
        for connection_id in self.connection_manager.list_connections().await {
            let Some((_, info)) = self.connection_manager.get_connection(&connection_id).await
            else {
                continue;
            };
            if info.authenticated
                && info.metadata.get("tenant_id").map(String::as_str) == Some(tenant_id)
            {
                targets.push(connection_id);
            }
        }
        targets
    }

    /// Write `packet` to every connection of `tenant_id` on this node; returns how many took it.
    async fn forward(&self, tenant_id: &str, packet: CapabilityPacket) -> usize {
        let targets = self.tenant_connections(tenant_id).await;
        if targets.is_empty() {
            return 0;
        }
        let payload = DataPacket {
            payload: Some(data_packet::Payload::Capability(packet)),
        }
        .encode_to_vec();
        let ctx: flare_im_contracts::Ctx =
            Arc::new(flare_server_core::Context::root().with_tenant_id(tenant_id));
        let payload_type = flare_core::common::protocol::payload_command::Type::Data as i32;
        let chunks: Vec<Vec<String>> = targets
            .chunks(CONNECTION_CHUNK)
            .map(<[String]>::to_vec)
            .collect();
        let delivered = futures::stream::iter(chunks)
            .map(|chunk| {
                let (ctx, payload, push_port) =
                    (ctx.clone(), payload.clone(), self.push_port.clone());
                async move {
                    match push_port
                        .push_payload_to_connections(&ctx, &chunk, payload_type, payload)
                        .await
                    {
                        Ok((ok, _)) => ok.max(0) as usize,
                        Err(error) => {
                            debug!(%error, "tenant notice fan-out failed for a chunk");
                            0
                        }
                    }
                }
            })
            .buffer_unordered(CHUNK_CONCURRENCY)
            .fold(0usize, |sum, ok| async move { sum + ok })
            .await;
        info!(
            %tenant_id,
            connections = targets.len(),
            delivered,
            "tenant notice forwarded to local connections"
        );
        delivered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_core::transport::connection::Connection;
    use flare_im_contracts::Ctx;
    use std::collections::HashMap;
    use std::sync::Mutex;

    struct Silent {
        last_active: flare_core::common::platform::MonotonicInstant,
    }

    #[async_trait::async_trait]
    impl Connection for Silent {
        fn add_observer(&mut self, _: flare_core::transport::events::ArcObserver) {}
        fn remove_observer(&mut self, _: flare_core::transport::events::ArcObserver) {}
        async fn send(&mut self, _: &[u8]) -> flare_core::common::error::Result<()> {
            Ok(())
        }
        async fn close(&mut self) -> flare_core::common::error::Result<()> {
            Ok(())
        }
        fn last_active_time(&self) -> flare_core::common::platform::MonotonicInstant {
            self.last_active
        }
        fn update_active_time(&mut self) {
            self.last_active = flare_core::common::platform::monotonic_now();
        }
    }

    async fn connect(
        manager: &flare_core::server::connection::ConnectionManager,
        connection_id: &str,
        tenant_id: &str,
        authenticated: bool,
    ) {
        manager
            .add_connection(
                connection_id.into(),
                Box::new(Silent {
                    last_active: flare_core::common::platform::monotonic_now(),
                }),
                Some("u1".into()),
                true,
            )
            .unwrap();
        manager
            .update_connection_negotiation(
                connection_id,
                None,
                flare_core::common::protocol::SerializationFormat::Json,
                flare_core::common::CompressionAlgorithm::None,
                flare_core::common::EncryptionAlgorithm::None,
                Some("u1".into()),
                Some(HashMap::from([("tenant_id".into(), tenant_id.into())])),
            )
            .unwrap();
        if authenticated {
            ConnectionManagerTrait::set_connection_authenticated(
                manager,
                connection_id,
                Some("u1".into()),
            )
            .await
            .unwrap();
        }
    }

    /// One fan-out call: the context's tenant, the connections, the payload type and bytes.
    type FanOut = (Option<String>, Vec<String>, i32, Vec<u8>);

    /// Records each fan-out call.
    #[derive(Default)]
    struct Recorder(Mutex<Vec<FanOut>>);

    #[async_trait::async_trait]
    impl IPushPort for Recorder {
        async fn push_message_to_user(
            &self,
            _: &Ctx,
            _: &str,
            _: Vec<u8>,
        ) -> flare_server_core::error::Result<()> {
            Ok(())
        }
        async fn push_message_to_connection(
            &self,
            _: &Ctx,
            _: &str,
            _: Vec<u8>,
        ) -> flare_server_core::error::Result<()> {
            Ok(())
        }
        async fn push_payload_to_connection(
            &self,
            _: &Ctx,
            _: &str,
            _: i32,
            _: Vec<u8>,
        ) -> flare_server_core::error::Result<()> {
            Ok(())
        }
        async fn push_payload_to_user(
            &self,
            _: &Ctx,
            _: &str,
            _: i32,
            _: Vec<u8>,
        ) -> flare_server_core::error::Result<()> {
            Ok(())
        }
        async fn push_payload_to_connections(
            &self,
            tx: &Ctx,
            connection_ids: &[String],
            payload_type: i32,
            payload: Vec<u8>,
        ) -> flare_server_core::error::Result<(i32, i32)> {
            let mut ids = connection_ids.to_vec();
            ids.sort();
            self.0.lock().unwrap().push((
                tx.tenant_id().map(str::to_owned),
                ids,
                payload_type,
                payload,
            ));
            Ok((connection_ids.len() as i32, 0))
        }
    }

    #[test]
    fn only_well_formed_catalog_notices_of_a_canonical_tenant_are_forwarded() {
        let (tenant, packet) =
            parse_notice(r#"{"tenantId":"app-a","kind":"plugin.catalog.changed","version":"7"}"#)
                .unwrap();
        assert_eq!(tenant, "app-a");
        assert_eq!(packet.capability_id, "flare.plugin.catalog");
        assert_eq!(packet.packet_type, "changed");
        for bad in [
            r#"{"tenantId":"app-a","kind":"something.else","version":"7"}"#,
            r#"{"tenantId":"","kind":"plugin.catalog.changed","version":"7"}"#,
            r#"{"tenantId":" app-a","kind":"plugin.catalog.changed","version":"7"}"#,
            r#"{"tenantId":"app-a","kind":"plugin.catalog.changed","version":"v7"}"#,
            r#"{"tenantId":"app-a","kind":"plugin.catalog.changed"}"#,
            "app-a",
        ] {
            assert!(parse_notice(bad).is_none(), "{bad}");
        }
        let oversized = format!(
            r#"{{"tenantId":"app-a","kind":"plugin.catalog.changed","version":"7","x":"{}"}}"#,
            "x".repeat(MAX_TENANT_NOTICE_BYTES)
        );
        assert!(parse_notice(&oversized).is_none());
    }

    /// The packet reaches the authenticated connections of the notice's tenant only.
    #[tokio::test]
    async fn a_notice_reaches_the_authenticated_connections_of_its_tenant_only() {
        let manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
        connect(&manager, "a-1", "app-a", true).await;
        connect(&manager, "a-2", "app-a", true).await;
        connect(&manager, "a-anonymous", "app-a", false).await;
        connect(&manager, "b-1", "app-b", true).await;
        let recorder = Arc::new(Recorder::default());
        let forwarder = NoticeForwarder {
            connection_manager: manager.clone() as Arc<dyn ConnectionManagerTrait>,
            push_port: recorder.clone(),
        };
        let (tenant, packet) =
            parse_notice(r#"{"tenantId":"app-a","kind":"plugin.catalog.changed","version":"7"}"#)
                .unwrap();
        assert_eq!(forwarder.forward(&tenant, packet.clone()).await, 2);

        let calls = recorder.0.lock().unwrap().clone();
        assert_eq!(calls.len(), 1);
        let (ctx_tenant, ids, payload_type, payload) = &calls[0];
        assert_eq!(ctx_tenant.as_deref(), Some("app-a"));
        assert_eq!(ids, &vec!["a-1".to_string(), "a-2".to_string()]);
        assert_eq!(
            *payload_type,
            flare_core::common::protocol::payload_command::Type::Data as i32
        );
        assert_eq!(
            DataPacket::decode(payload.as_slice()).unwrap().payload,
            Some(data_packet::Payload::Capability(packet))
        );

        // A tenant with no connection here: nothing is sent.
        let (tenant, packet) =
            parse_notice(r#"{"tenantId":"app-c","kind":"plugin.catalog.changed","version":"1"}"#)
                .unwrap();
        assert_eq!(forwarder.forward(&tenant, packet).await, 0);
        assert_eq!(recorder.0.lock().unwrap().len(), 1);
    }
}
