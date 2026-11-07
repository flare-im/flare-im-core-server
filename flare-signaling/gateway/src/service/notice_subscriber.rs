//! Tenant config notices: subscribe to Redis `{namespace}:notice` (next to the kick channel
//! `{namespace}:kick`) and forward each notice to this node's authenticated connections of its
//! tenant, or of every tenant for `*`, as `DataPacket.tenant_notice`
//! (`flare_im_service_kit::tenant_notice`).
//!
//! Capability publishes a notice once per version of a tenant's topic. Forwarding is best effort
//! with bounded concurrency: a client that misses one re-reads on reconnect and when it returns to
//! the foreground. The packet tells clients how long to spread their re-reads over, from the number
//! of connections this node writes it to and the re-read rate one node may cause, so the read side
//! sees about `nodes x rate` requests per second whatever the tenant's size. Scanning the node's
//! connections is O(connections on this node), acceptable for configuration changes.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use flare_core::server::connection::ConnectionManagerTrait;
use flare_im_contracts::domain::tenant::is_valid_tenant_id;
use flare_im_service_kit::tenant_notice::{
    ALL_TENANTS, MAX_TENANT_NOTICE_BYTES, TenantNotice, notice_channel, spread_ms, valid_topic,
};
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
/// Re-reads per second one node may cause: `FLARE_NOTICE_REFRESH_RPS_PER_NODE`.
pub const DEFAULT_REFRESH_RPS_PER_NODE: u32 = 500;

/// The re-read rate of this node, from `FLARE_NOTICE_REFRESH_RPS_PER_NODE`.
pub fn refresh_rps_per_node_from_env() -> u32 {
    std::env::var("FLARE_NOTICE_REFRESH_RPS_PER_NODE")
        .ok()
        .and_then(|value| value.trim().parse::<u32>().ok())
        .filter(|rate| *rate > 0)
        .unwrap_or(DEFAULT_REFRESH_RPS_PER_NODE)
}

/// Start the background subscription (best effort, reconnects after a failure).
pub fn spawn_notice_subscriber(
    redis_url: String,
    namespace: String,
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    push_port: Arc<dyn IPushPort>,
    refresh_rps_per_node: u32,
) {
    let channel = notice_channel(&namespace);
    let forwarder = Arc::new(NoticeForwarder {
        connection_manager,
        push_port,
        refresh_rps_per_node,
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
        let Some(notice) = parse_notice(&payload) else {
            warn!("malformed tenant notice ignored");
            continue;
        };
        let Ok(permit) = permits.clone().acquire_owned().await else {
            continue;
        };
        let forwarder = forwarder.clone();
        tokio::spawn(async move {
            let _permit = permit;
            forwarder.forward(&notice).await;
        });
    }
    Ok(())
}

/// A notice of a canonical tenant (or `*`), a valid topic and a version above zero.
fn parse_notice(payload: &str) -> Option<TenantNotice> {
    if payload.len() > MAX_TENANT_NOTICE_BYTES {
        return None;
    }
    let notice: TenantNotice = serde_json::from_str(payload).ok()?;
    let tenant_ok = notice.tenant_wide()
        || (is_valid_tenant_id(&notice.tenant_id)
            && flare_im_contracts::utils::normalize_tenant_id(&notice.tenant_id)
                == notice.tenant_id);
    (tenant_ok && valid_topic(&notice.topic) && notice.version().is_some()).then_some(notice)
}

struct NoticeForwarder {
    connection_manager: Arc<dyn ConnectionManagerTrait>,
    push_port: Arc<dyn IPushPort>,
    refresh_rps_per_node: u32,
}

impl NoticeForwarder {
    /// This node's authenticated connections of `tenant_id` (every tenant for `*`), by tenant.
    async fn connections(&self, tenant_id: &str) -> BTreeMap<String, Vec<String>> {
        let mut targets: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for connection_id in self.connection_manager.list_connections().await {
            let Some((_, info)) = self.connection_manager.get_connection(&connection_id).await
            else {
                continue;
            };
            let Some(tenant) = info.metadata.get("tenant_id") else {
                continue;
            };
            if info.authenticated && (tenant_id == ALL_TENANTS || tenant == tenant_id) {
                targets.entry(tenant.clone()).or_default().push(connection_id);
            }
        }
        targets
    }

    /// Write `notice` to its connections on this node; returns how many took it.
    async fn forward(&self, notice: &TenantNotice) -> usize {
        let targets = self.connections(&notice.tenant_id).await;
        let connections: usize = targets.values().map(Vec::len).sum();
        if connections == 0 {
            return 0;
        }
        let spread = spread_ms(connections, self.refresh_rps_per_node);
        let Some(packet) = notice.downlink(spread) else {
            return 0;
        };
        let payload = packet.encode_to_vec();
        let payload_type = flare_core::common::protocol::payload_command::Type::Data as i32;
        let chunks: Vec<(String, Vec<String>)> = targets
            .iter()
            .flat_map(|(tenant, ids)| {
                ids.chunks(CONNECTION_CHUNK)
                    .map(|chunk| (tenant.clone(), chunk.to_vec()))
            })
            .collect();
        let delivered = futures::stream::iter(chunks)
            .map(|(tenant, chunk)| {
                let (payload, push_port) = (payload.clone(), self.push_port.clone());
                async move {
                    let ctx: flare_im_contracts::Ctx =
                        Arc::new(flare_server_core::Context::root().with_tenant_id(&tenant));
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
            tenant_id = %notice.tenant_id,
            topic = %notice.topic,
            connections,
            delivered,
            spread_ms = spread,
            "tenant notice forwarded to local connections"
        );
        delivered
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flare_proto::common::{DataPacket, data_packet};
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
    fn only_well_formed_notices_of_a_canonical_tenant_or_every_tenant_are_forwarded() {
        let notice =
            parse_notice(r#"{"tenantId":"app-a","topic":"social.features","version":"7"}"#)
                .unwrap();
        assert_eq!(notice.tenant_id, "app-a");
        assert_eq!(notice.version(), Some(7));
        assert!(
            parse_notice(r#"{"tenantId":"*","topic":"flare.plugin.catalog","version":"1"}"#)
                .unwrap()
                .tenant_wide()
        );
        for bad in [
            r#"{"tenantId":"app-a","topic":"social","version":"7"}"#,
            r#"{"tenantId":"","topic":"social.features","version":"7"}"#,
            r#"{"tenantId":" app-a","topic":"social.features","version":"7"}"#,
            r#"{"tenantId":"app-a","topic":"social.features","version":"v7"}"#,
            r#"{"tenantId":"app-a","topic":"social.features","version":"0"}"#,
            r#"{"tenantId":"app-a","topic":"social.features"}"#,
            r#"{"tenantId":"app-a","kind":"plugin.catalog.changed","version":"7"}"#,
            "app-a",
        ] {
            assert!(parse_notice(bad).is_none(), "{bad}");
        }
        let oversized = format!(
            r#"{{"tenantId":"app-a","topic":"social.features","version":"7","x":"{}"}}"#,
            "x".repeat(MAX_TENANT_NOTICE_BYTES)
        );
        assert!(parse_notice(&oversized).is_none());
    }

    fn forwarder(
        manager: &Arc<flare_core::server::connection::ConnectionManager>,
        recorder: &Arc<Recorder>,
    ) -> NoticeForwarder {
        NoticeForwarder {
            connection_manager: manager.clone() as Arc<dyn ConnectionManagerTrait>,
            push_port: recorder.clone(),
            refresh_rps_per_node: DEFAULT_REFRESH_RPS_PER_NODE,
        }
    }

    fn sent_notice(payload: &[u8]) -> flare_proto::common::TenantNotice {
        match DataPacket::decode(payload).unwrap().payload {
            Some(data_packet::Payload::TenantNotice(notice)) => notice,
            other => panic!("expected a tenant notice, got {other:?}"),
        }
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
        let forwarder = forwarder(&manager, &recorder);
        let notice =
            parse_notice(r#"{"tenantId":"app-a","topic":"social.features","version":"7"}"#)
                .unwrap();
        assert_eq!(forwarder.forward(&notice).await, 2);

        let calls = recorder.0.lock().unwrap().clone();
        assert_eq!(calls.len(), 1);
        let (ctx_tenant, ids, payload_type, payload) = &calls[0];
        assert_eq!(ctx_tenant.as_deref(), Some("app-a"));
        assert_eq!(ids, &vec!["a-1".to_string(), "a-2".to_string()]);
        assert_eq!(
            *payload_type,
            flare_core::common::protocol::payload_command::Type::Data as i32
        );
        let sent = sent_notice(payload);
        assert_eq!(
            (sent.topic.as_str(), sent.version, sent.spread_ms, sent.tenant_wide),
            ("social.features", 7, 3_000, false)
        );

        // A tenant with no connection here: nothing is sent.
        let notice =
            parse_notice(r#"{"tenantId":"app-c","topic":"social.features","version":"1"}"#)
                .unwrap();
        assert_eq!(forwarder.forward(&notice).await, 0);
        assert_eq!(recorder.0.lock().unwrap().len(), 1);
    }

    /// The access and release topics are Core's own and travel like any other notice: the serial
    /// or generation as the version, with the spread the gateway computes for this node.
    #[tokio::test]
    async fn access_and_release_notices_carry_their_watermark_and_spread() {
        let manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
        connect(&manager, "a-1", "app-a", true).await;
        let recorder = Arc::new(Recorder::default());
        let forwarder = forwarder(&manager, &recorder);
        for (topic, version) in [
            (flare_im_service_kit::tenant_notice::ACCESS_TOPIC, 42),
            (flare_im_service_kit::tenant_notice::RELEASE_TOPIC, 3),
        ] {
            let payload =
                serde_json::to_string(&TenantNotice::new("app-a", topic, version)).unwrap();
            let notice = parse_notice(&payload).unwrap();
            assert_eq!(forwarder.forward(&notice).await, 1);
            let calls = recorder.0.lock().unwrap().clone();
            let sent = sent_notice(&calls.last().unwrap().3);
            assert_eq!(
                (sent.topic.as_str(), sent.version, sent.spread_ms),
                (topic, version, 3_000)
            );
        }
    }

    /// `*` reaches every tenant's authenticated connections, each chunk under its own tenant.
    #[tokio::test]
    async fn a_tenant_wide_notice_reaches_every_tenant() {
        let manager = Arc::new(flare_core::server::connection::ConnectionManager::new());
        connect(&manager, "a-1", "app-a", true).await;
        connect(&manager, "b-1", "app-b", true).await;
        connect(&manager, "b-anonymous", "app-b", false).await;
        let recorder = Arc::new(Recorder::default());
        let forwarder = forwarder(&manager, &recorder);
        let notice = parse_notice(r#"{"tenantId":"*","topic":"social.features","version":"3"}"#)
            .unwrap();
        assert_eq!(forwarder.forward(&notice).await, 2);
        let mut calls = recorder.0.lock().unwrap().clone();
        calls.sort();
        let tenants: Vec<(Option<String>, Vec<String>)> = calls
            .iter()
            .map(|(tenant, ids, _, _)| (tenant.clone(), ids.clone()))
            .collect();
        assert_eq!(
            tenants,
            vec![
                (Some("app-a".into()), vec!["a-1".into()]),
                (Some("app-b".into()), vec!["b-1".into()]),
            ]
        );
        assert!(sent_notice(&calls[0].3).tenant_wide);
    }
}
