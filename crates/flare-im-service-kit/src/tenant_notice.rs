//! Tenant config notices on Redis `{namespace}:notice` (the namespace of the gateway kick channel
//! `{namespace}:kick`): "topic `T` of tenant `X` moved to version `V`". Capability publishes one
//! after it commits a newer plugin authorization projection (`flare.plugin.catalog`), through its
//! outbox after a client policy projection moves a watermark (`flare.access`, `flare.release`), and
//! on behalf of a business service that committed a newer read model of its own topic
//! (`TenantNoticeService.Publish`). Every signaling gateway forwards it to its connections of that
//! tenant, or of every tenant for `*`, as `DataPacket.tenant_notice`. Core never interprets a
//! topic. Best effort: a client that misses one re-reads on reconnect and on returning to the
//! foreground.
use flare_proto::common::{DataPacket, TenantNotice as TenantNoticePacket, data_packet};
use serde::{Deserialize, Serialize};

/// Topic of the tenant's plugin catalog (Core's own notice).
pub const PLUGIN_CATALOG_TOPIC: &str = "flare.plugin.catalog";
/// Topic of the application's access manifest; the version is its serial.
pub const ACCESS_TOPIC: &str = "flare.access";
/// Topic of the application's release manifests; the version is its release generation.
pub const RELEASE_TOPIC: &str = "flare.release";
/// Topic prefix reserved for Core; services may not publish under it.
pub const CORE_TOPIC_PREFIX: &str = "flare.";
/// The tenant of a platform-wide notice.
pub const ALL_TENANTS: &str = "*";
/// A notice larger than this is ignored by subscribers.
pub const MAX_TENANT_NOTICE_BYTES: usize = 4096;
/// The longest topic.
pub const MAX_TOPIC_BYTES: usize = 64;
/// The shortest and the longest client spread of a notice.
pub const MIN_SPREAD_MS: u32 = 3_000;
pub const MAX_SPREAD_MS: u32 = 60_000;

/// The notice channel of a token store namespace.
pub fn notice_channel(namespace: &str) -> String {
    format!("{namespace}:notice")
}

/// `[a-z][a-z0-9_-]*(\.[a-z0-9_-]+)+`, at most [`MAX_TOPIC_BYTES`].
pub fn valid_topic(topic: &str) -> bool {
    let segment = |s: &str| {
        !s.is_empty()
            && s.bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
    };
    let mut parts = topic.split('.');
    let first_ok = parts
        .next()
        .is_some_and(|first| segment(first) && first.as_bytes()[0].is_ascii_lowercase());
    let rest: Vec<&str> = parts.collect();
    topic.len() <= MAX_TOPIC_BYTES && first_ok && !rest.is_empty() && rest.iter().all(|s| segment(s))
}

/// How long clients spread their re-reads of one notice: one re-read per `per_second` of this
/// node's `connections`, within [`MIN_SPREAD_MS`, `MAX_SPREAD_MS`].
pub fn spread_ms(connections: usize, per_second: u32) -> u32 {
    let per_second = u64::from(per_second.max(1));
    let millis = (connections as u64)
        .saturating_mul(1000)
        .div_ceil(per_second);
    u32::try_from(millis)
        .unwrap_or(u32::MAX)
        .clamp(MIN_SPREAD_MS, MAX_SPREAD_MS)
}

/// `{"tenantId","topic","version"}`; `version` is decimal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TenantNotice {
    pub tenant_id: String,
    pub topic: String,
    pub version: String,
}

impl TenantNotice {
    pub fn new(tenant_id: &str, topic: &str, version: u64) -> Self {
        Self {
            tenant_id: tenant_id.to_owned(),
            topic: topic.to_owned(),
            version: version.to_string(),
        }
    }

    pub fn tenant_wide(&self) -> bool {
        self.tenant_id == ALL_TENANTS
    }

    /// The decimal version, if well formed and above zero.
    pub fn version(&self) -> Option<u64> {
        if self.version.is_empty() || !self.version.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        self.version.parse().ok().filter(|version| *version > 0)
    }

    /// The downlink packet of this notice for clients that spread their re-reads over `spread_ms`.
    pub fn downlink(&self, spread_ms: u32) -> Option<DataPacket> {
        let version = self.version()?;
        Some(DataPacket {
            payload: Some(data_packet::Payload::TenantNotice(TenantNoticePacket {
                topic: self.topic.clone(),
                version,
                spread_ms,
                tenant_wide: self.tenant_wide(),
            })),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_notice_and_its_packet_follow_the_wire_contract() {
        let notice = TenantNotice::new("app-a", PLUGIN_CATALOG_TOPIC, 42);
        assert_eq!(
            serde_json::to_value(&notice).unwrap(),
            serde_json::json!({"tenantId": "app-a", "topic": "flare.plugin.catalog", "version": "42"})
        );
        assert_eq!(notice_channel("flare:token"), "flare:token:notice");
        let packet = notice.downlink(3_000).unwrap();
        assert_eq!(
            packet.payload,
            Some(data_packet::Payload::TenantNotice(TenantNoticePacket {
                topic: "flare.plugin.catalog".into(),
                version: 42,
                spread_ms: 3_000,
                tenant_wide: false,
            }))
        );
        let wide = TenantNotice::new(ALL_TENANTS, "social.features", 7);
        assert!(wide.tenant_wide());
        let Some(data_packet::Payload::TenantNotice(packet)) = wide.downlink(5_000).unwrap().payload
        else {
            panic!("tenant notice packet");
        };
        assert!(packet.tenant_wide);
    }

    #[test]
    fn malformed_versions_have_no_packet() {
        for version in ["", "0", "-1", "1.5", "x"] {
            let notice = TenantNotice {
                tenant_id: "app-a".into(),
                topic: "social.features".into(),
                version: version.into(),
            };
            assert!(notice.downlink(3_000).is_none(), "{version:?}");
        }
    }

    #[test]
    fn topics_are_lowercase_dotted_names() {
        for topic in ["social.features", "flare.plugin.catalog", "a.b", "x1.y_z-w"] {
            assert!(valid_topic(topic), "{topic}");
        }
        let long = format!("a.{}", "b".repeat(MAX_TOPIC_BYTES));
        for topic in ["", "social", "Social.features", "1a.b", "a..b", "a.", ".a", "a.b c", &long] {
            assert!(!valid_topic(topic), "{topic:?}");
        }
    }

    #[test]
    fn spread_grows_with_the_connections_of_the_node_within_bounds() {
        assert_eq!(spread_ms(0, 500), MIN_SPREAD_MS);
        assert_eq!(spread_ms(1_500, 500), 3_000);
        assert_eq!(spread_ms(1_501, 500), 3_002);
        assert_eq!(spread_ms(10_000, 500), 20_000);
        assert_eq!(spread_ms(30_000, 500), MAX_SPREAD_MS);
        assert_eq!(spread_ms(usize::MAX, 0), MAX_SPREAD_MS);
    }
}
