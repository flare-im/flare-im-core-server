//! Tenant-wide notices on Redis `{namespace}:notice` (the namespace of the gateway kick channel
//! `{namespace}:kick`). Capability publishes one after it commits a newer plugin authorization
//! projection of a tenant; every signaling gateway forwards it to its connections of that tenant
//! as a [`CapabilityPacket`]. Best effort: a client that misses one refreshes its plugin catalog
//! on reconnect and when it returns to the foreground.
use std::collections::HashMap;

use flare_proto::common::CapabilityPacket;
use serde::{Deserialize, Serialize};

/// `TenantNotice.kind` of a newer plugin authorization projection.
pub const PLUGIN_CATALOG_CHANGED: &str = "plugin.catalog.changed";
/// `CapabilityPacket.capability_id` of the catalog-changed packet.
pub const PLUGIN_CATALOG_CAPABILITY_ID: &str = "flare.plugin.catalog";
/// `CapabilityPacket.packet_type` of the catalog-changed packet.
pub const PLUGIN_CATALOG_PACKET_TYPE: &str = "changed";
/// A notice larger than this is ignored by subscribers.
pub const MAX_TENANT_NOTICE_BYTES: usize = 4096;

/// The notice channel of a token store namespace.
pub fn notice_channel(namespace: &str) -> String {
    format!("{namespace}:notice")
}

/// `{"tenantId","kind","version"}`; `version` is the decimal projection version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TenantNotice {
    pub tenant_id: String,
    pub kind: String,
    pub version: String,
}

impl TenantNotice {
    pub fn plugin_catalog_changed(tenant_id: &str, version: u64) -> Self {
        Self {
            tenant_id: tenant_id.to_owned(),
            kind: PLUGIN_CATALOG_CHANGED.to_owned(),
            version: version.to_string(),
        }
    }

    /// The downlink packet of a catalog-changed notice; `None` for any other kind.
    pub fn catalog_packet(&self) -> Option<CapabilityPacket> {
        (self.kind == PLUGIN_CATALOG_CHANGED).then(|| CapabilityPacket {
            capability_id: PLUGIN_CATALOG_CAPABILITY_ID.to_owned(),
            packet_type: PLUGIN_CATALOG_PACKET_TYPE.to_owned(),
            version: "1".to_owned(),
            payload: serde_json::json!({ "version": self.version })
                .to_string()
                .into_bytes(),
            attributes: HashMap::from([("tenant_id".to_owned(), self.tenant_id.clone())]),
            correlation_id: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_notice_and_its_packet_follow_the_wire_contract() {
        let notice = TenantNotice::plugin_catalog_changed("app-a", 42);
        assert_eq!(
            serde_json::to_value(&notice).unwrap(),
            serde_json::json!({"tenantId": "app-a", "kind": "plugin.catalog.changed", "version": "42"})
        );
        assert_eq!(notice_channel("flare:token"), "flare:token:notice");
        let packet = notice.catalog_packet().unwrap();
        assert_eq!(packet.capability_id, "flare.plugin.catalog");
        assert_eq!(packet.packet_type, "changed");
        assert_eq!(packet.version, "1");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&packet.payload).unwrap(),
            serde_json::json!({"version": "42"})
        );
        assert_eq!(
            packet.attributes,
            HashMap::from([("tenant_id".to_string(), "app-a".to_string())])
        );
        let other = TenantNotice {
            kind: "something.else".into(),
            ..notice
        };
        assert!(other.catalog_packet().is_none());
    }
}
