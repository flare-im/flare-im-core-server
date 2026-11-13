//! Contribution variant selection for the client catalog (`flare.plugin.ui.catalog`).
//!
//! A client sends what it can render (`{"descriptor": ClientUiDescriptor}`); every contribution
//! of every plugin goes out as the first variant that client satisfies, or as an upgrade
//! placeholder ([`UiContribution::select_with_bundles`]). Clients of the same build send the same
//! descriptor, so the selection of a projection is cached by `(tenant, projection version,
//! descriptor hash, web bundle fingerprint)`. The fingerprint covers the web bundles the snapshot
//! serves: a withdrawal in the registry changes it without a new projection version.
//! The cache holds the selection only: authorization, emergency switches, closed features and
//! target pruning run on every request, as before.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use flare_im_capability_core::plugin::bundle::WebBundleDecl;
use flare_im_capability_core::plugin::manifest::UiContribution;
use flare_im_capability_core::plugin::runtime::PluginRuntimeSnapshot;
use flare_im_capability_core::plugin::ui::ClientUiDescriptor;
use flare_im_service_kit::metrics::REGISTRY;
use flare_server_core::error::{ErrorCode, FlareError};
use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, Opts};
use serde_json::Value;

use crate::domain::capability::{CapabilityError, Result};

/// Selections kept: one per tenant, projection version and descriptor; the least recently used
/// goes first.
pub const SELECTION_CACHE_CAPACITY: usize = 1024;
/// Selections kept for one tenant: its clients' distinct builds and kits, with room to spare.
pub const SELECTION_CACHE_PER_TENANT: usize = 64;
/// Distinct `descriptor_hash` label values; later descriptors count under `other`.
const MAX_DESCRIPTOR_LABELS: usize = 256;
/// Hex digits of the descriptor hash used as its label.
const DESCRIPTOR_LABEL_HEX: usize = 12;

/// A missing, unreadable or out-of-bounds descriptor: `INVALID_ARGUMENT`. There is no catalog
/// without one.
pub(crate) fn invalid_descriptor() -> CapabilityError {
    CapabilityError::Flare(FlareError::localized(
        ErrorCode::InvalidParameter,
        "a valid client UI descriptor is required",
    ))
}

/// The descriptor of a catalog request, validated. With web bundles switched off for the
/// deployment its `bundle` is dropped first, so only declarative variants are selected and
/// clients that differ only in their bundle runtime share one selection.
pub(crate) fn read_descriptor(
    payload: Option<&Value>,
    web_bundles: bool,
) -> Result<ClientUiDescriptor> {
    let payload = payload
        .filter(|payload| !payload.is_null())
        .ok_or_else(invalid_descriptor)?;
    // The size bound applies to the descriptor as sent, before it is parsed.
    let raw = payload.get("descriptor").ok_or_else(invalid_descriptor)?;
    let mut descriptor = ClientUiDescriptor::from_value(raw).map_err(|_| invalid_descriptor())?;
    if !web_bundles {
        descriptor.bundle = None;
    }
    Ok(descriptor)
}

/// A contribution as one client gets it, with how it was chosen (`variant`, `fallback`,
/// `upgrade`, `unsupported`; [`flare_im_capability_core::plugin::ui::Selected::outcome`]).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SelectedContribution {
    pub contribution: UiContribution,
    pub outcome: &'static str,
}

/// Every contribution selected for `descriptor`, in manifest order, with `bundles` the web
/// bundles this application may run (approved, not withdrawn, projected with its addresses).
/// Marketplace samples (`preview`) are not part of the running client surface and are dropped.
pub(crate) fn select_contributions(
    contributions: &[UiContribution],
    descriptor: &ClientUiDescriptor,
    bundles: &[WebBundleDecl],
) -> Vec<SelectedContribution> {
    contributions
        .iter()
        .map(|contribution| {
            let selected = contribution.select_with_bundles(descriptor, bundles);
            let outcome = selected.outcome();
            let mut contribution = selected.into_contribution();
            contribution.preview = None;
            SelectedContribution {
                contribution,
                outcome,
            }
        })
        .collect()
}

/// The selection of one projection for one descriptor, by `(plugin id, manifest digest)`.
pub(crate) type SnapshotSelection = HashMap<(String, String), Arc<[SelectedContribution]>>;

type CacheKey = (String, u64, String, u64);

/// What of the snapshot's web bundles selection depends on: per plugin, each served bundle's id,
/// digest, bridge and digital goods flag. Addresses and permissions do not change a selection.
pub(crate) fn web_bundle_fingerprint(snapshot: &PluginRuntimeSnapshot) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for projected in &snapshot.plugins {
        if projected.web_bundles.is_empty() {
            continue;
        }
        projected.grant.manifest_digest.hash(&mut hasher);
        for bundle in &projected.web_bundles {
            (
                &bundle.id,
                &bundle.sha256,
                bundle.bridge,
                bundle.digital_goods,
            )
                .hash(&mut hasher);
        }
    }
    hasher.finish()
}

struct CacheEntry {
    used: u64,
    selection: Arc<SnapshotSelection>,
}

#[derive(Default)]
struct SelectionCache {
    tick: u64,
    entries: HashMap<CacheKey, CacheEntry>,
}

/// The deployment's web bundle switch and the selection cache.
pub struct CatalogSelection {
    web_bundles: bool,
    capacity: usize,
    cache: Mutex<SelectionCache>,
}

impl Default for CatalogSelection {
    fn default() -> Self {
        Self::new(true, SELECTION_CACHE_CAPACITY)
    }
}

impl CatalogSelection {
    /// `web_bundles`: `plugin.webBundles.enabled` of the deployment.
    pub fn new(web_bundles: bool, capacity: usize) -> Self {
        Self {
            web_bundles,
            capacity: capacity.max(1),
            cache: Mutex::new(SelectionCache::default()),
        }
    }

    pub fn web_bundles(&self) -> bool {
        self.web_bundles
    }

    /// Selections currently cached.
    pub fn cached(&self) -> usize {
        self.lock().entries.len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, SelectionCache> {
        self.cache
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The selection of `snapshot` (the projection of `tenant`) for the descriptor hashing to
    /// `hash`: cached, or computed outside the lock and cached.
    pub(crate) fn selection(
        &self,
        tenant: &str,
        snapshot: &PluginRuntimeSnapshot,
        descriptor: &ClientUiDescriptor,
        hash: &str,
    ) -> Arc<SnapshotSelection> {
        let key: CacheKey = (
            tenant.to_owned(),
            snapshot.version,
            hash.to_owned(),
            web_bundle_fingerprint(snapshot),
        );
        {
            let mut cache = self.lock();
            cache.tick += 1;
            let tick = cache.tick;
            if let Some(entry) = cache.entries.get_mut(&key) {
                entry.used = tick;
                return Arc::clone(&entry.selection);
            }
        }
        let selection: Arc<SnapshotSelection> = Arc::new(
            snapshot
                .plugins
                .iter()
                .map(|projected| {
                    (
                        (
                            projected.grant.plugin_id.clone(),
                            projected.grant.manifest_digest.clone(),
                        ),
                        select_contributions(
                            &projected.manifest().client_extensions.contributions,
                            descriptor,
                            &projected.web_bundles,
                        )
                        .into(),
                    )
                })
                .collect(),
        );
        let mut cache = self.lock();
        cache.tick += 1;
        let tick = cache.tick;
        if !cache.entries.contains_key(&key) {
            // A tenant whose clients cycle descriptors (every `appBuild` is a new key) evicts
            // its own selections first, never more than its share of everyone else's.
            let tenant_entries = cache.entries.keys().filter(|k| k.0 == key.0).count();
            let tenant_full = tenant_entries >= SELECTION_CACHE_PER_TENANT.min(self.capacity);
            if tenant_full || cache.entries.len() >= self.capacity {
                let oldest = cache
                    .entries
                    .iter()
                    .filter(|(k, _)| !tenant_full || k.0 == key.0)
                    .min_by_key(|(_, entry)| entry.used)
                    .map(|(key, _)| key.clone());
                if let Some(oldest) = oldest {
                    cache.entries.remove(&oldest);
                }
            }
        }
        cache.entries.insert(
            key,
            CacheEntry {
                used: tick,
                selection: Arc::clone(&selection),
            },
        );
        selection
    }
}

/// `plugin_catalog_selection_total{outcome}`: contributions served by how they were chosen
/// (`variant`, `fallback`, `upgrade`, `unsupported`); `plugin_catalog_descriptor_total{descriptor_hash}`:
/// catalog requests by descriptor (the first 12 hex digits of its hash; at most 256 values,
/// then `other`).
pub struct CatalogSelectionMetrics {
    pub selection: IntCounterVec,
    pub descriptors: IntCounterVec,
    labels: Mutex<HashSet<String>>,
}

static METRICS: Lazy<Arc<CatalogSelectionMetrics>> = Lazy::new(|| {
    let metrics = CatalogSelectionMetrics::new();
    let _ = REGISTRY.register(Box::new(metrics.selection.clone()));
    let _ = REGISTRY.register(Box::new(metrics.descriptors.clone()));
    Arc::new(metrics)
});

impl CatalogSelectionMetrics {
    pub fn new() -> Self {
        Self {
            selection: IntCounterVec::new(
                Opts::new(
                    "plugin_catalog_selection_total",
                    "Plugin UI contributions served in client catalogs by selection outcome",
                ),
                &["outcome"],
            )
            .expect("valid metric"),
            descriptors: IntCounterVec::new(
                Opts::new(
                    "plugin_catalog_descriptor_total",
                    "Plugin UI catalog requests by client UI descriptor hash",
                ),
                &["descriptor_hash"],
            )
            .expect("valid metric"),
            labels: Mutex::new(HashSet::new()),
        }
    }

    pub fn registered() -> Arc<Self> {
        Arc::clone(&METRICS)
    }

    pub fn record_outcome(&self, outcome: &str) {
        self.selection.with_label_values(&[outcome]).inc();
    }

    pub fn record_descriptor(&self, hash: &str) {
        let label = descriptor_label(
            hash,
            &mut self.labels.lock().unwrap_or_else(|p| p.into_inner()),
        );
        self.descriptors.with_label_values(&[label.as_str()]).inc();
    }
}

impl Default for CatalogSelectionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// The label of `hash`: its first hex digits while fewer than [`MAX_DESCRIPTOR_LABELS`] values
/// are in use, then `other` for any new one.
fn descriptor_label(hash: &str, seen: &mut HashSet<String>) -> String {
    let digits = hash.strip_prefix("sha256:").unwrap_or(hash);
    let Some(short) = digits.get(..DESCRIPTOR_LABEL_HEX) else {
        return "other".into();
    };
    if seen.contains(short) || seen.len() < MAX_DESCRIPTOR_LABELS {
        seen.insert(short.to_owned());
        short.to_owned()
    } else {
        "other".into()
    }
}

#[cfg(test)]
#[path = "catalog_selection_tests.rs"]
mod tests;
