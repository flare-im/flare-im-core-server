//! `PluginVersion`: one signed package. The bytes never change; only `published`, `revoked` and
//! `suspended` do.
use flare_im_core_capability_core::plugin::PluginManifest;
use flare_im_core_capability_core::plugin::manifest::PluginKind;
use flare_im_core_capability_core::plugin::package::PluginPackage;
use flare_im_core_capability_core::plugin::runtime::digest_bytes;
use serde::{Deserialize, Serialize};

use super::{PluginRegistryError, RegistryResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginVersion {
    pub plugin_id: String,
    pub plugin_version: String,
    pub display_name: String,
    pub publisher_id: String,
    pub manifest_digest: String,
    pub kind: String,
    pub published: bool,
    pub revoked: bool,
    pub suspended: bool,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginAssetRef {
    pub path: String,
    pub digest: String,
    pub media_type: String,
    pub size: i64,
}

/// A version with its signed manifest, as `get`, `verify` and `register` answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PluginVersionDetail {
    #[serde(flatten)]
    pub version: PluginVersion,
    pub manifest_json: String,
    pub key_id: String,
    pub signature: String,
    pub readme: Option<String>,
    pub changelog: Option<String>,
    pub assets: Vec<PluginAssetRef>,
    /// `register` only: `false` when the same digest was registered before.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPluginAsset {
    pub path: String,
    pub digest: String,
    pub media_type: &'static str,
    pub bytes: Vec<u8>,
}

/// An opened, verified package as it is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPluginVersion {
    pub plugin_id: String,
    pub plugin_version: String,
    pub display_name: String,
    pub publisher_id: String,
    pub manifest_json: String,
    pub manifest_digest: String,
    pub key_id: String,
    pub signature: String,
    pub kind: &'static str,
    pub readme: Option<String>,
    pub changelog: Option<String>,
    /// The namespaces the version claims: its capability ids.
    pub namespaces: Vec<String>,
    pub assets: Vec<NewPluginAsset>,
}

impl NewPluginVersion {
    pub fn from_package(package: &PluginPackage) -> RegistryResult<Self> {
        let signed = package.manifest();
        let manifest = signed.manifest();
        let manifest_json = std::str::from_utf8(package.manifest_json())
            .map_err(|_| {
                PluginRegistryError::Invalid(
                    "plugin publisher signature or manifest is invalid".into(),
                )
            })?
            .to_owned();
        let text = |bytes: Option<&[u8]>| {
            bytes
                .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                .filter(|text| !text.is_empty())
        };
        Ok(Self {
            plugin_id: manifest.plugin_id.clone(),
            plugin_version: manifest.version.clone(),
            display_name: manifest.display_name.clone(),
            publisher_id: manifest.publisher.publisher_id.clone(),
            manifest_json,
            manifest_digest: signed.digest().to_owned(),
            key_id: package.signature().key_id.clone(),
            signature: package.signature().signature.clone(),
            kind: match manifest.kind() {
                PluginKind::Im => "im",
                PluginKind::Business => "business",
            },
            readme: text(package.readme()),
            changelog: text(package.changelog()),
            namespaces: manifest.claimed_namespaces().into_iter().collect(),
            assets: package
                .assets()
                .iter()
                .map(|(path, bytes)| NewPluginAsset {
                    path: path.clone(),
                    digest: digest_bytes(bytes),
                    media_type: asset_media_type(path),
                    bytes: bytes.clone(),
                })
                .collect(),
        })
    }

    /// What `verify` answers: the version as it would be stored, nothing written.
    pub fn preview(&self) -> PluginVersionDetail {
        PluginVersionDetail {
            version: PluginVersion {
                plugin_id: self.plugin_id.clone(),
                plugin_version: self.plugin_version.clone(),
                display_name: self.display_name.clone(),
                publisher_id: self.publisher_id.clone(),
                manifest_digest: self.manifest_digest.clone(),
                kind: self.kind.to_owned(),
                published: false,
                revoked: false,
                suspended: false,
                created_at: 0,
                updated_at: 0,
            },
            manifest_json: self.manifest_json.clone(),
            key_id: self.key_id.clone(),
            signature: self.signature.clone(),
            readme: self.readme.clone(),
            changelog: self.changelog.clone(),
            assets: self
                .assets
                .iter()
                .map(|asset| PluginAssetRef {
                    path: asset.path.clone(),
                    digest: asset.digest.clone(),
                    media_type: asset.media_type.to_owned(),
                    size: asset.bytes.len() as i64,
                })
                .collect(),
            created: None,
        }
    }
}

/// Only what a browser shows as an image gets its type. Anything else, SVG with its scripts
/// included, is stored as opaque bytes and served as a download.
fn asset_media_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("webp") => "image/webp",
        Some("gif") => "image/gif",
        _ => "application/octet-stream",
    }
}

/// Whether `namespace` overlaps a namespace `owner` holds, for a plugin that is not `owner`.
pub fn namespace_conflict(plugin_id: &str, owner: &str, held: &str, namespace: &str) -> bool {
    owner != plugin_id
        && (held == namespace
            || held.starts_with(&format!("{namespace}."))
            || namespace.starts_with(&format!("{held}.")))
}

/// `published`, `revoked`, `suspended`: an absent key keeps its value. A revoked version stays
/// revoked.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionStateChange {
    pub published: Option<bool>,
    pub revoked: Option<bool>,
    pub suspended: Option<bool>,
}

impl VersionStateChange {
    /// `(published, revoked, suspended)` after this change.
    pub fn applied_to(&self, version: &PluginVersion) -> RegistryResult<(bool, bool, bool)> {
        let revoked = self.revoked.unwrap_or(version.revoked);
        let published = self.published.unwrap_or(version.published && !revoked);
        let suspended = self.suspended.unwrap_or(version.suspended);
        if version.revoked && !revoked {
            return Err(PluginRegistryError::Conflict(
                "a revoked version stays revoked".into(),
            ));
        }
        if published && revoked {
            return Err(PluginRegistryError::Invalid(
                "revoked versions cannot be published".into(),
            ));
        }
        Ok((published, revoked, suspended))
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionFilter {
    pub plugin_id: Option<String>,
    pub query: Option<String>,
    pub updated_since: Option<i64>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VersionKey {
    Release {
        plugin_id: String,
        plugin_version: String,
    },
    Digest(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginAssetContent {
    pub digest: String,
    pub media_type: String,
    pub bytes: Vec<u8>,
}

/// A web bundle of a version the platform approved (`plugin_web_bundle.attach`). The platform
/// reviewed the bytes; Core keeps the outcome so it serves a bundle only while it stands, and
/// stops serving it everywhere at once when it is withdrawn. Where each application fetches it and
/// which bridge permissions the application approved come with the application's projection
/// ([`flare_im_core_capability_core::plugin::runtime::ProjectedWebBundle`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WebBundleAttachment {
    pub plugin_id: String,
    pub plugin_version: String,
    pub bundle_id: String,
    /// 64 lowercase hex digits, as the manifest declares it.
    pub sha256: String,
    pub size: i64,
    pub approved_at: i64,
    /// Set once: a withdrawn bundle of a version stays withdrawn.
    pub withdrawn_at: Option<i64>,
    pub updated_at: i64,
}

impl WebBundleAttachment {
    pub fn withdrawn(&self) -> bool {
        self.withdrawn_at.is_some()
    }
}

/// `plugin_web_bundle.attach`: the platform approved this bundle of this version.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WebBundleApproval {
    pub plugin_id: String,
    pub plugin_version: String,
    pub bundle_id: String,
    pub sha256: String,
    pub size: u64,
}

impl WebBundleApproval {
    /// The approval names a bundle the version's signed manifest declares, with the same digest
    /// and size: Core never serves bytes the publisher did not sign.
    pub fn check(&self, manifest: &PluginManifest) -> RegistryResult<()> {
        if manifest.plugin_id != self.plugin_id || manifest.version != self.plugin_version {
            return Err(PluginRegistryError::Invalid(
                "web bundle names another version".into(),
            ));
        }
        let decl = manifest
            .client_extensions
            .web_bundle(&self.bundle_id)
            .ok_or_else(|| {
                PluginRegistryError::Invalid("the manifest does not declare this web bundle".into())
            })?;
        if decl.sha256 != self.sha256 || decl.size != self.size {
            return Err(PluginRegistryError::Invalid(
                "web bundle digest or size differs from the manifest".into(),
            ));
        }
        Ok(())
    }
}

/// `plugin_web_bundle.withdraw`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WebBundleRef {
    pub plugin_id: String,
    pub plugin_version: String,
    pub bundle_id: String,
}

/// `plugin_web_bundle.list`: the bundles of one version, or of every version of a plugin.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WebBundleFilter {
    pub plugin_id: String,
    #[serde(default)]
    pub plugin_version: Option<String>,
}

/// What a resolution serves of one projected plugin: each projected bundle the manifest
/// declares with the same digest and the registry holds approved, not withdrawn, with the same
/// digest; in catalog form. Anything else is left out (logged by the caller), never served.
pub fn served_web_bundles(
    manifest: &PluginManifest,
    projected: &[flare_im_core_capability_core::plugin::runtime::ProjectedWebBundle],
    approved: Option<&std::collections::BTreeMap<String, String>>,
) -> Vec<flare_im_core_capability_core::plugin::bundle::WebBundleDecl> {
    let Some(approved) = approved else {
        return Vec::new();
    };
    projected
        .iter()
        .filter_map(|bundle| {
            let decl = manifest.client_extensions.web_bundle(&bundle.id)?;
            (decl.sha256 == bundle.sha256
                && approved
                    .get(&bundle.id)
                    .is_some_and(|sha| *sha == bundle.sha256))
            .then(|| bundle.catalog_entry(decl))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn version(published: bool, revoked: bool) -> PluginVersion {
        PluginVersion {
            plugin_id: "p".into(),
            plugin_version: "1.0.0".into(),
            display_name: "P".into(),
            publisher_id: "pub".into(),
            manifest_digest: "sha256:x".into(),
            kind: "im".into(),
            published,
            revoked,
            suspended: false,
            created_at: 1,
            updated_at: 1,
        }
    }

    #[test]
    fn revoking_withdraws_publication_and_cannot_be_undone() {
        let change = VersionStateChange {
            revoked: Some(true),
            ..VersionStateChange::default()
        };
        assert_eq!(
            change.applied_to(&version(true, false)),
            Ok((false, true, false))
        );
        let undo = VersionStateChange {
            revoked: Some(false),
            ..VersionStateChange::default()
        };
        assert!(matches!(
            undo.applied_to(&version(false, true)),
            Err(PluginRegistryError::Conflict(_))
        ));
        let publish = VersionStateChange {
            published: Some(true),
            ..VersionStateChange::default()
        };
        assert!(matches!(
            publish.applied_to(&version(false, true)),
            Err(PluginRegistryError::Invalid(_))
        ));
    }

    #[test]
    fn namespaces_conflict_when_one_contains_the_other_across_plugins() {
        assert!(namespace_conflict("a", "b", "flare.pay", "flare.pay"));
        assert!(namespace_conflict("a", "b", "flare.pay.x", "flare.pay"));
        assert!(namespace_conflict("a", "b", "flare.pay", "flare.pay.x"));
        assert!(!namespace_conflict("a", "b", "flare.pay", "flare.payment"));
        assert!(!namespace_conflict("a", "a", "flare.pay", "flare.pay"));
    }
}
