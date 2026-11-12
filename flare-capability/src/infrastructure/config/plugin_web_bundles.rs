//! `plugin.webBundles.enabled`: whether this deployment delivers plugin web bundles (L3).
//!
//! On by default. `FLARE_PLUGIN_WEB_BUNDLES_ENABLED=0` (or `false`) turns it off for a private
//! deployment or a channel that must not run plugin web code: client catalogs then select
//! declarative variants only.

pub const PLUGIN_WEB_BUNDLES_ENV: &str = "FLARE_PLUGIN_WEB_BUNDLES_ENABLED";

pub fn plugin_web_bundles_enabled() -> bool {
    let enabled = std::env::var(PLUGIN_WEB_BUNDLES_ENV).map_or(true, |value| {
        !matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "0" | "false" | "off"
        )
    });
    if !enabled {
        tracing::info!(
            "{PLUGIN_WEB_BUNDLES_ENV} is off: plugin catalogs select declarative variants only"
        );
    }
    enabled
}
