//! Explicit private per-application credential mount. No default project/topic,
//! environment credential fallback, remote secret fetch or live-file reload.
use crate::{
    application::{
        application_delivery::{ApplicationChannels, ApplicationDelivery},
        mobile_fanout::MobileFanout,
    },
    config::OfflineDeliveryBackend,
    domain::offline_delivery::{DeviceTokenRepository, OfflinePushExecutor},
    infrastructure::{
        apns_push::{ApnsCredentials, ApnsOfflinePushExecutor},
        fcm_push::{FcmOfflinePushExecutor, FcmServiceAccount},
        getui_push::{GetuiClient, GetuiConfig, GetuiOfflinePushExecutor},
        vendor_http,
    },
    interface::messaging::offline_consumer::UsageCountedExecutor,
};
use flare_server_core::{ErrorCode, FlareError};
use std::{collections::HashMap, fs::File, io::Read, path::Path, sync::Arc};
const MAX_BYTES: u64 = 4 * 1024 * 1024;
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema: String,
    applications: Vec<Application>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Application {
    app_id: String,
    fcm: Option<FcmServiceAccount>,
    apns: Option<ApnsCredentials>,
    getui: Option<GetuiCredentials>,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GetuiCredentials {
    app_id: String,
    app_key: String,
    master_secret: String,
}
fn rejected() -> FlareError {
    FlareError::localized(
        ErrorCode::InvalidParameter,
        "private per-application push credentials rejected",
    )
}

fn read(path: &Path) -> Result<Document, FlareError> {
    let before = std::fs::symlink_metadata(path).map_err(|_| rejected())?;
    if !before.is_file() || before.len() > MAX_BYTES {
        return Err(rejected());
    }
    let file = File::open(path).map_err(|_| rejected())?;
    let actual = file.metadata().map_err(|_| rejected())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if actual.mode() & 0o077 != 0
            || actual.nlink() != 1
            || before.dev() != actual.dev()
            || before.ino() != actual.ino()
        {
            return Err(rejected());
        }
    }
    let mut bytes = Vec::new();
    file.take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| rejected())?;
    if bytes.len() as u64 > MAX_BYTES {
        return Err(rejected());
    }
    let doc: Document = serde_json::from_slice(&bytes).map_err(|_| rejected())?;
    if doc.schema != "flare.mobile-push-applications/v1"
        || doc.applications.is_empty()
        || doc.applications.len() > 1024
    {
        return Err(rejected());
    }
    Ok(doc)
}

pub fn build(
    path: &Path,
    allowed: &[OfflineDeliveryBackend],
    tokens: Arc<dyn DeviceTokenRepository>,
    usage: flare_im_usage::UsageRecorder,
    ttl_ms: u64,
    request_ms: u64,
) -> Result<ApplicationDelivery, FlareError> {
    let document = read(path)?;
    assemble(document, allowed, tokens, usage, ttl_ms, request_ms)
}
fn assemble(
    document: Document,
    allowed: &[OfflineDeliveryBackend],
    tokens: Arc<dyn DeviceTokenRepository>,
    usage: flare_im_usage::UsageRecorder,
    ttl_ms: u64,
    request_ms: u64,
) -> Result<ApplicationDelivery, FlareError> {
    let http = vendor_http::client()?;
    let mut apps = HashMap::new();
    for app in document.applications {
        if app.app_id.is_empty()
            || app.app_id.len() > 128
            || !app
                .app_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
            || apps.contains_key(&app.app_id)
        {
            return Err(rejected());
        }
        let mut providers = Vec::<Arc<dyn OfflinePushExecutor>>::new();
        let mut names = Vec::new();
        if let Some(account) = app.fcm {
            names.push("fcm");
            if !allowed.contains(&OfflineDeliveryBackend::Fcm) {
                return Err(rejected());
            }
            providers.push(UsageCountedExecutor::wrap(
                Arc::new(FcmOfflinePushExecutor::new(
                    account,
                    http.clone(),
                    tokens.clone(),
                )?),
                "fcm",
                usage.clone(),
            ));
        }
        if let Some(account) = app.apns {
            names.push("apns");
            if !allowed.contains(&OfflineDeliveryBackend::Apns) {
                return Err(rejected());
            }
            providers.push(UsageCountedExecutor::wrap(
                Arc::new(ApnsOfflinePushExecutor::new(
                    account,
                    http.clone(),
                    tokens.clone(),
                )?),
                "apns",
                usage.clone(),
            ));
        }
        if let Some(account) = app.getui {
            names.push("getui");
            if !allowed.contains(&OfflineDeliveryBackend::Getui) {
                return Err(rejected());
            }
            let cfg = GetuiConfig::new(
                account.app_id,
                account.app_key,
                account.master_secret,
                None,
                ttl_ms,
                request_ms,
            )?;
            providers.push(UsageCountedExecutor::wrap(
                Arc::new(GetuiOfflinePushExecutor::new(
                    tokens.clone(),
                    Arc::new(GetuiClient::new(cfg)?),
                    ttl_ms,
                )),
                "getui",
                usage.clone(),
            ));
        }
        if providers.is_empty() {
            return Err(rejected());
        }
        apps.insert(
            app.app_id,
            ApplicationChannels {
                providers: names,
                executor: Arc::new(MobileFanout(providers)),
            },
        );
    }
    Ok(ApplicationDelivery {
        applications: apps,
        tokens,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn application_map_rejects_duplicates_and_disallowed_providers() {
        let make = |ids: &[&str]| -> Document {
            serde_json::from_value(serde_json::json!({"schema":"flare.mobile-push-applications/v1","applications":ids.iter().map(|app|serde_json::json!({"app_id":app,"getui":{"app_id":"vendor-a","app_key":"private-test-key","master_secret":"private-test-secret"}})).collect::<Vec<_>>()})).unwrap()
        };
        let tokens = Arc::new(crate::infrastructure::push_test_support::Tokens::default());
        let usage = flare_im_usage::UsageRecorder::disabled();
        let good = assemble(
            make(&["app-a", "app-b"]),
            &[OfflineDeliveryBackend::Getui],
            tokens.clone(),
            usage.clone(),
            60_000,
            5_000,
        )
        .unwrap();
        assert_eq!(good.applications.len(), 2);
        assert!(
            assemble(
                make(&["app-a", "app-a"]),
                &[OfflineDeliveryBackend::Getui],
                tokens.clone(),
                usage.clone(),
                60_000,
                5_000
            )
            .is_err()
        );
        assert!(
            assemble(
                make(&["app-a"]),
                &[OfflineDeliveryBackend::Fcm],
                tokens.clone(),
                usage.clone(),
                60_000,
                5_000
            )
            .is_err()
        );
        assert!(
            assemble(
                make(&["app.*"]),
                &[OfflineDeliveryBackend::Getui],
                tokens,
                usage,
                60_000,
                5_000
            )
            .is_err()
        );
    }
    #[test]
    fn schema_unknown_fields_and_private_file_permissions_are_checked() {
        use std::io::Write;
        #[cfg(unix)]
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
        let dir =
            std::env::temp_dir().join(format!("flare-push-credentials-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let path = dir.join("applications.json");
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&path).unwrap();
        file.write_all(br#"{"schema":"flare.mobile-push-applications/v1","applications":[{"app_id":"app-a","getui":{"app_id":"vendor-a","app_key":"private-test-key","master_secret":"private-test-secret"}}]}"#).unwrap();
        drop(file);
        assert!(read(&path).is_ok());
        #[cfg(unix)]
        {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            let err = read(&path).err().unwrap().to_string();
            assert!(!err.contains("private-test"));
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            let link = dir.join("alias");
            std::os::unix::fs::symlink(&path, &link).unwrap();
            assert!(read(&link).is_err());
            std::fs::remove_file(link).unwrap();
        }
        assert!(
            serde_json::from_str::<Document>(
                r#"{"schema":"x","applications":[],"unknown":"secret"}"#
            )
            .is_err()
        );
        std::fs::remove_file(path).unwrap();
        std::fs::remove_dir(dir).unwrap();
    }
}
