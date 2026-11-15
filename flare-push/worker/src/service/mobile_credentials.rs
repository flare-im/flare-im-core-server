//! Explicit private per-application credential mount. No default project/topic,
//! environment credential fallback or remote secret fetch. A separate leased
//! file adapter supports generic authorities without Core business dependencies.
mod leased;
use crate::{
    application::{
        application_delivery::ApplicationDelivery,
        binding_delivery::{DeliveryRuntime, DeviceBindingsDelivery, DevicePush},
    },
    config::OfflineDeliveryBackend,
    domain::{
        offline_delivery::DeviceTokenRepository,
        vendor_delivery::{BindingDelivery, Vendor},
    },
    infrastructure::{
        apns_push::{ApnsCredentials, ApnsOfflinePushExecutor},
        fcm_push::{FcmOfflinePushExecutor, FcmServiceAccount},
        getui_push::{GetuiAndroidVendor, GetuiClient, GetuiConfig, GetuiOfflinePushExecutor},
        vendor_http,
    },
};
use flare_server_core::{ErrorCode, FlareError};
pub use leased::LeasedApplications;
use std::{collections::HashMap, fs::File, io::Read, path::Path, sync::Arc};
const MAX_BYTES: u64 = 4 * 1024 * 1024;
/// 现行的凭据文件格式。v2 起 APNs 凭据列出允许的包名（`topics`），topic 与沙盒按绑定定；
/// 个推可带 Android 厂商通道参数（`android_vendor`：华为分类、荣耀重要级、小米单聊与群聊的
/// 私信通道和模板、OPPO 旧私信通道与新分类的私信模板和提醒级别、vivo 分类），取值与搭配规则见
/// `GetuiAndroidVendor::validate`。
const SCHEMA: &str = "flare.mobile-push-applications/v2";
/// 不再接受的旧格式：它的 APNs 凭据只有一个 topic 和一个全局的沙盒开关。
const SCHEMA_V1: &str = "flare.mobile-push-applications/v1";
/// 只读格式名，用来在完整解析之前认出旧格式：旧文件带着 v2 不认识的字段，
/// 完整解析只会报一个说不清原因的错误。
#[derive(serde::Deserialize)]
struct SchemaProbe {
    schema: String,
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Document {
    schema: String,
    #[serde(default)]
    valid_until_ms: Option<i64>,
    applications: Vec<Application>,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct Application {
    app_id: String,
    #[serde(default)]
    disabled_providers: Vec<String>,
    fcm: Option<FcmServiceAccount>,
    apns: Option<ApnsCredentials>,
    getui: Option<GetuiCredentials>,
}
#[derive(serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct GetuiCredentials {
    app_id: String,
    app_key: String,
    master_secret: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    android_vendor: Option<GetuiAndroidVendor>,
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
    parse(&bytes)
}

fn parse(bytes: &[u8]) -> Result<Document, FlareError> {
    let probe: SchemaProbe = serde_json::from_slice(bytes).map_err(|_| rejected())?;
    if probe.schema == SCHEMA_V1 {
        return Err(FlareError::localized(
            ErrorCode::InvalidParameter,
            "private per-application push credentials use flare.mobile-push-applications/v1, \
             which is no longer accepted: publish flare.mobile-push-applications/v2, where \
             apns.topics lists the allowed bundle ids and apns.topic and apns.sandbox are gone \
             (the APNs environment now comes from each device binding)",
        ));
    }
    if probe.schema != SCHEMA {
        return Err(rejected());
    }
    let doc: Document = serde_json::from_slice(bytes).map_err(|_| rejected())?;
    if doc.schema != SCHEMA || doc.applications.len() > 1024 {
        return Err(rejected());
    }
    Ok(doc)
}

/// `runtime` outlives the document: a lease rotation rebuilds the vendor adapters of every
/// application, not the governance, the digest hold, the trace or the usage meter behind them.
pub fn build(
    path: &Path,
    allowed: &[OfflineDeliveryBackend],
    tokens: Arc<dyn DeviceTokenRepository>,
    runtime: Arc<DeliveryRuntime>,
    ttl_ms: u64,
    request_ms: u64,
) -> Result<ApplicationDelivery, FlareError> {
    let document = read(path)?;
    assemble(document, allowed, tokens, runtime, ttl_ms, request_ms)
}
fn assemble(
    document: Document,
    allowed: &[OfflineDeliveryBackend],
    tokens: Arc<dyn DeviceTokenRepository>,
    runtime: Arc<DeliveryRuntime>,
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
        // Usage (`push.attempts`) is counted per attempt by the binding selection, with the
        // vendor as provider.
        let mut providers = Vec::<(Vendor, Arc<dyn BindingDelivery>)>::new();
        let mut disabled = Vec::new();
        for p in app.disabled_providers {
            let name = match p.as_str() {
                "apns" => Vendor::Apns,
                "fcm" => Vendor::Fcm,
                "getui" => Vendor::Getui,
                _ => return Err(rejected()),
            };
            if disabled.contains(&name) {
                return Err(rejected());
            }
            disabled.push(name);
        }
        if let Some(account) = app.fcm {
            if !allowed.contains(&OfflineDeliveryBackend::Fcm) {
                return Err(rejected());
            }
            providers.push((
                Vendor::Fcm,
                Arc::new(FcmOfflinePushExecutor::new(
                    account,
                    http.clone(),
                    tokens.clone(),
                )?),
            ));
        }
        if let Some(account) = app.apns {
            if !allowed.contains(&OfflineDeliveryBackend::Apns) {
                return Err(rejected());
            }
            providers.push((
                Vendor::Apns,
                Arc::new(ApnsOfflinePushExecutor::new(
                    account,
                    http.clone(),
                    tokens.clone(),
                )?),
            ));
        }
        if let Some(account) = app.getui {
            if !allowed.contains(&OfflineDeliveryBackend::Getui) {
                return Err(rejected());
            }
            let android_vendor = account.android_vendor.unwrap_or_default();
            android_vendor.validate()?;
            let cfg = GetuiConfig::new(
                account.app_id,
                account.app_key,
                account.master_secret,
                None,
                ttl_ms,
                request_ms,
            )?;
            providers.push((
                Vendor::Getui,
                Arc::new(GetuiOfflinePushExecutor::new(
                    tokens.clone(),
                    Arc::new(GetuiClient::new(cfg)?),
                    ttl_ms,
                    android_vendor,
                )),
            ));
        }
        if (providers.is_empty() && disabled.is_empty())
            || providers
                .iter()
                .any(|(vendor, _)| disabled.contains(vendor))
        {
            return Err(rejected());
        }
        let device: Arc<dyn DeviceBindingsDelivery> = Arc::new(DevicePush::new(
            app.app_id.clone(),
            providers,
            disabled,
            runtime.clone(),
        ));
        apps.insert(app.app_id, device);
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
            serde_json::from_value(serde_json::json!({"schema":"flare.mobile-push-applications/v2","applications":ids.iter().map(|app|serde_json::json!({"app_id":app,"getui":{"app_id":"vendor-a","app_key":"private-test-key","master_secret":"private-test-secret"}})).collect::<Vec<_>>()})).unwrap()
        };
        let tokens = Arc::new(crate::infrastructure::push_test_support::Tokens::default());
        let runtime = DeliveryRuntime::for_tests();
        let good = assemble(
            make(&["app-a", "app-b"]),
            &[OfflineDeliveryBackend::Getui],
            tokens.clone(),
            runtime.clone(),
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
                runtime.clone(),
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
                runtime.clone(),
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
                runtime,
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
        file.write_all(br#"{"schema":"flare.mobile-push-applications/v2","applications":[{"app_id":"app-a","getui":{"app_id":"vendor-a","app_key":"private-test-key","master_secret":"private-test-secret"}}]}"#).unwrap();
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

    /// v1 文件给出说得清的拒绝原因；v2 的 APNs 凭据列出包名，个推可带 Android 厂商参数。
    #[test]
    fn v1_is_refused_with_a_reason_and_v2_carries_topics_and_vendor_options() {
        let v1 = br#"{"schema":"flare.mobile-push-applications/v1","applications":[{"app_id":"app-a","apns":{"team_id":"TEAM000000","key_id":"KEY0000000","private_key_pem":"private-test-key","topic":"com.example.app","sandbox":true}}]}"#;
        let error = parse(v1).err().unwrap().to_string();
        assert!(
            error.contains("flare.mobile-push-applications/v2"),
            "{error}"
        );
        assert!(!error.contains("private-test"), "{error}");
        assert!(
            parse(br#"{"schema":"flare.mobile-push-applications/v3","applications":[]}"#).is_err()
        );

        let v2 = serde_json::json!({
            "schema": "flare.mobile-push-applications/v2",
            "applications": [{
                "app_id": "app-a",
                "apns": {
                    "team_id": "TEAM000000",
                    "key_id": "KEY0000000",
                    "private_key_pem": "private-test-key",
                    "topics": ["com.example.app", "com.example.app.beta"],
                },
                "getui": {
                    "app_id": "vendor-a",
                    "app_key": "private-test-key",
                    "master_secret": "private-test-secret",
                    "android_vendor": {
                        "huawei_category": "IM",
                        "xiaomi_channel_id": "100001",
                        "xiaomi_template_id": "xm-direct",
                        "xiaomi_group_channel_id": "100002",
                        "xiaomi_group_template_id": "xm-group",
                        "oppo_private_template_id": "op-im",
                        "oppo_notify_level": 16,
                    },
                },
            }],
        });
        let document = parse(&serde_json::to_vec(&v2).unwrap()).unwrap();
        let app = &document.applications[0];
        assert_eq!(
            app.apns.as_ref().unwrap().topics,
            vec!["com.example.app", "com.example.app.beta"]
        );
        let vendor = app.getui.as_ref().unwrap().android_vendor.clone().unwrap();
        assert_eq!(vendor.huawei_category.as_deref(), Some("IM"));
        assert_eq!(vendor.honor_importance, None);
        assert_eq!(vendor.xiaomi_group_template_id.as_deref(), Some("xm-group"));
        assert_eq!(vendor.oppo_notify_level, Some(16));
        assert!(vendor.validate().is_ok());

        // 厂商参数写错是启动错误，不是悄悄不带：取值不合形状、小米通道缺模板、OPPO 提醒级别不认得。
        let getui_only = |mutate: &dyn Fn(&mut serde_json::Value)| {
            let mut bad = v2.clone();
            bad["applications"][0]
                .as_object_mut()
                .unwrap()
                .remove("apns");
            mutate(&mut bad["applications"][0]["getui"]["android_vendor"]);
            parse(&serde_json::to_vec(&bad).unwrap()).unwrap()
        };
        let good = getui_only(&|_| {});
        let assembled = |document| {
            assemble(
                document,
                &[OfflineDeliveryBackend::Getui],
                Arc::new(crate::infrastructure::push_test_support::Tokens::default()),
                DeliveryRuntime::for_tests(),
                60_000,
                5_000,
            )
        };
        assert!(assembled(good).is_ok());
        for bad in [
            getui_only(&|vendor| vendor["vivo_category"] = serde_json::json!("I M")),
            getui_only(&|vendor| {
                vendor.as_object_mut().unwrap().remove("xiaomi_template_id");
            }),
            getui_only(&|vendor| vendor["oppo_notify_level"] = serde_json::json!(5)),
        ] {
            assert!(assembled(bad).is_err());
        }
    }
}
