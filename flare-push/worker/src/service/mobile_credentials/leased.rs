use super::*;
use flare_im_contracts::Ctx;
use flare_proto::PushTaskEnvelope;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
struct Cached {
    checked: Instant,
    expires: i64,
    digest: [u8; 32],
    delivery: Arc<ApplicationDelivery>,
}
/// One bounded cache per worker, not per tenant/user/device. Refresh uses
/// try_lock: no unbounded queue when the private authority fails.
pub struct LeasedApplications {
    path: PathBuf,
    allowed: Vec<OfflineDeliveryBackend>,
    tokens: Arc<dyn DeviceTokenRepository>,
    usage: flare_im_usage::UsageRecorder,
    ttl_ms: u64,
    request_ms: u64,
    cache: tokio::sync::Mutex<Option<Cached>>,
}
fn unavailable() -> FlareError {
    FlareError::localized(
        ErrorCode::ServiceUnavailable,
        "leased push configuration unavailable",
    )
}
fn now_ms() -> Result<i64, FlareError> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| unavailable())?
            .as_millis(),
    )
    .map_err(|_| unavailable())
}
impl LeasedApplications {
    pub fn new(
        path: PathBuf,
        allowed: Vec<OfflineDeliveryBackend>,
        tokens: Arc<dyn DeviceTokenRepository>,
        usage: flare_im_usage::UsageRecorder,
        ttl_ms: u64,
        request_ms: u64,
    ) -> Self {
        Self {
            path,
            allowed,
            tokens,
            usage,
            ttl_ms,
            request_ms,
            cache: tokio::sync::Mutex::new(None),
        }
    }
    async fn current(&self) -> Result<Arc<ApplicationDelivery>, FlareError> {
        let now = now_ms()?;
        let mut cache = self.cache.try_lock().map_err(|_| unavailable())?;
        if let Some(c) = cache
            .as_ref()
            .filter(|c| c.checked.elapsed() < Duration::from_secs(1) && c.expires > now)
        {
            return Ok(c.delivery.clone());
        }
        let path = self.path.clone();
        let doc = tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::spawn_blocking(move || read(&path)),
        )
        .await
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?
        .map_err(|_| unavailable())?;
        let expires = doc.valid_until_ms.ok_or_else(unavailable)?;
        let now = now_ms()?;
        if expires <= now || expires > now.saturating_add(30_000) {
            return Err(unavailable());
        }
        // Exclude heartbeat lease from identity: retain vendor OAuth caches.
        let bytes = serde_json::to_vec(&doc.applications).map_err(|_| unavailable())?;
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        if let Some(c) = cache.as_mut().filter(|c| c.digest == digest) {
            c.expires = expires;
            c.checked = Instant::now();
            return Ok(c.delivery.clone());
        }
        let delivery = Arc::new(assemble(
            doc,
            &self.allowed,
            self.tokens.clone(),
            self.usage.clone(),
            self.ttl_ms,
            self.request_ms,
        )?);
        *cache = Some(Cached {
            checked: Instant::now(),
            expires,
            digest,
            delivery: delivery.clone(),
        });
        Ok(delivery)
    }
}
#[async_trait::async_trait]
impl OfflinePushExecutor for LeasedApplications {
    async fn deliver(&self, ctx: &Ctx, envelope: &PushTaskEnvelope) -> Result<(), FlareError> {
        if ctx.tenant_id() != Some(envelope.tenant_id.as_str()) {
            return Err(FlareError::localized(
                ErrorCode::PermissionDenied,
                "push application context mismatch",
            ));
        }
        if self
            .tokens
            .list_user_tokens(ctx, &envelope.tenant_id, &envelope.user_id)
            .await?
            .is_empty()
        {
            return Ok(());
        }
        self.current().await?.deliver(ctx, envelope).await
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn write(path: &std::path::Path, secret: &str, expires: i64) {
        let value = serde_json::json!({"schema":"flare.mobile-push-applications/v1","valid_until_ms":expires,"applications":[{"app_id":"t1","getui":{"app_id":"vendor-a","app_key":"key","master_secret":secret}}]});
        std::fs::write(path, serde_json::to_vec(&value).unwrap()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }
    fn path() -> PathBuf {
        std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("flare-leased-push-{}", uuid::Uuid::new_v4()))
    }
    fn executor(path: PathBuf) -> LeasedApplications {
        LeasedApplications::new(
            path,
            vec![OfflineDeliveryBackend::Getui],
            Arc::new(crate::infrastructure::push_test_support::Tokens::default()),
            flare_im_usage::UsageRecorder::disabled(),
            60_000,
            5_000,
        )
    }
    async fn refresh(e: &LeasedApplications) {
        if let Some(c) = e.cache.lock().await.as_mut() {
            c.checked = Instant::now() - Duration::from_secs(2);
        }
    }
    #[tokio::test]
    async fn hot_rotation_preserves_same_credentials_cache_and_expires_closed() {
        let path = path();
        write(&path, "secret-one", now_ms().unwrap() + 20_000);
        let e = executor(path.clone());
        let first = e.current().await.unwrap();
        write(&path, "secret-one", now_ms().unwrap() + 25_000);
        refresh(&e).await;
        assert!(Arc::ptr_eq(&first, &e.current().await.unwrap()));
        write(&path, "secret-two", now_ms().unwrap() + 25_000);
        refresh(&e).await;
        assert!(!Arc::ptr_eq(&first, &e.current().await.unwrap()));
        write(&path, "secret-two", now_ms().unwrap() - 1);
        refresh(&e).await;
        assert!(e.current().await.is_err());
        std::fs::remove_file(&path).unwrap();
        assert!(e.current().await.is_err());
    }
    #[tokio::test]
    async fn lease_and_private_file_are_required_and_no_stale_or_cross_app_fallback() {
        let path = path();
        let e = executor(path.clone());
        assert!(e.current().await.is_err());
        write(&path, "secret", now_ms().unwrap() + 90_000);
        assert!(e.current().await.is_err());
        std::fs::write(
            &path,
            br#"{"schema":"flare.mobile-push-applications/v1","applications":[]}"#,
        )
        .unwrap();
        assert!(e.current().await.is_err());
        write(&path, "secret", now_ms().unwrap() + 20_000);
        let current = e.current().await.unwrap();
        assert!(current.applications.contains_key("t1"));
        assert!(!current.applications.contains_key("another-app"));
        let held = e.cache.lock().await;
        assert!(e.current().await.is_err());
        drop(held);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            refresh(&e).await;
            assert!(e.current().await.is_err());
        }
        std::fs::remove_file(path).unwrap();
    }
    #[tokio::test]
    async fn explicit_disabled_channel_is_an_intentional_no_send_not_missing_configuration() {
        let path = path();
        let value = serde_json::json!({"schema":"flare.mobile-push-applications/v1","valid_until_ms":now_ms().unwrap()+20_000,"applications":[{"app_id":"t1","disabled_providers":["fcm"]}]});
        std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let tokens = Arc::new(crate::infrastructure::push_test_support::Tokens::default());
        tokens
            .rows
            .lock()
            .unwrap()
            .push(crate::infrastructure::push_test_support::token(
                "device-one",
            ));
        let e = LeasedApplications::new(
            path.clone(),
            vec![OfflineDeliveryBackend::Fcm],
            tokens,
            flare_im_usage::UsageRecorder::disabled(),
            60_000,
            5_000,
        );
        let ctx = Arc::new(Ctx::default().with_tenant_id("t1"));
        let envelope = PushTaskEnvelope {
            tenant_id: "t1".into(),
            user_id: "u1".into(),
            ..Default::default()
        };
        e.deliver(&ctx, &envelope).await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
