//! Controlled test peer; not a vendor/phone or final system-stack fixture.
use crate::domain::DeviceTokenRepository;
use flare_im_contracts::{Ctx, DevicePushToken};
use flare_proto::PushTaskEnvelope;
use flare_server_core::FlareError;
use std::collections::HashSet;
use std::sync::Mutex;

pub(crate) fn alert_payload() -> Vec<u8> {
    use prost::Message;
    flare_grpc_proto::access_gateway::PushMessageRequest {
        messages: vec![flare_proto::common::Message::default()],
        ..Default::default()
    }
    .encode_to_vec()
}

/// A message whose sender filled in `offline_push_info`.
pub(crate) fn push_info_payload(title: &str, body: &str) -> Vec<u8> {
    use prost::Message;
    flare_grpc_proto::access_gateway::PushMessageRequest {
        messages: vec![flare_proto::common::Message {
            offline_push_info: Some(flare_proto::common::OfflinePushInfo {
                title: title.into(),
                body: body.into(),
                ..Default::default()
            }),
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec()
}

pub(crate) fn token(device: &str) -> DevicePushToken {
    DevicePushToken {
        tenant_id: "t1".into(),
        user_id: "u1".into(),
        device_id: device.into(),
        platform: "android".into(),
        provider: "fcm".into(),
        token: device.into(),
        registration_id: format!("generation-{device}"),
        registered_at_ms: 1,
        ..Default::default()
    }
}
/// A device-targeted task with a renderable payload; tests override what they exercise.
pub(crate) fn device_task(
    device: &str,
    disposition: flare_proto::common::PushDisposition,
) -> PushTaskEnvelope {
    PushTaskEnvelope {
        user_id: "u1".into(),
        message_id: "m1".into(),
        conversation_id: "c1".into(),
        tenant_id: "t1".into(),
        push_payload: alert_payload(),
        payload_kind: flare_proto::common::PushTaskPayloadKind::Message as i32,
        device_id: device.into(),
        disposition: disposition as i32,
        ..Default::default()
    }
}

/// Mirrors the registry: the router's key names the task when it gave one.
fn task_identity(e: &PushTaskEnvelope) -> String {
    if e.idempotency_key.is_empty() {
        e.message_id.clone()
    } else {
        e.idempotency_key.clone()
    }
}

#[derive(Default)]
pub(crate) struct Tokens {
    pub rows: Mutex<Vec<DevicePushToken>>,
    pub accepted: Mutex<HashSet<(String, String)>>,
    pub removed: Mutex<Vec<String>>,
}
#[async_trait::async_trait]
impl DeviceTokenRepository for Tokens {
    async fn is_current(&self, t: &DevicePushToken) -> Result<bool, FlareError> {
        Ok(!self.removed.lock().unwrap().contains(&t.registration_id))
    }
    async fn list_device_tokens(
        &self,
        _: &Ctx,
        _: &str,
        _: &str,
        device_id: &str,
    ) -> Result<Vec<DevicePushToken>, FlareError> {
        Ok(self
            .rows
            .lock()
            .unwrap()
            .iter()
            .filter(|row| row.device_id == device_id)
            .cloned()
            .collect())
    }
    async fn remove_device_token(&self, _: &Ctx, t: &DevicePushToken) -> Result<(), FlareError> {
        self.removed.lock().unwrap().push(t.registration_id.clone());
        Ok(())
    }
    async fn was_accepted(
        &self,
        t: &DevicePushToken,
        e: &PushTaskEnvelope,
    ) -> Result<bool, FlareError> {
        Ok(self
            .accepted
            .lock()
            .unwrap()
            .contains(&(t.registration_id.clone(), task_identity(e))))
    }
    async fn record_accepted(
        &self,
        t: &DevicePushToken,
        e: &PushTaskEnvelope,
    ) -> Result<(), FlareError> {
        self.accepted
            .lock()
            .unwrap()
            .insert((t.registration_id.clone(), task_identity(e)));
        Ok(())
    }
}

/// Delivers `task` the way the worker does, with `adapter` as the application's only vendor:
/// the target device's bindings listed once, then the governed binding selection (one push per
/// device, fresh governance per call). A device without bindings needs no vendor.
pub(crate) async fn deliver_governed(
    vendor: crate::domain::vendor_delivery::Vendor,
    adapter: std::sync::Arc<dyn crate::domain::vendor_delivery::BindingDelivery>,
    tokens: &dyn DeviceTokenRepository,
    task: &PushTaskEnvelope,
) -> Result<(), FlareError> {
    use crate::application::binding_delivery::{
        DeliveryRuntime, DeviceBindingsDelivery, DevicePush,
    };
    let ctx = Ctx::default();
    let bindings = tokens
        .list_device_tokens(&ctx, &task.tenant_id, &task.user_id, &task.device_id)
        .await?;
    if bindings.is_empty() {
        return Ok(());
    }
    DevicePush::new(
        task.tenant_id.clone(),
        vec![(vendor, adapter)],
        Vec::new(),
        DeliveryRuntime::for_tests(),
    )
    .deliver_bindings(&ctx, task, bindings)
    .await
}

pub(crate) async fn http_peer(
    responses: Vec<(u16, String)>,
) -> (String, tokio::task::JoinHandle<Vec<serde_json::Value>>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let mut bodies = Vec::new();
        for (status, body) in responses {
            let (mut socket, _) =
                tokio::time::timeout(std::time::Duration::from_secs(5), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
            let mut bytes = Vec::new();
            let (boundary, len) = loop {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
                assert!(bytes.len() < 65536);
                if let Some(boundary) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                    let header = String::from_utf8(bytes[..boundary].to_vec()).unwrap();
                    let len = header
                        .lines()
                        .find_map(|s| {
                            s.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|v| v.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (boundary + 4, len);
                }
            };
            while bytes.len() < boundary + len {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&buf[..n]);
            }
            bodies.push(serde_json::from_slice(&bytes[boundary..boundary + len]).unwrap());
            socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        }
        bodies
    });
    (format!("http://{address}"), task)
}
