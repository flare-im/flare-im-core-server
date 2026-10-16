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
    async fn list_user_tokens(
        &self,
        _: &Ctx,
        _: &str,
        _: &str,
    ) -> Result<Vec<DevicePushToken>, FlareError> {
        Ok(self.rows.lock().unwrap().clone())
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
            .contains(&(t.registration_id.clone(), e.message_id.clone())))
    }
    async fn record_accepted(
        &self,
        t: &DevicePushToken,
        e: &PushTaskEnvelope,
    ) -> Result<(), FlareError> {
        self.accepted
            .lock()
            .unwrap()
            .insert((t.registration_id.clone(), e.message_id.clone()));
        Ok(())
    }
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
