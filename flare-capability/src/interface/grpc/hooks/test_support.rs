//! 测试用：Cell 签名密钥、按 Message Ingest 的方式调 HookPlugin、记录投递的 Webhook 接收端。

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ed25519_dalek::SigningKey;
use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
use flare_grpc_proto::capability::hook_plugin_server::HookPluginServer;
use flare_im_hooks::hooks::adapters::GrpcHookFactory;
use flare_im_hooks::{MessageDraft, PreSendDecision};
use flare_server_core::context::Ctx;
use flare_server_core::grpc::middleware::signed_context::verify_signed_context_metadata;
use flare_server_core::{
    SignedActorType, SignedContextIdentity, SignedContextSigner, SignedContextVerifier,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::domain::model::{HookConfigItem, HookSelectorConfig, HookTransportConfig};
use crate::interface::grpc::hooks::ImHookPluginServer;

/// Cell 内部上下文的签名与验签密钥（同一对）。
pub(crate) struct CellKeys {
    pub signer: SignedContextSigner,
    pub verifier: SignedContextVerifier,
}

pub(crate) fn cell_keys(seed: u8) -> CellKeys {
    let key = SigningKey::from_bytes(&[seed; 32]);
    let private = key.to_pkcs8_pem(Default::default()).unwrap();
    let public = key
        .verifying_key()
        .to_public_key_pem(Default::default())
        .unwrap();
    CellKeys {
        signer: SignedContextSigner::from_private_key(&private).unwrap(),
        verifier: SignedContextVerifier::from_public_key(&public).unwrap(),
    }
}

/// Message Ingest 调 Hook 时手里的上下文：网关签名、Ingest 验过的身份（带原始令牌）。
pub(crate) fn signed_ctx(keys: &CellKeys, actor: SignedActorType, id: &str, tenant: &str) -> Ctx {
    let request_id = format!("req-{}", uuid::Uuid::new_v4().simple());
    let token = keys
        .signer
        .sign(&SignedContextIdentity::new(actor, id, tenant, &request_id))
        .unwrap();
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert("x-flare-ctx", token.parse().unwrap());
    verify_signed_context_metadata(&metadata, &keys.verifier).unwrap()
}

/// 本机端口上的 HookPlugin 服务（与进程里一样：不套任何上下文层），返回 `http://` 地址。
pub(crate) async fn serve_hook_plugin(server: ImHookPluginServer) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(HookPluginServer::new(server))
            .serve_with_incoming(tonic::transport::server::TcpIncoming::from(listener)),
    );
    format!("http://{address}")
}

/// 以 Message Ingest 的 gRPC Hook 适配器调 `endpoint` 的 PreSend（原样转发 `ctx` 的签名令牌）。
pub(crate) async fn ingest_pre_send(endpoint: &str, ctx: &Ctx, marker: &str) -> PreSendDecision {
    let hook = GrpcHookFactory::new().build_pre_send(HashMap::new(), endpoint.into());
    let mut draft = MessageDraft::new(b"hello".to_vec());
    draft.set_client_message_id(marker.to_string());
    hook.handle(ctx, &mut draft).await
}

/// 一条 PreSend Webhook 配置。
pub(crate) fn webhook_hook(name: &str, endpoint: &str) -> HookConfigItem {
    HookConfigItem {
        name: name.into(),
        version: None,
        description: None,
        enabled: true,
        priority: 900,
        group: None,
        timeout_ms: 5_000,
        max_retries: 0,
        error_policy: "fail_fast".into(),
        require_success: true,
        selector: HookSelectorConfig::default(),
        transport: HookTransportConfig::Webhook {
            endpoint: endpoint.into(),
            secret: None,
            headers: HashMap::new(),
        },
        metadata: HashMap::new(),
    }
}

/// 收到的一次 Webhook 投递。
#[derive(Debug, Clone)]
pub(crate) struct Delivery {
    pub path: String,
    pub body: serde_json::Value,
}

impl Delivery {
    pub fn tenant(&self) -> Option<&str> {
        self.body["context"]["tenant_id"].as_str()
    }

    pub fn marker(&self) -> Option<&str> {
        self.body["draft"]["client_message_id"].as_str()
    }
}

/// 只绑 127.0.0.1 的 Webhook 接收端：记下每次投递的路径与 JSON 体，回 `{"allow":true}`。
pub(crate) struct WebhookReceiver {
    base: String,
    deliveries: Arc<Mutex<Vec<Delivery>>>,
}

impl WebhookReceiver {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let deliveries = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&deliveries);
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    if let Some(delivery) = read_request(&mut stream).await {
                        seen.lock().unwrap().push(delivery);
                    }
                    let body = br#"{"allow":true}"#;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(head.as_bytes()).await;
                    let _ = stream.write_all(body).await;
                    let _ = stream.shutdown().await;
                });
            }
        });
        Self { base, deliveries }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// 带 `marker`（草稿的 `client_message_id`）的投递。
    pub fn with_marker(&self, marker: &str) -> Vec<Delivery> {
        self.deliveries
            .lock()
            .unwrap()
            .iter()
            .filter(|delivery| delivery.marker() == Some(marker))
            .cloned()
            .collect()
    }

    pub fn count(&self) -> usize {
        self.deliveries.lock().unwrap().len()
    }

    /// 等到带 `marker` 的投递至少有 `count` 次（或超时），返回它们。
    pub async fn wait_for(&self, marker: &str, count: usize) -> Vec<Delivery> {
        for _ in 0..50 {
            let found = self.with_marker(marker);
            if found.len() >= count {
                return found;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        self.with_marker(marker)
    }
}

async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<Delivery> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            return None;
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(position) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let path = head.split_whitespace().nth(1)?.to_string();
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while buffer.len() < head_end + length {
        let read = stream.read(&mut chunk).await.ok()?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = serde_json::from_slice(&buffer[head_end..]).unwrap_or(serde_json::Value::Null);
    Some(Delivery { path, body })
}
