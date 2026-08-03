use async_trait::async_trait;
use flare_core::client::{ClientEventHandler, ObserverClientBuilder};
use flare_core::common::MessageParser;
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::config_types::{HeartbeatConfig, TransportProtocol};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::error::Result;
use flare_core::common::protocol::flare::core::commands::command::Type;
use flare_core::common::protocol::flare::core::commands::notification_command::Type as NotifType;
use flare_core::common::protocol::flare::core::commands::payload_command::Type as MsgType;
use flare_core::common::protocol::flare::core::commands::system_command::Type as SysType;
use flare_core::common::protocol::{
    Frame, Reliability, frame_with_message_command, generate_message_id, send_message,
};
use flare_core::transport::events::{ConnectionEvent, ConnectionObserver};
use prost::Message;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, error, info, warn};

fn gen_single_conversation_id(a: &str, b: &str) -> String {
    let (min_id, max_id) = if a <= b { (a, b) } else { (b, a) };
    let combined = format!("{}:{}", min_id, max_id);
    let mut hasher = Sha256::new();
    hasher.update(combined.as_bytes());
    let hash = hasher.finalize();
    let hex_str = hex::encode(&hash[..16]);
    format!("1-{}", hex_str)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .init();
    let default_host =
        std::env::var("NEGOTIATION_HOST").unwrap_or_else(|_| "localhost:60051".to_string());
    let ws_url =
        std::env::var("NEGOTIATION_WS_URL").unwrap_or_else(|_| format!("ws://{}", default_host));
    let platform = std::env::var("DEVICE_PLATFORM")
        .map(|v| DevicePlatform::from_str(&v))
        .unwrap_or(DevicePlatform::PC);
    let device_info = DeviceInfo::new(
        format!(
            "integration-client-{}-{}",
            platform.as_str(),
            std::process::id()
        ),
        platform.clone(),
    )
    .with_model(platform.as_str().to_string())
    .with_app_version("1.0.0".to_string());
    let user_id = std::env::var("USER_ID").unwrap_or_else(|_| "123456".to_string());
    let peer_id = std::env::var("PEER_ID").unwrap_or_else(|_| "user1".to_string());
    let conversation_id = std::env::var("SESSION_ID")
        .ok()
        .unwrap_or_else(|| gen_single_conversation_id(&user_id, &peer_id));
    info!(%user_id, %peer_id, %conversation_id, host = %default_host, "integration client start");

    let heartbeat = HeartbeatConfig::default()
        .with_interval(Duration::from_secs(30))
        .with_timeout(Duration::from_secs(90));
    let observer = Arc::new(Observer {
        user_id: user_id.clone(),
    });
    let handler = Arc::new(Events);
    let token = std::env::var("TOKEN").unwrap_or_else(|_| {
        use flare_server_core::TokenService;
        TokenService::new(
            std::env::var("TOKEN_SECRET").unwrap_or_else(|_| "insecure-secret".to_string()),
            "flare-im-core".to_string(),
            3600,
        )
        .generate_token(&user_id, None, None)
        .unwrap_or_default()
    });
    let mut builder = ObserverClientBuilder::new(&ws_url)
        .with_observer(observer as Arc<dyn ConnectionObserver>)
        .with_event_handler(handler as Arc<dyn ClientEventHandler>)
        .with_protocol_race(vec![TransportProtocol::WebSocket])
        .with_protocol_url(TransportProtocol::WebSocket, ws_url.clone())
        .with_format(flare_core::common::protocol::SerializationFormat::Json)
        .with_compression(CompressionAlgorithm::None)
        .with_device_info(device_info)
        .with_user_id(user_id.clone())
        .with_heartbeat(heartbeat)
        .with_connect_timeout(Duration::from_secs(10))
        .with_reconnect_interval(Duration::from_secs(3))
        .with_max_reconnect_attempts(Some(5));
    if !token.is_empty() {
        builder = builder.with_token(token);
    }
    let mut client = builder.build_with_race().await?;
    info!("connected");

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();
    loop {
        tokio::select! {
            read = reader.read_line(&mut line) => {
                match read {
                    Ok(0) => { break; }
                    Ok(_) => {
                        let message = line.trim().to_string();
                        line.clear();
                        if message.is_empty() { continue; }
                        if message == "quit" || message == "exit" { break; }
                        let mut metadata = std::collections::HashMap::new();
                        metadata.insert("conversation_id".to_string(), conversation_id.as_bytes().to_vec());
                        metadata.insert("message_type".to_string(), "text".as_bytes().to_vec());
                        metadata.insert("conversation_type".to_string(), "single".as_bytes().to_vec());
                        metadata.insert("business_type".to_string(), "chat".as_bytes().to_vec());
                        metadata.insert("receiver_id".to_string(), peer_id.as_bytes().to_vec());
                        let cmd = send_message(generate_message_id(), message.into_bytes(), Some(metadata), None);
                        let frame = frame_with_message_command(cmd, Reliability::AtLeastOnce);
                        match client.send_frame(&frame).await { Ok(_) => debug!("sent"), Err(err) => { error!(?err, "send failed"); println!("send failed: {}", err); } }
                    }
                    Err(_) => { break; }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                if !client.is_connected() { warn!("disconnected, will reconnect"); }
            }
        }
    }
    client.disconnect().await?;
    Ok(())
}

struct Observer {
    user_id: String,
}
#[async_trait]
impl ConnectionObserver for Observer {
    fn on_event(&self, event: &ConnectionEvent) {
        match event {
            ConnectionEvent::Connected => {
                info!(user = %self.user_id, "connected");
            }
            ConnectionEvent::Disconnected(reason) => {
                warn!(%reason, "disconnected");
            }
            ConnectionEvent::Error(err) => {
                error!(?err, "error");
            }
            ConnectionEvent::Message(data) => {
                let parser = MessageParser::protobuf();
                if let Ok(frame) = parser.parse(data)
                    && let Some(cmd) = &frame.command
                    && let Some(Type::Payload(msg)) = &cmd.r#type
                {
                    if msg.r#type != MsgType::Message as i32 {
                        return;
                    }
                    match flare_proto::common::Message::decode(msg.payload.as_slice()) {
                        Ok(message) => {
                            let sender = message.sender_id;
                            let text = match message.content.and_then(|c| c.content) {
                                Some(flare_proto::common::message_content::Content::Text(t)) => {
                                    t.text
                                }
                                _ => String::from_utf8_lossy(&msg.payload).to_string(),
                            };
                            println!("\n[{}] {}", sender, text);
                        }
                        Err(_) => {
                            let text = String::from_utf8_lossy(&msg.payload).to_string();
                            println!("\n[?] {}", text);
                        }
                    }
                }
            }
        }
    }
}

struct Events;
#[async_trait]
impl ClientEventHandler for Events {
    async fn handle_system_command(&self, _t: SysType, _f: &Frame) -> Result<Option<Frame>> {
        Ok(None)
    }
    async fn handle_message_command(&self, _t: MsgType, _f: &Frame) -> Result<Option<Frame>> {
        Ok(None)
    }
    async fn handle_notification_command(
        &self,
        _t: NotifType,
        _f: &Frame,
    ) -> Result<Option<Frame>> {
        Ok(None)
    }
    async fn handle_connection_event(&self, _e: &ConnectionEvent) -> Result<()> {
        Ok(())
    }
}
