//! # 一对一聊天客户端示例
//!
//! 这是一个基于 Flare IM Core 的一对一聊天客户端示例，连接到 `flare-signaling-gateway`，
//! 支持两人之间的私聊。消息直接发送给指定的接收方，不经过聊天室广播。
//!
//! ## 使用方法
//!
//! ### 基本使用
//!
//! ```bash
//! # 启动客户端（使用默认用户ID）
//! cargo run --example chatroom_client
//!
//! # 指定用户ID和接收方ID
//! cargo run --example chatroom_client -- user1 user2
//!
//! # 使用环境变量指定用户ID和接收方ID
//! USER_ID=user1 RECIPIENT_ID=user2 cargo run --example chatroom_client
//! ```
//!
//! ### 跨地区网关路由（多网关部署）
//!
//! ```bash
//! # 连接到北京网关
//! NEGOTIATION_HOST=gateway-beijing.example.com:60051 cargo run --example chatroom_client -- user1 user2
//!
//! # 连接到上海网关
//! NEGOTIATION_HOST=gateway-shanghai.example.com:60051 cargo run --example chatroom_client -- user1 user2
//!
//! # 连接到本地网关（开发环境）
//! NEGOTIATION_HOST=localhost:60051 cargo run --example chatroom_client -- user1 user2
//! ```
//!
//! ### 工作原理
//!
//! 1. **客户端连接**：客户端通过 `NEGOTIATION_HOST` 连接到指定的 Access Gateway
//! 2. **网关注册**：Access Gateway 在用户登录时，将 `gateway_id` 注册到 Signaling Online 服务
//! 3. **消息路由**：消息通过 Signaling Online 查询接收方所在的 `gateway_id`，然后路由到对应的 Access Gateway
//! 4. **点对点通信**：消息直接发送给指定接收方，不经过聊天室广播

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use flare_core::client::{FlareClientBuilder, MessageListener};
use flare_core::common::compression::{CompressionAlgorithm, CompressionUtil};
use flare_core::common::config_types::{HeartbeatConfig, TransportProtocol};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::encryption::{Aes256GcmEncryptor, EncryptionUtil};
use flare_core::common::error::Result;
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::flare::core::commands::payload_command::Type as PayloadType;
use flare_core::common::protocol::{
    Frame, PayloadCommand, Reliability, frame_with_payload_command, generate_message_id,
    send_message,
};
use prost::Message;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, error, info, warn};

use chrono::{DateTime, Local, Utc};
use flare_core::common::conversation::generate_single_chat_conversation_id;
use flare_proto::access_gateway::PushMessageRequest;
use flare_proto::common::{
    Ack, AckType, ConversationAck, EventEnvelope, Message as ProtoMessage, MessageContent,
    MessagePush, SendAck, ack::Payload as AckPayload, event::Payload as EventPayload,
};
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .init();

    // 从环境变量或命令行参数获取配置
    // 支持多网关连接：通过 NEGOTIATION_HOST 指定不同的网关地址
    // 示例：
    //   NEGOTIATION_HOST=localhost:60051 cargo run --example chatroom_client -- user1 user2  # 连接到网关1
    let default_host =
        std::env::var("NEGOTIATION_HOST").unwrap_or_else(|_| "localhost:60051".to_string());
    let default_ws = format!("ws://{default_host}");

    let host = std::env::var("NEGOTIATION_HOST").unwrap_or(default_host);
    let ws_url = std::env::var("NEGOTIATION_WS_URL").unwrap_or(default_ws);

    let platform = std::env::var("DEVICE_PLATFORM")
        .map(|value| DevicePlatform::from_str(&value))
        .unwrap_or(DevicePlatform::PC);

    let device_info = DeviceInfo::new(
        format!("p2p-client-{}-{}", platform.as_str(), std::process::id()),
        platform.clone(),
    )
    .with_model(platform.as_str().to_string())
    .with_app_version("1.0.0".to_string());

    // ============================================================
    // 注册加密器（如果服务端启用了加密，客户端也需要注册）
    // ============================================================
    // 注意：在生产环境中，密钥应该从安全配置中读取，不要硬编码
    // 这里使用与服务端相同的示例密钥（32 字节）
    let encryption_key = b"01234567890123456789012345678901"; // 32 bytes for AES-256
    match Aes256GcmEncryptor::new(encryption_key) {
        Ok(encryptor) => {
            EncryptionUtil::register_custom(Arc::new(encryptor));
            info!("🔐 已注册 AES-256-GCM 加密器");
        }
        Err(e) => {
            warn!(error = %e, "无法注册加密器，如果服务端启用加密，连接可能失败");
        }
    }

    // 解析用户ID和接收方ID
    let (user_id, recipient_id) = resolve_user_and_recipient_id().await;
    info!(
        %user_id,
        %recipient_id,
        platform = %platform.as_str(),
        host = %host,
        "🚀 启动一对一聊天客户端"
    );

    let heartbeat = HeartbeatConfig::default()
        .with_interval(Duration::from_secs(30))
        .with_timeout(Duration::from_secs(90));

    let chat_listener = Arc::new(ChatListener {
        message_count: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        user_id: user_id.clone(),
        recipient_id: recipient_id.clone(),
        seen_message_ids: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        pending_acks: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        client: Arc::new(tokio::sync::Mutex::new(None)),
    });

    // 获取 token（从环境变量或生成测试 token）
    let token = std::env::var("TOKEN").unwrap_or_else(|_| {
        // 如果没有提供 token，生成一个测试 token
        use flare_server_core::TokenService;
        let token_service = TokenService::new(
            "insecure-secret".to_string(),
            "flare-im-core".to_string(),
            3600,
        );
        match token_service.generate_token(&user_id, None, Some("default")) {
            Ok(t) => {
                info!("🔑 自动生成测试 token");
                t
            }
            Err(e) => {
                warn!(?e, "无法生成 token，连接可能失败");
                String::new()
            }
        }
    });

    // 使用 FlareClientBuilder 构建客户端
    let mut client_builder = FlareClientBuilder::new(&ws_url)
        .with_listener(chat_listener.clone() as Arc<dyn MessageListener>)
        .with_protocol_race(vec![TransportProtocol::WebSocket]) // 只使用 WebSocket，避免协议竞速超时
        .with_protocol_url(TransportProtocol::WebSocket, ws_url.clone())
        .with_format(flare_core::common::protocol::SerializationFormat::Protobuf)
        .with_compression(CompressionAlgorithm::None)
        .with_device_info(device_info)
        .with_user_id(user_id.clone())
        .with_heartbeat(heartbeat)
        .with_connect_timeout(Duration::from_secs(10))
        .with_reconnect_interval(Duration::from_secs(3))
        .with_max_reconnect_attempts(Some(5));

    // 如果提供了 token，添加到客户端配置
    if !token.is_empty() {
        client_builder = client_builder.with_token(token);
    }

    let client = client_builder.build_with_race().await?;

    // 设置客户端引用到监听器
    {
        let mut client_ref = chat_listener.client.lock().await;
        *client_ref = Some(client);
    }

    info!("✅ 已连接到 {host}");
    info!("   当前用户ID: {user_id}");
    info!("   接收方用户ID: {recipient_id}");
    info!("   输入聊天内容后回车即可发送，输入 'quit' 或 'exit' 退出");
    info!("   输入 '/userid' 查看当前用户ID");
    info!("   输入 '/recipient' 查看接收方ID");
    info!("   输入 '/help' 查看帮助");

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    loop {
        tokio::select! {
            read = reader.read_line(&mut line) => {
                match read {
                    Ok(0) => {
                        info!("输入结束，退出客户端");
                        break;
                    }
                    Ok(_) => {
                        let message = line.trim().to_string();
                        line.clear();

                        if message.is_empty() {
                            continue;
                        }

                        // 处理命令
                        match message.as_str() {
                            "quit" | "exit" => {
                                info!("退出客户端");
                                break;
                            }
                            "/userid" => {
                                info!("当前用户ID: {user_id}");
                                continue;
                            }
                            "/recipient" => {
                                info!("接收方用户ID: {recipient_id}");
                                continue;
                            }
                            "/help" => {
                                print_help();
                                continue;
                            }
                            _ => {}
                        }

                        // 发送一对一消息
                        // 构造消息内容
                        let text_content = flare_proto::common::TextContent {
                            text: message.clone(),
                            mentions: vec![],
                        };

                        let message_content = flare_proto::common::MessageContent {
                            content: Some(flare_proto::common::message_content::Content::Text(text_content)),
                        };

                        // 构造完整的Message对象，将recipient_id作为conversation_id
                        let timestamp = prost_types::Timestamp {
                            seconds: chrono::Utc::now().timestamp(),
                            nanos: 0,
                        };

                        // 使用工具类生成单聊会话ID（格式：1-{hash}）；服务端会在首条消息时自行创建会话，客户端不调用创建会话接口
                        let conversation_id = generate_single_chat_conversation_id(&user_id, &recipient_id);

                        let mut extra = std::collections::HashMap::new();
                        extra.insert("recipient_id".to_string(), recipient_id.clone());
                        extra.insert("source".to_string(), "user".to_string());
                        extra.insert("conversation_type".to_string(), "single".to_string());

                        let content_bytes = message_content.encode_to_vec();

                        // 单聊时 channel_id = 对方 user_id（proto 无 receiver_id 字段）
                        let msg = flare_proto::common::Message {
                            server_id: generate_message_id(),
                            conversation_id: conversation_id.clone(),
                            client_msg_id: String::new(),
                            sender_id: user_id.clone(),
                            source: flare_proto::common::MessageSource::User as i32,
                            seq: 0,
                            timestamp: Some(timestamp.clone()),
                            conversation_type: flare_proto::common::ConversationType::Single as i32,
                            message_type: flare_proto::common::MessageType::Text as i32,
                            channel_id: recipient_id.clone(),
                            content: content_bytes,
                            extra,
                            ..Default::default()
                        };

                        // 序列化消息对象
                        let mut buf = Vec::new();
                        msg.encode(&mut buf).map_err(|e| flare_core::common::error::FlareError::serialization_error(
                            format!("Failed to encode message: {}", e)
                        ))?;

                        // 构建 metadata，包含 conversation_id（Gateway 需要从 metadata 中提取）
                        let mut metadata = std::collections::HashMap::new();
                        metadata.insert("conversation_id".to_string(), msg.conversation_id.as_bytes().to_vec());

                        let cmd = send_message(
                            msg.server_id.clone(),
                            buf,
                            Some(metadata),
                            None,
                        );
                        let frame = frame_with_payload_command(cmd, Reliability::AtLeastOnce);
                        // 记录发送开始时间
                        let send_start = std::time::Instant::now();

                        // 记录待确认的消息ID
                        {
                            let mut pending = chat_listener.pending_acks.lock().unwrap();
                            pending.insert(msg.server_id.clone(), std::time::Instant::now());
                        }

                        // 使用较长的超时时间发送消息，确保消息真正发送
                        let frame_clone = frame.clone();
                        let listener_clone = chat_listener.clone();
                        let message_id = msg.server_id.clone();
                        let recipient_id_clone = recipient_id.clone();
                        match tokio::time::timeout(
                            std::time::Duration::from_secs(5), // 5秒超时，确保消息真正发送
                            async move {
                                let client_ref = listener_clone.client.lock().await;
                                if let Some(client) = client_ref.as_ref() {
                                    debug!("开始发送消息: message_id={}, receiver_id={}", message_id, recipient_id_clone);
                                    let result = client.send_frame(&frame_clone).await;
                                    debug!("消息发送完成: message_id={}, result={:?}", message_id, result.is_ok());
                                    result
                                } else {
                                    Err(flare_core::common::error::FlareError::system("Client not initialized".to_string()))
                                }
                            }
                        ).await {
                            Ok(result) => {
                                match result {
                                    Ok(_) => {
                                        let elapsed = send_start.elapsed();
                                        info!("消息已发送给 {} (耗时: {:?})", recipient_id, elapsed);
                                        let now = chrono::Local::now();
                                        let send_time = format!("{}.{:03}", now.format("%H:%M:%S"), now.timestamp_subsec_millis());
                                        println!("[{}] 我 → {}: {}", send_time, recipient_id, message);
                                    }
                                    Err(err) => {
                                        let elapsed = send_start.elapsed();
                                        error!(?err, message_id = %msg.server_id, "发送消息失败 (耗时: {:?})", elapsed);
                                        eprintln!("❌ 发送失败: {}", err);
                                        // 移除待确认的消息ID
                                        let mut pending = chat_listener.pending_acks.lock().unwrap();
                                        pending.remove(&msg.server_id);
                                    }
                                }
                            }
                            Err(_) => {
                                // 超时，消息发送失败
                                let elapsed = send_start.elapsed();
                                error!(message_id = %msg.server_id, "消息发送超时 (耗时: {:?})", elapsed);
                                eprintln!("❌ 发送超时: 消息可能未成功发送");
                                // 移除待确认的消息ID
                                let mut pending = chat_listener.pending_acks.lock().unwrap();
                                pending.remove(&msg.server_id);
                            }
                        }

                    }
                    Err(err) => {
                        error!(?err, "读取输入失败");
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(Duration::from_millis(100)) => {
                // 客户端会自动重连
            }
        }
    }

    {
        let mut client_ref = chat_listener.client.lock().await;
        if let Some(client) = client_ref.take() {
            // FlareClient可能没有disconnect方法，或者会自动断开
            drop(client);
        }
    }
    info!("客户端已断开");
    Ok(())
}

fn print_help() {
    println!();
    println!("=== 一对一聊天客户端帮助 ===");
    println!("命令:");
    println!("  /userid    - 显示当前用户ID");
    println!("  /recipient - 显示接收方用户ID");
    println!("  /help      - 显示此帮助信息");
    println!("  quit/exit  - 退出客户端");
    println!();
    println!("使用:");
    println!("  直接输入消息内容后回车即可发送");
    println!("  消息会直接发送给指定的接收方");
    println!();
}

async fn resolve_user_and_recipient_id() -> (String, String) {
    let args: Vec<String> = std::env::args().collect();

    // 1. 优先使用命令行参数
    if args.len() >= 3 {
        info!(
            "📝 使用命令行提供的用户ID: {} 和接收方ID: {}",
            args[1], args[2]
        );
        return (args[1].clone(), args[2].clone());
    }

    // 2. 使用环境变量
    let user_id = if let Ok(env_user) = std::env::var("USER_ID") {
        info!("📝 使用环境变量 USER_ID: {env_user}");
        env_user
    } else {
        // 交互式输入用户ID
        info!("📝 请输入用户ID（直接回车使用默认值）:");
        print!("用户ID (默认: user-{}): ", std::process::id());
        use std::io::Write;
        std::io::stdout().flush().unwrap();

        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut buffer = String::new();
        match reader.read_line(&mut buffer).await {
            Ok(_) => {
                let trimmed = buffer.trim();
                if trimmed.is_empty() {
                    format!("user-{}", std::process::id())
                } else {
                    trimmed.to_string()
                }
            }
            Err(err) => {
                error!(?err, "读取用户输入失败，使用默认用户ID");
                format!("user-{}", std::process::id())
            }
        }
    };

    let recipient_id = if let Ok(env_recipient) = std::env::var("RECIPIENT_ID") {
        info!("📝 使用环境变量 RECIPIENT_ID: {env_recipient}");
        env_recipient
    } else {
        // 交互式输入接收方ID
        info!("📝 请输入接收方用户ID:");
        print!("接收方用户ID: ");
        use std::io::Write;
        std::io::stdout().flush().unwrap();

        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut buffer = String::new();
        match reader.read_line(&mut buffer).await {
            Ok(_) => buffer.trim().to_string(),
            Err(err) => {
                error!(?err, "读取接收方用户ID失败");
                "unknown".to_string()
            }
        }
    };

    (user_id, recipient_id)
}

/// 格式化时间戳为可读格式（包含毫秒）
fn format_timestamp(timestamp: &prost_types::Timestamp) -> String {
    let utc_time = DateTime::<Utc>::from_timestamp(timestamp.seconds, timestamp.nanos as u32);
    match utc_time {
        Some(utc) => {
            let local_time = utc.with_timezone(&Local);
            // 格式：HH:MM:SS.mmm（包含毫秒）
            let millis = timestamp.nanos / 1_000_000; // 将纳秒转换为毫秒
            format!("{}.{:03}", local_time.format("%H:%M:%S"), millis)
        }
        None => "未知时间".to_string(),
    }
}

/// 获取消息类型的显示名称和图标
fn get_message_type_display(message_type: i32) -> (&'static str, &'static str) {
    match message_type {
        x if x == flare_proto::common::MessageType::Text as i32 => ("文本", "📝"),
        x if x == flare_proto::common::MessageType::Image as i32 => ("图片", "🖼️"),
        x if x == flare_proto::common::MessageType::Video as i32 => ("视频", "🎬"),
        x if x == flare_proto::common::MessageType::Audio as i32 => ("音频", "🎵"),
        x if x == flare_proto::common::MessageType::File as i32 => ("文件", "📎"),
        x if x == flare_proto::common::MessageType::Location as i32 => ("位置", "📍"),
        x if x == flare_proto::common::MessageType::Card as i32 => ("卡片", "📇"),
        x if x == flare_proto::common::MessageType::Custom as i32 => ("自定义", "🔧"),
        _ => ("未知", "❓"),
    }
}

/// 解析消息内容
fn parse_message_content(content: &MessageContent) -> String {
    match &content.content {
        Some(flare_proto::common::message_content::Content::Text(text_content)) => {
            text_content.text.clone()
        }
        Some(flare_proto::common::message_content::Content::Image(_)) => "[图片消息]".to_string(),
        Some(flare_proto::common::message_content::Content::File(_)) => "[文件消息]".to_string(),
        Some(flare_proto::common::message_content::Content::Audio(_)) => "[音频消息]".to_string(),
        Some(flare_proto::common::message_content::Content::Video(_)) => "[视频消息]".to_string(),
        Some(flare_proto::common::message_content::Content::Location(location_content)) => {
            format!(
                "[位置] 经度:{}, 纬度:{}",
                location_content.longitude, location_content.latitude
            )
        }
        Some(flare_proto::common::message_content::Content::Card(card_content)) => {
            // CardContent 包含用户信息，不是传统意义的卡片
            format!(
                "[名片] {} ({})",
                card_content.nickname, card_content.user_id
            )
        }
        _ => "[无法解析的消息内容]".to_string(),
    }
}

/// 若 payload 为 Gzip 等压缩数据则解压，否则返回原数据。
fn ensure_decompressed_payload(payload: &[u8]) -> Vec<u8> {
    match CompressionUtil::auto_decompress(payload) {
        Ok((decompressed, _)) => decompressed,
        Err(_) => payload.to_vec(),
    }
}

fn messages_from_event_envelope(envelope: EventEnvelope) -> Vec<ProtoMessage> {
    let mut out = Vec::new();
    for ev in envelope.events {
        if let Some(EventPayload::Message(m)) = ev.payload {
            out.push(m);
        }
    }
    out
}

/// 下行 `PayloadCommand.payload` 解码出的全部 `Message`（单帧可多包：MessagePush / EventEnvelope / …）。
fn collect_payload_messages(data: &[u8]) -> Vec<ProtoMessage> {
    if let Ok(push) = MessagePush::decode(data) {
        let mut v = push.messages;
        v.extend(push.notifications);
        if !v.is_empty() {
            return v;
        }
    }
    if let Ok(req) = PushMessageRequest::decode(data) {
        if !req.messages.is_empty() {
            return req.messages;
        }
    }
    if let Ok(envelope) = EventEnvelope::decode(data) {
        let v = messages_from_event_envelope(envelope);
        if !v.is_empty() {
            return v;
        }
    }
    if let Ok(m) = ProtoMessage::decode(data) {
        return vec![m];
    }
    Vec::new()
}

/// 解析单条消息（统一的消息解析逻辑）
fn parse_single_message(message: &ProtoMessage) -> Option<MessageDisplayInfo> {
    let is_recalled = message.status == flare_proto::common::MessageStatus::Recalled as i32;
    let receiver_id_hint = message
        .extra
        .get("receiver_id")
        .or_else(|| message.extra.get("recipient_id"))
        .cloned()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| message.channel_id.clone());
    if is_recalled {
        return Some(MessageDisplayInfo {
            id: message.server_id.clone(),
            sender_id: message.sender_id.clone(),
            receiver_id: receiver_id_hint,
            content: "[消息已撤回]".to_string(),
            message_type: "撤回".to_string(),
            timestamp: message
                .timestamp
                .as_ref()
                .map(|ts| format_timestamp(ts))
                .unwrap_or_else(|| "未知时间".to_string()),
            is_self: false, // 这个会在调用时设置
        });
    }

    let content = if message.content.is_empty() {
        "[空消息]".to_string()
    } else {
        match MessageContent::decode(message.content.as_slice()) {
            Ok(msg_content) => parse_message_content(&msg_content),
            Err(_) => format!("[无法解析的消息内容 ({} bytes)]", message.content.len()),
        }
    };

    let (type_name, _) = get_message_type_display(message.message_type);

    // 单聊投递目标：优先 extra（编排层常写 receiver_id/recipient_id），避免 channel_id 为会话哈希时 is_to_me 失效
    Some(MessageDisplayInfo {
        id: message.server_id.clone(),
        sender_id: message.sender_id.clone(),
        receiver_id: receiver_id_hint,
        content,
        message_type: type_name.to_string(),
        timestamp: message
            .timestamp
            .as_ref()
            .map(|ts| format_timestamp(ts))
            .unwrap_or_else(|| "未知时间".to_string()),
        is_self: false, // 这个会在调用时设置
    })
}
/// 格式化消息显示（简化版：只显示时间、发送者、接收者、内容）
/// 使用当前时间而不是消息中的时间戳
fn format_message_display(info: &MessageDisplayInfo, current_user_id: &str) -> String {
    let is_self = info.sender_id == current_user_id;
    let sender = if is_self { "我" } else { &info.sender_id };
    let receiver = if is_self { &info.receiver_id } else { "我" };

    // 使用当前时间（客户端收到/发送消息的时间）
    let now = chrono::Local::now();
    let current_time = format!(
        "{}.{:03}",
        now.format("%H:%M:%S"),
        now.timestamp_subsec_millis()
    );

    format!(
        "[{}] {} → {}: {}",
        current_time, sender, receiver, info.content
    )
}

/// 消息显示信息
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct MessageDisplayInfo {
    id: String,
    sender_id: String,
    receiver_id: String,
    content: String,
    message_type: String,
    timestamp: String,
    is_self: bool,
}

/// 一对一聊天消息监听器
struct ChatListener {
    message_count: Arc<std::sync::atomic::AtomicU64>,
    user_id: String,
    recipient_id: String,
    /// 已展示的 `server_id`（非空才入集；空 id 不做去重）
    seen_message_ids: Arc<std::sync::Mutex<HashSet<String>>>,
    // 待确认的消息ID集合（用于ACK处理）
    pending_acks: Arc<std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>>,
    // 客户端引用（用于发送ACK）
    client: Arc<tokio::sync::Mutex<Option<flare_core::client::FlareClient>>>,
}

#[async_trait]
impl MessageListener for ChatListener {
    async fn on_message(&self, frame: &Frame) -> Result<Option<Frame>> {
        debug!("ChatListener::on_message 被调用");

        // 为Frame生成唯一标识，用于调试
        let frame_id = format!(
            "{}-{:?}",
            frame.message_id,
            frame.metadata.get("timestamp").unwrap_or(&vec![])
        );
        debug!("处理Frame: {}", frame_id);

        // 首先检查 Frame 是否包含命令
        if let Some(command) = &frame.command {
            debug!("Frame 包含命令");

            // 检查是否为消息命令
            if let Some(CommandType::Payload(msg_cmd)) = &command.r#type {
                debug!(
                    "收到消息命令: type={}, message_id={}, payload_len={}",
                    msg_cmd.r#type,
                    msg_cmd.message_id,
                    msg_cmd.payload.len()
                );

                // Payload: Message=1, Event=2, Ack=3, Data=4（与 flare.core.commands 一致）
                if msg_cmd.r#type == PayloadType::Ack as i32 {
                    return self.handle_server_ack(msg_cmd).await;
                }

                if msg_cmd.payload.is_empty() {
                    return Ok(None);
                }

                let payload = ensure_decompressed_payload(&msg_cmd.payload);
                let messages = collect_payload_messages(&payload);
                if messages.is_empty() {
                    debug!(len = payload.len(), "下行 payload 未解码出任何 Message");
                    return Ok(None);
                }

                for proto in messages {
                    let Some(mut display_info) = parse_single_message(&proto) else {
                        continue;
                    };

                    display_info.is_self = display_info.sender_id == self.user_id;
                    let is_from_recipient = display_info.sender_id == self.recipient_id;
                    let is_to_me = display_info.receiver_id == self.user_id;
                    let is_system_message = display_info.sender_id == "system";
                    let is_from_self = display_info.sender_id == self.user_id;

                    // 单聊：会话 ID 与「我 + 对方」一致且发送者非本人时，视为对方消息（避免 channel_id/extra 与线上一致时仍漏显）
                    let expected_cid =
                        generate_single_chat_conversation_id(&self.user_id, &self.recipient_id);
                    let is_single_chat_peer = proto.conversation_type
                        == flare_proto::common::ConversationType::Single as i32
                        && proto.conversation_id == expected_cid
                        && !proto.sender_id.is_empty()
                        && proto.sender_id != self.user_id;

                    let dedup_key = (!display_info.id.is_empty()).then(|| display_info.id.clone());
                    let is_duplicate = if let Some(ref id) = dedup_key {
                        let mut seen = self.seen_message_ids.lock().unwrap();
                        if seen.contains(id) {
                            true
                        } else {
                            seen.insert(id.clone());
                            false
                        }
                    } else {
                        false
                    };

                    let should_display = ((is_from_recipient || is_to_me)
                        || is_system_message
                        || is_single_chat_peer)
                        && !is_from_self
                        && !is_duplicate;

                    if should_display {
                        println!("{}", format_message_display(&display_info, &self.user_id));
                        self.message_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }

                    if !is_from_self {
                        if let Err(e) = self
                            .send_delivery_ack(&display_info.id, &display_info.sender_id)
                            .await
                        {
                            warn!(error = %e, message_id = %display_info.id, "送达 ACK 发送失败");
                        }
                    }
                }
            } else {
                debug!("收到非消息命令类型");
            }
        } else {
            debug!("收到空命令");
        }

        Ok(None)
    }
    async fn on_connect(&self) -> Result<()> {
        info!("✅ 已连接到服务器");
        info!("   用户ID: {}", self.user_id);
        info!("   接收方ID: {}", self.recipient_id);
        Ok(())
    }

    async fn on_disconnect(&self, reason: Option<&str>) -> Result<()> {
        warn!("🔴 连接断开: {}", reason.unwrap_or("未知原因"));
        Ok(())
    }

    async fn on_error(&self, error: &str) -> Result<()> {
        error!("连接错误: {}", error);
        Ok(())
    }
}

impl ChatListener {
    /// 服务端对「客户端发消息」的回执：`Ack.payload = Send(SendAck)`。
    async fn handle_server_ack(&self, msg_cmd: &PayloadCommand) -> Result<Option<Frame>> {
        let raw = ensure_decompressed_payload(&msg_cmd.payload);

        let ack = match Ack::decode(raw.as_slice()) {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "解码下行 Ack 失败");
                return Ok(None);
            }
        };

        let send_ack: SendAck = match ack.payload {
            Some(AckPayload::Send(s)) => s,
            other => {
                debug!(?other, "下行 Ack 非 SendAck，忽略");
                return Ok(None);
            }
        };

        let success = send_ack.success;
        let message_id = if send_ack.server_msg_id.is_empty() {
            send_ack.client_msg_id.as_str()
        } else {
            send_ack.server_msg_id.as_str()
        };
        let mut pending = self.pending_acks.lock().unwrap();
        let sent_at = pending
            .remove(&send_ack.client_msg_id)
            .or_else(|| pending.remove(&send_ack.server_msg_id));
        if let Some(sent_at) = sent_at {
            let elapsed = sent_at.elapsed();
            if success {
                debug!(
                    message_id = %message_id,
                    elapsed_ms = elapsed.as_millis(),
                    "收到服务器ACK确认"
                );
            } else {
                warn!(
                    message_id = %message_id,
                    error_code = send_ack.error_code,
                    error_message = %send_ack.error_message,
                    "收到服务器ACK失败"
                );
            }
        } else {
            debug!(message_id = %message_id, "收到未知消息的ACK");
        }

        Ok(None)
    }

    /// 上行送达确认：`AckType::CONVERSTION` + `ConversationAck`（与 `common/ack.proto` 一致，不用 `send`）。
    async fn send_delivery_ack(&self, message_id: &str, sender_id: &str) -> Result<()> {
        let conversation_id = self
            .get_conversation_id_for_sender(sender_id)
            .unwrap_or_default();
        let conv = ConversationAck {
            conversation_id,
            server_msg_ids: vec![message_id.to_string()],
            last_delivered_seq: 0,
            metadata: std::collections::HashMap::new(),
        };

        let ack = Ack {
            r#type: AckType::Converstion as i32,
            ack_id: None,
            at: None,
            payload: Some(AckPayload::Conversation(conv)),
        };

        // 序列化
        let mut payload = Vec::new();
        ack.encode(&mut payload).map_err(|e| {
            flare_core::common::error::FlareError::serialization_error(format!(
                "Failed to encode Ack: {}",
                e
            ))
        })?;

        // 构建ACK metadata
        let mut metadata = std::collections::HashMap::new();
        // 添加conversation_id等元数据
        if let Some(conversation_id) = self.get_conversation_id_for_sender(sender_id) {
            metadata.insert(
                "conversation_id".to_string(),
                conversation_id.as_bytes().to_vec(),
            );
        }

        // 创建ACK命令
        let ack_cmd = flare_core::common::protocol::PayloadCommand {
            r#type: flare_core::common::protocol::payload_command::Type::Ack as i32,
            message_id: message_id.to_string(),
            payload,
            metadata,
            seq: 0,
        };

        let ack_frame = flare_core::common::protocol::frame_with_payload_command(
            ack_cmd,
            flare_core::common::protocol::Reliability::AtLeastOnce,
        );

        // 发送ACK
        let client_guard = self.client.lock().await;
        if let Some(client) = client_guard.as_ref() {
            client.send_frame(&ack_frame).await?;
            debug!(message_id = %message_id, "客户端ACK已发送");
        } else {
            return Err(flare_core::common::error::FlareError::system(
                "Client not initialized".to_string(),
            ));
        }

        Ok(())
    }

    /// 获取会话ID（用于ACK metadata）
    #[allow(dead_code)]
    fn get_conversation_id_for_sender(&self, sender_id: &str) -> Option<String> {
        // 使用工具类生成单聊会话ID（格式：1-{hash}，自动排序）
        Some(generate_single_chat_conversation_id(
            &self.user_id,
            sender_id,
        ))
    }
}
