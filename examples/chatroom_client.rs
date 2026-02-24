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

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use flare_core::client::{FlareClientBuilder, MessageListener};
use flare_core::common::compression::CompressionAlgorithm;
use flare_core::common::config_types::{HeartbeatConfig, TransportProtocol};
use flare_core::common::device::{DeviceInfo, DevicePlatform};
use flare_core::common::encryption::{Aes256GcmEncryptor, EncryptionUtil};
use flare_core::common::error::Result;
use flare_core::common::protocol::flare::core::commands::command::Type as CommandType;
use flare_core::common::protocol::{
    Frame, MessageCommand, Reliability, frame_with_message_command, generate_message_id,
    send_message,
};
use prost::Message;
use tokio::io::{AsyncBufReadExt, BufReader};
use tracing::{debug, error, info, warn};

use chrono::{DateTime, Local, Utc};
use flare_core::common::conversation::generate_single_chat_conversation_id;
use flare_proto::common::{Message as ProtoMessage, MessageContent, ServerPacket};
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
        seen_message_ids: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
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
        match token_service.generate_token(&user_id, None, None) {
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
                            extensions: vec![],
                        };

                        // 构造完整的Message对象，将recipient_id作为conversation_id
                        let timestamp = prost_types::Timestamp {
                            seconds: chrono::Utc::now().timestamp(),
                            nanos: 0,
                        };

                        // 设置接收方用户ID到attributes中
                        let mut attributes = std::collections::HashMap::new();
                        attributes.insert("recipient_id".to_string(), recipient_id.clone());

                        // 确保消息的receiver_id正确设置
                        let receiver_id = recipient_id.clone();

                        // 使用工具类生成单聊会话ID（格式：1-{hash}）
                        let conversation_id = generate_single_chat_conversation_id(&user_id, &recipient_id);

                        let msg = flare_proto::common::Message {
                            server_id: generate_message_id(),
                            conversation_id,  // 使用正确的conversation_id格式
                            client_msg_id: String::new(),
                            sender_id: user_id.clone(),
                            receiver_id: receiver_id.clone(), // 单聊：直接设置接收者ID
                            channel_id: String::new(), // 单聊：channel_id 为空
                            source: flare_proto::common::MessageSource::User as i32,
                            seq: 0,
                            timestamp: Some(timestamp.clone()),
                            conversation_type: flare_proto::common::ConversationType::Single as i32,
                            message_type: flare_proto::common::MessageType::Text as i32,
                            business_type: String::new(),
                            content: Some(message_content),
                            content_type: flare_proto::common::ContentType::PlainText as i32,
                            attachments: vec![],
                            quote: None,
                            extra: std::collections::HashMap::new(),
                            attributes,
                            status: flare_proto::common::MessageStatus::Created as i32,
                            is_recalled: false,
                            recalled_at: None,
                            recall_reason: String::new(),
                            is_burn_after_read: false,
                            burn_after_seconds: 0,
                            timeline: Some(flare_proto::common::MessageTimeline {
                                created_at: Some(timestamp.clone()),
                                persisted_at: None,
                                delivered_at: None,
                                read_at: None,
                            }),
                            visibility: std::collections::HashMap::new(),
                            read_by: vec![],
                            reactions: vec![],
                            edit_history: vec![],
                            current_edit_version: 0,
                            last_edited_at: None,
                            tenant: "default".to_string(),
                            audit: None,
                            tags: vec![],
                            offline_push_info: None,
                            extensions: vec![],
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
                        let frame = frame_with_message_command(cmd, Reliability::AtLeastOnce);
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

/// 快速提取消息ID（用于去重，避免完整解析）
fn extract_message_id_fast(data: &[u8]) -> Option<String> {
    // 尝试快速提取消息ID，避免完整解析
    // 首先尝试解析为 ServerPacket -> Envelope -> Message
    if let Ok(server_packet) = ServerPacket::decode(data) {
        if let Some(flare_proto::common::server_packet::Payload::Envelope(envelope)) =
            server_packet.payload
        {
            if let Some(first_msg) = envelope.messages.first() {
                return Some(first_msg.server_id.clone());
            }
        }
    }

    // 尝试解析为 MessageEnvelope
    if let Ok(envelope) = flare_proto::common::MessageEnvelope::decode(data) {
        if let Some(first_msg) = envelope.messages.first() {
            return Some(first_msg.server_id.clone());
        }
    }

    // 尝试直接解析为 Message
    if let Ok(message) = ProtoMessage::decode(data) {
        return Some(message.server_id.clone());
    }

    None
}

/// 解析 Protocol Buffer 消息
fn parse_received_message(data: &[u8]) -> Option<MessageDisplayInfo> {
    // 首先尝试解析为 ServerPacket（网关推送的消息格式）
    match ServerPacket::decode(data) {
        Ok(server_packet) => {
            // 检查 ServerPacket 的 payload 类型
            match server_packet.payload {
                Some(flare_proto::common::server_packet::Payload::Envelope(envelope)) => {
                    // 只处理第一条消息（避免重复）
                    if let Some(message) = envelope.messages.first() {
                        return parse_single_message(message);
                    }
                }
                Some(flare_proto::common::server_packet::Payload::SendAck(ack)) => {
                    debug!(
                        "收到 SendAck: message_id={}, status={}",
                        ack.server_msg_id, ack.status
                    );
                    // SendAck 不是我们要处理的消息类型，返回 None
                    return None;
                }
                Some(flare_proto::common::server_packet::Payload::SyncMessagesResp(sync_resp)) => {
                    // 处理同步响应中的消息（只处理第一条）
                    if let Some(envelope) = sync_resp.envelope {
                        if let Some(message) = envelope.messages.first() {
                            return parse_single_message(message);
                        }
                    }
                    return None;
                }
                Some(flare_proto::common::server_packet::Payload::SyncConversationsResp(_)) => {
                    // 会话同步响应，暂不处理
                    return None;
                }
                Some(flare_proto::common::server_packet::Payload::SyncConversationsAllResp(_)) => {
                    // 全量会话同步响应，暂不处理
                    return None;
                }
                Some(flare_proto::common::server_packet::Payload::GetConversationDetailResp(_)) => {
                    // 会话详情响应，暂不处理
                    return None;
                }
                Some(flare_proto::common::server_packet::Payload::CustomPushData(_)) => {
                    // 自定义推送数据，暂不处理
                    return None;
                }
                Some(flare_proto::common::server_packet::Payload::Error(_)) => {
                    // 错误消息，暂不处理
                    return None;
                }
                None => {
                    // ServerPacket 没有 payload
                    return None;
                }
            }
        }
        Err(_) => {
            // 如果不是 ServerPacket，尝试解析为 MessageEnvelope
            match flare_proto::common::MessageEnvelope::decode(data) {
                Ok(envelope) => {
                    // 只处理第一条消息（避免重复）
                    if let Some(message) = envelope.messages.first() {
                        return parse_single_message(message);
                    }
                }
                Err(_) => {
                    // 尝试直接解析为 ProtoMessage（向后兼容）
                    match ProtoMessage::decode(data) {
                        Ok(message) => {
                            return parse_single_message(&message);
                        }
                        Err(_) => {
                            // 所有解析方式都失败
                            return None;
                        }
                    }
                }
            }
        }
    }

    None
}

/// 解析单条消息（统一的消息解析逻辑）
fn parse_single_message(message: &ProtoMessage) -> Option<MessageDisplayInfo> {
    // 检查消息是否已撤回
    if message.is_recalled {
        return Some(MessageDisplayInfo {
            id: message.server_id.clone(),
            sender_id: message.sender_id.clone(),
            receiver_id: message.receiver_id.clone(),
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

    // 解析消息内容
    let content = if let Some(msg_content) = &message.content {
        parse_message_content(msg_content)
    } else {
        "[空消息]".to_string()
    };

    let (type_name, _) = get_message_type_display(message.message_type);

    Some(MessageDisplayInfo {
        id: message.server_id.clone(),
        sender_id: message.sender_id.clone(),
        receiver_id: message.receiver_id.clone(),
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
    // 用于去重的消息ID集合（使用更完善的去重机制）
    seen_message_ids: Arc<std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>>,
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
            if let Some(CommandType::Message(msg_cmd)) = &command.r#type {
                debug!(
                    "收到消息命令: type={}, message_id={}, payload_len={}",
                    msg_cmd.r#type,
                    msg_cmd.message_id,
                    msg_cmd.payload.len()
                );

                // 处理ACK消息（Type::Ack = 1）
                if msg_cmd.r#type == 1 {
                    return self.handle_server_ack(msg_cmd).await;
                }

                // 使用原来的解析逻辑解析消息负载
                if msg_cmd.payload.len() < 10 {
                    debug!("忽略短消息(可能是心跳): {} 字节", msg_cmd.payload.len());
                    return Ok(None);
                }

                // 快速提取消息ID用于去重（简化：只基于 message_id）
                let message_id_for_dedup = extract_message_id_fast(&msg_cmd.payload);

                // 基于消息ID去重（服务端已处理重复推送，客户端只需简单去重）
                // 修改去重逻辑：允许同一会话中的消息，但防止完全重复的消息显示
                if let Some(msg_id) = &message_id_for_dedup {
                    let now = std::time::Instant::now();
                    let should_skip = {
                        let mut seen_ids = self.seen_message_ids.lock().unwrap();

                        // 检查是否在极短时间内收到过相同的消息ID（1秒内），防止完全重复
                        let should_skip = if let Some(&received_at) = seen_ids.get(msg_id) {
                            let elapsed = now.duration_since(received_at);
                            if elapsed.as_millis() < 1000 {
                                // 缩短到1秒内
                                debug!("跳过重复消息: {} (距离上次接收: {:?})", msg_id, elapsed);
                                true
                            } else {
                                false
                            }
                        } else {
                            false
                        };

                        if !should_skip {
                            // 立即记录，防止并发处理
                            seen_ids.insert(msg_id.clone(), now);

                            // 清理过期的记录（超过1分钟的记录）
                            seen_ids.retain(|_, &mut received_at| {
                                now.duration_since(received_at).as_secs() < 60 // 缩短清理时间到1分钟
                            });
                        }

                        should_skip
                    };

                    if should_skip {
                        return Ok(None);
                    }
                }

                debug!("开始调用 parse_received_message 解析消息");
                // 解析收到的消息
                if let Some(mut display_info) = parse_received_message(&msg_cmd.payload) {
                    debug!(
                        "parse_received_message 返回了 Some 值，消息ID: {}",
                        display_info.id
                    );

                    // 设置是否为自己的消息
                    display_info.is_self = display_info.sender_id == self.user_id;

                    // 检查是否是发给当前用户的单聊消息（只显示接收到的消息，不显示自己发送的消息）
                    // 修复逻辑：确保能正确显示来自接收方的消息，同时避免重复显示自己发送的消息
                    let is_from_recipient = display_info.sender_id == self.recipient_id;
                    let is_to_me = display_info.receiver_id == self.user_id; // 检查消息是否是发给我的
                    let is_system_message = display_info.sender_id == "system";
                    let is_from_self = display_info.sender_id == self.user_id;

                    debug!(
                        "消息来源检查: sender_id={}, receiver_id={}, user_id={}, recipient_id={}, is_from_recipient={}, is_to_me={}, is_system_message={}, is_from_self={}",
                        display_info.sender_id,
                        display_info.receiver_id,
                        self.user_id,
                        self.recipient_id,
                        is_from_recipient,
                        is_to_me,
                        is_system_message,
                        is_from_self
                    );

                    // 修正消息显示逻辑：只要消息是发给我的(is_to_me)或者来自聊天对方(is_from_recipient)，都应该显示
                    // 特别注意：即使消息没有正确显示，也要确保发送ACK给服务器
                    let should_display =
                        ((is_from_recipient || is_to_me) || is_system_message) && !is_from_self;

                    if should_display {
                        // 格式化并显示消息（接收到的消息：发送者 → 我）
                        let formatted_message =
                            format_message_display(&display_info, &self.user_id);
                        println!("{}", formatted_message);

                        // 更新消息计数
                        self.message_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    } else {
                        debug!(
                            "消息不会显示但会发送ACK: sender_id={}, receiver_id={}, user_id={}, recipient_id={}",
                            display_info.sender_id,
                            display_info.receiver_id,
                            self.user_id,
                            self.recipient_id
                        );
                    }

                    // 发送ACK给服务器（确认收到消息）
                    // 注意：无论消息是否显示，都要发送ACK以确保消息正确处理
                    if !is_from_self {
                        // 不要对自己发送的消息发送ACK
                        if let Err(e) = self
                            .send_client_ack(&display_info.id, &display_info.sender_id)
                            .await
                        {
                            warn!(error = %e, message_id = %display_info.id, "Failed to send client ACK");
                        }
                    }
                } else {
                    debug!("parse_received_message 返回了 None");
                    // 如果无法解析消息，不显示（避免干扰）
                    debug!(
                        "收到无法解析的消息 (数据长度: {} 字节)",
                        msg_cmd.payload.len()
                    );
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
    /// 处理服务器发送的ACK（确认消息已收到）
    async fn handle_server_ack(&self, msg_cmd: &MessageCommand) -> Result<Option<Frame>> {
        // 解析SendEnvelopeAck
        match flare_proto::common::SendEnvelopeAck::decode(&msg_cmd.payload[..]) {
            Ok(ack) => {
                let message_id = &ack.server_msg_id;
                let status = ack.status;

                // 检查是否是我们发送的消息的ACK
                let mut pending = self.pending_acks.lock().unwrap();
                if let Some(sent_at) = pending.remove(message_id) {
                    let elapsed = sent_at.elapsed();

                    if status == flare_proto::common::AckStatus::Success as i32 {
                        debug!(
                            message_id = %message_id,
                            elapsed_ms = elapsed.as_millis(),
                            "收到服务器ACK确认"
                        );
                    } else {
                        warn!(
                            message_id = %message_id,
                            error_code = ack.error_code,
                            error_message = %ack.error_message,
                            "收到服务器ACK失败"
                        );
                    }
                } else {
                    debug!(message_id = %message_id, "收到未知消息的ACK");
                }
            }
            Err(e) => {
                warn!(error = %e, "解析SendEnvelopeAck失败");
            }
        }

        Ok(None)
    }

    /// 发送客户端ACK给服务器（确认收到消息）
    async fn send_client_ack(&self, _message_id: &str, _sender_id: &str) -> Result<()> {
        // 构建SendEnvelopeAck
        // let send_ack = flare_proto::common::SendEnvelopeAck {
        //     message_id: message_id.to_string(),
        //     status: flare_proto::common::AckStatus::Success as i32,
        //     seq:0,
        //     error_code: 0,
        //     error_message: String::new(),
        // };

        // // 序列化
        // let mut payload = Vec::new();
        // send_ack.encode(&mut payload).map_err(|e| {
        //     flare_core::common::error::FlareError::serialization_error(format!(
        //         "Failed to encode SendEnvelopeAck: {}",
        //         e
        //     ))
        // })?;

        // // 构建ACK metadata
        // let mut metadata = std::collections::HashMap::new();
        // // 可以添加conversation_id等元数据
        // if let Some(conversation_id) = self.get_conversation_id_for_sender(sender_id) {
        //     metadata.insert("conversation_id".to_string(), conversation_id.as_bytes().to_vec());
        // }

        // // 创建ACK命令
        // let ack_cmd = flare_core::common::protocol::MessageCommand {
        //     r#type: flare_core::common::protocol::flare::core::commands::message_command::Type::Ack
        //         as i32,
        //     message_id: message_id.to_string(),
        //     payload,
        //     metadata,
        //     seq: 0,
        // };

        // let ack_frame = flare_core::common::protocol::frame_with_message_command(
        //     ack_cmd,
        //     flare_core::common::protocol::Reliability::AtLeastOnce,
        // );

        // // 发送ACK
        // let client_guard = self.client.lock().await;
        // if let Some(client) = client_guard.as_ref() {
        //     client.send_frame(&ack_frame).await?;
        //     debug!(message_id = %message_id, "客户端ACK已发送");
        // } else {
        //     return Err(flare_core::common::error::FlareError::system(
        //         "Client not initialized".to_string(),
        //     ));
        // }

        Ok(())
    }

    /// 获取会话ID（用于ACK metadata）
    fn get_conversation_id_for_sender(&self, sender_id: &str) -> Option<String> {
        // 使用工具类生成单聊会话ID（格式：1-{hash}，自动排序）
        Some(generate_single_chat_conversation_id(&self.user_id, sender_id))
    }
}
