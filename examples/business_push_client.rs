//! # 业务系统推送消息示例
//!
//! 这是一个业务系统接入示例，演示如何通过 `flare-core-gateway` 给所有在线用户推送消息。
//! 所有消息都发送到同一个聊天室（conversation_id: "chatroom"），只支持文本消息。
//!
//! ## 使用方法
//!
//! ```bash
//! # 推送消息给所有在线用户
//! cargo run --example business_push_client
//!
//! # 推送指定消息内容
//! cargo run --example business_push_client -- "系统通知：服务器将在10分钟后维护"
//!
//! # 推送给指定用户列表
//! USER_IDS=user1,user2 cargo run --example business_push_client -- "重要通知"
//!
//! # 使用自定义 JWT Token
//! TOKEN=your_jwt_token cargo run --example business_push_client
//! ```
//!
//! ## 工作原理
//!
//! 1. **连接 Core Gateway**：业务系统通过 gRPC 连接到 `flare-core-gateway`
//! 2. **查询在线状态**：Core Gateway 查询 `signaling-online` 获取用户在线状态和网关信息
//! 3. **跨地区路由**：根据用户的 `gateway_id`，路由到对应的 `access-gateway`
//! 4. **推送消息**：Access Gateway 通过长连接推送消息给客户端
//! 5. **消息持久化**：消息会通过 Message Orchestrator 持久化到数据库

use std::env;

use anyhow::Result;
use flare_grpc_proto::access_gateway::{
    PushMessageRequest, PushMessageResponse, access_gateway_client::AccessGatewayClient,
};
use flare_proto::common::{
    ContentType, Message, MessageContent, MessageSource, MessageStatus, MessageType, TextContent,
};
use flare_server_core::TokenService;
use tonic::Request;
use tracing::{error, info, warn};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_thread_ids(true)
        .init();

    // 从环境变量获取配置
    // 注意：生产环境应该使用服务发现，这里仅用于示例
    let gateway_endpoint =
        env::var("CORE_GATEWAY_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50050".to_string());

    let message_content = env::args()
        .nth(1)
        .unwrap_or_else(|| "这是一条来自业务系统的测试消息".to_string());

    let target_user_ids: Vec<String> = env::var("USER_IDS")
        .map(|ids| ids.split(',').map(|s| s.trim().to_string()).collect())
        .unwrap_or_default(); // 如果为空，表示推送给所有在线用户（聊天室模式）

    info!(
        gateway_endpoint = %gateway_endpoint,
        message_content = %message_content,
        target_count = target_user_ids.len(),
        "🚀 启动业务系统推送客户端"
    );

    // 生成 JWT Token（业务系统应该使用自己的密钥）
    let token_secret = env::var("TOKEN_SECRET").unwrap_or_else(|_| "insecure-secret".to_string());
    let tenant_id = env::var("TENANT_ID").unwrap_or_else(|_| "default-tenant".to_string());
    let business_user_id =
        env::var("BUSINESS_USER_ID").unwrap_or_else(|_| "business-system".to_string());

    let token_service = TokenService::new(token_secret.clone(), "flare-im-core".to_string(), 3600);

    let token = env::var("TOKEN").unwrap_or_else(|_| {
        match token_service.generate_token(&business_user_id, None, Some(&tenant_id)) {
            Ok(t) => {
                info!("🔑 自动生成测试 JWT Token");
                t
            }
            Err(e) => {
                warn!(?e, "无法生成 token，连接可能失败");
                String::new()
            }
        }
    });

    if token.is_empty() {
        error!("❌ Token 为空，无法连接 Core Gateway");
        return Err(anyhow::anyhow!("Token is required"));
    }

    // 连接到 Core Gateway
    info!("📡 连接到 Core Gateway: {}", gateway_endpoint);
    let mut client = match AccessGatewayClient::connect(gateway_endpoint.clone()).await {
        Ok(client) => {
            info!("✅ 已连接到 Core Gateway");
            client
        }
        Err(e) => {
            error!(
                error = %e,
                endpoint = %gateway_endpoint,
                "❌ 连接 Core Gateway 失败"
            );
            eprintln!();
            eprintln!("💡 提示：");
            eprintln!("   1. 确保 Core Gateway 服务已启动：");
            eprintln!("      ./scripts/start_server.sh [single|multi]");
            eprintln!("   2. 检查服务端口是否正确（默认: 50050）");
            eprintln!("   3. 可以通过环境变量指定其他地址：");
            eprintln!(
                "      CORE_GATEWAY_ENDPOINT=http://localhost:50050 cargo run --example business_push_client"
            );
            eprintln!();
            return Err(anyhow::anyhow!(
                "Failed to connect to Core Gateway at {}: {}",
                gateway_endpoint,
                e
            ));
        }
    };

    // 构建推送消息请求
    // 如果 target_user_ids 为空，表示推送给所有在线用户（聊天室广播）
    let is_broadcast = target_user_ids.is_empty();

    info!(
        is_broadcast = is_broadcast,
        target_count = target_user_ids.len(),
        "📤 准备推送消息"
    );

    // 构建 Message（统一消息定义，用于持久化和推送）
    let now = chrono::Utc::now();
    let mut extra = std::collections::HashMap::new();
    extra.insert("source".to_string(), "business_system".to_string());
    if is_broadcast {
        extra.insert("chatroom".to_string(), "true".to_string());
    }

    // 统一使用 "chatroom" 作为 conversation_id，确保所有消息都发送到同一个聊天室
    // 注意：business_push_client 和 chatroom_client 都使用相同的 conversation_id
    let conversation_id = "chatroom".to_string();

    extra.insert("business_type".to_string(), "chatroom".to_string());
    extra.insert("source".to_string(), "system".to_string());
    extra.insert("conversation_type".to_string(), "group".to_string());
    extra.insert("tenant_id".to_string(), tenant_id.clone());

    let message = Message {
        server_id: format!("msg-{}", Uuid::new_v4()),
        conversation_id: conversation_id.clone(),
        client_msg_id: String::new(),
        sender_id: business_user_id.clone(),
        receiver_id: String::new(),
        seq: 0,
        timestamp: Some(prost_types::Timestamp {
            seconds: now.timestamp(),
            nanos: 0,
        }),
        message_type: MessageType::Text as i32,
        content: vec![],
        content_type: ContentType::PlainText as i32,
        extra,
        quote: None,
        tags: std::collections::HashMap::new(),
        offline_push: None,
        priority: 0,
        thread_root_id: None,
        ..Default::default()
    };

    // 构建 PushMessageRequest（直接使用 StorageMessage）
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("source".to_string(), "business_push_client".to_string());

    let push_request = PushMessageRequest {
        request_id: Uuid::new_v4().to_string(),
        target_user_ids: if is_broadcast {
            vec![] // 空列表表示广播给所有在线用户
        } else {
            target_user_ids.clone()
        },
        message: Some(message),
        options: None,
        metadata,
    };

    // 创建 gRPC 请求，添加认证头
    let mut request = Request::new(push_request);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );

    // 发送推送请求
    info!("📨 发送推送请求...");
    let start_time = std::time::Instant::now();

    match client.push_message(request).await {
        Ok(response) => {
            let elapsed = start_time.elapsed();
            let push_response: PushMessageResponse = response.into_inner();

            info!(
                elapsed_ms = elapsed.as_millis(),
                success_count = push_response
                    .statistics
                    .as_ref()
                    .map(|s| s.success_count)
                    .unwrap_or(0),
                failure_count = push_response
                    .statistics
                    .as_ref()
                    .map(|s| s.failure_count)
                    .unwrap_or(0),
                "✅ 推送请求完成"
            );

            if let Some(stats) = &push_response.statistics {
                println!();
                println!("📊 推送统计:");
                println!("  总用户数: {}", stats.total_users);
                println!("  在线用户数: {}", stats.online_users);
                println!("  离线用户数: {}", stats.offline_users);
                println!("  成功推送: {} 用户", stats.success_count);
                println!("  失败推送: {} 用户", stats.failure_count);
                println!("  耗时: {}ms", elapsed.as_millis());
            }

            // 显示推送结果详情
            if !push_response.results.is_empty() {
                println!();
                println!("📋 推送结果详情:");
                for result in &push_response.results {
                    println!(
                        "  - {}: 成功 {} 连接, 失败 {} 连接",
                        result.user_id, result.success_count, result.failure_count
                    );
                    if !result.error_message.is_empty() {
                        println!("    错误: {}", result.error_message);
                    }
                }
            }

            Ok(())
        }
        Err(e) => {
            error!(error = %e, "❌ 推送请求失败");
            Err(anyhow::anyhow!("Push request failed: {}", e))
        }
    }
}
