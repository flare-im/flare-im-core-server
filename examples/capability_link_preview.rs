//! 开源能力插件样例：链接预览（URL unfurl）。
//!
//! # 与 hook 样例的根本不同
//!
//! `hook_rate_limit` / `hook_audit_log` 是**配置挂上去**的：改 `hooks.toml`、重启核心。
//! 这个是**动态能力插件**，与 SFU 走同一条路：
//!
//! ```text
//!   插件进程启动
//!     └─ 调 CapabilityService.RegisterPluginEndpoint 把自己登记进去
//!          └─ 核心的 PluginRouteBook 立刻多出一条路由（不用重启核心）
//!               └─ 客户端 dispatch(capability_id) → 核心转发到本插件
//!   插件退出前 DeregisterPluginEndpoint 摘掉自己
//! ```
//!
//! 也就是「装上就能用、拔掉就没有」。核心不认识这个插件的任何业务语义，
//! 它只按 `capability_id` 转发 JSON。
//!
//! # 跑起来
//!
//! ```bash
//! # 1. 先起 flare-im-core（capability 服务默认 50110）
//! ./scripts/start_server.sh
//!
//! # 2. 起插件：它会自己注册上去
//! cargo run --example capability_link_preview
//!
//! # 3. 客户端调用（也可以直接用 grpcurl 打 CapabilityService.Dispatch）
//! #    capability_id = "link.preview.v1"
//! #    payload_json  = {"url":"https://example.com"}
//! ```
//!
//! 环境变量：
//!
//! - `CAPABILITY_CORE_ADDR`：核心 capability 服务地址（默认 `http://127.0.0.1:50110`）
//! - `LINK_PREVIEW_ADDR`：本插件监听地址（默认 `127.0.0.1:7803`）
//! - `LINK_PREVIEW_TENANT`：注册到哪个租户（默认 `0`）
//!
//! # 写自己的能力插件时，照抄这四步
//!
//! 1. 实现 `ExtensionPlugin.Call`：`operation` 就是你的 `capability_id`，
//!    payload 是 JSON 字符串（`type_url` 为 `...PayloadJson`）。
//! 2. 启动后调 `RegisterPluginEndpoint` 把 `(tenant, plugin_id, capability_id, 地址)`
//!    登记进核心。
//! 3. 退出前调 `DeregisterPluginEndpoint`，否则核心会把请求发给一个已经死掉的地址。
//! 4. 返回的 JSON 结构由你定义 —— 核心不解析它，原样交给客户端。

use std::collections::HashMap;
use std::time::Duration;

use flare_grpc_proto::capability::capability_service_client::CapabilityServiceClient;
use flare_grpc_proto::capability::extension_plugin_server::{
    ExtensionPlugin, ExtensionPluginServer,
};
use flare_grpc_proto::capability::{
    DeregisterPluginEndpointRequest, GenericRequest, GenericResponse, RegisterPluginEndpointRequest,
};
use tonic::{Request, Response, Status, transport::Server};

/// 能力 id。客户端就是用这个字符串来调用本插件。
///
/// 带上版本后缀（`.v1`）是有意的：将来要改返回结构时，注册一个 `.v2` 与旧版并存，
/// 老客户端不受影响 —— 能力插件是对外契约，改它等于改 API。
const CAPABILITY_ID: &str = "link.preview.v1";

/// 插件 id。同一个能力可以由多个插件提供，核心按健康状态在它们之间选。
const PLUGIN_ID: &str = "flare-link-preview";

/// 返回给客户端的 JSON。核心不解析它，原样透传。
#[derive(serde::Serialize)]
struct Preview {
    url: String,
    title: Option<String>,
    description: Option<String>,
    site_name: Option<String>,
}

struct LinkPreviewPlugin {
    http: reqwest::Client,
}

/// 逐个 `<meta` 标签扫，取出 property/name 与 content。
///
/// 有意不引入 HTML 解析库或正则依赖：链接预览只需要 head 里的几个 meta，
/// 而少一个依赖对「样例」比什么都重要——读的人不必先搞懂第三方库。
/// 真上生产建议换成正经的解析器，并对畸形 HTML 做防御。
fn find_meta(html: &str, keys: &[&str]) -> Option<String> {
    let lower = html.to_lowercase();
    let mut cursor = 0usize;
    while let Some(rel) = lower[cursor..].find("<meta") {
        let start = cursor + rel;
        let end = lower[start..].find('>').map(|e| start + e)?;
        let tag = &html[start..end];
        let tag_lower = &lower[start..end];
        for key in keys {
            let needle = format!("\"{key}\"");
            let needle_single = format!("'{key}'");
            if (tag_lower.contains(&needle) || tag_lower.contains(&needle_single))
                && let Some(v) = attr_value(tag, tag_lower, "content")
                && !v.trim().is_empty()
            {
                return Some(v.trim().to_string());
            }
        }
        cursor = end + 1;
    }
    None
}

fn attr_value(tag: &str, tag_lower: &str, attr: &str) -> Option<String> {
    let at = tag_lower.find(&format!("{attr}="))? + attr.len() + 1;
    let rest = &tag[at..];
    let quote = rest.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let body = &rest[1..];
    let close = body.find(quote)?;
    Some(body[..close].to_string())
}

fn find_title(html: &str) -> Option<String> {
    let lower = html.to_lowercase();
    let start = lower.find("<title")?;
    let open_end = lower[start..].find('>')? + start + 1;
    let close = lower[open_end..].find("</title>")? + open_end;
    let title = html[open_end..close].trim();
    (!title.is_empty()).then(|| title.to_string())
}

impl LinkPreviewPlugin {
    async fn preview(&self, url: &str) -> Result<Preview, String> {
        // 只允许 http(s)。不加这条的话，插件会变成一个能读本机文件与内网地址的
        // 代理 —— 任何接受用户输入 URL 的服务都必须先想清楚这件事。
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err("only http(s) urls are supported".to_string());
        }

        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| format!("fetch failed: {e}"))?;
        let html = resp.text().await.map_err(|e| format!("read body: {e}"))?;
        // 只看前 64KB：预览需要的 meta 都在 head 里，整页读进来对大页面是浪费
        let head = &html[..html.len().min(64 * 1024)];

        Ok(Preview {
            url: url.to_string(),
            title: find_meta(head, &["og:title", "twitter:title"]).or_else(|| find_title(head)),
            description: find_meta(head, &["og:description", "description"]),
            site_name: find_meta(head, &["og:site_name"]),
        })
    }
}

#[tonic::async_trait]
impl ExtensionPlugin for LinkPreviewPlugin {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        let req = request.into_inner();
        let request_id = req.request_id.clone();

        // operation 就是 capability_id。不认识的一律明确拒绝——
        // 与 hook 相反：hook 在链路上，不认识要放行；能力插件是被显式调用的，
        // 不认识就该报错，否则调用方会拿到一个空响应还以为成功了。
        if req.operation != CAPABILITY_ID {
            return Ok(Response::new(GenericResponse {
                ok: false,
                request_id,
                error_code: "UNSUPPORTED_CAPABILITY".to_string(),
                error_message: format!("this plugin only serves {CAPABILITY_ID}"),
                ..Default::default()
            }));
        }

        let payload_json = req
            .payload
            .map(|a| String::from_utf8_lossy(&a.value).to_string())
            .unwrap_or_default();
        let url = serde_json::from_str::<serde_json::Value>(&payload_json)
            .ok()
            .and_then(|v| v.get("url").and_then(|u| u.as_str()).map(str::to_string));

        let Some(url) = url else {
            return Ok(Response::new(GenericResponse {
                ok: false,
                request_id,
                error_code: "INVALID_PAYLOAD".to_string(),
                error_message: r#"expected {"url": "https://..."}"#.to_string(),
                ..Default::default()
            }));
        };

        match self.preview(&url).await {
            Ok(preview) => {
                let body = serde_json::to_vec(&preview).unwrap_or_default();
                Ok(Response::new(GenericResponse {
                    ok: true,
                    request_id,
                    payload: Some(prost_types::Any {
                        type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".to_string(),
                        value: body,
                    }),
                    ..Default::default()
                }))
            }
            Err(message) => Ok(Response::new(GenericResponse {
                ok: false,
                request_id,
                error_code: "PREVIEW_FAILED".to_string(),
                error_message: message,
                ..Default::default()
            })),
        }
    }
}

/// 把自己登记进核心。这一步就是「安装」。
async fn register(core_addr: &str, tenant: &str, authority: &str) -> Result<(), String> {
    let mut client = CapabilityServiceClient::connect(core_addr.to_string())
        .await
        .map_err(|e| format!("连不上核心 capability 服务（{core_addr}）：{e}"))?;
    let mut labels = HashMap::new();
    labels.insert("source".to_string(), "example".to_string());

    client
        .register_plugin_endpoint(RegisterPluginEndpointRequest {
            tenant_id: tenant.to_string(),
            plugin_id: PLUGIN_ID.to_string(),
            capability_id: CAPABILITY_ID.to_string(),
            grpc_authority: authority.to_string(),
            labels,
            request_id: uuid::Uuid::new_v4().to_string(),
            // 注册契约 v2：填满这些字段才是 verified 的实例。
            // 留空协议上也接受，但实例会被标成 unverified —— 那样
            // declared_operations 这条边界对它就无从强制了。
            plugin_version: env!("CARGO_PKG_VERSION").to_string(),
            api_version: "1".to_string(),
            manifest_sha256: String::new(),
            declared_operations: vec![CAPABILITY_ID.to_string()],
            // 装了就全员可用。链接预览没有按人计费的边际成本。
            seat_model: "tenant".to_string(),
        })
        .await
        .map_err(|e| format!("注册失败：{e}"))?;
    Ok(())
}

/// 退出前摘掉自己。不做这一步，核心会继续把请求发到一个死地址上。
async fn deregister(core_addr: &str, tenant: &str) {
    let Ok(mut client) = CapabilityServiceClient::connect(core_addr.to_string()).await else {
        tracing::warn!("注销时连不上核心，路由需要等健康检查摘除");
        return;
    };
    let _ = client
        .deregister_plugin_endpoint(DeregisterPluginEndpointRequest {
            tenant_id: tenant.to_string(),
            plugin_id: PLUGIN_ID.to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
        })
        .await;
    tracing::info!("已从核心注销");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let core_addr = std::env::var("CAPABILITY_CORE_ADDR")
        .unwrap_or_else(|_| "http://127.0.0.1:50110".to_string());
    let listen =
        std::env::var("LINK_PREVIEW_ADDR").unwrap_or_else(|_| "127.0.0.1:7803".to_string());
    let tenant = std::env::var("LINK_PREVIEW_TENANT").unwrap_or_else(|_| "0".to_string());
    let addr = listen.parse()?;

    let plugin = LinkPreviewPlugin {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            // 不跟随跳转到未知地址的次数太多——限制一下，顺带避免重定向环
            .redirect(reqwest::redirect::Policy::limited(3))
            .user_agent("flare-link-preview/0.1 (+https://github.com/flare-im)")
            .build()?,
    };

    // 先起服务再注册：反过来的话，核心可能在服务就绪之前就把请求发过来。
    let serving = tokio::spawn(
        Server::builder()
            .add_service(ExtensionPluginServer::new(plugin))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    let authority = format!("http://{listen}");
    match register(&core_addr, &tenant, &authority).await {
        Ok(()) => tracing::info!(
            capability = CAPABILITY_ID, plugin = PLUGIN_ID, %authority, tenant = %tenant,
            "已注册到核心，客户端现在可以 dispatch 这个能力了"
        ),
        Err(err) => tracing::warn!(
            %err,
            "注册失败：插件仍在监听，但核心不知道它的存在。\
             先确认 flare-im-core 已启动，再重跑本插件。"
        ),
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("收到中断，正在注销");
            deregister(&core_addr, &tenant).await;
        }
        r = serving => { r??; }
    }
    Ok(())
}

#[cfg(test)]
mod link_preview_tests {
    use super::*;

    const PAGE: &str = r#"<html><head>
        <title>Fallback Title</title>
        <meta property="og:title" content="Real Title">
        <meta property="og:description" content="A short description.">
        <meta property="og:site_name" content="Example">
      </head><body>x</body></html>"#;

    #[test]
    fn prefers_open_graph_over_title_tag() {
        assert_eq!(
            find_meta(PAGE, &["og:title", "twitter:title"]).as_deref(),
            Some("Real Title")
        );
    }

    #[test]
    fn falls_back_to_title_tag() {
        let html = "<html><head><title>Only Title</title></head></html>";
        assert_eq!(find_meta(html, &["og:title"]), None);
        assert_eq!(find_title(html).as_deref(), Some("Only Title"));
    }

    #[test]
    fn reads_description_and_site_name() {
        assert_eq!(
            find_meta(PAGE, &["og:description"]).as_deref(),
            Some("A short description.")
        );
        assert_eq!(
            find_meta(PAGE, &["og:site_name"]).as_deref(),
            Some("Example")
        );
    }

    #[test]
    fn survives_malformed_html() {
        // 畸形输入不该 panic —— 它来自互联网上的任意页面
        for bad in ["<meta", "<title>", "<meta content=", "", "<<<>>>"] {
            let _ = find_meta(bad, &["og:title"]);
            let _ = find_title(bad);
        }
    }

    #[tokio::test]
    async fn rejects_non_http_schemes() {
        // 不挡住的话，这个插件就成了能读本机文件与内网地址的代理
        let p = LinkPreviewPlugin {
            http: reqwest::Client::new(),
        };
        for url in ["file:///etc/passwd", "ftp://x", "gopher://x"] {
            assert!(p.preview(url).await.is_err(), "{url} 应被拒绝");
        }
    }
}
