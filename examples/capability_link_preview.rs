//! 开源能力插件样例：链接预览（URL unfurl）。
//!
//! # 与 hook 样例的根本不同
//!
//! `hook_rate_limit` / `hook_audit_log` 是**配置挂上去**的：改 `hooks.toml`、重启核心。
//! 这个是**动态能力插件**：
//!
//! ```text
//!   插件进程启动
//!     └─ 向平台换一张注册凭据，调 CapabilityService.RegisterPluginEndpoint 登记自己
//!          └─ 登记只进诊断用的路由簿（谁在、健不健康），不授予任何调用权
//!   控制面给租户下发插件授权投影（平台签名、带租约，不用重启核心）
//!     └─ 客户端 dispatch(capability_id) → 核心按投影找到本插件，签一张调用票据转发过来
//!          └─ 本插件验过票据才执行
//!   插件退出前 DeregisterPluginEndpoint 摘掉自己
//! ```
//!
//! 也就是「装上就能用、拔掉就没有」。核心不认识这个插件的任何业务语义，
//! 它只按 `capability_id` 转发 JSON。
//!
//! # 跑起来
//!
//! 授权投影与注册凭据都由控制面签发。开源栈没有控制面，`capability_dev_control` 样例在
//! 本机扮演它：`scripts/smoke_plugin_lifecycle.sh` 就是这样走完整个生命周期的，照着它的
//! `--prepare` 与各步做即可。
//!
//! 环境变量：
//!
//! - `CAPABILITY_CORE_ADDR`：核心 capability 服务地址（默认 `http://127.0.0.1:50110`）
//! - `LINK_PREVIEW_ADDR`：本插件监听地址（默认 `127.0.0.1:7803`）
//! - `LINK_PREVIEW_TENANT`：注册到哪个租户（默认 `0`）
//! - `FLARE_PLUGIN_CALL_PUBLIC_KEYS`：核心调用票据的验签公钥，
//!   `[{"key_id":"…","public_key_base64":"…"}]`。没有它，本插件拒绝一切调用。
//! - `FLARE_PLUGIN_BROKER_URL` / `FLARE_PLUGIN_WORKLOAD_TOKEN_FILE` /
//!   `FLARE_PLUGIN_DEPLOYMENT_ID`：平台的注册凭据交换（与 flare-plugin-host 同一协议）。
//!   不配就不登记——核心拒绝不带凭据的注册；插件照常服务，调用只看授权投影。
//!
//! 清单在 `capability_link_preview.manifest.json`：注册时声明它的摘要，控制面审批、
//! 投影里带的也是这份字节。
//!
//! # 写自己的能力插件时，照抄这几步
//!
//! 1. 写清单：能力 id 加操作 id 就是调用方用的 `capability_id`（这里 `link.preview` + `v1`）。
//! 2. 实现 `ExtensionPlugin.Call`：先验 `x-flare-plugin-call` 票据，再按 `operation` 分发；
//!    payload 是 JSON 字符串（`type_url` 为 `...PayloadJson`）。
//! 3. 启动后带平台签的注册凭据调 `RegisterPluginEndpoint`，退出前同样调
//!    `DeregisterPluginEndpoint`，否则路由簿里会留着一个死地址。
//! 4. 返回的 JSON 结构由你定义 —— 核心不解析它，原样交给客户端。

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use flare_grpc_proto::capability::capability_service_client::CapabilityServiceClient;
use flare_grpc_proto::capability::extension_plugin_server::{
    ExtensionPlugin, ExtensionPluginServer,
};
use flare_grpc_proto::capability::{
    DeregisterPluginEndpointRequest, GenericRequest, GenericResponse, RegisterPluginEndpointRequest,
};
use flare_im_capability_core::plugin::invocation::{PLUGIN_CALL_HEADER, PluginCallVerifier};
use flare_im_capability_core::plugin::workload::{
    EndpointDeclaration, REGISTRATION_HEADER, RegistrationCredential, RegistrationExchange,
    RegistrationOperation, RegistrationRequest,
};
use sha2::{Digest, Sha256};
use tonic::{Request, Response, Status, transport::Server};

/// 能力 id。客户端就是用这个字符串来调用本插件。
///
/// 带上版本后缀（`.v1`）是有意的：将来要改返回结构时，注册一个 `.v2` 与旧版并存，
/// 老客户端不受影响 —— 能力插件是对外契约，改它等于改 API。
const CAPABILITY_ID: &str = "link.preview.v1";

/// 插件 id。同一个能力可以由多个插件提供，核心按健康状态在它们之间选。
const PLUGIN_ID: &str = "flare-link-preview";

/// 本插件的清单：注册时声明它的摘要，控制面审批、投影里带的也是这份字节。
const MANIFEST: &str = include_str!("capability_link_preview.manifest.json");

/// 核心的通用健康探针。它不带调用票据，也不碰任何业务，直接回答。
const HEALTH_CHECK: &str = "flare.capability.v1.health_check";

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
    /// 核心调用票据的验签公钥；`None` 时拒绝一切调用。
    tickets: Option<PluginCallVerifier>,
    /// 本部署的 id（与注册凭据交换用的同一个）；配置了就要求票据是签给它的。
    deployment_id: Option<String>,
}

/// 核心每次调用都带一张调用票据（`x-flare-plugin-call`）：核心用自己的密钥签，只对这一次
/// 调用有效（租户、用户、操作、请求号、部署）。验不过就拒绝，命令不执行——否则任何能连上
/// 本插件端口的人都能冒用任意用户调它。
fn verify_ticket(
    verifier: Option<&PluginCallVerifier>,
    deployment_id: Option<&str>,
    request: &Request<GenericRequest>,
    now_ms: i64,
) -> Result<(), Status> {
    let verifier = verifier
        .ok_or_else(|| Status::unauthenticated("plugin invocation keys are not configured"))?;
    let mut values = request.metadata().get_all(PLUGIN_CALL_HEADER).iter();
    let ticket = values
        .next()
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| Status::unauthenticated("plugin invocation ticket required"))?;
    if values.next().is_some() {
        return Err(Status::unauthenticated(
            "exactly one plugin invocation ticket required",
        ));
    }
    let claims = verifier
        .verify(ticket, now_ms)
        .map_err(|_| Status::unauthenticated("invalid plugin invocation ticket"))?;
    let call = request.get_ref();
    let matches = claims.plugin_id == PLUGIN_ID
        && claims.operation == call.operation
        && claims.request_id == call.request_id
        && call
            .metadata
            .get("tenant_id")
            .is_none_or(|tenant| *tenant == claims.tenant_id)
        && deployment_id.is_none_or(|id| id == claims.deployment_id);
    if !matches {
        return Err(Status::permission_denied(
            "plugin invocation ticket does not match this call",
        ));
    }
    Ok(())
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
        if request.get_ref().operation == HEALTH_CHECK {
            return Ok(Response::new(GenericResponse {
                ok: true,
                request_id: request.into_inner().request_id,
                ..Default::default()
            }));
        }
        verify_ticket(
            self.tickets.as_ref(),
            self.deployment_id.as_deref(),
            &request,
            chrono::Utc::now().timestamp_millis(),
        )?;
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

/// 平台的注册凭据交换（与 flare-plugin-host 同一协议）：用本部署的工作负载令牌换一张只对
/// 这一次请求体有效、一分钟内过期的注册凭据。令牌每次现读，轮换后不必重启。
struct RegistrationBroker {
    http: reqwest::Client,
    url: String,
    workload_token_file: PathBuf,
    deployment_id: String,
}

const PARTIAL_BROKER_CONFIG: &str = "FLARE_PLUGIN_BROKER_URL、FLARE_PLUGIN_WORKLOAD_TOKEN_FILE、\
     FLARE_PLUGIN_DEPLOYMENT_ID 要一起配置";

impl RegistrationBroker {
    /// 三个变量都给才启用；只给一部分是配置错误。
    fn from_env() -> Result<Option<Self>, String> {
        let var = |name| std::env::var(name).ok().filter(|v: &String| !v.is_empty());
        match (
            var("FLARE_PLUGIN_BROKER_URL"),
            var("FLARE_PLUGIN_WORKLOAD_TOKEN_FILE"),
            var("FLARE_PLUGIN_DEPLOYMENT_ID"),
        ) {
            (None, None, None) => Ok(None),
            (Some(url), Some(token_file), Some(deployment_id)) => Ok(Some(Self {
                // 凭据交换只走配置的那个地址：不经代理、不跟随跳转。
                http: reqwest::Client::builder()
                    .no_proxy()
                    .redirect(reqwest::redirect::Policy::none())
                    .timeout(Duration::from_secs(5))
                    .build()
                    .map_err(|e| format!("注册凭据交换客户端：{e}"))?,
                url,
                workload_token_file: token_file.into(),
                deployment_id,
            })),
            _ => Err(PARTIAL_BROKER_CONFIG.to_string()),
        }
    }

    async fn credential(&self, request: &RegistrationRequest) -> Result<String, String> {
        let token = std::fs::read_to_string(&self.workload_token_file)
            .map_err(|e| format!("读不到工作负载令牌：{e}"))?;
        let response = self
            .http
            .post(&self.url)
            .bearer_auth(token.trim())
            .json(&RegistrationExchange {
                deployment_id: self.deployment_id.clone(),
                request: request.clone(),
            })
            .send()
            .await
            .map_err(|e| format!("注册凭据交换不可用：{e}"))?;
        if !response.status().is_success() {
            return Err(format!("平台没有签发注册凭据：{}", response.status()));
        }
        let credential: RegistrationCredential = response
            .json()
            .await
            .map_err(|e| format!("注册凭据响应无效：{e}"))?;
        Ok(credential.token)
    }
}

/// 凭据绑定的就是这份请求体：与核心按收到的请求重建的一致（标签按键排序）。
fn registration_intent(r: &RegisterPluginEndpointRequest) -> RegistrationRequest {
    RegistrationRequest {
        plugin_id: r.plugin_id.clone(),
        tenant_id: r.tenant_id.clone(),
        request_id: r.request_id.clone(),
        action: RegistrationOperation::Register(EndpointDeclaration {
            capability_id: r.capability_id.clone(),
            grpc_authority: r.grpc_authority.clone(),
            plugin_version: r.plugin_version.clone(),
            api_version: r.api_version.clone(),
            manifest_sha256: r.manifest_sha256.clone(),
            declared_operations: r.declared_operations.clone(),
            labels: r.labels.clone().into_iter().collect(),
        }),
    }
}

fn deregistration_intent(r: &DeregisterPluginEndpointRequest) -> RegistrationRequest {
    RegistrationRequest {
        plugin_id: r.plugin_id.clone(),
        tenant_id: r.tenant_id.clone(),
        request_id: r.request_id.clone(),
        action: RegistrationOperation::Deregister,
    }
}

fn with_credential<T>(body: T, credential: &str) -> Result<Request<T>, String> {
    let mut request = Request::new(body);
    let value = credential
        .parse()
        .map_err(|_| "注册凭据不是合法的 gRPC 元数据".to_string())?;
    request.metadata_mut().insert(REGISTRATION_HEADER, value);
    Ok(request)
}

/// 清单摘要（64 位小写十六进制），注册时声明。
fn manifest_sha256() -> String {
    hex::encode(Sha256::digest(MANIFEST.as_bytes()))
}

/// 登记实例地址。登记只供诊断；平台部署、租户审批和启用由控制面单独管理。
async fn register(
    core_addr: &str,
    tenant: &str,
    authority: &str,
    broker: &RegistrationBroker,
) -> Result<(), String> {
    let mut client = CapabilityServiceClient::connect(core_addr.to_string())
        .await
        .map_err(|e| format!("连不上核心 capability 服务（{core_addr}）：{e}"))?;
    let mut labels = HashMap::new();
    labels.insert("source".to_string(), "example".to_string());
    let body = RegisterPluginEndpointRequest {
        tenant_id: tenant.to_string(),
        plugin_id: PLUGIN_ID.to_string(),
        capability_id: CAPABILITY_ID.to_string(),
        grpc_authority: authority.to_string(),
        labels,
        request_id: uuid::Uuid::new_v4().to_string(),
        // 完整声明是注册前提，不等同于平台审批或发布者签名验证。
        plugin_version: env!("CARGO_PKG_VERSION").to_string(),
        api_version: "1.0".to_string(),
        manifest_sha256: manifest_sha256(),
        declared_operations: vec![CAPABILITY_ID.to_string()],
    };
    let credential = broker.credential(&registration_intent(&body)).await?;
    client
        .register_plugin_endpoint(with_credential(body, &credential)?)
        .await
        .map_err(|e| format!("注册失败：{e}"))?;
    Ok(())
}

/// 退出前摘掉自己。不做这一步，路由簿里会留着一个死地址，直到健康检查把它清掉。
async fn deregister(core_addr: &str, tenant: &str, broker: &RegistrationBroker) {
    let body = DeregisterPluginEndpointRequest {
        tenant_id: tenant.to_string(),
        plugin_id: PLUGIN_ID.to_string(),
        request_id: uuid::Uuid::new_v4().to_string(),
    };
    let request = match broker.credential(&deregistration_intent(&body)).await {
        Ok(credential) => with_credential(body, &credential),
        Err(err) => Err(err),
    };
    let request = match request {
        Ok(request) => request,
        Err(err) => {
            tracing::warn!(%err, "注销拿不到凭据，路由需要等健康检查摘除");
            return;
        }
    };
    let Ok(mut client) = CapabilityServiceClient::connect(core_addr.to_string()).await else {
        tracing::warn!("注销时连不上核心，路由需要等健康检查摘除");
        return;
    };
    match client.deregister_plugin_endpoint(request).await {
        Ok(_) => tracing::info!("已从核心注销"),
        Err(err) => tracing::warn!(%err, "注销失败，路由需要等健康检查摘除"),
    }
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
    let tickets = match std::env::var("FLARE_PLUGIN_CALL_PUBLIC_KEYS") {
        Ok(raw) => Some(
            PluginCallVerifier::from_public_keys_json(&raw)
                .map_err(|_| "FLARE_PLUGIN_CALL_PUBLIC_KEYS 不是有效的公钥列表")?,
        ),
        Err(_) => {
            tracing::warn!(
                "没有 FLARE_PLUGIN_CALL_PUBLIC_KEYS：无法验证核心的调用票据，拒绝一切调用"
            );
            None
        }
    };
    let broker = RegistrationBroker::from_env()?;

    let plugin = LinkPreviewPlugin {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            // 不跟随跳转到未知地址的次数太多——限制一下，顺带避免重定向环
            .redirect(reqwest::redirect::Policy::limited(3))
            .user_agent("flare-link-preview/0.1 (+https://github.com/flare-im)")
            .build()?,
        tickets,
        deployment_id: broker.as_ref().map(|b| b.deployment_id.clone()),
    };

    // 先起服务再注册：反过来的话，核心可能在服务就绪之前就把请求发过来。
    let serving = tokio::spawn(
        Server::builder()
            .add_service(ExtensionPluginServer::new(plugin))
            .serve(addr),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;

    let authority = format!("http://{listen}");
    match &broker {
        Some(broker) => match register(&core_addr, &tenant, &authority, broker).await {
            Ok(()) => tracing::info!(
                capability = CAPABILITY_ID, plugin = PLUGIN_ID, %authority, tenant = %tenant,
                "已登记到核心；控制面给租户下发授权投影后，客户端就能 dispatch 这个能力"
            ),
            Err(err) => tracing::warn!(
                %err,
                "登记失败：插件仍在监听，调用只看授权投影；路由簿里看不到它。\
                 先确认 flare-im-core 与注册凭据交换都已启动，再重跑本插件。"
            ),
        },
        None => tracing::warn!(
            "没有配置平台的注册凭据交换（FLARE_PLUGIN_BROKER_URL 等）：不登记。\
             核心拒绝不带凭据的注册；调用只看控制面的授权投影，与登记无关。"
        ),
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("收到中断，正在注销");
            if let Some(broker) = &broker {
                deregister(&core_addr, &tenant, broker).await;
            }
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
            tickets: None,
            deployment_id: None,
        };
        for url in ["file:///etc/passwd", "ftp://x", "gopher://x"] {
            assert!(p.preview(url).await.is_err(), "{url} 应被拒绝");
        }
    }

    #[test]
    fn manifest_is_valid_and_declares_this_plugin() {
        let manifest =
            flare_im_capability_core::plugin::PluginManifest::from_json(MANIFEST.as_bytes())
                .expect("清单要能通过协议校验");
        assert_eq!(manifest.plugin_id, PLUGIN_ID);
        // 注册声明的版本取自本包版本，控制面按清单审批：两者要一致。
        assert_eq!(manifest.version, env!("CARGO_PKG_VERSION"));
        assert!(manifest.operation(CAPABILITY_ID).is_some());
        assert_eq!(manifest_sha256().len(), 64);
    }

    #[test]
    fn the_registration_credential_binds_the_exact_request_body() {
        let body = RegisterPluginEndpointRequest {
            tenant_id: "app-a".into(),
            plugin_id: PLUGIN_ID.into(),
            capability_id: CAPABILITY_ID.into(),
            grpc_authority: "http://127.0.0.1:7803".into(),
            labels: HashMap::from([("source".to_string(), "example".to_string())]),
            request_id: "r1".into(),
            plugin_version: env!("CARGO_PKG_VERSION").into(),
            api_version: "1.0".into(),
            manifest_sha256: manifest_sha256(),
            declared_operations: vec![CAPABILITY_ID.into()],
        };
        let audience = registration_intent(&body)
            .audience()
            .expect("声明要能通过协议校验");
        let mut moved = body.clone();
        moved.grpc_authority = "http://127.0.0.1:7804".into();
        assert_ne!(
            registration_intent(&moved).audience().unwrap(),
            audience,
            "换一个地址，原来的凭据就不能用"
        );
    }

    #[test]
    fn a_call_needs_a_ticket_core_signed_for_exactly_this_call() {
        use flare_im_capability_core::plugin::invocation::{PluginCallClaims, PluginCallSigner};
        use tonic::Code;
        let now = chrono::Utc::now().timestamp_millis();
        let core = PluginCallSigner::new("core".into(), [7; 32]).unwrap();
        let mut verifier = PluginCallVerifier::default();
        verifier.insert("core".into(), core.public_key()).unwrap();
        let claims = PluginCallClaims {
            tenant_id: "app-a".into(),
            plugin_id: PLUGIN_ID.into(),
            plugin_version: env!("CARGO_PKG_VERSION").into(),
            manifest_digest: format!("sha256:{}", manifest_sha256()),
            deployment_id: "dep-1".into(),
            grant_id: "grant-1".into(),
            grant_revision: 1,
            configuration_digest: format!("sha256:{}", "0".repeat(64)),
            actor_id: "u1".into(),
            operation: CAPABILITY_ID.into(),
            request_id: "r1".into(),
            conversation_id: None,
            issued_at_ms: now,
            expires_at_ms: now + 10_000,
        };
        let ticket = core.sign(&claims, now).unwrap();
        let call = |ticket: Option<&str>, operation: &str, request_id: &str| {
            let mut request = Request::new(GenericRequest {
                operation: operation.into(),
                request_id: request_id.into(),
                metadata: HashMap::from([("tenant_id".to_string(), "app-a".to_string())]),
                payload: None,
            });
            if let Some(ticket) = ticket {
                request
                    .metadata_mut()
                    .insert(PLUGIN_CALL_HEADER, ticket.parse().unwrap());
            }
            request
        };
        let check = |request: &Request<GenericRequest>, deployment: Option<&str>, at: i64| {
            verify_ticket(Some(&verifier), deployment, request, at).map_err(|s| s.code())
        };

        let genuine = call(Some(&ticket), CAPABILITY_ID, "r1");
        assert_eq!(check(&genuine, Some("dep-1"), now), Ok(()));
        // 不是核心发来的：没有票据、本插件没有公钥、别的密钥签的、过期的。
        let missing = call(None, CAPABILITY_ID, "r1");
        assert_eq!(check(&missing, None, now), Err(Code::Unauthenticated));
        assert_eq!(
            verify_ticket(None, None, &genuine, now).map_err(|s| s.code()),
            Err(Code::Unauthenticated)
        );
        let forged = PluginCallSigner::new("core".into(), [8; 32])
            .unwrap()
            .sign(&claims, now)
            .unwrap();
        let forged = call(Some(&forged), CAPABILITY_ID, "r1");
        assert_eq!(check(&forged, None, now), Err(Code::Unauthenticated));
        assert_eq!(
            check(&genuine, None, now + 20_000),
            Err(Code::Unauthenticated)
        );
        // 票据是真的，但签给的是另一次调用、另一个操作、另一个部署。
        let replayed = call(Some(&ticket), CAPABILITY_ID, "r2");
        assert_eq!(check(&replayed, None, now), Err(Code::PermissionDenied));
        let other_operation = call(Some(&ticket), "link.preview.v2", "r1");
        assert_eq!(
            check(&other_operation, None, now),
            Err(Code::PermissionDenied)
        );
        assert_eq!(
            check(&genuine, Some("dep-2"), now),
            Err(Code::PermissionDenied)
        );
    }

    #[tokio::test]
    async fn answers_the_health_probe_but_no_call_without_a_ticket() {
        let plugin = LinkPreviewPlugin {
            http: reqwest::Client::new(),
            tickets: Some(PluginCallVerifier::default()),
            deployment_id: None,
        };
        let health = plugin
            .call(Request::new(GenericRequest {
                operation: HEALTH_CHECK.into(),
                request_id: "h1".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(health.ok);
        let refused = plugin
            .call(Request::new(GenericRequest {
                operation: CAPABILITY_ID.into(),
                request_id: "r1".into(),
                payload: Some(prost_types::Any {
                    type_url: "type.googleapis.com/flare.capability.v1.PayloadJson".into(),
                    value: br#"{"url":"https://example.com"}"#.to_vec(),
                }),
                ..Default::default()
            }))
            .await;
        assert_eq!(refused.unwrap_err().code(), tonic::Code::Unauthenticated);
    }
}
