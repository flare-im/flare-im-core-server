//! 开源插件样例：消息审计落盘。
//!
//! # 与 `hook_rate_limit` 的区别
//!
//! 这两个样例是插件的两种基本形态，值得对照着读：
//!
//! | | `hook_rate_limit`（PreSend） | 本例（PostSend） |
//! | --- | --- | --- |
//! | 时机 | 消息**还没落库** | 消息**已经落库** |
//! | 能做什么 | 拒绝、改写内容 | 只能观察与产生副作用 |
//! | 失败的代价 | 可能拒发正常消息 | 审计缺一条 |
//! | 该不该 `require_success` | 是（拦不住就别放行） | 否（不该因为写日志失败而影响消息） |
//!
//! 最后一行是这个样例真正想说的：**PostSend 拿到的是既成事实**，消息已经落库、
//! seq 已经分配。这时候返回失败并不能让消息回退，只会让核心记一条错误。
//! 所以它在 hooks.toml 里应当配 `require_success = false`。
//!
//! # 跑起来
//!
//! ```bash
//! cargo run --example hook_audit_log            # 默认写 ./logs/audit.jsonl
//! ```
//!
//! ```toml
//! # config/hooks.toml
//! [[post_send]]
//! name = "audit-log"
//! priority = 0
//! timeout_ms = 1000
//! require_success = false     # 审计失败不该影响消息
//!
//! [post_send.transport]
//! type = "grpc"
//! endpoint = "http://127.0.0.1:7802"
//! ```
//!
//! 环境变量：`HOOK_AUDIT_ADDR`（默认 `127.0.0.1:7802`）、
//! `HOOK_AUDIT_FILE`（默认 `logs/audit.jsonl`）。
//!
//! # 为什么写 JSON Lines
//!
//! 一行一条、追加写、不需要解析整个文件就能增量消费——审计场景最省事的格式。
//! 换成投递到 Kafka / ClickHouse 也只是把 `append_line` 换掉，其余不动。

use std::fs::{OpenOptions, create_dir_all};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use flare_grpc_proto::capability::hook_plugin_server::{HookPlugin, HookPluginServer};
use flare_grpc_proto::capability::{
    GenericRequest, GenericResponse, PostSendHookRequest, PostSendHookResponse,
};
use prost::Message as _;
use tonic::{Request, Response, Status, transport::Server};

const OP_POST_SEND: &str = "flare.hook.v1.post_send";
const POST_SEND_RESPONSE_TYPE: &str =
    "type.googleapis.com/flare.capability.v1.PostSendHookResponse";

struct AuditPlugin {
    /// 串行化写入。审计量大时该换成有界 channel + 后台写线程，
    /// 但那会让样例的重点从「插件契约」偏到「怎么写日志」。
    sink: Mutex<PathBuf>,
}

impl AuditPlugin {
    fn append_line(&self, line: &str) -> std::io::Result<()> {
        let path = self.sink.lock().expect("audit sink poisoned");
        if let Some(dir) = path.parent() {
            create_dir_all(dir)?;
        }
        let mut f = OpenOptions::new().create(true).append(true).open(&*path)?;
        writeln!(f, "{line}")
    }

    fn handle_post_send(&self, payload: &[u8]) -> Result<PostSendHookResponse, Status> {
        let request = PostSendHookRequest::decode(payload)
            .map_err(|e| Status::invalid_argument(format!("decode PostSendHookRequest: {e}")))?;

        let ctx = request.context.unwrap_or_default();
        let record = request.record.unwrap_or_default();
        let draft = request.draft.unwrap_or_default();

        // 只记审计需要的字段，**不记消息正文**：审计日志往往比消息本身留存更久，
        // 把正文抄一份进去等于凭空多一个泄漏面。要留证据就留 message_id，
        // 需要时回消息库取。
        let line = serde_json::json!({
            "tenant_id": ctx.tenant_id,
            "conversation_id": ctx.conversation_id,
            "conversation_type": ctx.conversation_type,
            "operator_user_id": ctx.operator_user_id,
            "client_message_id": draft.client_message_id,
            "message_type": draft.message_type,
            "server_seq": record.server_seq,
            "persisted_at_secs": record.persisted_at.map(|t| t.seconds),
            "request_id": ctx.request_id,
        })
        .to_string();

        if let Err(err) = self.append_line(&line) {
            // 写不进去也要如实回报，但**不要**把它变成消息失败：
            // 消息此刻已经落库了，返回 success=false 不会让它回退。
            // require_success=false 时核心只记一条错误，这正是期望的行为。
            tracing::error!(error = %err, "审计写入失败");
            return Ok(PostSendHookResponse {
                success: false,
                error_code: "AUDIT_SINK_UNAVAILABLE".to_string(),
                error_message: format!("write audit line: {err}"),
                ..Default::default()
            });
        }

        Ok(PostSendHookResponse {
            success: true,
            ..Default::default()
        })
    }
}

#[tonic::async_trait]
impl HookPlugin for AuditPlugin {
    async fn call(
        &self,
        request: Request<GenericRequest>,
    ) -> Result<Response<GenericResponse>, Status> {
        let req = request.into_inner();
        let request_id = req.request_id.clone();

        let inner = match req.operation.as_str() {
            OP_POST_SEND => {
                let payload = req.payload.map(|a| a.value).unwrap_or_default();
                self.handle_post_send(&payload)?
            }
            // 同 hook_rate_limit：不认识的 operation 要按「成功」返回，
            // 否则核心新增 hook 点时这个插件会开始刷错误日志。
            other => {
                tracing::debug!(operation = %other, "未处理的 operation");
                PostSendHookResponse {
                    success: true,
                    ..Default::default()
                }
            }
        };

        Ok(Response::new(GenericResponse {
            ok: true,
            request_id,
            payload: Some(prost_types::Any {
                type_url: POST_SEND_RESPONSE_TYPE.to_string(),
                value: inner.encode_to_vec(),
            }),
            ..Default::default()
        }))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let addr = std::env::var("HOOK_AUDIT_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:7802".to_string())
        .parse()?;
    let file = PathBuf::from(
        std::env::var("HOOK_AUDIT_FILE").unwrap_or_else(|_| "logs/audit.jsonl".to_string()),
    );

    tracing::info!(%addr, sink = %file.display(), "audit hook plugin 已启动");

    Server::builder()
        .add_service(HookPluginServer::new(AuditPlugin {
            sink: Mutex::new(file),
        }))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod audit_tests {
    use super::*;

    fn temp_plugin() -> (AuditPlugin, PathBuf) {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "flare-audit-test-{}.jsonl",
            std::process::id() as u64 + rand::random::<u16>() as u64
        ));
        let _ = std::fs::remove_file(&p);
        (
            AuditPlugin {
                sink: Mutex::new(p.clone()),
            },
            p,
        )
    }

    #[test]
    fn writes_one_json_line_per_message() {
        let (plugin, path) = temp_plugin();
        plugin.append_line(r#"{"a":1}"#).expect("write");
        plugin.append_line(r#"{"a":2}"#).expect("write");
        let body = std::fs::read_to_string(&path).expect("read");
        assert_eq!(body.lines().count(), 2, "一条消息一行");
        // 每行都要能单独解析——这正是 JSON Lines 的意义
        for line in body.lines() {
            serde_json::from_str::<serde_json::Value>(line).expect("每行都是合法 JSON");
        }
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn creates_the_directory_if_missing() {
        // 首次运行时 logs/ 往往还不存在；这里失败会让审计静默丢第一批记录
        let mut dir = std::env::temp_dir();
        dir.push(format!("flare-audit-dir-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let path = dir.join("nested").join("audit.jsonl");
        let plugin = AuditPlugin {
            sink: Mutex::new(path.clone()),
        };
        plugin.append_line("{}").expect("应自动建目录");
        assert!(path.exists());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
