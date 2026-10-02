//! flare-smoke-grpc：单容器 Core 的带签名 gRPC 消息链路冒烟（DEP-06）。
//!
//! 由 `scripts/smoke_message_flow.sh --mode grpc` 调用，也可单独运行（见 `--help`）。
//! 发送经 Message Ingest gRPC，`x-flare-ctx` 用 Cell 私钥在进程内签名；回读与落库
//! 确认走只读数据库会话。私钥与口令只从 600 文件读取，不进 argv、不回显。

mod cid;
mod config;
mod durability;
mod error;
mod ingest;
mod redact;
mod secret_file;
mod signed;

use std::io::Write;
use std::process::ExitCode;
use std::time::Duration;

use crate::error::Result;

use config::{Config, Parsed};
use durability::{DurabilityReader, Expectation, MINIMUM_LEDGER_STATE, PgReader};
use ingest::SmokeMessage;
use signed::ContextSigner;

const POLL_INTERVAL: Duration = Duration::from_secs(1);
const SEND_RPC: &str = "flare.message.v1.MessageSendService/SendMessage";
const SIGNED_CONTEXT: &str = "x-flare-ctx;alg=EdDSA;actor_type=user;ttl_seconds=60";

const EXIT_FAILED: u8 = 1;
const EXIT_INVALID: u8 = 2;

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut out = std::io::stdout().lock();
    let mut err = std::io::stderr().lock();
    ExitCode::from(
        execute(
            &args,
            |name| std::env::var(name).ok(),
            &mut out,
            &mut err,
            POLL_INTERVAL,
        )
        .await,
    )
}

/// 一次运行前全部准备好的东西：私钥已加载并试签，口令已过策略检查。
struct Prepared {
    signer: ContextSigner,
    password: String,
    message: SmokeMessage,
}

impl Prepared {
    fn load(config: &Config) -> Result<Self> {
        let signer = ContextSigner::load(&config.signing_key_file)?;
        signer.sign_user(&config.tenant_id, &config.sender_id)?;
        let password = secret_file::read_db_password(
            &config.db.password_file,
            &config.db.user,
            &config.db.database,
        )?;
        let run_id = uuid::Uuid::new_v4().simple().to_string();
        let run_id = &run_id[..12];
        Ok(Self {
            signer,
            password,
            message: SmokeMessage {
                tenant_id: config.tenant_id.clone(),
                conversation_id: cid::single_chat_conversation_id(
                    &config.sender_id,
                    &config.recipient_id,
                ),
                sender_id: config.sender_id.clone(),
                recipient_id: config.recipient_id.clone(),
                client_msg_id: format!("smoke-grpc-{run_id}"),
                text: format!("flare message-flow smoke grpc {run_id}"),
            },
        })
    }
}

async fn execute(
    args: &[String],
    env: impl Fn(&str) -> Option<String>,
    out: &mut impl Write,
    err: &mut impl Write,
    poll: Duration,
) -> u8 {
    let config = match config::parse(args, env) {
        Ok(Parsed::Help) => {
            let _ = out.write_all(config::USAGE.as_bytes());
            return 0;
        }
        Ok(Parsed::Run(config)) => *config,
        Err(error) => {
            let _ = writeln!(err, "flare-smoke-grpc: {}", error::message(&error));
            return EXIT_INVALID;
        }
    };
    let prepared = match Prepared::load(&config) {
        Ok(prepared) => prepared,
        Err(error) => {
            let _ = writeln!(err, "flare-smoke-grpc: {}", error::message(&error));
            return EXIT_INVALID;
        }
    };
    if config.dry_run {
        let _ = write_plan(out, &config, &prepared);
        return 0;
    }
    let result = async {
        // 先建只读会话：数据库连不上就不发消息，免得留下无法确认的一条。
        let mut reader = PgReader::connect(&config.db, &prepared.password).await?;
        let result = smoke(&config, &prepared, &mut reader, out, poll).await;
        reader.close().await;
        result
    }
    .await;
    match result {
        Ok(()) => 0,
        Err(error) => {
            let _ = writeln!(err, "flare-smoke-grpc: {}", error::message(&error));
            EXIT_FAILED
        }
    }
}

async fn smoke<R: DurabilityReader>(
    config: &Config,
    prepared: &Prepared,
    reader: &mut R,
    out: &mut impl Write,
    poll: Duration,
) -> Result<()> {
    let sent = ingest::send(
        &config.ingest_endpoint,
        &prepared.signer,
        &prepared.message,
        config.timeout,
    )
    .await?;
    let expected = Expectation {
        tenant_id: config.tenant_id.clone(),
        server_msg_id: sent.server_msg_id.clone(),
        conversation_id: prepared.message.conversation_id.clone(),
        sender_id: config.sender_id.clone(),
        recipient_id: config.recipient_id.clone(),
        seq: sent.conversation_seq as i64,
    };
    let observed = durability::wait_until_durable(reader, &expected, config.timeout, poll).await?;
    let lines = [
        "flare_message_flow_smoke_report".to_owned(),
        "mode=grpc".to_owned(),
        format!("ingest_endpoint={}", config.ingest_endpoint),
        format!("ingest_rpc={SEND_RPC}"),
        format!("signed_context={SIGNED_CONTEXT}"),
        format!("tenant_id={}", config.tenant_id),
        format!("sender_user_id={}", config.sender_id),
        format!("recipient_user_id={}", config.recipient_id),
        format!("conversation_id={}", expected.conversation_id),
        format!("client_msg_id={}", prepared.message.client_msg_id),
        format!("server_msg_id={}", sent.server_msg_id),
        format!("conversation_seq={}", sent.conversation_seq),
        format!("send_durability={}", sent.durability.as_str_name()),
        format!("db_target={}", db_target(config)),
        "db_session_read_only=on".to_owned(),
        format!("messages_row_count={}", observed.messages_row_count),
        format!(
            "ledger_write_state={}",
            observed.ledger_write_state.as_deref().unwrap_or("")
        ),
        format!("minimum_ledger_write_state={MINIMUM_LEDGER_STATE}"),
        format!(
            "readback_recipient_conversation_seq={}",
            observed.recipient_conversation_seq.unwrap_or_default()
        ),
        format!(
            "readback_sender_conversation_seq={}",
            observed.sender_conversation_seq.unwrap_or_default()
        ),
        "status=pass".to_owned(),
    ];
    for line in lines {
        writeln!(out, "{line}")?;
    }
    Ok(())
}

fn db_target(config: &Config) -> String {
    format!(
        "{}:{}/{} user={}",
        config.db.host, config.db.port, config.db.database, config.db.user
    )
}

fn write_plan(out: &mut impl Write, config: &Config, prepared: &Prepared) -> std::io::Result<()> {
    let lines = [
        "flare_message_flow_smoke_plan".to_owned(),
        "mode=grpc".to_owned(),
        "dry_run=true".to_owned(),
        format!("ingest_endpoint={}", config.ingest_endpoint),
        format!("ingest_rpc={SEND_RPC}"),
        format!("signed_context={SIGNED_CONTEXT}"),
        "signing_key=loaded;test_signature=ok".to_owned(),
        format!("tenant_id={}", config.tenant_id),
        format!("sender_user_id={}", config.sender_id),
        format!("recipient_user_id={}", config.recipient_id),
        format!("conversation_id={}", prepared.message.conversation_id),
        format!("db_target={}", db_target(config)),
        "db_password=loaded;policy=ok".to_owned(),
        "db_session=default_transaction_read_only=on;BEGIN TRANSACTION READ ONLY;ROLLBACK"
            .to_owned(),
        "readback=messages,message_write_ledger,conversations+conversation_participants".to_owned(),
        format!("minimum_ledger_write_state={MINIMUM_LEDGER_STATE}"),
        format!("timeout_seconds={}", config.timeout.as_secs()),
        "connections_opened=0".to_owned(),
        "status=dry-run".to_owned(),
    ];
    for line in lines {
        writeln!(out, "{line}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
