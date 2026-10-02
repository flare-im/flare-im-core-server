//! 命令行设置。argv 只带非秘密值；签名私钥与数据库口令只给文件路径，
//! 由 [`crate::secret_file`] 在本进程内读取。

use std::path::PathBuf;
use std::time::Duration;

use crate::error::{Result, ensure, err, fail};

pub const DEFAULT_DB_PORT: u16 = 5432;
pub const DEFAULT_DB_NAME: &str = "flare_core";
pub const DEFAULT_TIMEOUT_SECS: u64 = 60;
pub const MAX_TIMEOUT_SECS: u64 = 900;

/// 这些环境变量会让人以为秘密能经环境传进来；本工具从不读它们，出现就拒绝，
/// 免得调用方误以为口令或私钥已生效（也免得它们被继续传给子进程）。
pub const REFUSED_SECRET_ENV: [&str; 6] = [
    "PGPASSWORD",
    "PGPASSFILE",
    "DATABASE_URL",
    "FLARE_POSTGRES_URL",
    "FLARE_INTERNAL_CTX_PRIVATE_KEY",
    "FLARE_INTERNAL_CTX_SIGNING_KEY",
];

pub const USAGE: &str = "\
Usage: flare-smoke-grpc [--dry-run] --ingest-endpoint http://HOST:PORT
         --tenant-id e2e-<20 hex> --sender-id ID --recipient-id ID
         --signing-key-file PATH
         --db-host HOST [--db-port 5432] [--db-name flare_core] --db-user ROLE
         --db-password-file PATH [--timeout-seconds 60]

Signed gRPC message-flow smoke for a single-container Core (DEP-06).

Sends one text message from --sender-id to --recipient-id through Message
Ingest's MessageSendService/SendMessage. The call carries x-flare-ctx, an
EdDSA internal context signed in this process with the Cell private key read
from --signing-key-file (mode 600). No call is ever made without it, and no
legacy identity metadata is sent.

Read-back goes through a read-only PostgreSQL session (startup option
default_transaction_read_only=on plus BEGIN TRANSACTION READ ONLY): the
message row (storage), the write ledger at archive_persisted or later
(archive), and both participants' conversation seq at least the sent seq,
read the way Core's conversation list reads it: conversation and member not
deleted, seq = GREATEST(conversations.last_message_seq, newest message the
member may see) (conversation). The password comes only from
--db-password-file (mode 600) and default or weak passwords are refused.
Core's read RPCs (conversation, storage-reader) only take unsigned identity
metadata, so they are not called.

--dry-run validates every setting, loads and test-signs with the key, checks
the password policy and prints the plan without opening any connection.

The sender and recipient must already be friends in the fixture tenant
(Core's PreSend Hook asks Social), e.g. the two users an HTTP-mode run of
smoke_message_flow.sh reports.

Exit status: 0 pass (or dry run), 1 smoke failed, 2 invalid settings.
";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbTarget {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub dry_run: bool,
    pub ingest_endpoint: String,
    pub tenant_id: String,
    pub sender_id: String,
    pub recipient_id: String,
    pub signing_key_file: PathBuf,
    pub db: DbTarget,
    pub timeout: Duration,
}

pub enum Parsed {
    Help,
    Run(Box<Config>),
}

/// `env` 只用来检查 [`REFUSED_SECRET_ENV`]，测试注入，避免改进程环境。
pub fn parse(args: &[String], env: impl Fn(&str) -> Option<String>) -> Result<Parsed> {
    for name in REFUSED_SECRET_ENV {
        if env(name).is_some_and(|value| !value.is_empty()) {
            fail!(
                "{name} is set; secrets are only read from the 600 files given by \
                 --signing-key-file and --db-password-file, unset it"
            );
        }
    }

    let mut dry_run = false;
    let mut values: Vec<(&'static str, String)> = Vec::new();
    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        index += 1;
        if arg == "-h" || arg == "--help" {
            return Ok(Parsed::Help);
        }
        if arg == "--dry-run" {
            dry_run = true;
            continue;
        }
        let (name, inline) = match arg.split_once('=') {
            Some((name, value)) => (name, Some(value.to_owned())),
            None => (arg, None),
        };
        let key = FLAGS
            .iter()
            .copied()
            .find(|flag| *flag == name)
            .ok_or_else(|| err!("unknown argument (see --help)"))?;
        let value = match inline {
            Some(value) => value,
            None => {
                let value = args
                    .get(index)
                    .ok_or_else(|| err!("{key} needs a value"))?
                    .clone();
                index += 1;
                value
            }
        };
        ensure!(
            values.iter().all(|(seen, _)| *seen != key),
            "{key} given more than once"
        );
        values.push((key, value));
    }

    let take = |key: &str| {
        values
            .iter()
            .find(|(name, _)| *name == key)
            .map(|(_, value)| value.clone())
    };
    let required = |key: &str| take(key).ok_or_else(|| err!("{key} is required (see --help)"));

    let ingest_endpoint = validate_endpoint(&required("--ingest-endpoint")?)?;
    let tenant_id = required("--tenant-id")?;
    ensure!(
        is_fixture_tenant(&tenant_id),
        "--tenant-id must be an isolated fixture tenant e2e-<20 lowercase hex>; the default tenant is never used"
    );
    let sender_id = validate_user("--sender-id", required("--sender-id")?)?;
    let recipient_id = validate_user("--recipient-id", required("--recipient-id")?)?;
    ensure!(
        sender_id != recipient_id,
        "--sender-id and --recipient-id must differ"
    );
    let signing_key_file = PathBuf::from(required("--signing-key-file")?);
    let db = DbTarget {
        host: validate_host("--db-host", &required("--db-host")?)?,
        port: match take("--db-port") {
            Some(port) => parse_port("--db-port", &port)?,
            None => DEFAULT_DB_PORT,
        },
        database: validate_identifier(
            "--db-name",
            take("--db-name").unwrap_or_else(|| DEFAULT_DB_NAME.to_owned()),
        )?,
        user: validate_identifier("--db-user", required("--db-user")?)?,
        password_file: PathBuf::from(required("--db-password-file")?),
    };
    let timeout = match take("--timeout-seconds") {
        Some(value) => {
            let secs: u64 = value
                .parse()
                .map_err(|_| err!("--timeout-seconds must be an integer"))?;
            ensure!(
                (1..=MAX_TIMEOUT_SECS).contains(&secs),
                "--timeout-seconds must be between 1 and {MAX_TIMEOUT_SECS}"
            );
            Duration::from_secs(secs)
        }
        None => Duration::from_secs(DEFAULT_TIMEOUT_SECS),
    };

    Ok(Parsed::Run(Box::new(Config {
        dry_run,
        ingest_endpoint,
        tenant_id,
        sender_id,
        recipient_id,
        signing_key_file,
        db,
        timeout,
    })))
}

const FLAGS: [&str; 11] = [
    "--ingest-endpoint",
    "--tenant-id",
    "--sender-id",
    "--recipient-id",
    "--signing-key-file",
    "--db-host",
    "--db-port",
    "--db-name",
    "--db-user",
    "--db-password-file",
    "--timeout-seconds",
];

pub fn is_fixture_tenant(value: &str) -> bool {
    value.len() == 24
        && value.starts_with("e2e-")
        && value[4..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Message Ingest 只在 compose 内网可达且不带 TLS：只接受 `http://主机:端口`。
fn validate_endpoint(value: &str) -> Result<String> {
    let rest = value.strip_prefix("http://").ok_or_else(|| {
        err!(
            "--ingest-endpoint must be http://HOST:PORT (plaintext gRPC inside the compose network)"
        )
    })?;
    let authority = rest.strip_suffix('/').unwrap_or(rest);
    let (host, port) = authority
        .rsplit_once(':')
        .ok_or_else(|| err!("--ingest-endpoint must name an explicit port"))?;
    validate_host("--ingest-endpoint host", host)?;
    parse_port("--ingest-endpoint port", port)?;
    Ok(format!("http://{host}:{port}"))
}

fn validate_host(what: &str, host: &str) -> Result<String> {
    let valid = !host.is_empty()
        && host.len() <= 253
        && host.as_bytes()[0].is_ascii_alphanumeric()
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.' || byte == b'-');
    ensure!(valid, "{what} must be a host name or IPv4 address");
    Ok(host.to_owned())
}

fn parse_port(what: &str, value: &str) -> Result<u16> {
    let port: u16 = value.parse().map_err(|_| err!("{what} must be 1-65535"))?;
    ensure!(port != 0, "{what} must be 1-65535");
    Ok(port)
}

fn validate_identifier(what: &str, value: String) -> Result<String> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && (value.as_bytes()[0].is_ascii_alphabetic() || value.starts_with('_'))
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
    ensure!(valid, "{what} must be a plain PostgreSQL identifier");
    Ok(value)
}

fn validate_user(what: &str, value: String) -> Result<String> {
    let valid = !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
    ensure!(valid, "{what} must be 1-64 characters of [A-Za-z0-9_-]");
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TENANT: &str = "e2e-0123456789abcdef0123";

    fn base() -> Vec<String> {
        [
            "--ingest-endpoint",
            "http://core:50182",
            "--tenant-id",
            TENANT,
            "--sender-id",
            "7312345678901234567",
            "--recipient-id",
            "7312345678901234568",
            "--signing-key-file",
            "/run/key",
            "--db-host",
            "postgres",
            "--db-user",
            "flare_core_app",
            "--db-password-file",
            "/run/db.password",
        ]
        .iter()
        .map(|value| value.to_string())
        .collect()
    }

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn config(args: &[String]) -> Result<Config> {
        match parse(args, no_env)? {
            Parsed::Run(config) => Ok(*config),
            Parsed::Help => fail!("help"),
        }
    }

    fn with(mut args: Vec<String>, flag: &str, value: &str) -> Vec<String> {
        let position = args.iter().position(|arg| arg == flag).unwrap();
        args[position + 1] = value.to_owned();
        args
    }

    fn without(mut args: Vec<String>, flag: &str) -> Vec<String> {
        let position = args.iter().position(|arg| arg == flag).unwrap();
        args.drain(position..position + 2);
        args
    }

    #[test]
    fn parses_settings_and_defaults() {
        let parsed = config(&base()).unwrap();
        assert!(!parsed.dry_run);
        assert_eq!(parsed.ingest_endpoint, "http://core:50182");
        assert_eq!(parsed.db.port, 5432);
        assert_eq!(parsed.db.database, "flare_core");
        assert_eq!(parsed.timeout, Duration::from_secs(60));

        let mut args = base();
        args.extend(
            ["--dry-run", "--db-port=15432", "--db-name", "core_db"]
                .iter()
                .map(|value| value.to_string()),
        );
        args.push("--timeout-seconds=5".into());
        let parsed = config(&args).unwrap();
        assert!(parsed.dry_run);
        assert_eq!(parsed.db.port, 15432);
        assert_eq!(parsed.db.database, "core_db");
        assert_eq!(parsed.timeout, Duration::from_secs(5));
    }

    #[test]
    fn default_or_malformed_tenants_are_refused() {
        for tenant in [
            "0",
            "default",
            "",
            "e2e-old",
            "e2e-0123456789ABCDEF0123",
            "e2e-0123456789abcdef01234",
            "tenant-0123456789abcdef0123",
        ] {
            let error = config(&with(base(), "--tenant-id", tenant)).unwrap_err();
            assert!(
                error.to_string().contains("isolated fixture tenant"),
                "{tenant}: {error}"
            );
        }
    }

    #[test]
    fn every_required_setting_is_enforced() {
        for flag in [
            "--ingest-endpoint",
            "--tenant-id",
            "--sender-id",
            "--recipient-id",
            "--signing-key-file",
            "--db-host",
            "--db-user",
            "--db-password-file",
        ] {
            let error = config(&without(base(), flag)).unwrap_err();
            assert!(error.to_string().contains(flag), "{flag}: {error}");
        }
    }

    #[test]
    fn endpoint_must_be_plain_internal_http() {
        for endpoint in [
            "https://core:50182",
            "core:50182",
            "http://core",
            "http://core:0",
            "http://core:50182/flare.message.v1.MessageSendService",
            "http://user:pw@core:50182",
            "http://-core:50182",
        ] {
            assert!(
                config(&with(base(), "--ingest-endpoint", endpoint)).is_err(),
                "{endpoint}"
            );
        }
        let parsed = config(&with(
            base(),
            "--ingest-endpoint",
            "http://127.0.0.1:50182/",
        ))
        .unwrap();
        assert_eq!(parsed.ingest_endpoint, "http://127.0.0.1:50182");
    }

    #[test]
    fn identifiers_and_users_are_plain() {
        assert!(config(&with(base(), "--db-user", "role; DROP")).is_err());
        assert!(config(&with(base(), "--db-host", "-oProxyCommand")).is_err());
        assert!(config(&with(base(), "--sender-id", "a b")).is_err());
        assert!(config(&with(base(), "--recipient-id", "7312345678901234567")).is_err());
        for timeout in ["0", "901", "1.5", "-1"] {
            let mut args = base();
            args.push(format!("--timeout-seconds={timeout}"));
            assert!(config(&args).is_err(), "{timeout}");
        }
        let mut repeated = base();
        repeated.extend(["--db-host".to_string(), "other".to_string()]);
        assert!(config(&repeated).is_err());
        let mut unknown = base();
        unknown.push("--postgres-url=postgres://u:p@h/db".into());
        let error = config(&unknown).unwrap_err().to_string();
        assert!(!error.contains("u:p@"), "{error}");
    }

    #[test]
    fn secrets_in_environment_are_refused() {
        for name in REFUSED_SECRET_ENV {
            let env = |key: &str| (key == name).then(|| "hunter2-env-secret".to_string());
            let error = match parse(&base(), env) {
                Err(error) => error.to_string(),
                Ok(_) => panic!("{name} accepted"),
            };
            assert!(error.contains(name), "{error}");
            assert!(!error.contains("hunter2-env-secret"), "{error}");
        }
    }

    #[test]
    fn help_is_recognised() {
        assert!(matches!(
            parse(&["--help".to_string()], no_env).unwrap(),
            Parsed::Help
        ));
    }
}
