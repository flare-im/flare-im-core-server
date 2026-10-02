//! 只读回读：存储（messages 行）、归档（写账本 archive_persisted 及以后）、
//! 会话（收发双方在会话列表里看到的会话 seq，口径见 `CONVERSATION_ROWS`）。
//!
//! 只读有两层保证：建连的启动参数 `default_transaction_read_only=on`（建连后核对），
//! 每轮查询再显式 `BEGIN TRANSACTION READ ONLY` 并核对 `transaction_read_only`，
//! 结束一律 ROLLBACK。所有语句都是本文件的常量，测试核对除事务首尾外只有 SELECT。

use std::time::Duration;

use crate::error::{Result, err, fail};
use sqlx::postgres::{PgConnectOptions, PgConnection, PgSslMode};
use sqlx::{Connection, Row};
use tokio::time::Instant;

use crate::config::DbTarget;
use crate::redact::redact;

pub const APPLICATION_NAME: &str = "flare-smoke-grpc";
pub const MINIMUM_LEDGER_STATE: &str = "archive_persisted";
/// 与 HTTP 模式相同：archive_persisted 及其后的状态都算已落库。
pub const ACCEPTED_LEDGER_STATES: [&str; 4] = [
    "archive_persisted",
    "storage_persisted",
    "wal_cleaned",
    "ack_published",
];

pub const BEGIN_READ_ONLY: &str = "BEGIN TRANSACTION READ ONLY";
pub const END_TRANSACTION: &str = "ROLLBACK";
pub const SESSION_READ_ONLY: &str = "SELECT current_setting('default_transaction_read_only')";
pub const TRANSACTION_READ_ONLY: &str = "SELECT current_setting('transaction_read_only')";
pub const MESSAGE_ROWS: &str = "SELECT COUNT(*)::BIGINT FROM messages \
     WHERE tenant_id = $1 AND server_id = $2 AND conversation_id = $3 \
       AND sender_id = $4 AND seq = $5";
pub const LEDGER_STATE: &str = "SELECT write_state FROM message_write_ledger \
     WHERE tenant_id = $1 AND server_id = $2";
/// 收发双方各自在会话列表里看到的会话 seq，与 Core 会话列表同口径
/// （flare-conversation `postgres_repository.rs` 的用户会话查询）：会话未删除、成员未删除，
/// seq 取 `GREATEST(conversations.last_message_seq, 该成员可见范围内 messages 的最大 seq)`。
/// 不能只看 `conversations.last_message_seq`：会话服务的消息事件消费者只为租户 0 推进它，
/// 其他租户恒为 NULL，Core 自己的会话列表也是靠消息尾补上的。
pub const CONVERSATION_ROWS: &str = "SELECT p.user_id, GREATEST(COALESCE(c.last_message_seq, 0), \
       COALESCE((SELECT MAX(m.seq) FROM messages m \
                  WHERE m.tenant_id = p.tenant_id AND m.conversation_id = p.conversation_id \
                    AND m.seq > COALESCE(p.visible_from_seq, 0)), 0))::BIGINT \
     FROM conversation_participants p \
     JOIN conversations c ON c.tenant_id = p.tenant_id AND c.conversation_id = p.conversation_id \
     WHERE p.tenant_id = $1 AND p.conversation_id = $2 AND p.user_id IN ($3, $4) \
       AND NOT COALESCE(p.is_deleted, FALSE) \
       AND c.lifecycle_state != 'deleted'";

/// 本工具发给数据库的全部语句；测试据此核对只有只读语句。
#[cfg(test)]
pub const STATEMENTS: [&str; 7] = [
    BEGIN_READ_ONLY,
    SESSION_READ_ONLY,
    TRANSACTION_READ_ONLY,
    MESSAGE_ROWS,
    LEDGER_STATE,
    CONVERSATION_ROWS,
    END_TRANSACTION,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Expectation {
    pub tenant_id: String,
    pub server_msg_id: String,
    pub conversation_id: String,
    pub sender_id: String,
    pub recipient_id: String,
    pub seq: i64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Observation {
    pub transaction_read_only: bool,
    pub messages_row_count: i64,
    pub ledger_write_state: Option<String>,
    pub sender_conversation_seq: Option<i64>,
    pub recipient_conversation_seq: Option<i64>,
}

impl Observation {
    /// 未满足的条件；为空即通过。
    pub fn problems(&self, expected: &Expectation) -> Vec<String> {
        let mut problems = Vec::new();
        if !self.transaction_read_only {
            problems.push("transaction is not read-only".to_owned());
        }
        if self.messages_row_count != 1 {
            problems.push(format!(
                "messages_row_count={} (want exactly 1)",
                self.messages_row_count
            ));
        }
        match self.ledger_write_state.as_deref() {
            Some(state) if ACCEPTED_LEDGER_STATES.contains(&state) => {}
            state => problems.push(format!(
                "ledger_write_state={} (want {MINIMUM_LEDGER_STATE} or later)",
                state.unwrap_or("missing")
            )),
        }
        for (who, seq) in [
            ("recipient", self.recipient_conversation_seq),
            ("sender", self.sender_conversation_seq),
        ] {
            match seq {
                Some(seq) if seq >= expected.seq => {}
                Some(seq) => problems.push(format!(
                    "{who} conversation seq={seq} (want >= {})",
                    expected.seq
                )),
                None => problems.push(format!("{who} conversation row missing")),
            }
        }
        problems
    }
}

pub trait DurabilityReader {
    async fn observe(&mut self, expected: &Expectation) -> Result<Observation>;
}

/// 轮询到全部条件满足或超时。超时报告最后一次观测（只有计数与状态，无内容）。
pub async fn wait_until_durable<R: DurabilityReader>(
    reader: &mut R,
    expected: &Expectation,
    timeout: Duration,
    poll: Duration,
) -> Result<Observation> {
    let deadline = Instant::now() + timeout;
    loop {
        let last = match reader.observe(expected).await {
            Ok(observation) => {
                let problems = observation.problems(expected);
                if problems.is_empty() {
                    return Ok(observation);
                }
                problems.join("; ")
            }
            Err(error) => format!("read failed: {}", redact(&error.to_string())),
        };
        if Instant::now() >= deadline {
            fail!(
                "message {} did not become durable and visible within {}s: {last}",
                expected.server_msg_id,
                timeout.as_secs()
            );
        }
        tokio::time::sleep(poll).await;
    }
}

pub fn connect_options(target: &DbTarget, password: &str) -> PgConnectOptions {
    PgConnectOptions::new_without_pgpass()
        .host(&target.host)
        .port(target.port)
        .username(&target.user)
        .password(password)
        .database(&target.database)
        .application_name(APPLICATION_NAME)
        .ssl_mode(PgSslMode::Prefer)
        .options([("default_transaction_read_only", "on")])
}

pub struct PgReader {
    conn: PgConnection,
}

impl PgReader {
    pub async fn connect(target: &DbTarget, password: &str) -> Result<Self> {
        let shown = format!(
            "{}:{}/{} as {}",
            target.host, target.port, target.database, target.user
        );
        let mut conn = PgConnection::connect_with(&connect_options(target, password))
            .await
            .map_err(|error| {
                err!(
                    "cannot open the read-only database session {shown}: {}",
                    redact(&error.to_string())
                )
            })?;
        let session: String = sqlx::query_scalar(SESSION_READ_ONLY)
            .fetch_one(&mut conn)
            .await
            .map_err(|error| {
                err!(
                    "cannot read the session mode: {}",
                    redact(&error.to_string())
                )
            })?;
        if session != "on" {
            let _ = conn.close().await;
            fail!(
                "database session {shown} is not read-only (default_transaction_read_only={session})"
            );
        }
        Ok(Self { conn })
    }

    async fn observe_in_transaction(&mut self, expected: &Expectation) -> Result<Observation> {
        let read_only: String = sqlx::query_scalar(TRANSACTION_READ_ONLY)
            .fetch_one(&mut self.conn)
            .await?;
        if read_only != "on" {
            fail!("transaction_read_only={read_only}");
        }
        let messages_row_count: i64 = sqlx::query_scalar(MESSAGE_ROWS)
            .bind(&expected.tenant_id)
            .bind(&expected.server_msg_id)
            .bind(&expected.conversation_id)
            .bind(&expected.sender_id)
            .bind(expected.seq)
            .fetch_one(&mut self.conn)
            .await?;
        let ledger_write_state: Option<String> = sqlx::query_scalar(LEDGER_STATE)
            .bind(&expected.tenant_id)
            .bind(&expected.server_msg_id)
            .fetch_optional(&mut self.conn)
            .await?;
        let rows = sqlx::query(CONVERSATION_ROWS)
            .bind(&expected.tenant_id)
            .bind(&expected.conversation_id)
            .bind(&expected.sender_id)
            .bind(&expected.recipient_id)
            .fetch_all(&mut self.conn)
            .await?;
        let mut observation = Observation {
            transaction_read_only: true,
            messages_row_count,
            ledger_write_state,
            ..Observation::default()
        };
        for row in rows {
            let user: String = row.try_get(0)?;
            let seq: i64 = row.try_get(1)?;
            if user == expected.sender_id {
                observation.sender_conversation_seq = Some(seq);
            } else if user == expected.recipient_id {
                observation.recipient_conversation_seq = Some(seq);
            }
        }
        Ok(observation)
    }

    pub async fn close(self) {
        let _ = self.conn.close().await;
    }

    #[cfg(test)]
    pub(crate) fn connection(&mut self) -> &mut PgConnection {
        &mut self.conn
    }
}

impl DurabilityReader for PgReader {
    async fn observe(&mut self, expected: &Expectation) -> Result<Observation> {
        sqlx::raw_sql(BEGIN_READ_ONLY)
            .execute(&mut self.conn)
            .await?;
        let observed = self.observe_in_transaction(expected).await;
        let ended = sqlx::raw_sql(END_TRANSACTION).execute(&mut self.conn).await;
        let observation = observed?;
        ended?;
        Ok(observation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn expectation() -> Expectation {
        Expectation {
            tenant_id: "e2e-0123456789abcdef0123".into(),
            server_msg_id: "srv-1".into(),
            conversation_id: "1AS05Q2E4KQHG1W05P".into(),
            sender_id: "10001".into(),
            recipient_id: "10002".into(),
            seq: 7,
        }
    }

    fn durable() -> Observation {
        Observation {
            transaction_read_only: true,
            messages_row_count: 1,
            ledger_write_state: Some("ack_published".into()),
            sender_conversation_seq: Some(7),
            recipient_conversation_seq: Some(8),
        }
    }

    #[test]
    fn every_statement_is_read_only() {
        let forbidden = [
            "INSERT", "UPDATE", "DELETE", "MERGE", "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT",
            "REVOKE", "COPY", "CALL", "DO", "SET", "RESET", "LOCK", "COMMIT", "VACUUM", "NOTIFY",
            "PREPARE", "EXECUTE",
        ];
        for statement in STATEMENTS {
            let upper = statement.to_ascii_uppercase();
            assert!(
                upper.starts_with("SELECT ")
                    || statement == BEGIN_READ_ONLY
                    || statement == END_TRANSACTION,
                "{statement}"
            );
            let words: Vec<&str> = upper
                .split(|char: char| !(char.is_ascii_alphanumeric() || char == '_'))
                .collect();
            for word in forbidden {
                assert!(!words.contains(&word), "{word} in {statement}");
            }
        }
        assert_eq!(BEGIN_READ_ONLY, "BEGIN TRANSACTION READ ONLY");
        assert_eq!(END_TRANSACTION, "ROLLBACK");
    }

    #[test]
    fn session_is_read_only_and_password_comes_from_the_caller() {
        let target = DbTarget {
            host: "postgres".into(),
            port: 5432,
            database: "flare_core".into(),
            user: "flare_core_app".into(),
            password_file: PathBuf::from("/run/db.password"),
        };
        let options = connect_options(&target, "Qx7mV2pL9sKd4RtN8wYb3HjF");
        assert_eq!(options.get_host(), "postgres");
        assert_eq!(options.get_port(), 5432);
        assert_eq!(options.get_username(), "flare_core_app");
        assert_eq!(options.get_database(), Some("flare_core"));
        assert_eq!(options.get_application_name(), Some(APPLICATION_NAME));
        let startup = options.get_options().unwrap_or_default();
        assert!(
            startup.contains("default_transaction_read_only=on"),
            "{startup}"
        );
    }

    #[test]
    fn verdict_needs_storage_archive_and_both_conversations() {
        let expected = expectation();
        assert!(durable().problems(&expected).is_empty());
        for state in ACCEPTED_LEDGER_STATES {
            let observation = Observation {
                ledger_write_state: Some(state.into()),
                ..durable()
            };
            assert!(observation.problems(&expected).is_empty(), "{state}");
        }
        let cases = [
            Observation {
                transaction_read_only: false,
                ..durable()
            },
            Observation {
                messages_row_count: 0,
                ..durable()
            },
            Observation {
                messages_row_count: 2,
                ..durable()
            },
            Observation {
                ledger_write_state: Some("broker_accepted".into()),
                ..durable()
            },
            Observation {
                ledger_write_state: Some("failed".into()),
                ..durable()
            },
            Observation {
                ledger_write_state: None,
                ..durable()
            },
            Observation {
                recipient_conversation_seq: None,
                ..durable()
            },
            Observation {
                sender_conversation_seq: Some(6),
                ..durable()
            },
        ];
        for observation in cases {
            assert!(
                !observation.problems(&expected).is_empty(),
                "{observation:?}"
            );
        }
    }

    struct Scripted(Vec<Result<Observation>>, usize);

    impl DurabilityReader for Scripted {
        async fn observe(&mut self, _: &Expectation) -> Result<Observation> {
            self.1 += 1;
            if self.0.is_empty() {
                return Ok(Observation::default());
            }
            self.0.remove(0)
        }
    }

    #[tokio::test]
    async fn waits_for_durability_and_reports_the_last_gap() {
        let mut reader = Scripted(
            vec![
                Err(err!("connection reset")),
                Ok(Observation {
                    ledger_write_state: Some("broker_accepted".into()),
                    ..durable()
                }),
                Ok(durable()),
            ],
            0,
        );
        let observation = wait_until_durable(
            &mut reader,
            &expectation(),
            Duration::from_secs(5),
            Duration::from_millis(5),
        )
        .await
        .unwrap();
        assert_eq!(observation, durable());
        assert_eq!(reader.1, 3);

        let mut stuck = Scripted(
            vec![Ok(Observation {
                ledger_write_state: Some("broker_accepted".into()),
                ..durable()
            })],
            0,
        );
        let error = wait_until_durable(
            &mut stuck,
            &expectation(),
            Duration::from_millis(30),
            Duration::from_millis(5),
        )
        .await
        .unwrap_err()
        .to_string();
        assert!(error.contains("did not become durable"), "{error}");
        assert!(error.contains("messages_row_count=0"), "{error}");
    }
}
