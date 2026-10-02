//! 带签名的 gRPC 回读（主证据）：Storage Reader 按 seq 读到刚发的这条消息，收发双方各自在
//! 会话列表里看到该会话的 seq 不低于发送 seq。
//!
//! 每次调用都现签读者本人的 `x-flare-ctx`（actor_type=user，与网关代用户读时签的身份相同），
//! 请求只经 [`signed_request`] 构造，不带任何旧的身份元数据。Core 的读接口以签名为准、在单机
//! 模板里拒绝未签名调用，所以这条回读本身也在验证读接口的签名入口。只读数据库核对保留为
//! 第二证据（见 [`crate::durability`]）。

use std::time::Duration;

use flare_grpc_proto::conversation::ListConversationsRequest;
use flare_grpc_proto::conversation::conversation_read_service_client::ConversationReadServiceClient;
use flare_grpc_proto::storage::QueryMessagesBySeqRequest;
use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use tokio::time::Instant;
use tonic::Code;
use tonic::transport::{Channel, Endpoint};

use crate::durability::Expectation;
use crate::error::{Result, err, fail};
use crate::redact::redact;
use crate::signed::{ContextSigner, signed_request};

pub const STORAGE_READ_RPC: &str = "flare.storage.v1.StorageReaderService/QueryMessagesBySeq";
pub const CONVERSATION_READ_RPC: &str =
    "flare.conversation.v1.ConversationReadService/ListConversations";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// 会话列表按更新时间倒序，刚发过消息的会话在第一页；多翻几页只为容忍并发的其他会话。
const CONVERSATION_PAGE: i32 = 50;
const CONVERSATION_PAGES: usize = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadbackTarget {
    pub storage_reader: String,
    pub conversation: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadbackObservation {
    /// 收件人按 seq 读到的这条消息的 seq（按 server_msg_id 找）。
    pub storage_message_seq: Option<i64>,
    pub sender_conversation_seq: Option<i64>,
    pub recipient_conversation_seq: Option<i64>,
}

impl ReadbackObservation {
    pub fn problems(&self, expected: &Expectation) -> Vec<String> {
        let mut problems = Vec::new();
        match self.storage_message_seq {
            Some(seq) if seq == expected.seq => {}
            Some(seq) => problems.push(format!(
                "storage reader returned the message at seq {seq}, not {}",
                expected.seq
            )),
            None => problems.push("storage reader did not return the message".to_owned()),
        }
        for (side, seq) in [
            ("sender", self.sender_conversation_seq),
            ("recipient", self.recipient_conversation_seq),
        ] {
            match seq {
                Some(seq) if seq >= expected.seq => {}
                Some(seq) => problems.push(format!(
                    "{side} conversation list seq {seq} is below {}",
                    expected.seq
                )),
                None => problems.push(format!("{side} conversation list lacks the conversation")),
            }
        }
        problems
    }
}

/// 签名被拒、参数被拒、接口不存在都不会因为等一会儿而好转，直接失败；其余（下游暂不可用、
/// 超时等）在截止时间内重试。
fn final_rejection(code: Code) -> bool {
    matches!(
        code,
        Code::Unauthenticated
            | Code::PermissionDenied
            | Code::InvalidArgument
            | Code::Unimplemented
    )
}

struct Readers {
    storage: StorageReaderServiceClient<Channel>,
    conversations: ConversationReadServiceClient<Channel>,
}

async fn connect(endpoint: &str, what: &str, timeout: Duration) -> Result<Channel> {
    Endpoint::from_shared(endpoint.to_owned())
        .map_err(|_| err!("invalid {what} endpoint"))?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(timeout)
        .connect()
        .await
        .map_err(|error| {
            err!(
                "cannot reach {what} at {endpoint}: {}",
                redact(&error.to_string())
            )
        })
}

/// 读到条件全部满足为止，最长 `timeout`。
pub async fn wait_for_signed_readback(
    target: &ReadbackTarget,
    signer: &ContextSigner,
    expected: &Expectation,
    timeout: Duration,
    poll: Duration,
) -> Result<ReadbackObservation> {
    let mut readers = Readers {
        storage: StorageReaderServiceClient::new(
            connect(&target.storage_reader, "storage-reader", timeout).await?,
        ),
        conversations: ConversationReadServiceClient::new(
            connect(&target.conversation, "conversation", timeout).await?,
        ),
    };
    let deadline = Instant::now() + timeout;
    loop {
        let last = match observe(&mut readers, signer, expected).await {
            Ok(observation) => {
                let problems = observation.problems(expected);
                if problems.is_empty() {
                    return Ok(observation);
                }
                problems.join("; ")
            }
            Err(Rejected::Final(message)) => fail!("{message}"),
            Err(Rejected::Retry(message)) => message,
        };
        if Instant::now() >= deadline {
            fail!(
                "signed gRPC read-back of {} did not succeed within {}s: {last}",
                expected.server_msg_id,
                timeout.as_secs()
            );
        }
        tokio::time::sleep(poll).await;
    }
}

enum Rejected {
    Final(String),
    Retry(String),
}

fn rejected(rpc: &str, status: &tonic::Status) -> Rejected {
    let message = format!(
        "{rpc} rejected the signed read: code={:?} message={}",
        status.code(),
        redact(status.message())
    );
    if final_rejection(status.code()) {
        Rejected::Final(message)
    } else {
        Rejected::Retry(message)
    }
}

fn local(error: crate::error::FlareError) -> Rejected {
    Rejected::Final(crate::error::message(&error))
}

async fn observe(
    readers: &mut Readers,
    signer: &ContextSigner,
    expected: &Expectation,
) -> std::result::Result<ReadbackObservation, Rejected> {
    let token = signer
        .sign_user(&expected.tenant_id, &expected.recipient_id)
        .map_err(local)?;
    let request = signed_request(
        QueryMessagesBySeqRequest {
            conversation_id: expected.conversation_id.clone(),
            after_seq: expected.seq - 1,
            before_seq: 0,
            limit: 1,
            user_id: expected.recipient_id.clone(),
            include_burned_placeholder: false,
        },
        &token,
    )
    .map_err(local)?;
    let messages = readers
        .storage
        .query_messages_by_seq(request)
        .await
        .map_err(|status| rejected(STORAGE_READ_RPC, &status))?
        .into_inner()
        .messages;
    let storage_message_seq = messages
        .iter()
        .find(|message| message.server_id == expected.server_msg_id)
        .map(|message| message.conversation_seq as i64);

    let sender_conversation_seq =
        conversation_seq(readers, signer, expected, &expected.sender_id).await?;
    let recipient_conversation_seq =
        conversation_seq(readers, signer, expected, &expected.recipient_id).await?;
    Ok(ReadbackObservation {
        storage_message_seq,
        sender_conversation_seq,
        recipient_conversation_seq,
    })
}

/// `user_id` 本人（签名身份）的会话列表里这个会话的 seq。
async fn conversation_seq(
    readers: &mut Readers,
    signer: &ContextSigner,
    expected: &Expectation,
    user_id: &str,
) -> std::result::Result<Option<i64>, Rejected> {
    let mut cursor = String::new();
    for _ in 0..CONVERSATION_PAGES {
        let token = signer
            .sign_user(&expected.tenant_id, user_id)
            .map_err(local)?;
        let request = signed_request(
            ListConversationsRequest {
                cursor: cursor.clone(),
                limit: CONVERSATION_PAGE,
                order: 0,
            },
            &token,
        )
        .map_err(local)?;
        let page = readers
            .conversations
            .list_conversations(request)
            .await
            .map_err(|status| rejected(CONVERSATION_READ_RPC, &status))?
            .into_inner();
        if let Some(summary) = page
            .conversations
            .iter()
            .find(|summary| summary.conversation_id == expected.conversation_id)
        {
            return Ok(Some(
                summary.max_conversation_seq.min(i64::MAX as u64) as i64
            ));
        }
        if !page.has_more || page.next_cursor.is_empty() {
            break;
        }
        cursor = page.next_cursor;
    }
    Ok(None)
}
