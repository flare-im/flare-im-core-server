use crate::domain::model::ConversationType;
use flare_im_contracts::utils::{TimelineMetadata, current_millis, normalize_tenant_id};
use flare_im_message_pipeline::SubmittedMessage;
use flare_proto::common::Message;
use flare_server_core::error::{ErrorCode, Result};
use flare_server_core::flare_err;
use uuid::Uuid;

use crate::domain::model::message_kind::MessageProfile;

#[derive(Clone, Debug)]
pub struct MessageDefaults {
    pub default_business_type: String,
    pub default_conversation_type: ConversationType,
    pub default_sender_type: String,
    pub default_tenant_id: Option<String>,
}

#[derive(Clone, Debug)]
pub struct MessageSubmission {
    pub message: Message,
    pub message_id: String,
    pub timeline: TimelineMetadata,
}

impl MessageSubmission {
    /// 从 common.Message 准备提交
    ///
    /// # 处理逻辑
    /// 1. 填充默认值（server_id, client_msg_id, conversation_type, status, source）
    /// 2. 推断消息类型（MessageProfile）
    /// 3. 设置时间戳和 timeline 元数据
    pub fn prepare(mut request: Message, defaults: &MessageDefaults) -> Result<Self> {
        if request.conversation_id.is_empty() {
            return Err(flare_err!(
                ErrorCode::BadRequest,
                "conversation_id is required"
            ));
        }
        if request.sender_id.is_empty() {
            return Err(flare_err!(ErrorCode::BadRequest, "sender_id is required"));
        }

        let client_provided_server_id = if !request.server_id.is_empty() {
            Some(request.server_id.clone())
        } else {
            None
        };
        request.server_id = Uuid::new_v4().to_string();

        if let Some(old_server_id) = client_provided_server_id {
            request
                .extensions
                .insert("original_server_id".to_string(), old_server_id.into_bytes());
        }

        if request.client_msg_id.is_empty() {
            request.client_msg_id = request.server_id.clone();
        }

        if request.source == 0 {
            request.source = match defaults.default_sender_type.as_str() {
                "user" => 1,
                "system" => 2,
                "bot" => 3,
                "admin" => 4,
                _ => 1,
            };
        }
        if request.conversation_type == 0 {
            request.conversation_type = defaults.default_conversation_type.as_int();
        }
        if request.status == 0 {
            request.status = 1;
        }

        // Ordering is server-authoritative. Client-provided sequence hints must not leak into
        // pre-allocation stages; `MessageIngestService::allocate_seq_for_submission` assigns the
        // durable conversation sequence after ensure/decorate succeeds.
        request.conversation_seq = 0;
        request.message_seq = None;

        let _profile = MessageProfile::ensure(&mut request);

        // 客户端声称的发出时刻先留存到 timeline.emit_ts，随后由服务端时间接管 created_at。
        //
        // created_at 曾经**原样存客户端时钟**（只在客户端没给时才填服务端时间）。
        // 后果有二：
        //   1. 客户端时钟错了，错误时间戳就直接进服务端库（实测有客户端慢 34 秒），
        //      跨客户端按时间戳排序不可靠；
        //   2. 服务端排查不能信这一列——拿它算服务端耗时会得出荒谬值。
        // 客户端的原始声称并没有丢，仍在 timeline.emit_ts 里，需要时可对照。
        let emit_ts = (request.created_at > 0).then_some(request.created_at);
        let ingestion_ts = current_millis();
        request.created_at = ingestion_ts;
        let timeline = TimelineMetadata {
            emit_ts,
            ingestion_ts,
            ..TimelineMetadata::default()
        };

        let _ = defaults.default_tenant_id.as_ref().map(normalize_tenant_id);

        let message_id = request.server_id.clone();
        Ok(Self {
            message: request,
            message_id,
            timeline,
        })
    }
}

impl SubmittedMessage for MessageSubmission {
    fn message(&self) -> &Message {
        &self.message
    }

    fn message_id(&self) -> &str {
        &self.message_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> MessageDefaults {
        MessageDefaults {
            default_business_type: "im".to_string(),
            default_conversation_type: ConversationType::Group,
            default_sender_type: "user".to_string(),
            default_tenant_id: Some("tenant-1".to_string()),
        }
    }

    /// created_at 必须由服务端接管，客户端声称的时刻退到 timeline.emit_ts。
    ///
    /// 这一列曾经原样存客户端时钟：客户端时钟错了，错误时间戳就直接进服务端库
    /// （实测有客户端慢 34 秒），跨客户端按时间戳排序不可靠，服务端排查也不能信它。
    #[test]
    fn server_time_wins_over_client_clock_but_client_claim_is_kept() {
        // 一个明显偏掉的客户端时钟：比真实时间慢一整天
        let bogus_client_ts = current_millis() - 86_400_000;
        let before = current_millis();
        let submission = MessageSubmission::prepare(
            Message {
                conversation_id: "conv-1".to_string(),
                sender_id: "user-1".to_string(),
                created_at: bogus_client_ts,
                ..Default::default()
            },
            &defaults(),
        )
        .expect("prepare");
        let after = current_millis();

        assert!(
            submission.message.created_at >= before && submission.message.created_at <= after,
            "created_at 必须是服务端此刻的时间，实际 {}",
            submission.message.created_at
        );
        assert_eq!(
            submission.timeline.emit_ts,
            Some(bogus_client_ts),
            "客户端声称的发出时刻不能丢，要留在 emit_ts 里"
        );
    }

    #[test]
    fn prepare_submission_clears_client_sequence_until_server_allocation() {
        let submission = MessageSubmission::prepare(
            Message {
                conversation_id: "conv-1".to_string(),
                sender_id: "user-1".to_string(),
                conversation_seq: 99,
                message_seq: Some(88),
                ..Default::default()
            },
            &defaults(),
        )
        .expect("prepare");

        assert_eq!(
            submission.message.conversation_seq, 0,
            "prepare must leave conversation_seq unassigned until server allocation"
        );
        assert_eq!(submission.message.message_seq, None);
    }
}
