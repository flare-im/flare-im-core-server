//! 客户端 ACK 领域服务：上行 `Ack` 仅允许 `push` / `conversation` / `read` / `batch`（见 `common/ack.proto`）。
//! `send` / `event` 为下行回执语义，不得经本路径上行。
//!
//! 送达确认（`ConversationAck`，单独上报或在 `AckBatch.conversation_acks` 里）只有网关用得上：
//! 它清掉这条连接在 [`DeliveryWatch`] 里等确认的条目，不出网关。其余回执（已读、PushAck）照旧
//! 上报路由。

use std::sync::Arc;

use flare_core::common::error::{FlareError, Result};
use flare_im_contracts::Ctx;
use flare_proto::common::ack::Payload as AckPayload;
use tracing::instrument;

use crate::application::commands::SendAckCommand;
use crate::domain::ports::IAckReportPort;
use crate::domain::service::DeliveryWatch;

pub struct SendAckDomainService {
    ack_port: Arc<dyn IAckReportPort>,
    delivery_watch: Arc<DeliveryWatch>,
}

impl SendAckDomainService {
    pub fn new(ack_port: Arc<dyn IAckReportPort>, delivery_watch: Arc<DeliveryWatch>) -> Self {
        Self {
            ack_port,
            delivery_watch,
        }
    }

    /// 在网关消化这条上行 ACK 里的送达确认，返回仍要上报路由的部分；没有剩余时返回 `None`。
    ///
    /// 不需要请求上下文也不做任何 I/O：客户端每隔几百毫秒就回一批送达确认，这条路径要够轻。
    pub fn absorb_delivery_acks(&self, cmd: &SendAckCommand) -> Result<Option<AckPayload>> {
        match cmd.ack.payload.as_ref() {
            None => Ok(None),
            Some(AckPayload::Conversation(conversation_ack)) => {
                self.delivery_watch
                    .confirm(&cmd.connection_id, conversation_ack);
                Ok(None)
            }
            Some(AckPayload::Batch(batch)) => {
                for conversation_ack in &batch.conversation_acks {
                    self.delivery_watch
                        .confirm(&cmd.connection_id, conversation_ack);
                }
                if batch.push_acks.is_empty()
                    && batch.send_acks.is_empty()
                    && batch.event_acks.is_empty()
                    && batch.read_acks.is_empty()
                {
                    return Ok(None);
                }
                let mut upstream = batch.clone();
                upstream.conversation_acks.clear();
                Ok(Some(AckPayload::Batch(upstream)))
            }
            Some(payload @ (AckPayload::Push(_) | AckPayload::Read(_))) => {
                Ok(Some(payload.clone()))
            }
            Some(AckPayload::Send(_)) => Err(FlareError::system(
                "uplink Ack.payload.send is invalid: SendAck is for downlink send receipt only",
            )),
            Some(AckPayload::Event(_)) => Err(FlareError::system(
                "uplink Ack.payload.event is invalid: use EVENT channel for events",
            )),
        }
    }

    /// 把 [`Self::absorb_delivery_acks`] 留下的部分上报路由。
    #[instrument(skip(self, tx, payload))]
    pub async fn report_upstream(&self, tx: &Ctx, payload: AckPayload) -> Result<()> {
        match payload {
            AckPayload::Push(push_ack) => self
                .ack_port
                .report_push_ack(tx, push_ack)
                .await
                .map_err(|e| FlareError::system(format!("report push ack failed: {e}"))),
            AckPayload::Read(read_ack) => self
                .ack_port
                .report_read_ack(tx, read_ack)
                .await
                .map_err(|e| FlareError::system(format!("report read ack failed: {e}"))),
            AckPayload::Batch(batch) => self
                .ack_port
                .report_ack_batch(tx, batch)
                .await
                .map_err(|e| FlareError::system(format!("report ack batch failed: {e}"))),
            // 送达确认不出网关；send / event 不是上行回执，早在消化那一步就被拒了。
            AckPayload::Conversation(_) | AckPayload::Send(_) | AckPayload::Event(_) => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::DeviceLinkState;
    use crate::domain::service::delivery_watch::testing::{detached, device, fixture};
    use crate::domain::service::{DeliveryItem, DeliveryWatchConfig};
    use async_trait::async_trait;
    use flare_proto::common::{Ack, AckBatch, ConversationAck, PushAck, ReadAck, SendAck};
    use std::sync::Mutex;

    /// 记下上报给路由的回执。
    #[derive(Default)]
    struct RecordingAckPort {
        reported: Mutex<Vec<AckPayload>>,
    }

    #[async_trait]
    impl IAckReportPort for RecordingAckPort {
        async fn report_push_ack(
            &self,
            _tx: &Ctx,
            ack: PushAck,
        ) -> flare_server_core::error::Result<()> {
            self.reported.lock().unwrap().push(AckPayload::Push(ack));
            Ok(())
        }

        async fn report_read_ack(
            &self,
            _tx: &Ctx,
            ack: ReadAck,
        ) -> flare_server_core::error::Result<()> {
            self.reported.lock().unwrap().push(AckPayload::Read(ack));
            Ok(())
        }

        async fn report_ack_batch(
            &self,
            _tx: &Ctx,
            ack: AckBatch,
        ) -> flare_server_core::error::Result<()> {
            self.reported.lock().unwrap().push(AckPayload::Batch(ack));
            Ok(())
        }
    }

    fn command(payload: AckPayload) -> SendAckCommand {
        SendAckCommand::new(
            "c1".to_string(),
            Ack {
                ack_id: None,
                ack_at: None,
                payload: Some(payload),
            },
            None,
        )
    }

    fn delivered(conversation_id: &str, last_delivered_seq: u64) -> ConversationAck {
        ConversationAck {
            conversation_id: conversation_id.to_string(),
            last_delivered_seq,
            ..Default::default()
        }
    }

    fn read(conversation_id: &str, read_seq: u64) -> ReadAck {
        ReadAck {
            conversation_id: conversation_id.to_string(),
            read_seq,
            ..Default::default()
        }
    }

    fn item(conversation_id: &str, seq: u64) -> DeliveryItem {
        DeliveryItem::new(conversation_id, seq, "")
    }

    /// 引擎回送达确认的形态：`Ack.payload = AckBatch`，只填 `conversation_acks`（重连后也按这个
    /// 形态重放）。整批在网关消化：清掉这条连接的环，没有任何东西上报路由。
    #[test]
    fn a_batch_of_delivery_confirmations_alone_never_leaves_the_gateway() {
        let f = fixture(DeliveryWatchConfig::default());
        f.watch
            .register("c1", device("u1"), DeviceLinkState::FOREGROUND);
        let mut targets = vec!["c1".to_string()];
        f.watch
            .admit(&mut targets, &[item("g1", 5), item("g1", 6), item("g2", 3)]);
        let port = Arc::new(RecordingAckPort::default());
        let service = SendAckDomainService::new(port.clone(), f.watch.clone());
        let confirmations = || {
            command(AckPayload::Batch(AckBatch {
                conversation_acks: vec![delivered("g1", 5), delivered("g2", 3)],
                device_id: Some("dev-u1".to_string()),
                ..Default::default()
            }))
        };

        let upstream = service
            .absorb_delivery_acks(&confirmations())
            .expect("batch accepted");
        assert_eq!(upstream, None);
        assert_eq!(f.watch.pending("c1"), [item("g1", 6)]);

        // 重放同一批（重连后的补发）：没有可清的条目，照样整批消化。
        assert_eq!(
            service
                .absorb_delivery_acks(&confirmations())
                .expect("replay accepted"),
            None
        );
        assert_eq!(f.watch.pending("c1"), [item("g1", 6)]);

        // 单独一条送达确认（不包在批里）同样就地消化。
        let upstream = service
            .absorb_delivery_acks(&command(AckPayload::Conversation(delivered("g1", 6))))
            .expect("delivery ack accepted");
        assert_eq!(upstream, None);
        assert!(f.watch.pending("c1").is_empty());
        assert!(port.reported.lock().unwrap().is_empty());
    }

    /// 批里除了送达确认还带别的回执：送达确认就地消化，上报路由的只有其余部分。
    #[tokio::test]
    async fn a_mixed_batch_forwards_only_the_parts_that_are_not_delivery_confirmations() {
        let f = fixture(DeliveryWatchConfig::default());
        f.watch
            .register("c1", device("u1"), DeviceLinkState::FOREGROUND);
        let mut targets = vec!["c1".to_string()];
        f.watch.admit(&mut targets, &[item("g1", 6), item("g2", 3)]);
        let port = Arc::new(RecordingAckPort::default());
        let service = SendAckDomainService::new(port.clone(), f.watch.clone());
        let push_ack = PushAck {
            window_id: "w1".to_string(),
            ack_seq: 3,
            ..Default::default()
        };

        let upstream = service
            .absorb_delivery_acks(&command(AckPayload::Batch(AckBatch {
                conversation_acks: vec![delivered("g1", 6), delivered("g2", 3)],
                read_acks: vec![read("g1", 6)],
                push_acks: vec![push_ack.clone()],
                device_id: Some("dev-u1".to_string()),
                ..Default::default()
            })))
            .expect("batch accepted")
            .expect("the read and push acks still go upstream");
        assert!(f.watch.pending("c1").is_empty());
        assert_eq!(
            upstream,
            AckPayload::Batch(AckBatch {
                read_acks: vec![read("g1", 6)],
                push_acks: vec![push_ack],
                device_id: Some("dev-u1".to_string()),
                ..Default::default()
            })
        );

        let ctx: Ctx = Arc::new(flare_server_core::Context::root());
        service
            .report_upstream(&ctx, upstream.clone())
            .await
            .expect("reported");
        assert_eq!(*port.reported.lock().unwrap(), vec![upstream]);
    }

    #[test]
    fn read_and_push_acks_pass_through_and_downlink_receipts_are_refused() {
        let service = SendAckDomainService::new(Arc::new(RecordingAckPort::default()), detached());
        let read_ack = AckPayload::Read(read("g1", 4));
        assert_eq!(
            service
                .absorb_delivery_acks(&command(read_ack.clone()))
                .expect("read ack accepted"),
            Some(read_ack)
        );
        let push_ack = AckPayload::Push(PushAck {
            window_id: "w1".to_string(),
            ack_seq: 3,
            ..Default::default()
        });
        assert_eq!(
            service
                .absorb_delivery_acks(&command(push_ack.clone()))
                .expect("push ack accepted"),
            Some(push_ack)
        );
        assert!(
            service
                .absorb_delivery_acks(&command(AckPayload::Send(SendAck::default())))
                .is_err()
        );
        let empty = SendAckCommand::new("c1".to_string(), Ack::default(), None);
        assert_eq!(service.absorb_delivery_acks(&empty).expect("empty"), None);
    }
}
