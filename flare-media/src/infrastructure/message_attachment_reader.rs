//! Bounded authoritative read under the original verified identity. User reads
//! keep participation/history checks; Staff inspection uses Reader's admin query
//! with the exact signed messages:read permission, never an impersonated User.
use crate::domain::repository::MessageAttachmentReader;
use flare_grpc_proto::storage::{
    QueryMessagesBySeqRequest, storage_reader_service_client::StorageReaderServiceClient,
};
use flare_im_service_kit::ServiceClient;
use flare_server_core::{
    context::Context,
    error::{ErrorCode, Result},
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tonic::transport::Channel;

enum Target {
    Static(Channel),
    Discovery(String, Box<Mutex<ServiceClient>>),
}
pub struct Reader {
    target: Target,
}

impl Reader {
    pub async fn from_environment() -> Result<Arc<Self>> {
        use flare_im_contracts::service_names::{STORAGE_READER, get_service_name};
        let name = get_service_name(STORAGE_READER);
        let explicit = std::env::var("MEDIA_STORAGE_READER_SERVICE_URL")
            .ok()
            .filter(|s| !s.trim().is_empty());
        let target = if let Some(url) = explicit {
            Target::Static(
                flare_im_service_kit::discovery::connect_static_channel_lazy(url.trim()).map_err(
                    |_| {
                        flare_server_core::flare_err!(
                            ErrorCode::ConfigurationError,
                            "invalid Media Storage Reader endpoint"
                        )
                    },
                )?,
            )
        } else if let Some(discover) = flare_im_service_kit::discovery::create_discover(&name)
            .await
            .map_err(|_| {
                flare_server_core::flare_err!(
                    ErrorCode::ConfigurationError,
                    "Media Storage Reader discovery unavailable"
                )
            })?
        {
            Target::Discovery(name, Box::new(Mutex::new(ServiceClient::new(discover))))
        } else {
            Target::Static(
                flare_im_service_kit::discovery::connect_static_channel_lazy(
                    flare_im_service_kit::discovery::default_static_grpc_fallback(STORAGE_READER),
                )
                .map_err(|_| {
                    flare_server_core::flare_err!(
                        ErrorCode::ConfigurationError,
                        "invalid Media Storage Reader endpoint"
                    )
                })?,
            )
        };
        Ok(Arc::new(Self { target }))
    }
    async fn channel(&self) -> Result<Channel> {
        match &self.target {
            Target::Static(channel) => Ok(channel.clone()),
            Target::Discovery(name, client) => {
                let mut client = client.lock().await;
                flare_im_service_kit::discovery::get_discovered_channel_with_timeout(
                    name,
                    &mut client,
                )
                .await
                .map_err(|_| {
                    flare_server_core::flare_err!(
                        ErrorCode::ServiceUnavailable,
                        "message attachment reader unavailable"
                    )
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl MessageAttachmentReader for Reader {
    async fn read_visible_message(
        &self,
        ctx: &Context,
        conversation_id: &str,
        message_seq: i64,
    ) -> Result<Option<flare_proto::common::Message>> {
        if ctx
            .actor()
            .is_some_and(|actor| actor.actor_type == flare_server_core::context::ActorType::Staff)
        {
            flare_server_core::require_staff_permission(ctx, "core:messages:read").map_err(
                |_| {
                    flare_server_core::flare_err!(
                        ErrorCode::PermissionDenied,
                        "message inspection permission required"
                    )
                },
            )?;
            let call = async {
                use flare_proto::common::{FilterExpression, FilterOperator, Pagination};
                let mut request =
                    tonic::Request::new(flare_grpc_proto::storage::SearchMessagesRequest {
                        filters: [
                            ("conversation_id", conversation_id.to_owned()),
                            ("after_seq", (message_seq - 1).to_string()),
                            ("before_seq", (message_seq + 1).to_string()),
                        ]
                        .into_iter()
                        .map(|(field, value)| FilterExpression {
                            field: field.into(),
                            op: FilterOperator::Eq as i32,
                            values: vec![value],
                        })
                        .collect(),
                        pagination: Some(Pagination {
                            limit: 1,
                            ..Default::default()
                        }),
                        // Admin search otherwise defaults to the last 7 days.
                        // The exact conversation/sequence index is authoritative
                        // for older persisted attachments too; epoch + 1s avoids
                        // the Reader's zero-as-unspecified boundary.
                        time_range: Some(flare_proto::common::TimeRange {
                            start_time: Some(1_000),
                            end_time: None,
                        }),
                        ..Default::default()
                    });
                if !flare_server_core::forward_signed_context(&mut request, ctx).unwrap_or(false) {
                    return Err(flare_server_core::flare_err!(
                        ErrorCode::PermissionDenied,
                        "verified message reader context required"
                    ));
                }
                let response = StorageReaderServiceClient::new(self.channel().await?)
                    .search_messages(request)
                    .await
                    .map_err(|_| {
                        flare_server_core::flare_err!(
                            ErrorCode::ServiceUnavailable,
                            "message attachment reader unavailable"
                        )
                    })?
                    .into_inner();
                if response.messages.len() > 1 {
                    return Err(flare_server_core::flare_err!(
                        ErrorCode::ServiceUnavailable,
                        "invalid message attachment reader response"
                    ));
                }
                Ok(response.messages.into_iter().next())
            };
            return tokio::time::timeout(Duration::from_secs(3), call)
                .await
                .map_err(|_| {
                    flare_server_core::flare_err!(
                        ErrorCode::ServiceUnavailable,
                        "message attachment reader unavailable"
                    )
                })?;
        }
        let user = ctx.user_id().filter(|id| !id.is_empty()).ok_or_else(|| {
            flare_server_core::flare_err!(
                ErrorCode::PermissionDenied,
                "message attachment requires a User"
            )
        })?;
        let call = async {
            let mut request = tonic::Request::new(QueryMessagesBySeqRequest {
                conversation_id: conversation_id.into(),
                after_seq: message_seq - 1,
                before_seq: message_seq + 1,
                limit: 1,
                user_id: user.into(),
                include_burned_placeholder: false,
            });
            if !flare_server_core::forward_signed_context(&mut request, ctx).map_err(|_| {
                flare_server_core::flare_err!(
                    ErrorCode::PermissionDenied,
                    "verified message reader context required"
                )
            })? {
                return Err(flare_server_core::flare_err!(
                    ErrorCode::PermissionDenied,
                    "verified message reader context required"
                ));
            }
            let mut client = StorageReaderServiceClient::new(self.channel().await?);
            let response = client
                .query_messages_by_seq(request)
                .await
                .map_err(|_| {
                    flare_server_core::flare_err!(
                        ErrorCode::ServiceUnavailable,
                        "message attachment reader unavailable"
                    )
                })?
                .into_inner();
            if response.messages.len() > 1 {
                return Err(flare_server_core::flare_err!(
                    ErrorCode::ServiceUnavailable,
                    "invalid message attachment reader response"
                ));
            }
            Ok(response.messages.into_iter().next())
        };
        tokio::time::timeout(Duration::from_secs(3), call)
            .await
            .map_err(|_| {
                flare_server_core::flare_err!(
                    ErrorCode::ServiceUnavailable,
                    "message attachment reader unavailable"
                )
            })?
    }
}
