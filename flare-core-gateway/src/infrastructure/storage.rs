use std::sync::Arc;

// Note: Storage service client is not directly used here
// Storage operations are handled through Message Orchestrator
use flare_proto::storage::{QueryMessagesRequest, QueryMessagesResponse};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};

pub trait StorageClient: Send + Sync {
    async fn query_messages(&self, request: QueryMessagesRequest) -> Result<QueryMessagesResponse>;
}

pub struct GrpcStorageClient {
    service_name: String,
    // Note: Storage operations are handled through Message Orchestrator
    // This client is kept for backward compatibility but may not be fully implemented
}

impl GrpcStorageClient {
    pub fn new(service_name: String) -> Arc<Self> {
        Arc::new(Self { service_name })
    }
}

impl StorageClient for GrpcStorageClient {
    async fn query_messages(
        &self,
        _request: QueryMessagesRequest,
    ) -> Result<QueryMessagesResponse> {
        // Note: Query operations should go through Storage Reader Service
        Err(ErrorBuilder::new(
            ErrorCode::ServiceUnavailable,
            "query_messages should be called through Storage Reader Service",
        )
        .build_error())
    }
}
