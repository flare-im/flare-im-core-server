//! Storage + Sync gRPC 客户端池

use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::sync::sync_service_client::SyncServiceClient;
use flare_im_core::discovery::connect_grpc_channel_resilient;
use flare_im_core::service_names::{STORAGE_READER, SYNC_ORCHESTRATOR, get_service_name};
use flare_server_core::error::{ErrorBuilder, ErrorCode, Result};
use tokio::sync::Mutex;
use tonic::transport::Channel;

const DEFAULT_STORAGE_READER_URI: &str = "http://127.0.0.1:60083";
const DEFAULT_SYNC_ORCHESTRATOR_URI: &str = "http://127.0.0.1:60084";

pub struct StorageSyncGrpcPool {
    storage_service_name: String,
    sync_service_name: String,
    storage_reader_grpc: Mutex<Option<StorageReaderServiceClient<Channel>>>,
    sync_grpc: Mutex<Option<SyncServiceClient<Channel>>>,
}

impl StorageSyncGrpcPool {
    pub fn new() -> Self {
        Self {
            storage_service_name: get_service_name(STORAGE_READER),
            sync_service_name: get_service_name(SYNC_ORCHESTRATOR),
            storage_reader_grpc: Mutex::new(None),
            sync_grpc: Mutex::new(None),
        }
    }

    pub async fn ensure_storage_reader_client(
        &self,
    ) -> Result<StorageReaderServiceClient<Channel>> {
        let mut guard = self.storage_reader_grpc.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }

        let channel =
            connect_grpc_channel_resilient(&self.storage_service_name, DEFAULT_STORAGE_READER_URI)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(ErrorCode::ServiceUnavailable, "storage reader unavailable")
                        .details(e)
                        .build_error()
                })?;

        let client = StorageReaderServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }

    pub async fn ensure_sync_client(&self) -> Result<SyncServiceClient<Channel>> {
        let mut guard = self.sync_grpc.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }

        let channel =
            connect_grpc_channel_resilient(&self.sync_service_name, DEFAULT_SYNC_ORCHESTRATOR_URI)
                .await
                .map_err(|e| {
                    ErrorBuilder::new(
                        ErrorCode::ServiceUnavailable,
                        "sync orchestrator unavailable",
                    )
                    .details(e)
                    .build_error()
                })?;

        let client = SyncServiceClient::new(channel);
        *guard = Some(client.clone());
        Ok(client)
    }
}

impl Default for StorageSyncGrpcPool {
    fn default() -> Self {
        Self::new()
    }
}
