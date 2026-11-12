//! Storage + Sync gRPC 客户端池

use flare_grpc_proto::storage::storage_reader_service_client::StorageReaderServiceClient;
use flare_grpc_proto::sync::sync_service_client::SyncServiceClient;
use flare_im_contracts::service_names::{STORAGE_READER, SYNC_ORCHESTRATOR, get_service_name};
use flare_im_service_kit::discovery::{connect_grpc_channel_resilient, connect_static_channel};
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
    static_storage_endpoint: Option<String>,
    static_sync_endpoint: Option<String>,
}

impl StorageSyncGrpcPool {
    pub fn new() -> Self {
        Self::new_with_service_names(
            get_service_name(STORAGE_READER),
            get_service_name(SYNC_ORCHESTRATOR),
        )
    }

    pub(super) fn new_with_service_names(
        storage_service_name: String,
        sync_service_name: String,
    ) -> Self {
        Self {
            storage_service_name,
            sync_service_name,
            storage_reader_grpc: Mutex::new(None),
            sync_grpc: Mutex::new(None),
            static_storage_endpoint: None,
            static_sync_endpoint: None,
        }
    }

    pub(crate) fn with_static_endpoints(
        mut self,
        storage_reader: Option<String>,
        sync: Option<String>,
    ) -> Self {
        self.static_storage_endpoint = storage_reader;
        self.static_sync_endpoint = sync;
        self
    }

    pub async fn ensure_storage_reader_client(
        &self,
    ) -> Result<StorageReaderServiceClient<Channel>> {
        let mut guard = self.storage_reader_grpc.lock().await;
        if let Some(c) = guard.as_ref() {
            return Ok(c.clone());
        }

        let channel = match self.static_storage_endpoint.as_deref() {
            Some(endpoint) => connect_static_channel(endpoint).await,
            None => {
                connect_grpc_channel_resilient(
                    &self.storage_service_name,
                    DEFAULT_STORAGE_READER_URI,
                )
                .await
            }
        }
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

        let channel = match self.static_sync_endpoint.as_deref() {
            Some(endpoint) => connect_static_channel(endpoint).await,
            None => {
                connect_grpc_channel_resilient(
                    &self.sync_service_name,
                    DEFAULT_SYNC_ORCHESTRATOR_URI,
                )
                .await
            }
        }
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
