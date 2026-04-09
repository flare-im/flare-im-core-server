use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

mod bytes_payload_serde {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;
    use serde::de::{Error as _, SeqAccess, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S>(payload: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(payload))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct BytesVisitor;

        impl<'de> Visitor<'de> for BytesVisitor {
            type Value = Vec<u8>;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("base64 string or byte array")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                STANDARD.decode(value).map_err(E::custom)
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_str(&value)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut payload = Vec::new();
                while let Some(byte) = seq.next_element::<u64>()? {
                    if byte > u8::MAX as u64 {
                        return Err(A::Error::custom("byte value out of range"));
                    }
                    payload.push(byte as u8);
                }
                Ok(payload)
            }
        }

        deserializer.deserialize_any(BytesVisitor)
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GetFileUrlHttpResponse {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdn_url: Option<String>,
}

impl From<flare_grpc_proto::media::GetFileUrlResponse> for GetFileUrlHttpResponse {
    fn from(value: flare_grpc_proto::media::GetFileUrlResponse) -> Self {
        Self {
            url: value.url,
            cdn_url: if value.cdn_url.is_empty() {
                None
            } else {
                Some(value.cdn_url)
            },
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct FileInfoHttpResponse {
    pub file_id: String,
    pub file_name: String,
    pub mime_type: String,
    pub size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdn_url: Option<String>,
}

impl From<flare_grpc_proto::media::FileInfo> for FileInfoHttpResponse {
    fn from(value: flare_grpc_proto::media::FileInfo) -> Self {
        Self {
            file_id: value.file_id,
            file_name: value.file_name,
            mime_type: value.mime_type,
            size: value.size,
            url: if value.url.is_empty() { None } else { Some(value.url) },
            cdn_url: if value.cdn_url.is_empty() {
                None
            } else {
                Some(value.cdn_url)
            },
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CreateReferenceHttpResponse {
    pub success: bool,
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<FileInfoHttpResponse>,
}

impl From<flare_grpc_proto::media::CreateReferenceResponse> for CreateReferenceHttpResponse {
    fn from(value: flare_grpc_proto::media::CreateReferenceResponse) -> Self {
        Self {
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
            info: value.info.map(FileInfoHttpResponse::from),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct DeleteReferenceHttpResponse {
    pub success: bool,
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<FileInfoHttpResponse>,
}

impl From<flare_grpc_proto::media::DeleteReferenceResponse> for DeleteReferenceHttpResponse {
    fn from(value: flare_grpc_proto::media::DeleteReferenceResponse) -> Self {
        Self {
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
            info: value.info.map(FileInfoHttpResponse::from),
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MediaReferenceHttpResponse {
    pub reference_id: String,
    pub file_id: String,
    pub namespace: String,
    pub owner_id: String,
    pub business_tag: String,
}

impl From<flare_grpc_proto::media::MediaReferenceInfo> for MediaReferenceHttpResponse {
    fn from(value: flare_grpc_proto::media::MediaReferenceInfo) -> Self {
        Self {
            reference_id: value.reference_id,
            file_id: value.file_id,
            namespace: value.namespace,
            owner_id: value.owner_id,
            business_tag: value.business_tag,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListReferencesHttpResponse {
    pub references: Vec<MediaReferenceHttpResponse>,
    pub success: bool,
    pub error_message: Option<String>,
}

impl From<flare_grpc_proto::media::ListReferencesResponse> for ListReferencesHttpResponse {
    fn from(value: flare_grpc_proto::media::ListReferencesResponse) -> Self {
        Self {
            references: value
                .references
                .into_iter()
                .map(MediaReferenceHttpResponse::from)
                .collect(),
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListObjectsHttpResponse {
    pub files: Vec<FileInfoHttpResponse>,
}

impl From<flare_grpc_proto::media::ListObjectsResponse> for ListObjectsHttpResponse {
    fn from(value: flare_grpc_proto::media::ListObjectsResponse) -> Self {
        Self {
            files: value
                .files
                .into_iter()
                .map(FileInfoHttpResponse::from)
                .collect(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct UploadFileMetadataHttp {
    pub file_name: String,
    pub mime_type: String,
    pub file_size: i64,
    pub file_type: i32,
    pub upload_id: String,
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub user_id: String,
    #[serde(default)]
    pub trace_id: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub business_tag: String,
    #[serde(default)]
    pub bucket: String,
    #[serde(default)]
    pub object_key: String,
    #[serde(default)]
    pub labels: std::collections::HashMap<String, String>,
}

impl From<UploadFileMetadataHttp> for flare_grpc_proto::media::UploadFileMetadata {
    fn from(value: UploadFileMetadataHttp) -> Self {
        Self {
            file_name: value.file_name,
            mime_type: value.mime_type,
            file_size: value.file_size,
            file_type: value.file_type,
            upload_id: value.upload_id,
            metadata: value.metadata,
            user_id: value.user_id,
            trace_id: value.trace_id,
            namespace: value.namespace,
            business_tag: value.business_tag,
            bucket: value.bucket,
            object_key: value.object_key,
            labels: value.labels,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct UploadFileHttpRequest {
    pub metadata: UploadFileMetadataHttp,
    #[serde(with = "bytes_payload_serde")]
    pub payload: Vec<u8>,
    #[serde(default)]
    pub chunk_size: usize,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UploadFileHttpResponse {
    pub file_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdn_url: Option<String>,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub info: Option<FileInfoHttpResponse>,
}

impl From<flare_grpc_proto::media::UploadFileResponse> for UploadFileHttpResponse {
    fn from(value: flare_grpc_proto::media::UploadFileResponse) -> Self {
        Self {
            file_id: value.file_id,
            url: if value.url.is_empty() { None } else { Some(value.url) },
            cdn_url: if value.cdn_url.is_empty() {
                None
            } else {
                Some(value.cdn_url)
            },
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
            info: value.info.map(FileInfoHttpResponse::from),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct InitiateMultipartUploadHttpRequest {
    pub metadata: UploadFileMetadataHttp,
    pub desired_chunk_size: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InitiateMultipartUploadHttpResponse {
    pub upload_id: String,
    pub chunk_size: i64,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl From<flare_grpc_proto::media::InitiateMultipartUploadResponse>
    for InitiateMultipartUploadHttpResponse
{
    fn from(value: flare_grpc_proto::media::InitiateMultipartUploadResponse) -> Self {
        Self {
            upload_id: value.upload_id,
            chunk_size: value.chunk_size,
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct UploadMultipartChunkHttpRequest {
    pub upload_id: String,
    pub chunk_index: u32,
    #[serde(with = "bytes_payload_serde")]
    pub payload: Vec<u8>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct UploadMultipartChunkHttpResponse {
    pub upload_id: String,
    pub chunk_index: u32,
    pub uploaded_size: u64,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl From<flare_grpc_proto::media::UploadMultipartChunkResponse>
    for UploadMultipartChunkHttpResponse
{
    fn from(value: flare_grpc_proto::media::UploadMultipartChunkResponse) -> Self {
        Self {
            upload_id: value.upload_id,
            chunk_index: value.chunk_index,
            uploaded_size: value.uploaded_size,
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct CompleteMultipartUploadHttpRequest {
    pub upload_id: String,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct AbortMultipartUploadHttpRequest {
    pub upload_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AbortMultipartUploadHttpResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl From<flare_grpc_proto::media::AbortMultipartUploadResponse> for AbortMultipartUploadHttpResponse {
    fn from(value: flare_grpc_proto::media::AbortMultipartUploadResponse) -> Self {
        Self {
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DirectUploadTransportKindHttp {
    SinglePut,
    MultipartPut,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct InitiateDirectUploadHttpRequest {
    pub metadata: UploadFileMetadataHttp,
    #[serde(default)]
    pub desired_part_size: i64,
    #[serde(default)]
    pub file_fingerprint: String,
    #[serde(default)]
    pub head_tail_sha256: String,
    #[serde(default)]
    pub full_sha256: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InitiateDirectUploadHttpResponse {
    pub upload_id: String,
    pub file_id: String,
    pub transport_kind: DirectUploadTransportKindHttp,
    pub bucket: String,
    pub object_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_upload_id: Option<String>,
    pub part_size: i64,
    pub total_parts: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_url: Option<String>,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl From<flare_grpc_proto::media::InitiateDirectUploadResponse> for InitiateDirectUploadHttpResponse {
    fn from(value: flare_grpc_proto::media::InitiateDirectUploadResponse) -> Self {
        Self {
            upload_id: value.upload_id,
            file_id: value.file_id,
            transport_kind: match flare_grpc_proto::media::DirectUploadTransportKind::try_from(
                value.transport_kind,
            )
            .unwrap_or(flare_grpc_proto::media::DirectUploadTransportKind::SinglePut)
            {
                flare_grpc_proto::media::DirectUploadTransportKind::SinglePut => {
                    DirectUploadTransportKindHttp::SinglePut
                }
                flare_grpc_proto::media::DirectUploadTransportKind::MultipartPut
                | flare_grpc_proto::media::DirectUploadTransportKind::Unspecified => {
                    DirectUploadTransportKindHttp::MultipartPut
                }
            },
            bucket: value.bucket,
            object_key: value.object_key,
            storage_upload_id: if value.storage_upload_id.is_empty() {
                None
            } else {
                Some(value.storage_upload_id)
            },
            part_size: value.part_size,
            total_parts: value.total_parts,
            upload_url: if value.upload_url.is_empty() {
                None
            } else {
                Some(value.upload_url)
            },
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct GetDirectUploadStatusHttpRequest {
    pub upload_id: String,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct UploadedPartInfoHttp {
    pub part_number: u32,
    pub etag: String,
    pub size: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

impl From<flare_grpc_proto::media::UploadedPartInfo> for UploadedPartInfoHttp {
    fn from(value: flare_grpc_proto::media::UploadedPartInfo) -> Self {
        Self {
            part_number: value.part_number,
            etag: value.etag,
            size: value.size,
            sha256: if value.sha256.is_empty() {
                None
            } else {
                Some(value.sha256)
            },
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GetDirectUploadStatusHttpResponse {
    pub upload_id: String,
    pub file_id: String,
    pub transport_kind: DirectUploadTransportKindHttp,
    pub bucket: String,
    pub object_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_upload_id: Option<String>,
    pub part_size: i64,
    pub total_size: i64,
    pub total_parts: u32,
    pub uploaded_parts: Vec<UploadedPartInfoHttp>,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl From<flare_grpc_proto::media::GetDirectUploadStatusResponse> for GetDirectUploadStatusHttpResponse {
    fn from(value: flare_grpc_proto::media::GetDirectUploadStatusResponse) -> Self {
        Self {
            upload_id: value.upload_id,
            file_id: value.file_id,
            transport_kind: match flare_grpc_proto::media::DirectUploadTransportKind::try_from(
                value.transport_kind,
            )
            .unwrap_or(flare_grpc_proto::media::DirectUploadTransportKind::SinglePut)
            {
                flare_grpc_proto::media::DirectUploadTransportKind::SinglePut => {
                    DirectUploadTransportKindHttp::SinglePut
                }
                flare_grpc_proto::media::DirectUploadTransportKind::MultipartPut
                | flare_grpc_proto::media::DirectUploadTransportKind::Unspecified => {
                    DirectUploadTransportKindHttp::MultipartPut
                }
            },
            bucket: value.bucket,
            object_key: value.object_key,
            storage_upload_id: if value.storage_upload_id.is_empty() {
                None
            } else {
                Some(value.storage_upload_id)
            },
            part_size: value.part_size,
            total_size: value.total_size,
            total_parts: value.total_parts,
            uploaded_parts: value
                .uploaded_parts
                .into_iter()
                .map(UploadedPartInfoHttp::from)
                .collect(),
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct PresignDirectUploadPartsHttpRequest {
    pub upload_id: String,
    pub part_numbers: Vec<u32>,
    #[serde(default)]
    pub expires_in: i32,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PresignedUploadPartHttp {
    pub part_number: u32,
    pub upload_url: String,
    pub headers: std::collections::HashMap<String, String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct PresignDirectUploadPartsHttpResponse {
    pub parts: Vec<PresignedUploadPartHttp>,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl From<flare_grpc_proto::media::PresignDirectUploadPartsResponse>
    for PresignDirectUploadPartsHttpResponse
{
    fn from(value: flare_grpc_proto::media::PresignDirectUploadPartsResponse) -> Self {
        Self {
            parts: value
                .parts
                .into_iter()
                .map(|part| PresignedUploadPartHttp {
                    part_number: part.part_number,
                    upload_url: part.upload_url,
                    headers: part.headers,
                })
                .collect(),
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct CommitDirectUploadPartsHttpRequest {
    pub upload_id: String,
    pub parts: Vec<UploadedPartInfoHttp>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct CommitDirectUploadPartsHttpResponse {
    pub committed_parts: Vec<u32>,
    pub uploaded_size: i64,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl From<flare_grpc_proto::media::CommitDirectUploadPartsResponse>
    for CommitDirectUploadPartsHttpResponse
{
    fn from(value: flare_grpc_proto::media::CommitDirectUploadPartsResponse) -> Self {
        Self {
            committed_parts: value.committed_parts,
            uploaded_size: value.uploaded_size,
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct CompleteDirectUploadHttpRequest {
    pub upload_id: String,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct AbortDirectUploadHttpRequest {
    pub upload_id: String,
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ImageOperationHttp {
    Resize { width: i32, height: i32, keep_aspect_ratio: bool },
    Compress { quality: i32 },
    Thumbnail { size: i32 },
    Watermark { text: String, position: i32 },
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct ProcessImageHttpRequest {
    pub file_id: String,
    #[serde(default)]
    pub operations: Vec<ImageOperationHttp>,
    #[serde(default)]
    pub target_bucket: String,
    #[serde(default)]
    pub output_prefix: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProcessImageHttpResponse {
    pub processed_file_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdn_url: Option<String>,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl From<flare_grpc_proto::media::ProcessImageResponse> for ProcessImageHttpResponse {
    fn from(value: flare_grpc_proto::media::ProcessImageResponse) -> Self {
        Self {
            processed_file_id: value.processed_file_id,
            url: if value.url.is_empty() { None } else { Some(value.url) },
            cdn_url: if value.cdn_url.is_empty() {
                None
            } else {
                Some(value.cdn_url)
            },
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
        }
    }
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum VideoOperationHttp {
    Transcode {
        format: String,
        quality: String,
        target_bitrate_kbps: i32,
        max_width: i32,
    },
    ExtractThumbnail { time: f64 },
    Compress { bitrate: i32, preset: String },
    SubtitleBurn { subtitle_file_id: String, language: String },
}

#[derive(Debug, Deserialize, Serialize, ToSchema)]
pub struct ProcessVideoHttpRequest {
    pub file_id: String,
    #[serde(default)]
    pub operations: Vec<VideoOperationHttp>,
    #[serde(default)]
    pub target_bucket: String,
    #[serde(default)]
    pub output_prefix: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ProcessVideoHttpResponse {
    pub processed_file_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cdn_url: Option<String>,
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl From<flare_grpc_proto::media::ProcessVideoResponse> for ProcessVideoHttpResponse {
    fn from(value: flare_grpc_proto::media::ProcessVideoResponse) -> Self {
        Self {
            processed_file_id: value.processed_file_id,
            url: if value.url.is_empty() { None } else { Some(value.url) },
            cdn_url: if value.cdn_url.is_empty() {
                None
            } else {
                Some(value.cdn_url)
            },
            success: value.success,
            error_message: if value.error_message.is_empty() {
                None
            } else {
                Some(value.error_message)
            },
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct SendMessageHttpRequest {
    pub conversation_id: String,
    pub content: serde_json::Value,
    pub message_type: i32,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SendMessageHttpResponse {
    pub server_msg_id: String,
    pub seq: u64,
    pub success: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct RecallMessageHttpRequest {
    pub conversation_id: String,
    pub message_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RecallMessageHttpResponse {
    pub success: bool,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct MarkReadHttpRequest {
    pub conversation_id: String,
    pub message_id: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MarkReadHttpResponse {
    pub success: bool,
}
