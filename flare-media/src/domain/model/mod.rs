use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};

pub const STORAGE_PATH_METADATA_KEY: &str = "storage_path";
pub const STORAGE_BUCKET_METADATA_KEY: &str = "storage_bucket";
pub const FILE_CATEGORY_METADATA_KEY: &str = "file_category";

/// 媒体领域配置值对象（只包含领域相关的配置）
#[derive(Clone, Debug)]
pub struct MediaDomainConfig {
    /// 默认 TTL（秒）
    pub default_ttl: i64,
    /// CDN 基础 URL
    pub cdn_base_url: Option<String>,
    /// 孤儿资源宽限期（秒）
    pub orphan_grace_seconds: i64,
    /// 分块上传根目录
    pub chunk_root_dir: std::path::PathBuf,
    /// 分块 TTL（秒）
    pub chunk_ttl_seconds: i64,
    /// 最大分块大小（字节）
    pub max_chunk_size_bytes: i64,
}

impl MediaDomainConfig {
    pub fn new(
        default_ttl: i64,
        cdn_base_url: Option<String>,
        orphan_grace_seconds: i64,
        chunk_root_dir: std::path::PathBuf,
        chunk_ttl_seconds: i64,
        max_chunk_size_bytes: i64,
    ) -> Self {
        Self {
            default_ttl,
            cdn_base_url,
            orphan_grace_seconds,
            chunk_root_dir,
            chunk_ttl_seconds,
            max_chunk_size_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaAssetStatus {
    Pending,
    Active,
    SoftDeleted,
}

impl MediaAssetStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            MediaAssetStatus::Pending => "pending",
            MediaAssetStatus::Active => "active",
            MediaAssetStatus::SoftDeleted => "soft_deleted",
        }
    }
}

impl FromStr for MediaAssetStatus {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "pending" => Ok(MediaAssetStatus::Pending),
            "active" => Ok(MediaAssetStatus::Active),
            "soft_deleted" => Ok(MediaAssetStatus::SoftDeleted),
            _ => Err(()),
        }
    }
}

impl Default for MediaAssetStatus {
    fn default() -> Self {
        MediaAssetStatus::Pending
    }
}

/// 文件访问类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileAccessType {
    /// 公开文件 - 可直接访问
    Public,
    /// 私有文件 - 需要授权访问
    Private,
}

impl Default for FileAccessType {
    fn default() -> Self {
        FileAccessType::Private
    }
}

#[derive(Debug, Clone)]
pub struct MediaFileMetadata {
    pub file_id: String,
    pub file_name: String,
    pub mime_type: String,
    pub file_size: i64,
    pub url: String,
    pub cdn_url: String,
    pub md5: Option<String>,
    pub sha256: Option<String>,
    pub metadata: HashMap<String, String>,
    pub uploaded_at: DateTime<Utc>,
    pub reference_count: u64,
    pub status: MediaAssetStatus,
    pub grace_expires_at: Option<DateTime<Utc>>,
    /// 文件访问类型
    pub access_type: FileAccessType,
    pub storage_bucket: Option<String>,
    pub storage_path: Option<String>,
}

impl Default for MediaFileMetadata {
    fn default() -> Self {
        Self {
            file_id: String::new(),
            file_name: String::new(),
            mime_type: String::new(),
            file_size: 0,
            url: String::new(),
            cdn_url: String::new(),
            md5: None,
            sha256: None,
            metadata: HashMap::new(),
            uploaded_at: Utc::now(),
            reference_count: 0,
            status: MediaAssetStatus::default(),
            grace_expires_at: None,
            access_type: FileAccessType::default(),
            storage_bucket: None,
            storage_path: None,
        }
    }
}

impl MediaFileMetadata {
    pub fn storage_bucket(&self) -> Option<&str> {
        self.storage_bucket.as_deref().or_else(|| {
            self.metadata
                .get(STORAGE_BUCKET_METADATA_KEY)
                .map(|s| s.as_str())
        })
    }

    pub fn storage_path(&self) -> Option<&str> {
        self.storage_path.as_deref().or_else(|| {
            self.metadata
                .get(STORAGE_PATH_METADATA_KEY)
                .map(|s| s.as_str())
        })
    }
}

#[derive(Debug, Clone)]
pub struct PresignedUrl {
    pub url: String,
    pub cdn_url: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DirectUploadTransportKind {
    SinglePut,
    MultipartPut,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, Default)]
pub struct UploadedPartRecord {
    pub part_number: u32,
    pub etag: String,
    pub size: i64,
    #[serde(default)]
    pub sha256: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DirectUploadSessionState {
    pub upload_id: String,
    pub file_id: String,
    pub transport_kind: DirectUploadTransportKind,
    pub bucket: String,
    pub object_key: String,
    pub storage_upload_id: Option<String>,
    pub part_size: i64,
    pub total_size: i64,
    pub total_parts: u32,
    pub upload_url: Option<String>,
    pub uploaded_parts: Vec<UploadedPartRecord>,
    pub uploaded_size: i64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default)]
pub struct ObjectStat {
    pub size: Option<i64>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct PresignedUploadPartUrl {
    pub part_number: u32,
    pub upload_url: String,
    pub headers: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct UploadContext<'a> {
    pub file_id: &'a str,
    pub file_name: &'a str,
    pub mime_type: &'a str,
    pub file_size: i64,
    pub payload: &'a [u8],
    pub file_category: String,
    pub user_id: &'a str,
    pub trace_id: Option<&'a str>,
    pub namespace: Option<&'a str>,
    pub business_tag: Option<&'a str>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct MediaReference {
    pub reference_id: String,
    pub file_id: String,
    pub namespace: String,
    pub owner_id: String,
    pub business_tag: Option<String>,
    pub metadata: HashMap<String, String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct MediaReferenceScope {
    pub namespace: String,
    pub owner_id: String,
    pub business_tag: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UploadSessionStatus {
    Pending,
    Completed,
    Aborted,
}

impl Default for UploadSessionStatus {
    fn default() -> Self {
        UploadSessionStatus::Pending
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UploadSession {
    pub upload_id: String,
    #[serde(default)]
    pub file_id: Option<String>,
    pub file_name: String,
    pub mime_type: String,
    pub file_type: String,
    pub chunk_size: i64,
    pub total_size: Option<i64>,
    pub uploaded_size: i64,
    #[serde(default)]
    pub uploaded_chunks: Vec<u32>,
    #[serde(default)]
    pub uploaded_parts: Vec<UploadedPartRecord>,
    #[serde(default)]
    pub transport_kind: Option<DirectUploadTransportKind>,
    #[serde(default)]
    pub storage_upload_id: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    #[serde(default)]
    pub object_key: Option<String>,
    #[serde(default)]
    pub single_part_upload_url: Option<String>,
    #[serde(default)]
    pub file_fingerprint: Option<String>,
    #[serde(default)]
    pub head_tail_sha256: Option<String>,
    #[serde(default)]
    pub full_sha256: Option<String>,
    #[serde(default)]
    pub total_parts: Option<u32>,
    pub user_id: String,
    pub namespace: Option<String>,
    pub business_tag: Option<String>,
    pub trace_id: Option<String>,
    pub metadata: HashMap<String, String>,
    pub status: UploadSessionStatus,
    pub expires_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl UploadSession {
    pub fn has_chunk(&self, index: u32) -> bool {
        self.uploaded_chunks.iter().any(|chunk| *chunk == index)
    }

    pub fn add_chunk(&mut self, index: u32, size: i64) {
        if !self.has_chunk(index) {
            self.uploaded_chunks.push(index);
            self.uploaded_size += size;
        }
    }

    pub fn merge_uploaded_part(&mut self, part: UploadedPartRecord) {
        if let Some(existing) = self
            .uploaded_parts
            .iter_mut()
            .find(|item| item.part_number == part.part_number)
        {
            *existing = part;
        } else {
            self.uploaded_parts.push(part);
        }
        self.uploaded_parts.sort_by_key(|item| item.part_number);
        self.uploaded_chunks = self
            .uploaded_parts
            .iter()
            .map(|item| item.part_number)
            .collect();
        self.uploaded_size = self.uploaded_parts.iter().map(|item| item.size).sum();
    }
}

#[derive(Debug, Clone)]
pub struct MultipartUploadInit {
    pub file_name: String,
    pub mime_type: String,
    pub file_size: Option<i64>,
    pub file_type: String,
    pub chunk_size: i64,
    pub user_id: String,
    pub namespace: Option<String>,
    pub business_tag: Option<String>,
    pub trace_id: Option<String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct MultipartUploadSession {
    pub upload_id: String,
    pub chunk_size: i64,
    pub uploaded_size: i64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct MultipartChunkPayload {
    pub upload_id: String,
    pub chunk_index: u32,
    pub bytes: Vec<u8>,
}

pub fn infer_file_category(file_type_hint: Option<&str>, mime_type: &str) -> String {
    let normalized_hint = file_type_hint
        .map(|value| value.trim().to_ascii_lowercase())
        .unwrap_or_default();

    let category = match normalized_hint.as_str() {
        "file_type_image" | "image" | "images" => "images",
        "file_type_video" | "video" | "videos" => "videos",
        "file_type_audio" | "audio" | "audios" => "audio",
        "file_type_document" | "document" | "documents" => "documents",
        "file_type_other" | "other" | "others" => "others",
        _ => match mime_type.split('/').next().unwrap_or_default() {
            "image" => "images",
            "video" => "videos",
            "audio" => "audio",
            "text" | "application" => "documents",
            _ => "others",
        },
    };

    category.to_string()
}

/// 媒体处理操作
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MediaOperation {
    /// 操作类型
    #[serde(rename = "type")]
    pub r#type: String,

    /// 操作参数
    pub parameters: std::collections::HashMap<String, serde_json::Value>,

    /// 输出格式
    pub output_format: Option<String>,

    /// 质量参数
    pub quality: Option<String>,
}

/// 媒体处理结果
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MediaProcessingResult {
    /// 操作类型
    pub operation_type: String,

    /// 输出URL
    pub output_url: String,

    /// 输出文件大小（字节）
    pub output_size: Option<u64>,

    /// 处理耗时（毫秒）
    pub processing_time_ms: u64,

    /// 是否成功
    pub success: bool,

    /// 错误消息
    pub error_message: Option<String>,
}
