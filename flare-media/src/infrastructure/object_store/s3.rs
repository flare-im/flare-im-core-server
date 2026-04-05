use std::sync::Arc;
use std::time::Duration;

use crate::error::{ErrorCode, Result, map_infra_error};
use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use chrono::{Datelike, Utc};

use crate::domain::model::UploadContext;
use crate::domain::repository::MediaObjectRepository;
use flare_im_core::config::ObjectStoreConfig;

#[derive(Clone)]
pub struct S3ObjectStore {
    client: S3Client,
    bucket: String,
    base_url: Option<String>,
    cdn_base_url: Option<String>,
    upload_prefix: Option<String>,
    bucket_root_prefix: Option<String>,
    #[allow(dead_code)] // 用于构建 base_url，虽然字段本身未直接读取，但在 from_config 中使用
    force_path_style: bool,
    presign_url_ttl_seconds: i64, // 预签名URL过期时间（秒）
    use_presign: bool,
}

impl S3ObjectStore {
    pub async fn from_config(cfg: &ObjectStoreConfig) -> Result<Self> {
        let bucket = cfg
            .bucket
            .clone()
            .ok_or_else(|| {
                flare_server_core::flare_err!(
                    ErrorCode::ConfigurationError,
                    "object storage bucket is required"
                )
            })?;

        let region_name = cfg
            .region
            .clone()
            .unwrap_or_else(|| "us-east-1".to_string());
        let region = Region::new(region_name.clone());

        // Build base AWS config
        let region_provider = RegionProviderChain::first_try(region.clone());
        let mut loader = aws_config::defaults(BehaviorVersion::latest()).region(region_provider);

        // If endpoint is provided, we are likely using an S3-compatible store (e.g. MinIO)
        let endpoint = cfg.endpoint.clone();
        let force_path_style = cfg.force_path_style.unwrap_or_else(|| endpoint.is_some());

        // Credentials (access_key/secret_key), if provided, use static
        let aws_cfg = if let (Some(access_key), Some(secret_key)) =
            (cfg.access_key.clone(), cfg.secret_key.clone())
        {
            let credentials =
                Credentials::new(access_key, secret_key, None, None, "static-credentials");
            loader = loader.credentials_provider(credentials);
            loader.load().await
        } else {
            loader.load().await
        };

        // Build S3 client config
        let mut s3_builder = S3ConfigBuilder::from(&aws_cfg).region(region.clone());
        if let Some(ep) = endpoint.clone() {
            s3_builder = s3_builder.endpoint_url(ep);
        }
        if force_path_style {
            s3_builder = s3_builder.force_path_style(true);
        }
        let s3_config = s3_builder.build();
        let client = S3Client::from_conf(s3_config);

        // 配置中的预签名TTL（秒），默认3600
        let presign_url_ttl_seconds = cfg.presign_url_ttl_seconds.unwrap_or(3600) as i64;

        let use_presign = cfg.use_presign.unwrap_or(true);
        let bucket_root_prefix = normalize_prefix(&cfg.bucket_root_prefix);
        let upload_prefix = normalize_prefix(&cfg.upload_prefix);

        let base_url = match endpoint {
            Some(ep) => {
                let trimmed = ep.trim_end_matches('/');
                let url = if force_path_style {
                    format!("{}/{}", trimmed, bucket)
                } else {
                    trimmed.to_string()
                };
                Some(url)
            }
            None => Some(format!(
                "https://{}.s3.{}.amazonaws.com",
                bucket, region_name
            )),
        };

        Ok(Self {
            client,
            bucket,
            base_url,
            cdn_base_url: cfg.cdn_base_url.clone(),
            upload_prefix,
            bucket_root_prefix,
            force_path_style,
            presign_url_ttl_seconds,
            use_presign,
        })
    }

    fn build_object_key(&self, context: &UploadContext<'_>) -> String {
        let mut segments: Vec<String> = Vec::with_capacity(6);

        if let Some(prefix) = &self.bucket_root_prefix {
            segments.push(prefix.clone());
        }
        if let Some(prefix) = &self.upload_prefix {
            segments.push(prefix.clone());
        }

        let category_segment = sanitize_segment(&context.file_category);
        if category_segment.is_empty() {
            segments.push("others".to_string());
        } else {
            segments.push(category_segment);
        }

        let now = Utc::now();
        segments.push(format!("{:04}", now.year()));
        segments.push(format!("{:02}", now.month()));
        segments.push(format!("{:02}", now.day()));

        segments.push(self.build_object_name(context));

        segments.join("/")
    }

    fn build_object_name(&self, context: &UploadContext<'_>) -> String {
        if let Some(extension) = extract_extension(context.file_name) {
            format!("{}{}", context.file_id, extension)
        } else {
            context.file_id.to_string()
        }
    }

    // 提供生成预签名GET URL的方法
    pub async fn presign_get_url(
        &self,
        object_path: &str,
        expires_in: Option<i64>,
    ) -> Result<String> {
        let key = object_path.to_string();
        let expires_in = expires_in.unwrap_or(self.presign_url_ttl_seconds);

        tracing::debug!(
            key = &key,
            bucket = &self.bucket,
            expires_in = expires_in,
            "开始生成S3对象的预签名GET URL"
        );

        // 生成预签名GET URL
        let presigned = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .presigned(
                aws_sdk_s3::presigning::PresigningConfig::expires_in(Duration::from_secs(
                    (expires_in.max(1) as u64).min(7 * 24 * 3600), // 最大7天
                ))
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::ConfigurationError, "invalid presign config")
                })?,
            )
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    format!("failed to presign s3 get url, key={}", key),
                )
            })?;

        let url = presigned.uri().to_string();
        tracing::debug!(key = &key, presigned_url = &url, "已生成预签名GET URL");
        Ok(url)
    }
}

fn normalize_prefix(prefix: &Option<String>) -> Option<String> {
    prefix.as_ref().and_then(|value| {
        let trimmed = value.trim_matches('/');
        if trimmed.is_empty() {
            None
        } else {
            let sanitized_segments: Vec<String> = trimmed
                .split('/')
                .filter_map(|segment| {
                    if segment.is_empty() {
                        None
                    } else {
                        let sanitized = sanitize_segment(segment);
                        if sanitized.is_empty() {
                            None
                        } else {
                            Some(sanitized)
                        }
                    }
                })
                .collect();
            if sanitized_segments.is_empty() {
                None
            } else {
                Some(sanitized_segments.join("/"))
            }
        }
    })
}

fn sanitize_segment(segment: &str) -> String {
    let trimmed = segment.trim_matches('/');
    let mut sanitized = String::with_capacity(trimmed.len());

    for ch in trimmed.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_lowercase() || lower.is_ascii_digit() || lower == '-' || lower == '_' {
            sanitized.push(lower);
        } else {
            sanitized.push('-');
        }
    }

    sanitized.trim_matches('-').to_string()
}

fn extract_extension(file_name: &str) -> Option<String> {
    let trimmed = file_name.trim();
    let dot_index = trimmed.rfind('.')?;
    if dot_index == trimmed.len() - 1 {
        return None;
    }
    if trimmed[dot_index + 1..].contains(['/', '\\']) {
        return None;
    }
    Some(trimmed[dot_index..].to_ascii_lowercase())
}

#[async_trait::async_trait]
impl MediaObjectRepository for S3ObjectStore {
    async fn put_object(&self, context: &UploadContext<'_>) -> Result<String> {
        let key = self.build_object_key(context);
        tracing::debug!(
            file_id = context.file_id,
            key = &key,
            bucket = &self.bucket,
            file_size = context.payload.len(),
            "开始上传对象到S3存储"
        );

        let bs = ByteStream::from(context.payload.to_vec());
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .content_type(context.mime_type)
            .body(bs)
            .send()
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    format!("failed to upload object to s3, key={}", key),
                )
            })?;

        tracing::debug!(
            file_id = context.file_id,
            key = &key,
            bucket = &self.bucket,
            "对象已成功上传到S3存储"
        );
        Ok(key)
    }

    async fn delete_object(&self, object_path: &str) -> Result<()> {
        tracing::debug!(
            key = object_path,
            bucket = &self.bucket,
            "开始从S3存储删除对象"
        );

        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(object_path)
            .send()
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    format!("failed to delete object from s3, key={}", object_path),
                )
            })?;

        tracing::debug!(
            key = object_path,
            bucket = &self.bucket,
            "对象已成功从S3存储删除"
        );
        Ok(())
    }

    async fn presign_object(&self, object_path: &str, expires_in: i64) -> Result<String> {
        tracing::debug!(
            key = object_path,
            bucket = &self.bucket,
            expires_in = expires_in,
            "开始生成S3对象的预签名URL"
        );

        let presigned = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(object_path)
            .presigned(
                aws_sdk_s3::presigning::PresigningConfig::expires_in(Duration::from_secs(
                    (expires_in.max(1) as u64).min(7 * 24 * 3600),
                ))
                .map_err(|e| {
                    map_infra_error(e, ErrorCode::ConfigurationError, "invalid presign config")
                })?,
            )
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    format!("failed to presign s3 url, key={}", object_path),
                )
            })?;

        let url = presigned.uri().to_string();
        tracing::debug!(key = object_path, presigned_url = &url, "已生成预签名URL");
        Ok(url)
    }

    fn base_url(&self) -> Option<String> {
        self.base_url.clone()
    }

    fn cdn_base_url(&self) -> Option<String> {
        self.cdn_base_url.clone()
    }

    fn use_presigned_urls(&self) -> bool {
        self.use_presign
    }

    fn bucket_name(&self) -> Option<String> {
        Some(self.bucket.clone())
    }

    fn storage_provider(&self) -> Option<String> {
        Some("s3".to_string())
    }
}

pub type S3ObjectStoreRef = Arc<S3ObjectStore>;
