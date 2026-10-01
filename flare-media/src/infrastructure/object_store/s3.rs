use std::sync::Arc;
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    BucketLocationConstraint, CompletedMultipartUpload, CompletedPart, CreateBucketConfiguration,
};
use chrono::{Datelike, Utc};
use flare_server_core::error::{ErrorCode, Result, map_infra_error};

use crate::config::settings::normalize_optional_url;
use crate::domain::model::{ObjectStat, UploadContext, UploadedPartRecord};
use crate::domain::repository::MediaObjectRepository;
use flare_im_service_kit::config::ObjectStoreConfig;

#[derive(Clone)]
pub struct S3ObjectStore {
    client: S3Client,
    /// 只用于生成预签名 URL 的 client（配了 public_endpoint 时才与 `client` 不同）。
    /// 预签名是纯离线计算，它从不发起连接——正因如此，服务自身可以继续走内网
    /// 明文端点，而浏览器拿到公网 HTTPS 地址。
    presign_client: S3Client,
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
        let bucket = cfg.bucket.clone().ok_or_else(|| {
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

        // 对外端点单独建一个 client：签名里的 host 取自它，所以浏览器拿到的
        // URL 指向公网；而下面的桶检查仍用内网的 `client`，不需要公网 TLS。
        let public_endpoint = cfg
            .public_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let presign_client = match public_endpoint.clone() {
            Some(ep) => {
                let mut b = S3ConfigBuilder::from(&aws_cfg)
                    .region(region.clone())
                    .endpoint_url(ep);
                if force_path_style {
                    b = b.force_path_style(true);
                }
                S3Client::from_conf(b.build())
            }
            None => client.clone(),
        };

        if endpoint.is_some() {
            Self::ensure_bucket_exists(&client, &bucket, &region_name).await?;
        }

        // 配置中的预签名TTL（秒），默认3600
        let presign_url_ttl_seconds = cfg.presign_url_ttl_seconds.unwrap_or(3600) as i64;

        let use_presign = cfg.use_presign.unwrap_or(true);
        let bucket_root_prefix = normalize_prefix(&cfg.bucket_root_prefix);
        let upload_prefix = normalize_prefix(&cfg.upload_prefix);

        // base_url 是回给客户端的地址，优先用对外端点。
        let base_url = match resolve_client_facing_endpoint(&public_endpoint, &endpoint) {
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
            presign_client,
            bucket,
            base_url,
            cdn_base_url: normalize_optional_url(&cfg.cdn_base_url),
            upload_prefix,
            bucket_root_prefix,
            force_path_style,
            presign_url_ttl_seconds,
            use_presign,
        })
    }

    async fn ensure_bucket_exists(
        client: &S3Client,
        bucket: &str,
        region_name: &str,
    ) -> Result<()> {
        match client.head_bucket().bucket(bucket).send().await {
            Ok(_) => {
                tracing::info!(bucket = bucket, "object storage bucket already exists");
                return Ok(());
            }
            Err(err) => {
                tracing::warn!(
                    bucket = bucket,
                    error = %err,
                    "object storage bucket check failed, attempting to create bucket"
                );
            }
        }

        let create_bucket_configuration = if region_name.eq_ignore_ascii_case("us-east-1") {
            None
        } else {
            let constraint = BucketLocationConstraint::from(region_name);
            Some(
                CreateBucketConfiguration::builder()
                    .location_constraint(constraint)
                    .build(),
            )
        };

        match client
            .create_bucket()
            .bucket(bucket)
            .set_create_bucket_configuration(create_bucket_configuration)
            .send()
            .await
        {
            Ok(_) => {
                tracing::info!(
                    bucket = bucket,
                    region = region_name,
                    "object storage bucket created"
                );
                Ok(())
            }
            Err(create_err) => {
                tracing::warn!(
                    bucket = bucket,
                    error = %create_err,
                    "create bucket failed, re-checking if bucket became available"
                );
                client
                    .head_bucket()
                    .bucket(bucket)
                    .send()
                    .await
                    .map(|_| ())
                    .map_err(|_| {
                        map_infra_error(
                            create_err,
                            ErrorCode::ConfigurationError,
                            format!(
                                "failed to ensure object storage bucket exists, bucket={bucket}"
                            ),
                        )
                    })
            }
        }
    }

    fn build_object_key(&self, context: &UploadContext<'_>) -> String {
        let mut segments: Vec<String> = Vec::with_capacity(9);
        segments.push(crate::domain::model::tenant_storage_segment(
            context.tenant_id,
        ));

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

        tracing::trace!(
            key = &key,
            bucket = &self.bucket,
            expires_in = expires_in,
            "开始生成S3对象的预签名GET URL"
        );

        // 生成预签名GET URL
        let presigned = self
            .presign_client
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
        tracing::trace!(key = &key, "已生成预签名GET URL");
        Ok(url)
    }

    fn build_object_key_from_parts(
        &self,
        tenant_id: &str,
        file_id: &str,
        file_name: &str,
        file_category: &str,
    ) -> String {
        let empty = [];
        let context = UploadContext {
            tenant_id,
            file_id,
            file_name,
            mime_type: "",
            file_size: 0,
            payload: &empty,
            file_category: file_category.to_string(),
            user_id: "",
            trace_id: None,
            namespace: None,
            business_tag: None,
            metadata: std::collections::HashMap::new(),
        };
        self.build_object_key(&context)
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
        tracing::trace!(
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

        tracing::trace!(
            file_id = context.file_id,
            key = &key,
            bucket = &self.bucket,
            "对象已成功上传到S3存储"
        );
        Ok(key)
    }

    async fn delete_object(&self, object_path: &str) -> Result<()> {
        tracing::trace!(
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

        tracing::trace!(
            key = object_path,
            bucket = &self.bucket,
            "对象已成功从S3存储删除"
        );
        Ok(())
    }

    async fn presign_object(&self, object_path: &str, expires_in: i64) -> Result<String> {
        tracing::trace!(
            key = object_path,
            bucket = &self.bucket,
            expires_in = expires_in,
            "开始生成S3对象的预签名URL"
        );

        let presigned = self
            .presign_client
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
        tracing::trace!(key = object_path, presigned_url = &url, "已生成预签名URL");
        Ok(url)
    }

    async fn presign_put_object(
        &self,
        object_path: &str,
        content_type: &str,
        content_length: i64,
        expires_in: i64,
    ) -> Result<String> {
        // 显式的 Content-Length 会进 X-Amz-SignedHeaders，存储按签名校验实际请求头，
        // 长度与声明不符的 PUT 得到 SignatureDoesNotMatch，而不是写进一个更大的对象。
        let presigned = self
            .presign_client
            .put_object()
            .bucket(&self.bucket)
            .key(object_path)
            .content_type(content_type)
            .content_length(content_length)
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
                    format!("failed to presign s3 put url, key={}", object_path),
                )
            })?;
        Ok(presigned.uri().to_string())
    }

    async fn create_multipart_upload(
        &self,
        object_path: &str,
        content_type: &str,
    ) -> Result<String> {
        let response = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(object_path)
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    format!("failed to create multipart upload, key={}", object_path),
                )
            })?;
        response.upload_id().map(|s| s.to_string()).ok_or_else(|| {
            flare_server_core::flare_err!(
                ErrorCode::InternalError,
                "multipart upload id missing from object store response"
            )
        })
    }

    async fn presign_upload_part(
        &self,
        object_path: &str,
        upload_id: &str,
        part_number: u32,
        expires_in: i64,
    ) -> Result<String> {
        let presigned = self
            .presign_client
            .upload_part()
            .bucket(&self.bucket)
            .key(object_path)
            .upload_id(upload_id)
            .part_number(part_number as i32)
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
                    format!(
                        "failed to presign multipart part upload, key={} part={}",
                        object_path, part_number
                    ),
                )
            })?;
        Ok(presigned.uri().to_string())
    }

    async fn complete_multipart_upload(
        &self,
        object_path: &str,
        upload_id: &str,
        parts: &[UploadedPartRecord],
    ) -> Result<()> {
        let completed_parts = parts
            .iter()
            .map(|part| {
                CompletedPart::builder()
                    .set_e_tag(Some(part.etag.clone()))
                    .part_number(part.part_number as i32)
                    .build()
            })
            .collect::<Vec<_>>();

        let upload = CompletedMultipartUpload::builder()
            .set_parts(Some(completed_parts))
            .build();

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(object_path)
            .upload_id(upload_id)
            .multipart_upload(upload)
            .send()
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    format!("failed to complete multipart upload, key={}", object_path),
                )
            })?;
        Ok(())
    }

    async fn abort_multipart_upload(&self, object_path: &str, upload_id: &str) -> Result<()> {
        let result = self
            .client
            .abort_multipart_upload()
            .bucket(&self.bucket)
            .key(object_path)
            .upload_id(upload_id)
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            // 已完成或已中止的上传：目标状态已经达成，回收任务不必反复重试。
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|service| service.is_no_such_upload()) =>
            {
                Ok(())
            }
            Err(err) => Err(map_infra_error(
                err,
                ErrorCode::NetworkError,
                format!("failed to abort multipart upload, key={}", object_path),
            )),
        }
    }

    async fn stat_object(&self, object_path: &str) -> Result<ObjectStat> {
        let response = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(object_path)
            .send()
            .await
            .map_err(|e| {
                map_infra_error(
                    e,
                    ErrorCode::NetworkError,
                    format!("failed to stat object, key={}", object_path),
                )
            })?;
        Ok(ObjectStat {
            size: response.content_length(),
            etag: response.e_tag().map(|s| s.to_string()),
        })
    }

    fn build_object_key_for(
        &self,
        tenant_id: &str,
        file_id: &str,
        file_name: &str,
        file_category: &str,
    ) -> String {
        self.build_object_key_from_parts(tenant_id, file_id, file_name, file_category)
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

/// 回给客户端的端点：有对外端点就用它，否则回落到服务自用的端点。
///
/// 抽成纯函数是为了能直接测——走 `from_config` 的话，本机没有对象存储时
/// 构造会在桶检查那步失败，断言根本执行不到，测试会假绿。
fn resolve_client_facing_endpoint(
    public_endpoint: &Option<String>,
    endpoint: &Option<String>,
) -> Option<String> {
    public_endpoint
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| endpoint.clone())
}

pub type S3ObjectStoreRef = Arc<S3ObjectStore>;

#[cfg(test)]
mod public_endpoint_tests {
    use super::normalize_optional_url;
    use super::resolve_client_facing_endpoint;

    fn s(v: &str) -> Option<String> {
        Some(v.to_string())
    }

    /// 预签名 URL 是回给浏览器的，必须指向对外端点；服务自身仍走内网。
    ///
    /// 两者共用一个配置时只能二选一：填内网地址浏览器直传连不上（127.0.0.1
    /// 在浏览器里是它自己），填公网地址则服务启动时的桶检查要走公网 TLS，
    /// 自签证书下直接起不来——aws-sdk-s3 的 default-https-client 用的是编译进
    /// 二进制的 webpki 根证书，挂 CA、设 SSL_CERT_FILE/AWS_CA_BUNDLE 都无效。
    #[test]
    fn public_endpoint_wins_for_client_facing_urls() {
        assert_eq!(
            resolve_client_facing_endpoint(
                &s("https://cdn.example.com"),
                &s("http://127.0.0.1:29000")
            ),
            s("https://cdn.example.com"),
        );
    }

    /// 没配对外端点时行为不变，老部署不受影响。
    #[test]
    fn falls_back_to_the_private_endpoint_when_unset() {
        assert_eq!(
            resolve_client_facing_endpoint(&None, &s("http://127.0.0.1:29000")),
            s("http://127.0.0.1:29000"),
        );
    }

    /// 空串和纯空白按"没配"处理：环境变量没设时模板会展开成空串，
    /// 若当成有效值，base_url 会变成空的，客户端拿到一个相对路径。
    #[test]
    fn blank_public_endpoint_is_treated_as_unset() {
        for blank in ["", "   "] {
            assert_eq!(
                resolve_client_facing_endpoint(&s(blank), &s("http://127.0.0.1:29000")),
                s("http://127.0.0.1:29000"),
                "空白对外端点应回落，输入 {blank:?}"
            );
        }
    }

    /// 两个都没有时返回 None（走 AWS 官方端点那条分支）。
    #[test]
    fn none_when_neither_is_configured() {
        assert_eq!(resolve_client_facing_endpoint(&None, &None), None);
    }

    /// `cdn_base_url` 是回给客户端的地址，空串必须当成「没配」。
    ///
    /// base.toml 里写的是 `${FLARE_S3_CDN_BASE_URL:-}`，环境变量不设时会展开成
    /// 空串而不是消失。若把它当有效值，`build_full_url("", key)` 会拼出相对路径，
    /// 客户端拿到一个半可用的地址——比彻底没配更难查。
    #[test]
    fn blank_cdn_base_url_is_treated_as_unset() {
        assert_eq!(normalize_optional_url(&None), None);
        assert_eq!(normalize_optional_url(&s("")), None);
        assert_eq!(normalize_optional_url(&s("   ")), None);
        assert_eq!(
            normalize_optional_url(&s(" https://cdn.example.com/flare-media ")),
            s("https://cdn.example.com/flare-media")
        );
    }
}

#[cfg(test)]
mod tenant_key_tests {
    use super::*;

    fn store() -> S3ObjectStore {
        store_at("https://objects.example.test")
    }

    fn store_at(endpoint: &str) -> S3ObjectStore {
        let client = S3Client::from_conf(
            S3ConfigBuilder::new()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .endpoint_url(endpoint)
                .force_path_style(true)
                .credentials_provider(Credentials::new("test", "test", None, None, "unit-test"))
                .build(),
        );
        S3ObjectStore {
            presign_client: client.clone(),
            client,
            bucket: "media".to_string(),
            base_url: None,
            cdn_base_url: None,
            upload_prefix: Some("uploads".to_string()),
            bucket_root_prefix: Some("root".to_string()),
            force_path_style: true,
            presign_url_ttl_seconds: 60,
            use_presign: true,
        }
    }

    #[tokio::test]
    async fn stream_and_direct_keys_start_with_an_injective_tenant_prefix() {
        let store = store();
        let context = UploadContext {
            tenant_id: "tenant-a",
            file_id: "file-id",
            file_name: "photo.png",
            mime_type: "image/png",
            file_size: 0,
            payload: &[],
            file_category: "image".to_string(),
            user_id: "owner",
            trace_id: None,
            namespace: None,
            business_tag: None,
            metadata: std::collections::HashMap::new(),
        };
        let stream_key = store.build_object_key(&context);
        let direct_key = store.build_object_key_for("tenant-a", "file-id", "photo.png", "image");
        for key in [stream_key, direct_key] {
            assert!(key.starts_with("tenant-a/root/uploads/image/"));
            assert!(key.ends_with("/file-id.png"));
        }
        let other = store.build_object_key_for("../tenant-b", "file-id", "photo.png", "image");
        assert!(other.starts_with("%2E%2E%2Ftenant-b/"));
        assert!(!other.contains("../"));
    }

    /// `upload-url` 用的也是这套布局：租户在最前，部署配置的根前缀与上传前缀在其后。
    #[test]
    fn upload_url_keys_use_the_tenant_first_layout() {
        let temporary = std::env::temp_dir().join(format!(
            "flare-media-s3-upload-url-{}",
            uuid::Uuid::new_v4()
        ));
        let service = crate::domain::MediaService::new(
            Some(Arc::new(store())),
            None,
            None,
            None,
            None,
            None,
            crate::domain::MediaDomainConfig::new(
                3600,
                None,
                60,
                temporary.clone(),
                3600,
                8 * 1024 * 1024,
            ),
        );
        let ctx = flare_server_core::context::Context::root()
            .with_tenant_id("tenant-a")
            .with_user_id("owner");
        let generated = service.generate_upload_url(&ctx, None, None);
        std::fs::remove_dir_all(&temporary).expect("remove this test's temporary chunks");
        let (url, key) = generated.unwrap();
        assert!(key.starts_with("tenant-a/root/uploads/uploads/"), "{key}");
        assert_eq!(url, format!("/media/{key}"));
    }

    /// 直传单次 PUT 的地址把声明的长度签进去：存储按 signed headers 复算签名，
    /// 请求体长度与声明不符时签名不匹配，写不进超出声明大小的对象。
    #[tokio::test]
    async fn single_put_presign_signs_the_declared_content_length() {
        // 签名是离线计算，不连接任何存储。
        let url = store()
            .presign_put_object("tenant-a/file.png", "image/png", 1234, 60)
            .await
            .unwrap();
        let url = url::Url::parse(&url).unwrap();
        let signed_headers = url
            .query_pairs()
            .find(|(name, _)| name == "X-Amz-SignedHeaders")
            .map(|(_, value)| value.into_owned())
            .expect("presigned PUT carries X-Amz-SignedHeaders");
        let signed: Vec<&str> = signed_headers.split(';').collect();
        assert!(signed.contains(&"content-length"), "{signed_headers}");
        assert!(signed.contains(&"content-type"), "{signed_headers}");
        assert!(signed.contains(&"host"), "{signed_headers}");
    }

    /// 本机回环上的假 S3：每个连接读完请求头后回同一个错误响应，返回地址与收到的请求行。
    async fn s3_error_endpoint(
        status: &'static str,
        code: &'static str,
    ) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let requests = seen.clone();
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut head = Vec::new();
                let mut buffer = [0u8; 1024];
                while !head.windows(4).any(|window| window == b"\r\n\r\n") {
                    match socket.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => head.extend_from_slice(&buffer[..read]),
                    }
                }
                let line = String::from_utf8_lossy(&head)
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .to_string();
                requests.lock().unwrap().push(line);
                let body = format!(
                    "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code>\
                     <Message>unit test</Message><RequestId>r</RequestId></Error>"
                );
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/xml\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        (format!("http://{address}"), seen)
    }

    /// 回收任务对已完成或已中止的分片上传再 abort 时，存储回 NoSuchUpload：目标状态已达成，
    /// 按成功处理，不让同一会话每个租期都重试一遍。其他错误照常上报。
    #[tokio::test]
    async fn aborting_a_finished_multipart_upload_is_idempotent_but_other_errors_surface() {
        let (endpoint, seen) = s3_error_endpoint("404 Not Found", "NoSuchUpload").await;
        store_at(&endpoint)
            .abort_multipart_upload("tenant-a/file.bin", "storage-upload-id")
            .await
            .expect("NoSuchUpload means the upload is already gone");
        let lines = seen.lock().unwrap().clone();
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(
            lines[0].starts_with("DELETE /media/tenant-a/file.bin?")
                && lines[0].contains("uploadId=storage-upload-id"),
            "{lines:?}"
        );

        let (endpoint, _) = s3_error_endpoint("403 Forbidden", "AccessDenied").await;
        assert!(
            store_at(&endpoint)
                .abort_multipart_upload("tenant-a/file.bin", "storage-upload-id")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn legacy_presigned_download_uses_the_recorded_key_without_rewriting_it() {
        // Signing is offline; no S3 service is contacted by this regression.
        let url = store()
            .presign_get_url("old-layout/2025/file.png", Some(60))
            .await
            .unwrap();
        assert_eq!(
            url::Url::parse(&url).unwrap().path(),
            "/media/old-layout/2025/file.png"
        );
    }
}
