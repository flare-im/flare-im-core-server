use std::sync::Arc;
use std::time::Duration;

use aws_config::BehaviorVersion;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::config::{Builder as S3ConfigBuilder, Credentials, Region};
use aws_sdk_s3::error::ProvideErrorMetadata;
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
        content_length: i64,
        expires_in: i64,
    ) -> Result<String> {
        // 与单次 PUT 相同：Content-Length 进 X-Amz-SignedHeaders，长度不符的分片得到
        // SignatureDoesNotMatch，写不进比这一片应有长度更大的数据。
        let presigned = self
            .presign_client
            .upload_part()
            .bucket(&self.bucket)
            .key(object_path)
            .upload_id(upload_id)
            .part_number(part_number as i32)
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

        let result = self
            .client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(object_path)
            .upload_id(upload_id)
            .multipart_upload(upload)
            .send()
            .await;
        match result {
            Ok(_) => Ok(()),
            // 提交的分片在存储里不存在、ETag 不对或顺序不对：调用方没按地址上传就来完成，
            // 是参数错误，不是存储不可用。RustFS 在一片都没上传时回 404 `NoSuchKey`
            // （S3 回 `InvalidPart`），同样是参数错误。
            Err(err)
                if err.as_service_error().is_some_and(|service| {
                    matches!(
                        service.code(),
                        Some("InvalidPart" | "InvalidPartOrder" | "NoSuchKey")
                    )
                }) =>
            {
                Err(flare_server_core::flare_err_details!(
                    ErrorCode::InvalidParameter,
                    "committed parts do not match the uploaded parts",
                    format!("key={object_path}")
                ))
            }
            // 分片上传已合并（上一次完成）或已中止（回收任务、桶生命周期规则）：再试也不会
            // 成功，不能报成存储不可用让客户端一直重试。领域层先看合并出的对象在不在，
            // 不在才把它作为「重新发起上传」的参数错误答复。
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|service| service.code() == Some("NoSuchUpload")) =>
            {
                Err(flare_server_core::flare_err_details!(
                    ErrorCode::InvalidParameter,
                    "multipart upload no longer exists; start a new upload",
                    format!("key={object_path}")
                ))
            }
            Err(err) => Err(map_infra_error(
                err,
                ErrorCode::NetworkError,
                format!("failed to complete multipart upload, key={}", object_path),
            )),
        }
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

    async fn stat_object(&self, object_path: &str) -> Result<Option<ObjectStat>> {
        let result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(object_path)
            .send()
            .await;
        match result {
            Ok(response) => Ok(Some(ObjectStat {
                size: response.content_length(),
                etag: response.e_tag().map(|s| s.to_string()),
            })),
            // HEAD 的 404 没有响应体，SDK 把它建模成 NotFound：对象还没写入。
            Err(err)
                if err
                    .as_service_error()
                    .is_some_and(|service| service.is_not_found()) =>
            {
                Ok(None)
            }
            Err(err) => Err(map_infra_error(
                err,
                ErrorCode::NetworkError,
                format!("failed to stat object, key={}", object_path),
            )),
        }
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

    /// 分片地址与单次 PUT 一样签入这一片的长度：存储复算签名时长度不符即拒绝，
    /// 超大的分片在上传时就被挡下。
    #[tokio::test]
    async fn multipart_part_presign_signs_the_part_content_length() {
        // 签名是离线计算，不连接任何存储。
        let url = store()
            .presign_upload_part("tenant-a/file.bin", "storage-upload-id", 3, 5_242_880, 60)
            .await
            .unwrap();
        let url = url::Url::parse(&url).unwrap();
        let query: std::collections::HashMap<String, String> =
            url.query_pairs().into_owned().collect();
        assert_eq!(
            query.get("partNumber").map(String::as_str),
            Some("3"),
            "{url}"
        );
        assert_eq!(
            query.get("uploadId").map(String::as_str),
            Some("storage-upload-id")
        );
        let signed: Vec<&str> = query
            .get("X-Amz-SignedHeaders")
            .expect("presigned part carries X-Amz-SignedHeaders")
            .split(';')
            .collect();
        assert!(signed.contains(&"content-length"), "{signed:?}");
        assert!(signed.contains(&"host"), "{signed:?}");
    }

    /// 完成直传早于 PUT：HEAD 得到 404，表示对象还没写入，返回 None 交给领域层判为参数错误；
    /// 其他失败仍按存储故障上报。
    #[tokio::test]
    async fn stat_of_a_missing_object_is_none_but_other_failures_surface() {
        let (endpoint, seen) = s3_error_endpoint("404 Not Found", "NotFound").await;
        let stat = store_at(&endpoint)
            .stat_object("tenant-a/file.png")
            .await
            .expect("a missing object is not a storage failure");
        assert!(stat.is_none());
        let lines = seen.lock().unwrap().clone();
        assert_eq!(lines, vec!["HEAD /media/tenant-a/file.png HTTP/1.1"]);

        let (endpoint, _) = s3_error_endpoint("403 Forbidden", "AccessDenied").await;
        let err = store_at(&endpoint)
            .stat_object("tenant-a/file.png")
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::NetworkError));
    }

    /// 合并时存储报提交的分片不存在或顺序不对：调用方没按地址上传就来完成，回参数错误；
    /// 存储的其他拒绝仍是存储故障。RustFS 在一片都没上传时回 404 `NoSuchKey`。
    #[tokio::test]
    async fn completing_with_parts_the_store_does_not_hold_is_an_invalid_argument() {
        let parts = [UploadedPartRecord {
            part_number: 1,
            etag: "\"made-up\"".to_string(),
            size: 5,
            sha256: None,
        }];
        for (status, code) in [
            ("400 Bad Request", "InvalidPart"),
            ("400 Bad Request", "InvalidPartOrder"),
            ("404 Not Found", "NoSuchKey"),
        ] {
            let (endpoint, seen) = s3_error_endpoint(status, code).await;
            let err = store_at(&endpoint)
                .complete_multipart_upload("tenant-a/file.bin", "storage-upload-id", &parts)
                .await
                .unwrap_err();
            assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{code}");
            let lines = seen.lock().unwrap().clone();
            assert!(
                lines.len() == 1
                    && lines[0].starts_with("POST /media/tenant-a/file.bin?")
                    && lines[0].contains("uploadId=storage-upload-id"),
                "{lines:?}"
            );
        }

        let (endpoint, _) = s3_error_endpoint("403 Forbidden", "AccessDenied").await;
        let err = store_at(&endpoint)
            .complete_multipart_upload("tenant-a/file.bin", "storage-upload-id", &parts)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::NetworkError));
    }

    /// 分片上传已合并或已中止后再合并，存储回 NoSuchUpload：重试不会成功，回参数错误（由
    /// 领域层先核对合并出的对象），不报成存储不可用让客户端无限重试。
    #[tokio::test]
    async fn completing_an_upload_the_store_no_longer_has_is_an_invalid_argument() {
        let parts = [UploadedPartRecord {
            part_number: 1,
            etag: "\"etag-1\"".to_string(),
            size: 5,
            sha256: None,
        }];
        let (endpoint, seen) = s3_error_endpoint("404 Not Found", "NoSuchUpload").await;
        let err = store_at(&endpoint)
            .complete_multipart_upload("tenant-a/file.bin", "storage-upload-id", &parts)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
        let lines = seen.lock().unwrap().clone();
        assert!(
            lines.len() == 1
                && lines[0].starts_with("POST /media/tenant-a/file.bin?")
                && lines[0].contains("uploadId=storage-upload-id"),
            "{lines:?}"
        );
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
                // 读完请求体再应答：带体的请求（合并分片的 POST）若有未读数据就关连接，
                // 内核会回 RST，客户端看到的是连接错误而不是这个应答。
                let header_end = head
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map_or(head.len(), |at| at + 4);
                let body_length = String::from_utf8_lossy(&head[..header_end])
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.trim()
                            .eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().ok())
                            .flatten()
                    })
                    .unwrap_or(0);
                let mut received = head.len() - header_end;
                while received < body_length {
                    match socket.read(&mut buffer).await {
                        Ok(0) | Err(_) => break,
                        Ok(read) => received += read,
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

/// 对一次性的 S3 兼容存储核对直传依赖的存储行为：签进地址的长度确实被存储执行（单次 PUT
/// 与分片，含长度 0），对象不存在时 HEAD 回 404，提交存储里没有的分片时合并被拒，已合并的
/// 上传不能再合并，而这些都按领域层期望的类别报出。离线签名与回环假 S3 只能证明地址里有长度、错误码怎么映射，证明不了存储
/// 真的校验。
///
/// `FLARE_MEDIA_TEST_S3_ENDPOINT` 必须是回环上的 `http://127.0.0.1:<port>`，只供本测试使用；
/// `FLARE_MEDIA_TEST_S3_ENV_FILE` 是 0600 的文件，含 `RUSTFS_ACCESS_KEY=` 与
/// `RUSTFS_SECRET_KEY=` 两行（与 RustFS 容器的 env 文件同格式）。测试新建一个随机命名的桶，
/// 结束时删除它写过的对象与分片上传。
#[cfg(test)]
mod live_store_tests {
    use super::*;

    fn live_store() -> S3ObjectStore {
        let endpoint =
            std::env::var("FLARE_MEDIA_TEST_S3_ENDPOINT").expect("FLARE_MEDIA_TEST_S3_ENDPOINT");
        let parsed = url::Url::parse(&endpoint).expect("endpoint URL");
        assert!(
            parsed.scheme() == "http" && parsed.host_str() == Some("127.0.0.1"),
            "only a loopback store dedicated to this test"
        );
        let path =
            std::env::var("FLARE_MEDIA_TEST_S3_ENV_FILE").expect("FLARE_MEDIA_TEST_S3_ENV_FILE");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o600, "credential file must be 0600");
        }
        let text = std::fs::read_to_string(&path).unwrap();
        let value = |name: &str| {
            text.lines()
                .find_map(|line| line.strip_prefix(name)?.strip_prefix('='))
                .unwrap_or_else(|| panic!("{name} missing from the credential file"))
                .trim()
                .to_string()
        };
        let client = S3Client::from_conf(
            S3ConfigBuilder::new()
                .behavior_version(BehaviorVersion::latest())
                .region(Region::new("us-east-1"))
                .endpoint_url(&endpoint)
                .force_path_style(true)
                .credentials_provider(Credentials::new(
                    value("RUSTFS_ACCESS_KEY"),
                    value("RUSTFS_SECRET_KEY"),
                    None,
                    None,
                    "live-store-test",
                ))
                .build(),
        );
        S3ObjectStore {
            presign_client: client.clone(),
            client,
            bucket: format!("media-live-{}", uuid::Uuid::new_v4().simple()),
            base_url: None,
            cdn_base_url: None,
            upload_prefix: Some("uploads".to_string()),
            bucket_root_prefix: Some("root".to_string()),
            force_path_style: true,
            presign_url_ttl_seconds: 60,
            use_presign: true,
        }
    }

    /// 经预签名地址 PUT，返回状态码与 ETag。
    async fn put(url: &str, body: &[u8], content_type: Option<&str>) -> (u16, Option<String>) {
        let http = reqwest::Client::builder().no_proxy().build().unwrap();
        let mut request = http.put(url).body(body.to_vec());
        if let Some(content_type) = content_type {
            request = request.header("content-type", content_type);
        }
        let response = request.send().await.unwrap();
        let etag = response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        (response.status().as_u16(), etag)
    }

    #[tokio::test]
    #[ignore = "requires FLARE_MEDIA_TEST_S3_ENDPOINT and FLARE_MEDIA_TEST_S3_ENV_FILE"]
    async fn the_store_enforces_signed_lengths_and_reports_missing_objects_and_parts() {
        let store = live_store();
        S3ObjectStore::ensure_bucket_exists(&store.client, &store.bucket, "us-east-1")
            .await
            .unwrap();

        // 单次 PUT：长度不符被拒，按声明长度写入后 HEAD 报出该长度。
        let single = store.build_object_key_for("tenant-live", "single", "a.txt", "file");
        assert!(store.stat_object(&single).await.unwrap().is_none());
        let url = store
            .presign_put_object(&single, "text/plain", 3, 60)
            .await
            .unwrap();
        assert_eq!(put(&url, b"abcd", Some("text/plain")).await.0, 403);
        assert_eq!(put(&url, b"abc", Some("text/plain")).await.0, 200);
        let stat = store.stat_object(&single).await.unwrap().unwrap();
        assert_eq!(stat.size, Some(3));

        // 分片：每片签入自己的长度，长了短了都被拒。
        let merged = store.build_object_key_for("tenant-live", "merged", "b.bin", "file");
        let upload_id = store
            .create_multipart_upload(&merged, "application/octet-stream")
            .await
            .unwrap();
        let part_url = store
            .presign_upload_part(&merged, &upload_id, 1, 5, 60)
            .await
            .unwrap();
        assert_eq!(put(&part_url, b"123456", None).await.0, 403);
        assert_eq!(put(&part_url, b"1234", None).await.0, 403);
        let (status, etag) = put(&part_url, b"12345", None).await;
        assert_eq!(status, 200);
        let etag = etag.expect("part ETag");

        // 提交存储里没有的分片：参数错误，不是存储不可用。
        let made_up = [UploadedPartRecord {
            part_number: 1,
            etag: "\"00000000000000000000000000000000\"".to_string(),
            size: 5,
            sha256: None,
        }];
        let err = store
            .complete_multipart_upload(&merged, &upload_id, &made_up)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");

        // 分片已提交、一片都没上传就完成：RustFS 回 NoSuchKey（不是 InvalidPart），同样是
        // 参数错误；随后中止这个上传。
        let untouched = store.build_object_key_for("tenant-live", "untouched", "u.bin", "file");
        let untouched_id = store
            .create_multipart_upload(&untouched, "application/octet-stream")
            .await
            .unwrap();
        let err = store
            .complete_multipart_upload(&untouched, &untouched_id, &made_up)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
        assert!(store.stat_object(&untouched).await.unwrap().is_none());
        store
            .abort_multipart_upload(&untouched, &untouched_id)
            .await
            .unwrap();

        let uploaded = [UploadedPartRecord {
            part_number: 1,
            etag,
            size: 5,
            sha256: None,
        }];
        store
            .complete_multipart_upload(&merged, &upload_id, &uploaded)
            .await
            .unwrap();
        let stat = store.stat_object(&merged).await.unwrap().unwrap();
        assert_eq!(stat.size, Some(5));
        // 已合并的上传再合并：存储回 NoSuchUpload，报参数错误而不是存储故障（领域层随后核对
        // 合并出的对象，补登记合并后登记失败的上传）。
        let err = store
            .complete_multipart_upload(&merged, &upload_id, &uploaded)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some(ErrorCode::InvalidParameter), "{err}");
        // 已合并的上传再 abort：幂等成功（回收任务依赖这一点）。
        store
            .abort_multipart_upload(&merged, &upload_id)
            .await
            .unwrap();

        // 零字节：签了长度 0 的地址只接受空体，HEAD 报 0。长度在签名里，空体也必须带
        // `Content-Length: 0`：reqwest 等客户端对空体不发这个头，存储按签名不符拒绝（403）。
        let empty = store.build_object_key_for("tenant-live", "empty", "e.txt", "file");
        let url = store
            .presign_put_object(&empty, "text/plain", 0, 60)
            .await
            .unwrap();
        assert_eq!(put(&url, b"x", Some("text/plain")).await.0, 403);
        assert_eq!(put(&url, b"", Some("text/plain")).await.0, 403);
        let http = reqwest::Client::builder().no_proxy().build().unwrap();
        let response = http
            .put(&url)
            .header("content-type", "text/plain")
            .header("content-length", "0")
            .body(Vec::new())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let stat = store.stat_object(&empty).await.unwrap().unwrap();
        assert_eq!(stat.size, Some(0));

        for key in [&single, &merged, &empty] {
            store.delete_object(key).await.unwrap();
            assert!(store.stat_object(key).await.unwrap().is_none());
        }
        // 删除不存在的对象同样成功（回收任务对没写过对象的分片会话照常删除）。
        store.delete_object(&merged).await.unwrap();
    }
}
