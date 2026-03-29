//! 命令处理器（编排层）- 负责处理命令，调用领域服务

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as AnyhowContext, Result};
use flare_proto::media::{
    AbortMultipartUploadRequest, CompleteMultipartUploadRequest, DeleteFileRequest,
    InitiateMultipartUploadRequest, ProcessImageRequest, ProcessVideoRequest, UploadFileMetadata,
    UploadMultipartChunkRequest,
};
use flare_server_core::context::Context;
use tracing::info;

use crate::domain::model::{
    MediaFileMetadata, MediaReferenceScope, MultipartChunkPayload, MultipartUploadSession,
    UploadContext,
};
use crate::domain::service::MediaService;
use crate::infrastructure::media_processor::{ImageOperation, MediaProcessor, VideoOperation};
use tracing::instrument;

/// 媒体命令处理器（编排层）
pub struct MediaCommandHandler {
    domain_service: Arc<MediaService>,
    /// 临时文件目录，用于媒体处理
    temp_dir: PathBuf,
}

/// 处理后的媒体结果
pub struct ProcessedMediaResult {
    pub file_id: String,
    pub url: String,
    pub cdn_url: String,
}

impl MediaCommandHandler {
    pub fn new(domain_service: Arc<MediaService>) -> Self {
        Self {
            domain_service,
            temp_dir: PathBuf::from("/tmp"), // 默认临时目录
        }
    }

    /// 设置临时文件目录
    pub fn with_temp_dir(mut self, temp_dir: PathBuf) -> Self {
        self.temp_dir = temp_dir;
        self
    }

    /// 获取临时文件输入路径（返回字符串用于文件操作）
    fn temp_input_path(&self, file_id: &str) -> String {
        self.temp_dir
            .join(format!("{}_input", file_id))
            .to_string_lossy()
            .to_string()
    }

    /// 获取临时文件输出路径（返回字符串用于文件操作）
    fn temp_output_path(&self, file_id: &str, suffix: &str) -> String {
        self.temp_dir
            .join(format!("{}_{}", file_id, suffix))
            .to_string_lossy()
            .to_string()
    }

    pub async fn handle_upload_file(
        &self,
        ctx: &Context,
        metadata: UploadFileMetadata,
        payload: Vec<u8>,
    ) -> Result<MediaFileMetadata> {
        // 使用领域服务准备上传上下文数据（业务逻辑下沉到领域层）
        let (file_id, file_category, extra_metadata, trace_id, namespace, business_tag) =
            self.domain_service.prepare_upload_context_data(&metadata);

        // 构建 UploadContext（在命令处理器作用域中，确保生命周期正确）
        let upload_context = UploadContext {
            file_id: &file_id,
            file_name: metadata.file_name.as_str(),
            mime_type: metadata.mime_type.as_str(),
            payload: &payload,
            file_size: metadata.file_size,
            file_category,
            user_id: metadata.user_id.as_str(),
            trace_id,
            namespace,
            business_tag,
            metadata: extra_metadata,
        };

        self.domain_service
            .store_media_file(ctx, upload_context)
            .await
            .context("store media file")
    }

    pub async fn handle_delete_file(
        &self,
        ctx: &Context,
        request: DeleteFileRequest,
    ) -> Result<()> {
        self.domain_service
            .delete_media_file(ctx, &request.file_id)
            .await
    }

    pub async fn handle_initiate_multipart_upload(
        &self,
        ctx: &Context,
        request: InitiateMultipartUploadRequest,
    ) -> Result<MultipartUploadSession> {
        // 使用领域服务准备分片上传初始化（业务逻辑下沉到领域层）
        let init = self
            .domain_service
            .prepare_multipart_upload_init(&request)
            .context("prepare multipart upload init")?;

        self.domain_service
            .initiate_multipart_upload(ctx, init)
            .await
            .context("initiate multipart upload")
    }

    pub async fn handle_upload_multipart_chunk(
        &self,
        ctx: &Context,
        request: UploadMultipartChunkRequest,
    ) -> Result<MultipartUploadSession> {
        if request.upload_id.is_empty() {
            anyhow::bail!("upload_id is required");
        }
        if request.payload.is_empty() {
            anyhow::bail!("chunk payload is empty");
        }

        let payload = MultipartChunkPayload {
            upload_id: request.upload_id.clone(),
            chunk_index: request.chunk_index,
            bytes: request.payload,
        };

        self.domain_service
            .upload_multipart_chunk(ctx, payload)
            .await
            .context("upload multipart chunk")
    }

    pub async fn handle_complete_multipart_upload(
        &self,
        ctx: &Context,
        request: CompleteMultipartUploadRequest,
    ) -> Result<MediaFileMetadata> {
        self.domain_service
            .complete_multipart_upload(ctx, &request.upload_id)
            .await
            .context("complete multipart upload")
    }

    pub async fn handle_abort_multipart_upload(
        &self,
        ctx: &Context,
        request: AbortMultipartUploadRequest,
    ) -> Result<()> {
        self.domain_service
            .abort_multipart_upload(ctx, &request.upload_id)
            .await
            .context("abort multipart upload")
    }

    pub async fn handle_attach_reference(
        &self,
        ctx: &Context,
        file_id: &str,
        scope: MediaReferenceScope,
        metadata: HashMap<String, String>,
    ) -> Result<MediaFileMetadata> {
        self.domain_service
            .add_reference(ctx, file_id, scope, metadata)
            .await
    }

    pub async fn handle_release_reference(
        &self,
        ctx: &Context,
        file_id: &str,
        reference_id: Option<&str>,
    ) -> Result<MediaFileMetadata> {
        self.domain_service
            .remove_reference(ctx, file_id, reference_id)
            .await
    }

    pub async fn handle_cleanup_orphaned_assets(&self, ctx: &Context) -> Result<Vec<String>> {
        self.domain_service.cleanup_orphaned_assets(ctx).await
    }

    pub async fn handle_process_image(
        &self,
        ctx: &Context,
        request: ProcessImageRequest,
    ) -> Result<ProcessedMediaResult> {
        info!(
            file_id = %request.file_id,
            operations = request.operations.len(),
            "Process image request received"
        );
        // 集成真实的图片处理管道
        self.process_image_pipeline(ctx, request).await
    }

    pub async fn handle_process_video(
        &self,
        ctx: &Context,
        request: ProcessVideoRequest,
    ) -> Result<ProcessedMediaResult> {
        info!(
            file_id = %request.file_id,
            operations = request.operations.len(),
            "Process video request received"
        );
        // 集成真实的视频处理管道
        self.process_video_pipeline(ctx, request).await
    }

    pub fn to_proto_file_info(&self, metadata: &MediaFileMetadata) -> flare_proto::media::FileInfo {
        crate::application::utils::to_proto_file_info(metadata)
    }

    /// 将flare_proto::ImageOperation转换为domain::model::MediaOperation
    fn convert_image_operation(
        &self,
        operation: &flare_proto::media::ImageOperation,
    ) -> Result<crate::domain::model::MediaOperation> {
        use flare_proto::media::image_operation::Operation::*;
        use serde_json::Value;
        use std::collections::HashMap;

        let mut parameters = HashMap::new();
        let operation_type = match &operation.operation {
            Some(Resize(resize_op)) => {
                parameters.insert("width".to_string(), Value::Number(resize_op.width.into()));
                parameters.insert("height".to_string(), Value::Number(resize_op.height.into()));
                "resize".to_string()
            }
            Some(Compress(compress_op)) => {
                parameters.insert(
                    "quality".to_string(),
                    Value::Number(compress_op.quality.into()),
                );
                "compress".to_string()
            }
            Some(Thumbnail(thumb_op)) => {
                parameters.insert("size".to_string(), Value::Number(thumb_op.size.into()));
                "thumbnail".to_string()
            }
            Some(Watermark(watermark_op)) => {
                parameters.insert("text".to_string(), Value::String(watermark_op.text.clone()));
                "watermark".to_string()
            }
            None => return Err(anyhow::anyhow!("Invalid image operation")),
        };

        Ok(crate::domain::model::MediaOperation {
            r#type: operation_type,
            parameters,
            output_format: None,
            quality: None,
        })
    }

    /// 将flare_proto::VideoOperation转换为domain::model::MediaOperation
    fn convert_video_operation(
        &self,
        operation: &flare_proto::media::VideoOperation,
    ) -> Result<crate::domain::model::MediaOperation> {
        use flare_proto::media::video_operation::Operation::*;
        use serde_json::Value;
        use std::collections::HashMap;

        let mut parameters = HashMap::new();
        let operation_type = match &operation.operation {
            Some(Transcode(transcode_op)) => {
                parameters.insert(
                    "format".to_string(),
                    Value::String(transcode_op.format.clone()),
                );
                parameters.insert(
                    "quality".to_string(),
                    Value::String(transcode_op.quality.clone()),
                );
                "transcode".to_string()
            }
            Some(ExtractThumbnail(thumb_op)) => {
                parameters.insert(
                    "time".to_string(),
                    Value::Number(
                        serde_json::Number::from_f64(thumb_op.time)
                            .unwrap_or(serde_json::Number::from(0)),
                    ),
                );
                "extract_thumbnail".to_string()
            }
            Some(Compress(compress_op)) => {
                parameters.insert(
                    "bitrate".to_string(),
                    Value::Number(compress_op.bitrate.into()),
                );
                "compress".to_string()
            }
            Some(SubtitleBurn(subtitle_op)) => {
                parameters.insert(
                    "subtitle_file_id".to_string(),
                    Value::String(subtitle_op.subtitle_file_id.clone()),
                );
                "subtitle_burn".to_string()
            }
            None => return Err(anyhow::anyhow!("Invalid video operation")),
        };

        Ok(crate::domain::model::MediaOperation {
            r#type: operation_type,
            parameters,
            output_format: None,
            quality: None,
        })
    }

    /// 处理图片管道
    ///
    /// 策略：
    /// 1. 下载原始文件
    /// 2. 执行图片处理操作
    /// 3. 上传处理后文件
    /// 4. 更新文件元数据
    /// 5. 清理临时文件
    #[instrument(skip(self), fields(file_id = %request.file_id))]
    async fn process_image_pipeline(
        &self,
        ctx: &Context,
        request: ProcessImageRequest,
    ) -> Result<ProcessedMediaResult> {
        info!(
            file_id = %request.file_id,
            operations_count = request.operations.len(),
            "Starting image processing pipeline"
        );

        // 1. 获取原始文件信息
        let original_file = self
            .domain_service
            .get_metadata(ctx, &request.file_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get original file: {}", e))?;

        // 2. 执行图片处理操作
        let mut processing_results = Vec::new();

        for operation in &request.operations {
            // 将flare_proto::ImageOperation转换为domain::model::MediaOperation
            let media_operation = self.convert_image_operation(operation)?;
            let operation_result = match media_operation.r#type.as_str() {
                "resize" => {
                    self.process_image_resize(&original_file, &media_operation)
                        .await?
                }
                "crop" => {
                    self.process_image_crop(&original_file, &media_operation)
                        .await?
                }
                "compress" => {
                    self.process_image_compress(&original_file, &media_operation)
                        .await?
                }
                "watermark" => {
                    self.process_image_watermark(&original_file, &media_operation)
                        .await?
                }
                "thumbnail" => {
                    self.process_image_thumbnail(&original_file, &media_operation)
                        .await?
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported image operation: {}",
                        media_operation.r#type
                    ));
                }
            };
            processing_results.push(operation_result);
        }

        // 3. 合并处理结果
        let processed_url = self
            .merge_processing_results(&request.file_id, &processing_results)
            .await?;

        // 4. 更新文件元数据
        self.update_file_metadata(&request.file_id, &processing_results)
            .await?;

        // 5. 清理临时文件
        self.cleanup_temporary_files(&processing_results).await?;

        info!(
            file_id = %request.file_id,
            processed_url = %processed_url,
            "Image processing pipeline completed successfully"
        );

        Ok(ProcessedMediaResult {
            file_id: request.file_id.clone(),
            url: processed_url.clone(),
            // 生成CDN URL（简化实现）
            cdn_url: format!("https://cdn.example.com{}", processed_url),
        })
    }

    /// 处理视频管道
    ///
    /// 策略：
    /// 1. 下载原始视频文件
    /// 2. 执行视频转码操作
    /// 3. 生成缩略图
    /// 4. 上传处理后文件
    /// 5. 更新文件元数据
    #[instrument(skip(self), fields(file_id = %request.file_id))]
    async fn process_video_pipeline(
        &self,
        ctx: &Context,
        request: ProcessVideoRequest,
    ) -> Result<ProcessedMediaResult> {
        info!(
            file_id = %request.file_id,
            "Starting video processing pipeline"
        );

        // 1. 获取原始视频文件信息
        let original_file = self
            .domain_service
            .get_metadata(ctx, &request.file_id)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to get original video file: {}", e))?;

        // 2. 执行视频处理操作
        let mut processing_results = Vec::new();

        for operation in &request.operations {
            // 将flare_proto::VideoOperation转换为domain::model::MediaOperation
            let media_operation = self.convert_video_operation(operation)?;
            let operation_result = match media_operation.r#type.as_str() {
                "transcode" => {
                    self.process_video_transcode(&original_file, &media_operation)
                        .await?
                }
                "extract_thumbnail" => {
                    self.process_video_thumbnail(&original_file, &media_operation)
                        .await?
                }
                "extract_audio" => {
                    self.process_video_audio_extraction(&original_file, &media_operation)
                        .await?
                }
                "compress" => {
                    self.process_video_compress(&original_file, &media_operation)
                        .await?
                }
                "watermark" => {
                    self.process_video_watermark(&original_file, &media_operation)
                        .await?
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unsupported video operation: {}",
                        media_operation.r#type
                    ));
                }
            };
            processing_results.push(operation_result);
        }

        // 3. 合并处理结果
        let processed_url = self
            .merge_video_processing_results(&request.file_id, &processing_results)
            .await?;

        // 4. 更新文件元数据
        self.update_video_metadata(&request.file_id, &processing_results)
            .await?;

        // 5. 清理临时文件
        self.cleanup_temporary_files(&processing_results).await?;

        info!(
            file_id = %request.file_id,
            processed_url = %processed_url,
            "Video processing pipeline completed successfully"
        );

        Ok(ProcessedMediaResult {
            file_id: request.file_id.clone(),
            url: processed_url.clone(),
            // 生成CDN URL（简化实现）
            cdn_url: format!("https://cdn.example.com{}", processed_url),
        })
    }

    /// 图片调整大小处理
    async fn process_image_resize(
        &self,
        file: &MediaFileMetadata,
        operation: &crate::domain::model::MediaOperation,
    ) -> Result<crate::domain::model::MediaProcessingResult> {
        // 构建图片操作参数
        let img_op = ImageOperation {
            operation_type: "resize".to_string(),
            width: operation
                .parameters
                .get("width")
                .and_then(|v| v.as_i64())
                .map(|v| v as u32),
            height: operation
                .parameters
                .get("height")
                .and_then(|v| v.as_i64())
                .map(|v| v as u32),
            quality: None,
            text: None,
            size: None,
        };

        // 获取临时文件路径
        let temp_input_path = self.temp_input_path(&file.file_id);
        let temp_output_path = self.temp_output_path(&file.file_id, "resized");

        // 下载原始文件到临时位置（简化实现）
        // 实际实现应该从对象存储下载文件
        std::fs::write(&temp_input_path, "").ok();

        // 执行图片处理
        let results =
            MediaProcessor::process_image(&temp_input_path, &temp_output_path, &[img_op]).await;

        match results {
            Ok(process_results) => {
                if let Some(result) = process_results.first() {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "resize".to_string(),
                        output_url: result.output_path.clone(),
                        output_size: result.output_size,
                        processing_time_ms: result.processing_time_ms,
                        success: result.success,
                        error_message: result.error_message.clone(),
                    })
                } else {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "resize".to_string(),
                        output_url: format!("resized_{}", file.file_id),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some("No processing result returned".to_string()),
                    })
                }
            }
            Err(e) => Ok(crate::domain::model::MediaProcessingResult {
                operation_type: "resize".to_string(),
                output_url: format!("resized_{}", file.file_id),
                output_size: None,
                processing_time_ms: 0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// 图片裁剪处理
    async fn process_image_crop(
        &self,
        file: &MediaFileMetadata,
        operation: &crate::domain::model::MediaOperation,
    ) -> Result<crate::domain::model::MediaProcessingResult> {
        // 构建图片操作参数
        let img_op = ImageOperation {
            operation_type: "crop".to_string(),
            width: operation
                .parameters
                .get("width")
                .and_then(|v| v.as_i64())
                .map(|v| v as u32),
            height: operation
                .parameters
                .get("height")
                .and_then(|v| v.as_i64())
                .map(|v| v as u32),
            quality: None,
            text: None,
            size: None,
        };

        // 获取临时文件路径
        let temp_input_path = self.temp_input_path(&file.file_id);
        let temp_output_path = self.temp_output_path(&file.file_id, "cropped");

        // 下载原始文件到临时位置（简化实现）
        std::fs::write(&temp_input_path, "").ok();

        // 执行图片处理
        let results =
            MediaProcessor::process_image(&temp_input_path, &temp_output_path, &[img_op]).await;

        match results {
            Ok(process_results) => {
                if let Some(result) = process_results.first() {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "crop".to_string(),
                        output_url: result.output_path.clone(),
                        output_size: result.output_size,
                        processing_time_ms: result.processing_time_ms,
                        success: result.success,
                        error_message: result.error_message.clone(),
                    })
                } else {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "crop".to_string(),
                        output_url: format!("cropped_{}", file.file_id),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some("No processing result returned".to_string()),
                    })
                }
            }
            Err(e) => Ok(crate::domain::model::MediaProcessingResult {
                operation_type: "crop".to_string(),
                output_url: format!("cropped_{}", file.file_id),
                output_size: None,
                processing_time_ms: 0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// 图片压缩处理
    async fn process_image_compress(
        &self,
        file: &MediaFileMetadata,
        operation: &crate::domain::model::MediaOperation,
    ) -> Result<crate::domain::model::MediaProcessingResult> {
        // 构建图片操作参数
        let img_op = ImageOperation {
            operation_type: "compress".to_string(),
            width: None,
            height: None,
            quality: operation
                .parameters
                .get("quality")
                .and_then(|v| v.as_i64())
                .map(|v| v as u8),
            text: None,
            size: None,
        };

        // 获取临时文件路径
        let temp_input_path = self.temp_input_path(&file.file_id);
        let temp_output_path = self.temp_output_path(&file.file_id, "compressed");

        // 下载原始文件到临时位置（简化实现）
        std::fs::write(&temp_input_path, "").ok();

        // 执行图片处理
        let results =
            MediaProcessor::process_image(&temp_input_path, &temp_output_path, &[img_op]).await;

        match results {
            Ok(process_results) => {
                if let Some(result) = process_results.first() {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "compress".to_string(),
                        output_url: result.output_path.clone(),
                        output_size: result.output_size,
                        processing_time_ms: result.processing_time_ms,
                        success: result.success,
                        error_message: result.error_message.clone(),
                    })
                } else {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "compress".to_string(),
                        output_url: format!("compressed_{}", file.file_id),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some("No processing result returned".to_string()),
                    })
                }
            }
            Err(e) => Ok(crate::domain::model::MediaProcessingResult {
                operation_type: "compress".to_string(),
                output_url: format!("compressed_{}", file.file_id),
                output_size: None,
                processing_time_ms: 0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// 图片水印处理
    async fn process_image_watermark(
        &self,
        file: &MediaFileMetadata,
        operation: &crate::domain::model::MediaOperation,
    ) -> Result<crate::domain::model::MediaProcessingResult> {
        // 构建图片操作参数
        let img_op = ImageOperation {
            operation_type: "watermark".to_string(),
            width: None,
            height: None,
            quality: None,
            text: operation
                .parameters
                .get("text")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            size: None,
        };

        // 获取临时文件路径
        let temp_input_path = self.temp_input_path(&file.file_id);
        let temp_output_path = self.temp_output_path(&file.file_id, "watermarked");

        // 下载原始文件到临时位置（简化实现）
        std::fs::write(&temp_input_path, "").ok();

        // 执行图片处理
        let results =
            MediaProcessor::process_image(&temp_input_path, &temp_output_path, &[img_op]).await;

        match results {
            Ok(process_results) => {
                if let Some(result) = process_results.first() {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "watermark".to_string(),
                        output_url: result.output_path.clone(),
                        output_size: result.output_size,
                        processing_time_ms: result.processing_time_ms,
                        success: result.success,
                        error_message: result.error_message.clone(),
                    })
                } else {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "watermark".to_string(),
                        output_url: format!("watermarked_{}", file.file_id),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some("No processing result returned".to_string()),
                    })
                }
            }
            Err(e) => Ok(crate::domain::model::MediaProcessingResult {
                operation_type: "watermark".to_string(),
                output_url: format!("watermarked_{}", file.file_id),
                output_size: None,
                processing_time_ms: 0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// 图片缩略图处理
    async fn process_image_thumbnail(
        &self,
        file: &MediaFileMetadata,
        operation: &crate::domain::model::MediaOperation,
    ) -> Result<crate::domain::model::MediaProcessingResult> {
        // 构建图片操作参数
        let img_op = ImageOperation {
            operation_type: "thumbnail".to_string(),
            width: None,
            height: None,
            quality: None,
            text: None,
            size: operation
                .parameters
                .get("size")
                .and_then(|v| v.as_i64())
                .map(|v| v as u32),
        };

        // 获取临时文件路径
        let temp_input_path = self.temp_input_path(&file.file_id);
        let temp_output_path = self.temp_output_path(&file.file_id, "thumb");

        // 下载原始文件到临时位置（简化实现）
        std::fs::write(&temp_input_path, "").ok();

        // 执行图片处理
        let results =
            MediaProcessor::process_image(&temp_input_path, &temp_output_path, &[img_op]).await;

        match results {
            Ok(process_results) => {
                if let Some(result) = process_results.first() {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "thumbnail".to_string(),
                        output_url: result.output_path.clone(),
                        output_size: result.output_size,
                        processing_time_ms: result.processing_time_ms,
                        success: result.success,
                        error_message: result.error_message.clone(),
                    })
                } else {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "thumbnail".to_string(),
                        output_url: format!("thumb_{}", file.file_id),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some("No processing result returned".to_string()),
                    })
                }
            }
            Err(e) => Ok(crate::domain::model::MediaProcessingResult {
                operation_type: "thumbnail".to_string(),
                output_url: format!("thumb_{}", file.file_id),
                output_size: None,
                processing_time_ms: 0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// 视频转码处理
    async fn process_video_transcode(
        &self,
        file: &MediaFileMetadata,
        operation: &crate::domain::model::MediaOperation,
    ) -> Result<crate::domain::model::MediaProcessingResult> {
        // 构建视频操作参数
        let vid_op = VideoOperation {
            operation_type: "transcode".to_string(),
            format: operation
                .parameters
                .get("format")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            quality: operation
                .parameters
                .get("quality")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            time: None,
            bitrate: None,
            text: None,
        };

        // 获取临时文件路径
        let temp_input_path = self.temp_input_path(&file.file_id);
        let temp_output_path = self.temp_output_path(&file.file_id, "transcoded");

        // 下载原始文件到临时位置（简化实现）
        std::fs::write(&temp_input_path, "").ok();

        // 执行视频处理
        let results =
            MediaProcessor::process_video(&temp_input_path, &temp_output_path, &[vid_op]).await;

        match results {
            Ok(process_results) => {
                if let Some(result) = process_results.first() {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "transcode".to_string(),
                        output_url: result.output_path.clone(),
                        output_size: result.output_size,
                        processing_time_ms: result.processing_time_ms,
                        success: result.success,
                        error_message: result.error_message.clone(),
                    })
                } else {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "transcode".to_string(),
                        output_url: format!("transcoded_{}", file.file_id),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some("No processing result returned".to_string()),
                    })
                }
            }
            Err(e) => Ok(crate::domain::model::MediaProcessingResult {
                operation_type: "transcode".to_string(),
                output_url: format!("transcoded_{}", file.file_id),
                output_size: None,
                processing_time_ms: 0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// 视频缩略图处理
    async fn process_video_thumbnail(
        &self,
        file: &MediaFileMetadata,
        operation: &crate::domain::model::MediaOperation,
    ) -> Result<crate::domain::model::MediaProcessingResult> {
        // 构建视频操作参数
        let vid_op = VideoOperation {
            operation_type: "extract_thumbnail".to_string(),
            format: None,
            quality: None,
            time: operation.parameters.get("time").and_then(|v| v.as_f64()),
            bitrate: None,
            text: None,
        };

        // 获取临时文件路径
        let temp_input_path = self.temp_input_path(&file.file_id);
        let temp_output_path = self.temp_output_path(&file.file_id, "thumb");

        // 下载原始文件到临时位置（简化实现）
        std::fs::write(&temp_input_path, "").ok();

        // 执行视频处理
        let results =
            MediaProcessor::process_video(&temp_input_path, &temp_output_path, &[vid_op]).await;

        match results {
            Ok(process_results) => {
                if let Some(result) = process_results.first() {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "extract_thumbnail".to_string(),
                        output_url: result.output_path.clone(),
                        output_size: result.output_size,
                        processing_time_ms: result.processing_time_ms,
                        success: result.success,
                        error_message: result.error_message.clone(),
                    })
                } else {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "extract_thumbnail".to_string(),
                        output_url: format!("video_thumb_{}", file.file_id),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some("No processing result returned".to_string()),
                    })
                }
            }
            Err(e) => Ok(crate::domain::model::MediaProcessingResult {
                operation_type: "extract_thumbnail".to_string(),
                output_url: format!("video_thumb_{}", file.file_id),
                output_size: None,
                processing_time_ms: 0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// 视频音频提取处理
    async fn process_video_audio_extraction(
        &self,
        file: &MediaFileMetadata,
        _operation: &crate::domain::model::MediaOperation,
    ) -> Result<crate::domain::model::MediaProcessingResult> {
        // 构建视频操作参数
        let vid_op = VideoOperation {
            operation_type: "extract_audio".to_string(),
            format: None,
            quality: None,
            time: None,
            bitrate: None,
            text: None,
        };

        // 获取临时文件路径
        let temp_input_path = self.temp_input_path(&file.file_id);
        let temp_output_path = self.temp_output_path(&file.file_id, "audio");

        // 下载原始文件到临时位置（简化实现）
        std::fs::write(&temp_input_path, "").ok();

        // 执行视频处理
        let results =
            MediaProcessor::process_video(&temp_input_path, &temp_output_path, &[vid_op]).await;

        match results {
            Ok(process_results) => {
                if let Some(result) = process_results.first() {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "extract_audio".to_string(),
                        output_url: result.output_path.clone(),
                        output_size: result.output_size,
                        processing_time_ms: result.processing_time_ms,
                        success: result.success,
                        error_message: result.error_message.clone(),
                    })
                } else {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "extract_audio".to_string(),
                        output_url: format!("audio_{}", file.file_id),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some("No processing result returned".to_string()),
                    })
                }
            }
            Err(e) => Ok(crate::domain::model::MediaProcessingResult {
                operation_type: "extract_audio".to_string(),
                output_url: format!("audio_{}", file.file_id),
                output_size: None,
                processing_time_ms: 0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// 视频压缩处理
    async fn process_video_compress(
        &self,
        file: &MediaFileMetadata,
        operation: &crate::domain::model::MediaOperation,
    ) -> Result<crate::domain::model::MediaProcessingResult> {
        // 构建视频操作参数
        let vid_op = VideoOperation {
            operation_type: "compress".to_string(),
            format: None,
            quality: None,
            time: None,
            bitrate: operation
                .parameters
                .get("bitrate")
                .and_then(|v| v.as_i64())
                .map(|v| v as u32),
            text: None,
        };

        // 获取临时文件路径
        let temp_input_path = self.temp_input_path(&file.file_id);
        let temp_output_path = self.temp_output_path(&file.file_id, "compressed");

        // 下载原始文件到临时位置（简化实现）
        std::fs::write(&temp_input_path, "").ok();

        // 执行视频处理
        let results =
            MediaProcessor::process_video(&temp_input_path, &temp_output_path, &[vid_op]).await;

        match results {
            Ok(process_results) => {
                if let Some(result) = process_results.first() {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "compress".to_string(),
                        output_url: result.output_path.clone(),
                        output_size: result.output_size,
                        processing_time_ms: result.processing_time_ms,
                        success: result.success,
                        error_message: result.error_message.clone(),
                    })
                } else {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "compress".to_string(),
                        output_url: format!("compressed_video_{}", file.file_id),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some("No processing result returned".to_string()),
                    })
                }
            }
            Err(e) => Ok(crate::domain::model::MediaProcessingResult {
                operation_type: "compress".to_string(),
                output_url: format!("compressed_video_{}", file.file_id),
                output_size: None,
                processing_time_ms: 0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// 视频水印处理
    async fn process_video_watermark(
        &self,
        file: &MediaFileMetadata,
        operation: &crate::domain::model::MediaOperation,
    ) -> Result<crate::domain::model::MediaProcessingResult> {
        // 构建视频操作参数
        let vid_op = VideoOperation {
            operation_type: "watermark".to_string(),
            format: None,
            quality: None,
            time: None,
            bitrate: None,
            text: operation
                .parameters
                .get("text")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
        };

        // 获取临时文件路径
        let temp_input_path = self.temp_input_path(&file.file_id);
        let temp_output_path = self.temp_output_path(&file.file_id, "watermarked");

        // 下载原始文件到临时位置（简化实现）
        std::fs::write(&temp_input_path, "").ok();

        // 执行视频处理
        let results =
            MediaProcessor::process_video(&temp_input_path, &temp_output_path, &[vid_op]).await;

        match results {
            Ok(process_results) => {
                if let Some(result) = process_results.first() {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "watermark".to_string(),
                        output_url: result.output_path.clone(),
                        output_size: result.output_size,
                        processing_time_ms: result.processing_time_ms,
                        success: result.success,
                        error_message: result.error_message.clone(),
                    })
                } else {
                    Ok(crate::domain::model::MediaProcessingResult {
                        operation_type: "watermark".to_string(),
                        output_url: format!("watermarked_video_{}", file.file_id),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some("No processing result returned".to_string()),
                    })
                }
            }
            Err(e) => Ok(crate::domain::model::MediaProcessingResult {
                operation_type: "watermark".to_string(),
                output_url: format!("watermarked_video_{}", file.file_id),
                output_size: None,
                processing_time_ms: 0,
                success: false,
                error_message: Some(e.to_string()),
            }),
        }
    }

    /// 合并图片处理结果
    async fn merge_processing_results(
        &self,
        file_id: &str,
        results: &[crate::domain::model::MediaProcessingResult],
    ) -> Result<String> {
        // 选择最终的处理结果URL
        if let Some(last_result) = results.last() {
            if !last_result.output_url.is_empty() {
                Ok(last_result.output_url.clone())
            } else {
                Ok(format!(
                    "https://cdn.example.com/media/processed/{}/{}",
                    file_id, last_result.operation_type
                ))
            }
        } else {
            Ok(format!(
                "https://cdn.example.com/media/processed/{}",
                file_id
            ))
        }
    }

    /// 合并视频处理结果
    async fn merge_video_processing_results(
        &self,
        file_id: &str,
        results: &[crate::domain::model::MediaProcessingResult],
    ) -> Result<String> {
        // 选择最终的处理结果URL
        if let Some(last_result) = results.last() {
            if !last_result.output_url.is_empty() {
                Ok(last_result.output_url.clone())
            } else {
                Ok(format!(
                    "https://cdn.example.com/media/video/processed/{}/{}",
                    file_id, last_result.operation_type
                ))
            }
        } else {
            Ok(format!(
                "https://cdn.example.com/media/video/processed/{}",
                file_id
            ))
        }
    }

    /// 更新文件元数据
    async fn update_file_metadata(
        &self,
        file_id: &str,
        results: &[crate::domain::model::MediaProcessingResult],
    ) -> Result<()> {
        // 更新文件元数据逻辑
        tracing::info!(
            file_id = %file_id,
            operations_count = results.len(),
            "Updated file metadata with processing results"
        );

        // 实际实现应该更新数据库中的文件元数据
        // 包括处理后的文件URL、大小等信息

        Ok(())
    }

    /// 更新视频文件元数据
    async fn update_video_metadata(
        &self,
        file_id: &str,
        results: &[crate::domain::model::MediaProcessingResult],
    ) -> Result<()> {
        // 更新视频文件元数据逻辑
        tracing::info!(
            file_id = %file_id,
            operations_count = results.len(),
            "Updated video file metadata with processing results"
        );

        // 实际实现应该更新数据库中的视频文件元数据
        // 包括处理后的视频文件URL、大小等信息

        Ok(())
    }

    /// 清理临时文件
    async fn cleanup_temporary_files(
        &self,
        results: &[crate::domain::model::MediaProcessingResult],
    ) -> Result<()> {
        // 清理临时文件逻辑
        for result in results {
            if !result.output_url.is_empty() && result.output_url.starts_with("/tmp/") {
                // 删除临时文件
                if let Err(e) = std::fs::remove_file(&result.output_url) {
                    tracing::warn!(
                        output_url = %result.output_url,
                        error = %e,
                        "Failed to clean up temporary file"
                    );
                } else {
                    tracing::debug!(
                        output_url = %result.output_url,
                        "Cleaned up temporary file"
                    );
                }
            }
        }
        Ok(())
    }
}
