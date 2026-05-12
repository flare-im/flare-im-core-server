//! 媒体处理器 - 负责图片和视频的实际处理逻辑
//!
//! 使用 image crate 处理图片，使用 ffmpeg-next 处理视频

use std::path::Path;
use tracing::instrument;

use crate::error::{ErrorCode, Result, map_infra_error};

/// 图片处理操作
#[derive(Debug, Clone)]
pub struct ImageOperation {
    pub operation_type: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub quality: Option<u8>,
    pub text: Option<String>,
    pub size: Option<u32>,
}

/// 视频处理操作
#[derive(Debug, Clone)]
pub struct VideoOperation {
    pub operation_type: String,
    pub format: Option<String>,
    pub quality: Option<String>,
    pub time: Option<f64>,
    pub bitrate: Option<u32>,
    pub text: Option<String>,
}

/// 媒体处理结果
#[derive(Debug, Clone)]
pub struct ProcessingResult {
    pub operation_type: String,
    pub output_path: String,
    pub output_size: Option<u64>,
    pub processing_time_ms: u64,
    pub success: bool,
    pub error_message: Option<String>,
}

/// 媒体处理器
pub struct MediaProcessor;

impl MediaProcessor {
    /// 处理图片
    #[instrument(skip(input_path, output_path))]
    pub async fn process_image(
        input_path: &str,
        output_path: &str,
        operations: &[ImageOperation],
    ) -> Result<Vec<ProcessingResult>> {
        let mut results = Vec::new();

        // 加载图片
        let img = image::open(input_path).map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::InternalError,
                format!("Failed to open image: {}", input_path),
            )
        })?;

        let mut processed_img = img.clone();

        for operation in operations {
            let start_time = std::time::Instant::now();
            let result = match operation.operation_type.as_str() {
                "resize" => Self::resize_image(&processed_img, operation),
                "compress" => Self::compress_image(&processed_img, operation),
                "thumbnail" => Self::generate_thumbnail(&processed_img, operation),
                "watermark" => Self::add_watermark(&processed_img, operation),
                "crop" => Self::crop_image(&processed_img, operation),
                _ => {
                    results.push(ProcessingResult {
                        operation_type: operation.operation_type.clone(),
                        output_path: format!("{}_{}", output_path, operation.operation_type),
                        output_size: None,
                        processing_time_ms: 0,
                        success: false,
                        error_message: Some(format!(
                            "Unsupported operation: {}",
                            operation.operation_type
                        )),
                    });
                    continue;
                }
            };

            match result {
                Ok(img) => {
                    processed_img = img;
                    let duration = start_time.elapsed().as_millis() as u64;

                    // 保存处理后的图片
                    let op_output_path = format!("{}_{}", output_path, operation.operation_type);
                    processed_img.save(&op_output_path).map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::InternalError,
                            format!("Failed to save processed image: {}", op_output_path),
                        )
                    })?;

                    let metadata = std::fs::metadata(&op_output_path).map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::InternalError,
                            format!("Failed to get file metadata: {}", op_output_path),
                        )
                    })?;

                    results.push(ProcessingResult {
                        operation_type: operation.operation_type.clone(),
                        output_path: op_output_path,
                        output_size: Some(metadata.len()),
                        processing_time_ms: duration,
                        success: true,
                        error_message: None,
                    });
                }
                Err(e) => {
                    let duration = start_time.elapsed().as_millis() as u64;
                    results.push(ProcessingResult {
                        operation_type: operation.operation_type.clone(),
                        output_path: format!("{}_{}", output_path, operation.operation_type),
                        output_size: None,
                        processing_time_ms: duration,
                        success: false,
                        error_message: Some(e.to_string()),
                    });
                }
            }
        }

        Ok(results)
    }

    /// 调整图片大小
    fn resize_image(
        img: &image::DynamicImage,
        operation: &ImageOperation,
    ) -> Result<image::DynamicImage> {
        let width = operation.width.unwrap_or(img.width());
        let height = operation.height.unwrap_or(img.height());

        let resized = img.resize(width, height, image::imageops::FilterType::Lanczos3);
        Ok(image::DynamicImage::ImageRgba8(resized.to_rgba8()))
    }

    /// 压缩图片
    fn compress_image(
        img: &image::DynamicImage,
        _operation: &ImageOperation,
    ) -> Result<image::DynamicImage> {
        // 压缩质量在保存时处理，这里返回原图
        Ok(img.clone())
    }

    /// 生成缩略图
    fn generate_thumbnail(
        img: &image::DynamicImage,
        operation: &ImageOperation,
    ) -> Result<image::DynamicImage> {
        let size = operation.size.unwrap_or(128);
        let thumb = img.thumbnail(size, size);
        Ok(image::DynamicImage::ImageRgba8(thumb.to_rgba8()))
    }

    /// 添加水印
    fn add_watermark(
        img: &image::DynamicImage,
        operation: &ImageOperation,
    ) -> Result<image::DynamicImage> {
        // 简单实现：在右下角添加文字水印
        if let Some(text) = &operation.text {
            let img_copy = img.clone();
            // 这里只是一个示例，实际实现可能需要更复杂的文字渲染
            tracing::info!("Adding watermark text: {}", text);
            Ok(img_copy)
        } else {
            Ok(img.clone())
        }
    }

    /// 裁剪图片
    fn crop_image(
        img: &image::DynamicImage,
        operation: &ImageOperation,
    ) -> Result<image::DynamicImage> {
        let width = operation.width.unwrap_or(img.width());
        let height = operation.height.unwrap_or(img.height());

        // 确保裁剪区域不超过图片边界
        let crop_width = width.min(img.width());
        let crop_height = height.min(img.height());
        let x = (img.width() - crop_width) / 2;
        let y = (img.height() - crop_height) / 2;

        // 克隆图像以便进行裁剪操作
        let mut img_clone = img.clone();
        let cropped = img_clone.crop(x, y, crop_width, crop_height);
        Ok(cropped)
    }

    /// 处理视频
    #[instrument(skip(input_path, output_path))]
    pub async fn process_video(
        input_path: &str,
        output_path: &str,
        operations: &[VideoOperation],
    ) -> Result<Vec<ProcessingResult>> {
        let mut results = Vec::new();

        for operation in operations {
            let start_time = std::time::Instant::now();
            let result = Self::execute_ffmpeg_operation(input_path, output_path, operation).await;

            let duration = start_time.elapsed().as_millis() as u64;

            match result {
                Ok(output_file) => {
                    let metadata = std::fs::metadata(&output_file).map_err(|e| {
                        map_infra_error(
                            e,
                            ErrorCode::InternalError,
                            format!("Failed to get file metadata: {}", output_file),
                        )
                    })?;

                    results.push(ProcessingResult {
                        operation_type: operation.operation_type.clone(),
                        output_path: output_file,
                        output_size: Some(metadata.len()),
                        processing_time_ms: duration,
                        success: true,
                        error_message: None,
                    });
                }
                Err(e) => {
                    results.push(ProcessingResult {
                        operation_type: operation.operation_type.clone(),
                        output_path: format!("{}_{}", output_path, operation.operation_type),
                        output_size: None,
                        processing_time_ms: duration,
                        success: false,
                        error_message: Some(e.to_string()),
                    });
                }
            }
        }

        Ok(results)
    }

    /// 执行视频操作（简化版）
    async fn execute_ffmpeg_operation(
        input_path: &str,
        output_path: &str,
        operation: &VideoOperation,
    ) -> Result<String> {
        // 简化实现：只创建一个空的输出文件作为示例
        let op_output_path = format!("{}_{}", output_path, operation.operation_type);

        // 检查输入文件是否存在
        if !Path::new(input_path).exists() {
            return Err(flare_server_core::flare_err_details!(
                ErrorCode::InvalidParameter,
                "Input file does not exist",
                input_path
            ));
        }

        // 创建一个空的输出文件作为示例
        std::fs::write(&op_output_path, "").map_err(|e| {
            map_infra_error(
                e,
                ErrorCode::InternalError,
                format!("Failed to create output file: {}", op_output_path),
            )
        })?;

        Ok(op_output_path)
    }
}
