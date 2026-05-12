//! 应用层工具函数

use prost_types::Timestamp;

use crate::domain::model::MediaFileMetadata;
use crate::domain::model::MediaReference;

/// 将领域模型转换为 protobuf FileInfo
pub fn to_proto_file_info(metadata: &MediaFileMetadata) -> flare_grpc_proto::media::FileInfo {
    let bucket = metadata
        .storage_bucket()
        .map(|s| s.to_string())
        .unwrap_or_default();
    let object_key = metadata
        .storage_path()
        .map(|s| s.to_string())
        .unwrap_or_default();

    flare_grpc_proto::media::FileInfo {
        file_id: metadata.file_id.clone(),
        file_name: metadata.file_name.clone(),
        mime_type: metadata.mime_type.clone(),
        size: metadata.file_size,
        url: metadata.url.clone(),
        cdn_url: metadata.cdn_url.clone(),
        metadata: metadata.metadata.clone(),
        created_at: Some(to_proto_timestamp(metadata.uploaded_at)),
        tenant: "".to_string(),
        reference_count: metadata.reference_count,
        status: metadata.status.as_str().to_string(),
        sha256: metadata.sha256.clone().unwrap_or_default(),
        md5: metadata.md5.clone().unwrap_or_default(),
        grace_expires_at: metadata.grace_expires_at.map(to_proto_timestamp),
        bucket,
        object_key,
    }
}

/// 将领域模型转换为 protobuf MediaReferenceInfo
pub fn to_proto_reference(
    reference: &MediaReference,
) -> flare_grpc_proto::media::MediaReferenceInfo {
    flare_grpc_proto::media::MediaReferenceInfo {
        reference_id: reference.reference_id.clone(),
        file_id: reference.file_id.clone(),
        namespace: reference.namespace.clone(),
        owner_id: reference.owner_id.clone(),
        business_tag: reference.business_tag.clone().unwrap_or_default(),
        metadata: reference.metadata.clone(),
        created_at: Some(to_proto_timestamp(reference.created_at)),
        expires_at: reference.expires_at.map(to_proto_timestamp),
    }
}

fn to_proto_timestamp(value: chrono::DateTime<chrono::Utc>) -> Timestamp {
    Timestamp {
        seconds: value.timestamp(),
        nanos: value.timestamp_subsec_nanos() as i32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::model::{
        MediaFileMetadata, STORAGE_BUCKET_METADATA_KEY, STORAGE_PATH_METADATA_KEY,
    };

    #[test]
    fn test_to_proto_file_info_includes_storage_fields() {
        let mut metadata = MediaFileMetadata::default();
        metadata.file_id = "file-123".to_string();
        metadata.storage_bucket = Some("test-bucket".to_string());
        metadata.storage_path = Some("images/2025/01/01/file-123.png".to_string());
        metadata.metadata.insert(
            STORAGE_BUCKET_METADATA_KEY.to_string(),
            "test-bucket".to_string(),
        );
        metadata.metadata.insert(
            STORAGE_PATH_METADATA_KEY.to_string(),
            "images/2025/01/01/file-123.png".to_string(),
        );

        let proto = to_proto_file_info(&metadata);
        assert_eq!(proto.bucket, "test-bucket");
        assert_eq!(proto.object_key, "images/2025/01/01/file-123.png");
    }
}
