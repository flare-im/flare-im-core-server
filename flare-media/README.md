# Flare Media Service

English · [中文](README.zh-CN.md)

Flare Media is the media subsystem of Flare IM, responsible for deduplicated storage, reference management, transcoding, and distribution of audio/video/files. The service follows a DDD + CQRS architecture: the write path focuses on persisting media and dispatching events, while the query path provides business retrieval and back-office management capabilities.

## Core responsibilities
- Pre-upload processing: generate upload tokens, guide clients to upload directly or via multipart, and verify the content fingerprint after completion.
- Media deduplication and referencing: uniquify files by `sha256` (or perceptual hashing added later), store `media_asset` metadata and maintain `media_reference` reference records, with the reference count controlling the file lifecycle.
- Asynchronous processing: publish media-creation events to JetStream to trigger downstream workflows such as transcoding, thumbnails, and moderation.
- Management interface: provide back-office capabilities such as media queries, reference add/remove, tag and moderation-status updates, and recycle-bin operations.
- Reclamation and archiving: apply a reclamation policy to assets that are not referenced within the grace period, extensible to cold storage or object-storage archiving.

## Resource usage within the message system

### Resource storage approach
In the Flare IM message system, media resources are associated with messages in the following ways:

1. **Message content embedding**: the message body contains a `file_id` field pointing to the specific resource in the Media service.
2. **Metadata reference**: through `file_id`, the complete file information can be queried, including the access URL, file size, MIME type, etc.
3. **Reference management**: every message containing a media resource creates a corresponding reference record in the Media service, ensuring the resource is not accidentally deleted.
4. **Resource type support**: supports images, audio, video, documents, and other media types, distinguished by MIME type.

### Resource access flow
A user accessing a media resource in a message follows this flow:

1. **Get the message**: the client fetches the message containing the media resource from the message service.
2. **Extract the resource identifier**: extract `file_id` from the message body.
3. **Get the access address**:
   - Call the Media service's `GetFileUrl` interface to obtain a temporary signed link.
   - Or directly use the `cdn_url` field pre-populated in the message.
4. **Resource download**: download or play the media resource via the obtained URL.
5. **Permission verification**: before accessing the resource, the service verifies whether the user has permission to access it (based on the message's access permissions).

### Resource lifecycle management
The lifecycle of media resources in the message system is uniformly managed by the Media service:

1. **Creation stage**: when a message is sent, the file is uploaded and the Media service returns a `file_id`.
2. **Reference stage**: when the message is stored, a reference record corresponding to that `file_id` is created.
3. **Usage stage**: the message recipient accesses the resource via `file_id`.
4. **Cleanup stage**: when the message is deleted, the Media service decrements the reference count; when the reference count reaches 0 and the grace period expires, the resource is automatically cleaned up.

### Reference strategy for resources in messages
To ensure effective resource management and avoid accidental deletion, the system adopts the following reference strategies:

1. **Strong reference**: for a resource directly associated with a message, the reference count is incremented by 1.
2. **Weak reference**: when forwarding a message, you can choose whether to create a new reference.
3. **Temporary reference**: temporary access in preview scenarios, which does not increment the reference count.
4. **Batch reference**: batch reference handling for the same resource when broadcasting messages.

### Resource access security
To ensure the security of resource access, the system provides the following security mechanisms:

1. **Temporary access token**: the link obtained via `GetFileUrl` contains a time-limited temporary access token.
2. **Access permission control**: only the sender and recipient of a message can access the related resource.
3. **Anti-hotlinking mechanism**: prevents illegal access to resources through Referer checks and signature verification.
4. **HTTPS encrypted transport**: all resource access is transmitted over HTTPS encryption.

## Configuration

By default the service reads the `config/` directory at the workspace root; base dependencies are defined in `config/base.toml`, and service overrides live in `config/services/media.toml`. Core fields:

```toml
[services.media]
service_name = "flare-media"
metadata_store = "media"
metadata_cache = "media_metadata"
object_store = "default"
redis_ttl_seconds = 3600
orphan_grace_seconds = 86400
upload_session_store = "upload_conversations"
chunk_upload_dir = "./data/media/chunks"
chunk_ttl_seconds = 172800
max_chunk_size_bytes = 52428800
local_storage_dir = "./data/media"
local_base_url = "http://localhost:50092/files"
cdn_base_url = "http://localhost:50092/files"
```

- `metadata_store`: the Postgres/TimescaleDB connection alias, used to persist `media_asset` and `media_reference`.
- `metadata_cache`: the Redis alias, caching hot data and transient information for in-progress uploads.
- `object_store`: the object-storage configuration name (MinIO/S3), which stores media files and derivatives.
- `orphan_grace_seconds`: the grace period (seconds) for an upload that has completed but not yet established a reference; after it expires, the asset enters the reclamation task.
- `upload_session_store`: where multipart-upload session information is stored (Redis profile).
- `chunk_upload_dir`: the temporary staging directory for chunk data, which is automatically cleaned up after the final merge.
- `chunk_ttl_seconds`: the lifetime of a chunk session; beyond the TTL it automatically expires and the temporary files are cleaned up.
- `max_chunk_size_bytes`: the maximum size of a single chunk (default 50MB).
- `local_storage_dir`: an optional local cache directory, used in the development environment or during the transcoding stage.
- `cdn_base_url`: the external access base URL, which can be switched to a CDN.

### Object storage configuration conventions

The object-storage profile in `config/base.toml` has been unified to an S3-compatible implementation, so through configuration it can adapt to backends such as MinIO, AWS S3, Alibaba Cloud OSS, Tencent COS, GCP GCS, and Qiniu. Key fields:

- `presign_url_ttl_seconds`: the default validity period (seconds) for presigned URLs, used for `GetFileUrl` and the URLs returned on upload when no explicit parameter is passed.
- `use_presign`: a boolean switch; when `true`, a presigned URL is returned; when `false`, a direct/CDN address is concatenated directly, with access controlled by the object storage itself.
- `bucket_root_prefix`: the unified root path prefix within the bucket (supports multi-tenancy and environment isolation), which can be configured as a multi-level path such as `tenant-a/media`.
- `force_path_style`: controls whether the SDK uses path-style access (non-AWS endpoints typically need this enabled).

The actual object storage path follows: `{bucket_root_prefix?}/{file_type}/{yyyy}/{mm}/{dd}/{file_id}[.ext]`. `file_type` is automatically classified as `images` / `videos` / `audio` / `documents` / `others` based on the upload metadata or MIME, to facilitate tiered governance and lifecycle-policy orchestration.

> ⚠️ Access policy is left to the object storage itself: whether it is public, read/write permissions, CORS, bandwidth limits, etc. are all configured in the bucket policy / gateway layer; `flare-media` only decides whether to return a presigned or direct URL based on `use_presign`.

## Module structure

```text
flare-media/
├── application/         # Use-case orchestration and service facade
├── domain/              # Media aggregate root, repository interfaces, domain services
├── infrastructure/      # Redis, Postgres, object-storage adapters, transcoding/event publishing
├── interface/
│   └── grpc/            # gRPC Handler/Server (`MediaGrpcServer`)
└── src/main.rs          # Entry point: load config, register services, start gRPC
```

## System design

### Upload and deduplication flow
1. The client calls `PreUpload` to obtain an upload token and direct-upload target.
2. After the client finishes uploading, it calls back `CompleteUpload`, and the service computes the content fingerprint.
3. If the fingerprint already exists: return the existing `asset_id`, only add a reference record (`media_reference`), and incrementally update `ref_count`.
4. If it is entirely new content: write to object storage, persist `media_asset` metadata, and write a JetStream event to trigger transcoding/moderation.
5. Redis handles the temporary state and concurrency control during the write stage (preventing duplicate-upload conflicts).

### Multipart upload and resumable upload
- `InitiateMultipartUpload`: the service generates an `upload_id`, recommends a chunk size, and reserves a Redis session and a local temporary directory.
- `UploadMultipartChunk`: each chunk is identified by a `chunk_index`; the service writes it to a local temporary file and records upload progress; re-uploading the same chunk simply returns success.
- `CompleteMultipartUpload`: the service concatenates the local chunks in order, computes the hash, and reuses the `store_media_file` flow to generate the final media; after completion, temporary files and the Redis session are automatically cleaned up.
- `AbortMultipartUpload`: explicitly cancel the upload, cleaning up the session and chunk files.
- By default, only videos/large files go through the multipart flow; images can still use a single streaming upload.

### External application lifecycle example
The following example shows the complete flow of a business back office / client from file upload to final deletion:

1. **Prepare the context**
   - On the client side, prepare `RequestContext`, `TenantContext`, `user_id`, and custom `metadata` (business tags, etc.).
2. **Upload the file**
   - Small files (e.g. images): directly call `UploadFile` (streaming RPC) to transfer the whole file, obtaining `file_id`, `url`, and `cdn_url`.
   - Large files (e.g. videos):
     1. Call `InitiateMultipartUpload` to get an `upload_id` and a recommended `chunk_size`.
     2. Call `UploadMultipartChunk` sequentially or concurrently by `chunk_index`, with resumable upload (re-uploading the same `chunk_index` is automatically handled idempotently).
     3. After all chunks are uploaded, call `CompleteMultipartUpload`; the service concatenates the chunks, deduplicates, and returns the final media information.
     4. If the user abandons the upload, call `AbortMultipartUpload`; the service cleans up the Redis session and temporary chunk files.
3. **Business reference**
   - The `file_id` returned upon upload completion can be used directly in business data records.
   - To reuse the same media in multiple places, call `CreateReference` to establish a reference (supports setting namespace/business_tag, etc.); when querying, the `file_id` can be associated with multiple business entities.
4. **Read access**
   - Get file details: call `GetFileInfo` (returns `FileInfo` with reference count, hash, status, etc.).
   - Get the access address: call `GetFileUrl` to obtain a temporary signed link and CDN address.
5. **More management operations (back-office system)**
   - Maintain the reference list: `ListReferences` to see which businesses currently reference the media; `DeleteReference` to remove a single reference (once the reference count reaches zero, the service enters the grace period).
   - Optional: the business back office can also clean up unused assets, via its own logic or by subscribing to JetStream events.
6. **Delete the file**
   - Actively call `DeleteFile` (which decrements the reference count; when the count is > 1, it only updates the count, and when == 1, it deletes the object storage / session / metadata).
   - To thoroughly delete all references, call `DeleteReference` one by one before deletion, or invoke a back-office batch-processing tool.
   - The service's background cleanup task (`CleanupOrphanedAssets`) also removes media that remains unreferenced beyond the grace period.

### User avatar storage example
User avatars, as a special kind of media resource, require special handling:

1. **Upload the avatar**
   ```javascript
   // The client uploads the user's avatar
   const avatarFile = document.getElementById('avatar-input').files[0];
   const metadata = {
     fileName: `avatar_${userId}.jpg`,
     mimeType: 'image/jpeg',
     fileSize: avatarFile.size,
     fileType: FileType.IMAGE,
     userId: userId,
     namespace: 'user_avatars'
   };
   
   const response = await mediaClient.uploadFile({
     metadata: metadata,
     chunkData: avatarFile
   });
   
   // Important: store only the file_id, not the full URL
   const fileId = response.fileId;
   ```

2. **Store the avatar information**
   ```javascript
   // Store only the file_id in the user profile
   await userDatabase.updateUser(userId, {
     avatar_file_id: fileId  // store only the file_id
   });
   ```

3. **Display the avatar**
   ```javascript
   // Get the avatar file_id from the user profile
   const user = await userDatabase.getUser(userId);
   const fileId = user.avatar_file_id;
   
   if (fileId) {
     // Get the access URL
     const urlResponse = await mediaClient.getFileUrl({
       fileId: fileId,
       expiresIn: 3600  // expires in 1 hour
     });
     
     // Display the avatar on the page
     document.getElementById('user-avatar').src = urlResponse.url;
   }
   ```

4. **Update the avatar**
   ```javascript
   // When the user updates the avatar, first upload the new avatar to get a new file_id
   const newFileId = newAvatarResponse.fileId;
   
   // Update the avatar file_id in the user profile
   await userDatabase.updateUser(userId, {
     avatar_file_id: newFileId
   });
   
   // Optional: delete the old avatar (if no longer needed)
   // await mediaClient.deleteFile({fileId: oldFileId});
   ```

The advantages of this approach:
- **Security**: control access permissions and validity via presigned URLs.
- **Flexibility**: you can switch the CDN or storage backend at any time by simply updating the configuration.
- **Extensibility**: supports different access strategies (public/private).
- **Storage optimization**: storing only the file_id instead of the full URL saves storage space.

### Lifecycle and unreferenced-resource management
- Ordinary business uploads are marked `media_lifecycle_scope=external` by default; the asset remains `Active` and will not enter automatic cleanup just because it has no references; the business party or third-party system must actively call the delete interface.
- Message attachment uploads must explicitly be marked `media_lifecycle_scope=message`, or use `message` / `messages` / `im_message` / `im-message` as the `namespace` or `business_tag`. Only such assets create references, participate in content-hash deduplication, and enter automatic archiving/cleanup once the reference count reaches zero and the grace period expires.
- Non-message media are not subject to automatic compression, transcoding, thumbnail generation, or lifecycle migration. Compression should only be triggered by an explicit command, the message-media pipeline, or a business plugin.
- All delete/restore operations are recorded in the audit log (to be landed in Timescale/Elastic later).

### Management interface (for the business back office)
- `ListAssets`: supports paginated queries by user, business domain, tag, and time range.
- `GetAsset`: returns media details, the reference list, and transcoding/moderation status.
- `CreateReference` / `DeleteReference`: reference add/remove, driving changes to `ref_count`.
- `RestoreAsset` / `DeleteAsset`: recycle-bin management and permanent deletion.
- `UpdateAsset`: updates to moderation status, tags, permissions, CDN policy, etc.
- Event notifications (JetStream): media creation, transcoding completion, imminent cleanup, etc.

## Running and debugging

```bash
cargo run --bin flare-media
```

Before starting, ensure the dependent services are available (MinIO/S3, Postgres, Redis, JetStream). During development, you can use `docker-compose` to start local dependencies, and set:

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/flare_media"
export REDIS_URL="redis://localhost:6379/2"
export MINIO_ENDPOINT="http://localhost:29000"
```

## Monitoring and metrics
- Counters: `media_upload_total`, `media_deduplicated_total`, `media_reference_total`, `media_cleanup_total`
- Histograms: upload-completion duration, fingerprint-computation duration, transcoding/moderation processing duration
- Gauges: storage usage, number of unreferenced assets, reclamation queue length

## Future extensions
- Introduce perceptual hashing to identify similar content and support instant upload (dedup-based).
- Media versioning: management of different transcodings/specifications for the same `asset`.
- Multi-tier storage strategy: automatic migration from hot → warm → cold storage.
- Moderation/risk-control integration: connect to a content moderation platform to automatically freeze abnormal media and push alerts.
