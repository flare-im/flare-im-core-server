# Flare Media 服务

Flare Media 是 Flare IM 的媒资子系统，负责音视频/文件的去重存储、引用管理、转码与分发。服务遵循 DDD + CQRS 架构：写入链路聚焦媒资落库与事件派发，查询链路提供业务检索与后台管理能力。

## 核心职责
- 预处理上传：生成上传令牌、指导客户端直传或分片上传、完成后校验内容指纹。
- 媒资去重与引用：依据 `sha256`（或后续扩展的感知哈希）唯一化文件，存储 `media_asset` 元数据并维护 `media_reference` 引用记录，引用计数控制文件生命周期。
- 异步处理：向 JetStream 投递媒资创建事件，触发转码、缩略图、审核等后续工作流。
- 管理接口：对外提供媒资查询、引用增删、标签与审核状态更新、回收站操作等后台能力。
- 回收与归档：对未在宽限期内被引用的资产执行回收策略，可扩展冷存储或对象存储归档。

## 在消息系统中的资源使用

### 资源存储方式
在 Flare IM 消息系统中，媒体资源通过以下方式与消息关联：

1. **消息内容嵌入**：消息体中包含 `file_id` 字段，指向 Media 服务中的具体资源
2. **元数据引用**：通过 `file_id` 可以查询到完整的文件信息，包括访问 URL、文件大小、MIME 类型等
3. **引用管理**：每条包含媒体资源的消息都会在 Media 服务中创建对应的引用记录，确保资源不会被误删
4. **资源类型支持**：支持图片、音频、视频、文档等多种媒体类型，通过 MIME 类型进行区分

### 资源访问流程
用户访问消息中的媒体资源遵循以下流程：

1. **获取消息**：客户端从消息服务获取包含媒体资源的消息
2. **提取资源标识**：从消息体中提取 `file_id`
3. **获取访问地址**：
   - 调用 Media 服务的 `GetFileUrl` 接口获取临时签名链接
   - 或直接使用消息中预置的 `cdn_url` 字段进行访问
4. **资源下载**：通过获取的 URL 下载或播放媒体资源
5. **权限验证**：在访问资源前，服务会验证用户是否有权限访问该资源（基于消息的访问权限）

### 资源生命周期管理
消息系统中的媒体资源生命周期由 Media 服务统一管理：

1. **创建阶段**：消息发送时上传文件，Media 服务返回 `file_id`
2. **引用阶段**：消息存储时创建对应该 `file_id` 的引用记录
3. **使用阶段**：消息接收方通过 `file_id` 访问资源
4. **清理阶段**：消息删除时，Media 服务减少引用计数；当引用计数为 0 且超过宽限期后，资源被自动清理

### 消息中资源的引用策略
为了确保资源的有效管理和避免误删，系统采用以下引用策略：

1. **强引用**：消息直接关联的资源，引用计数+1
2. **弱引用**：转发消息时可以选择是否创建新的引用
3. **临时引用**：预览场景下的临时访问，不增加引用计数
4. **批量引用**：群发消息时对同一资源的批量引用处理

### 资源访问安全
为保障资源访问的安全性，系统提供以下安全机制：

1. **临时访问令牌**：通过 `GetFileUrl` 获取的链接包含临时访问令牌，具有时效性
2. **访问权限控制**：只有消息的发送方和接收方才能访问相关资源
3. **防盗链机制**：通过 Referer 检查和签名验证防止资源被非法访问
4. **HTTPS 加密传输**：所有资源访问均通过 HTTPS 加密传输

## 配置

服务默认读取工作空间根部 `config/` 目录，基础依赖定义在 `config/base.toml`，服务覆盖项位于 `config/services/media.toml`。核心字段：

```toml
[services.media]
service_name = "flare-media"
metadata_store = "media"
metadata_cache = "media_metadata"
object_store = "default"
redis_ttl_seconds = 3600
orphan_grace_seconds = 86400
upload_conversation_store = "upload_conversations"
chunk_upload_dir = "./data/media/chunks"
chunk_ttl_seconds = 172800
max_chunk_size_bytes = 52428800
local_storage_dir = "./data/media"
local_base_url = "http://localhost:50092/files"
cdn_base_url = "http://localhost:50092/files"
```

- `metadata_store`：Postgres/TImescale 连接别名，用于持久化 `media_asset`、`media_reference`。
- `metadata_cache`：Redis 别名，缓存热数据与上传中的瞬态信息。
- `object_store`：对象存储配置名（MinIO/S3），存放媒资文件与派生物。
- `orphan_grace_seconds`：上传完成但尚未建立引用的宽限期（秒），超时后将进入回收任务。
- `upload_conversation_store`：分片上传会话信息保存位置（Redis profile）。
- `chunk_upload_dir`：分片数据暂存目录，最终合并后自动清理。
- `chunk_ttl_seconds`：分片会话存活时间，超过 TTL 会自动过期并清理临时文件。
- `max_chunk_size_bytes`：单个分片的最大尺寸（默认 50MB）。
- `local_storage_dir`：可选的本地缓存目录，在开发环境或转码阶段使用。
- `cdn_base_url`：对外访问基准 URL，可切换至 CDN。

### 对象存储配置约定

`config/base.toml` 中的对象存储 profile 已统一为 S3 兼容实现，通过配置即可适配 MinIO、AWS S3、阿里云 OSS、腾讯 COS、GCP GCS、七牛等后端。关键字段：

- `presign_url_ttl_seconds`：预签名 URL 默认有效期（秒），未显式传参时用于 `GetFileUrl` 与上传返回的 URL。
- `use_presign`：布尔开关；为 `true` 时返回预签名 URL，`false` 时直接拼接直链/CDN 地址，由对象存储本身控制权限。
- `bucket_root_prefix`：桶内的统一根路径前缀（支持多租户、环境隔离），可配置为 `tenant-a/media` 这类多层路径。
- `force_path_style`：控制 SDK 是否使用 path-style 访问（非 AWS 端点通常需要开启）。

对象实际存储路径遵循：`{bucket_root_prefix?}/{file_type}/{yyyy}/{mm}/{dd}/{file_id}[.ext]`。`file_type` 会根据上传元数据或 MIME 自动归类为 `images` / `videos` / `audio` / `documents` / `others`，便于分级治理与生命周期策略编排。

> ⚠️ 访问策略交由对象存储自身管理：是否公有、读写权限、CORS、带宽限制等都在桶策略/网关层配置；`flare-media` 仅按照 `use_presign` 决定返回预签名还是直链。

## 模块结构

```text
flare-media/
├── application/         # 用例编排与服务门面
├── domain/              # 媒资聚合根、仓储接口、领域服务
├── infrastructure/      # Redis、Postgres、对象存储适配器、转码/事件发布
├── interface/
│   └── grpc/            # gRPC Handler/Server（`MediaGrpcServer`）
└── src/main.rs          # 入口，加载配置、注册服务、启动 gRPC
```

## 系统设计

### 上传与去重流程
1. 客户端调用 `PreUpload` 获取上传令牌与直传目标。
2. 客户端上传完成后回调 `CompleteUpload`，服务计算内容指纹。
3. 若指纹已存在：返回既有 `asset_id`，仅新增引用记录（`media_reference`），增量更新 `ref_count`。
4. 若为全新内容：写对象存储、落库 `media_asset` 元数据，写 JetStream 事件触发转码/审核。
5. Redis 负责写入阶段的临时状态与并发控制（防止重复上传冲突）。

### 分片上传与断点续传
- `InitiateMultipartUpload`：服务生成 `upload_id`、推荐分片大小并预留 Redis 会话与本地临时目录。
- `UploadMultipartChunk`：每个分片以 `chunk_index` 标识，服务写入本地临时文件并记录上传进度；重复上传相同分片直接返回成功。
- `CompleteMultipartUpload`：服务按序拼接本地分片、计算哈希并复用 `store_media_file` 流程生成最终媒资；完成后自动清理临时文件与 Redis 会话。
- `AbortMultipartUpload`：显式取消上传，清理会话与分片文件。
- 默认仅针对视频/大文件走分片流程，图片仍可使用单次流式上传。

### 外部应用生命周期示例
以下示例展示业务后台 / 客户端从文件上传到最终删除的完整流程：

1. **准备上下文**
   - 客户端侧准备 `RequestContext`、`TenantContext`、`user_id`、自定义 `metadata`（业务标签等）。
2. **上传文件**
   - 小文件（如图片）：直接调用 `UploadFile`（流式 RPC）传输整文件，获取 `file_id`、`url`、`cdn_url`。
   - 大文件（如视频）：
     1. 调用 `InitiateMultipartUpload`，得到 `upload_id` 与推荐的 `chunk_size`。
     2. 按 `chunk_index` 顺序或并发调用 `UploadMultipartChunk`，可断点续传（重复上传同一 `chunk_index` 自动幂等处理）。
     3. 所有分片上传后调用 `CompleteMultipartUpload`，服务会拼接分片、去重并返回最终媒资信息。
     4. 若用户放弃上传可调用 `AbortMultipartUpload`，服务会清理 Redis 会话和临时分片文件。
3. **业务引用**
   - 上传完成返回的 `file_id` 可直接用于业务数据记录。
   - 如需在多处复用同一媒资，调用 `CreateReference` 建立引用（支持设置 namespace/business_tag 等）；查询时可把 `file_id` 关联到多个业务实体。
4. **读取访问**
   - 获取文件详情：调用 `GetFileInfo`（返回 `FileInfo` 含引用数、哈希、状态等）。
   - 获取访问地址：调用 `GetFileUrl` 获取临时签名链接与 CDN 地址。
5. **更多管理操作（后台系统）**
   - 维护引用列表：`ListReferences` 查看当前媒资被哪些业务引用；`DeleteReference` 移除单个引用（引用数归零后服务会进入宽限期）。
   - 可选：业务后台也可清理未使用资资产，通过自身逻辑或订阅 JetStream 事件。
6. **删除文件**
   - 主动调用 `DeleteFile`（会减少引用数；引用数 > 1 时仅更新计数，==1 时删除对象存储/会话/元数据）。
   - 如需彻底删除所有引用，可在删除前逐个 `DeleteReference` 或调用后台批量处理工具。
   - 服务的后台清理任务（`CleanupOrphanedAssets`）也会清除超过宽限期仍未引用的媒资。

### 用户头像存储示例
用户头像作为特殊的媒体资源，需要特殊的处理方式：

1. **上传头像**
   ```javascript
   // 客户端上传用户头像
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
   
   // 重要：只存储file_id，不存储完整的URL
   const fileId = response.fileId;
   ```

2. **存储头像信息**
   ```javascript
   // 在用户资料中只存储file_id
   await userDatabase.updateUser(userId, {
     avatar_file_id: fileId  // 只存储file_id
   });
   ```

3. **显示头像**
   ```javascript
   // 获取用户资料中的头像file_id
   const user = await userDatabase.getUser(userId);
   const fileId = user.avatar_file_id;
   
   if (fileId) {
     // 获取访问URL
     const urlResponse = await mediaClient.getFileUrl({
       fileId: fileId,
       expiresIn: 3600  // 1小时过期
     });
     
     // 在页面中显示头像
     document.getElementById('user-avatar').src = urlResponse.url;
   }
   ```

4. **头像更新**
   ```javascript
   // 用户更新头像时，先上传新头像获取新的file_id
   const newFileId = newAvatarResponse.fileId;
   
   // 更新用户资料中的头像file_id
   await userDatabase.updateUser(userId, {
     avatar_file_id: newFileId
   });
   
   // 可选：删除旧头像（如果不再需要）
   // await mediaClient.deleteFile({fileId: oldFileId});
   ```

这种方式的优势：
- **安全性**：通过预签名URL控制访问权限和时效
- **灵活性**：可以随时更换CDN或存储后端，只需更新配置
- **可扩展性**：支持不同的访问策略（公开/私有）
- **存储优化**：只存储file_id而不是完整URL，节省存储空间

### 未引用资源管理
- 新资产初始状态为 `PendingReference`，宽限期（默认 24h，可配置）内若未产生引用则进入回收流程。
- 回收策略：删除对象存储与数据库记录，或迁移至冷存储桶并标注过期时间。
- 所有删除/恢复操作记录在审计日志（后续落地至 Timescale/Elastic）。

### 管理接口（供业务后台）
- `ListAssets`：支持按用户、业务域、标签、时间范围分页查询。
- `GetAsset`：返回媒资详情、引用列表、转码/审核状态。
- `CreateReference` / `DeleteReference`：引用增删，驱动 `ref_count` 变化。
- `RestoreAsset` / `DeleteAsset`：回收站管理与彻底删除。
- `UpdateAsset`：审核状态、标签、权限、CDN 策略等更新。
- 事件通知（JetStream）：媒资创建、转码完成、即将清理等。

## 运行与调试

```bash
cargo run --bin flare-media
```

启动前确保依赖服务可用（MinIO/S3、Postgres、Redis、JetStream）。开发阶段可使用 `docker-compose` 启动本地依赖，并设置：

```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/flare_media"
export REDIS_URL="redis://localhost:6379/2"
export MINIO_ENDPOINT="http://localhost:29000"
```

## 监控与指标
- Counters：`media_upload_total`、`media_deduplicated_total`、`media_reference_total`、`media_cleanup_total`
- Histograms：上传完成耗时、指纹计算耗时、转码/审核处理耗时
- Gauges：存储使用量、未引用资产数量、回收队列长度

## 后续扩展
- 引入感知哈希以识别相似内容、秒传支持。
- 媒资版本化：同一 `asset` 的不同转码/规格管理。
- 多级存储策略：热 → 温 → 冷存储的自动迁移。
- 审核/风控联动：对接内容审核平台，对异常媒资自动冻结并推送告警。