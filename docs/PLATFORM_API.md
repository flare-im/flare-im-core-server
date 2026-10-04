# Core Platform API：Service Account 只读入口

Core API Gateway新增独立Platform分区，不改消息/会话数据模型与client鉴权。
当前 `GET /platform/v1/apps/{app_id}/conversations/{conversation_id}/participants`
要求 `platform.conversations.read`，仅返回活跃成员ID、角色、next_cursor和has_more。
未知会话返回空页，偏好/attributes/内部total/version不输出。limit默认50、1–100；
仅keyset游标，拒绝历史数字OFFSET、畸形/超过512字节游标、未知/重复参数和请求体。

本库主库只读credential pool → 本地有界Key verifier → 独立strict active tenant
cache → Cell签名的真实Service Account/Key/request → 既有ConversationReadService。
不使用User或员工控制令牌、不填伪造user_id、不访问Control，不依赖Social运行时。
HTTP契约聚合真源位于Social `contracts/platform/platform-v1.openapi.json` 1.1.0。

默认 `FLARE_PLATFORM_API_ENABLED=false` 不注册路由；开启需独立
`FLARE_API_KEY_PEPPER`、`FLARE_INTERNAL_CTX_PRIVATE_KEY`、本库主库
`FLARE_API_GATEWAY_TENANT_RUNTIME_POSTGRES_URL`及0010凭据表，缺失启动拒绝。
`FLARE_API_GATEWAY_GRPC_CONVERSATION_SERVICE_URL` 是直接私网HTTP(S) authority；
不支持userinfo/path/query或借用client discovery缓存。Conversation保持生产
`FLARE_REQUIRE_SIGNED_READ_CTX=true` 与对应Cell公钥。

Platform的只读PG4连接/250ms获取、独立Channel（32并发、32缓冲、250ms建连、2s
请求、1MiB解码）、32HTTP并发/零排队/5s整请求、IP60/s/Key20/s/4096桶及熔断
均不与client共享。server-core共享的是机制不是状态。socket-rooted来源IP仅信
显式 `FLARE_PLATFORM_TRUSTED_PROXY_CIDRS`；无默认私网、无trust-all，首个不受信
右侧hop胜出。成功/错误/404/405均no-store/no-cache，远端/SQL正文不向外传播。
部署须另计最多4条PG连接和配置真实Edge/内部可信链；单实例TTL不代表全局SLO。

## 本机可复跑组件验证（不是九容器验收）

构建 `cargo build --locked -p flare-api-gateway -p flare-conversation --bins`；测试
环境安装同级Social `scripts/requirements-contract.txt`，运行
`python scripts/test_platform_conversation.py --contract ../flare-social/contracts/platform/platform-v1.openapi.json`。
只允许本机PG，随机独占库/回环端口/Cell与pepper，秘密留在0600临时配置和进程
内存，finally清理本轮库、进程组与临时目录。采用基线三张相关表的原样DDL+
0010；不是完整Core/Timescale迁移验收。Projection与业务数据是明确SQL夹具。

最终两轮32条契约响应通过，末轮额外实际client路由对API Key为401、Conversation
停机时Gateway health为200；真业务Service分页/并列排序/成员过滤/两租户同会话ID/
参数负测/未知不建号/暂停/本地撤销及重启/停机恢复/可信链/关闭桥404/缺表拒绝/
秘密扫描通过。Gateway91个lib测试与相关严格Clippy通过。独立真PG凭据驱动还验
11条签名RPC、64并发、撤销缓存；新增连接池断言4连接全只读/写入25006/第五拒绝/
缺表拒绝。没有弱化Core既有客户端、同步或权限模型。

尚缺Social统一Gateway/Edge转发、安装预算、Control实际投递/恢复、真实容器
压测和安全桥回切；不能据此勾选MIG-05/06或整个CRED。
