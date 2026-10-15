# Core 源事务审计（开发期功能）

## 已接正常入口

Capability 的以下员工写操作由实际 PostgreSQL 拥有方同时提交业务事实与 `core_audit_outbox`：

- Hook 配置保存、更新、启停、删除。
- 用户能力授予、撤销、应用能力开关。
- 插件 endpoint 登记、注销。

正常 RPC 仍验证 `kid=gateway`、Staff、精确应用和对应权限码。Hook 内部调用保留完整的已验证 Context，不再重建不带签名的 Staff；`GenericRequest.request_id` 不能覆盖签名 request_id。被等待的命令通过任务作用域传播来源，取消/嵌套退出后恢复，不传播到另起的任务。不以普通用户或插件宿主 Service 自注册伪造员工。

同事务证据仅包含可信 actor/application/permission/request_id、source、聚合类型/ID、动作、时间及 `outcome=committed`，不记录消息正文、Hook transport、headers、metadata、凭证或令牌。审计入队失败则回滚该业务写。无变化的 Hook 状态、缺失授权撤销和缺失登记注销不追加效果；save/upsert、授权续期和开关更新实际写入的时间/事实仍记录。原本地 `capability_audit_log` 是额外观察日志，不替代出箱证据。

Hook 更新现在真正保存名称；数字 ID 删除直接按不可变 ID 删除，不再拿早先读出的可变名称定位另一行。存储写错误不向 RPC 回显 SQL/driver/行内容。应用/权限拒绝仍由正常入口负责，任务作用域不是鉴权替代。

## 持久投递

`flare-im-service-kit::audit_effects::durable` 提供源事务入队与独立发布。Capability 装配正常数据库池及 `FLARE_AUDIT_NATS_URL`，启动与停机由现有服务生命周期管理，无额外常驻容器。

每次锁一条到期未投递行，`FOR UPDATE SKIP LOCKED`；发布等待最多两秒，失败持久退避至最多60秒。只有 Broker ACK 后才写 delivered；取消、进程退出或提交失败可再次投递相同 event UUID 和相同存储字节。消费端继续幂等聚合。定时清理只处理30天前已投递行，批次最多256，未投递证据不按TTL丢弃。配置未启用 Broker 时不启动发布任务，但源事务仍留队列；已配置却启动连接失败则启动报错。

Hook DB 提交在运行时 reload 之前。reload 失败意味着运行态尚未确认，不得删除已经提交事实的审计证据；调用结果与源效果分别表达，不能把RPC错误一概解释为没有业务提交。

`migrations/0012_20261015_core_audit_outbox.sql` 是最终初始化的源 DDL 输入。本批不在存量开发库应用迁移，不做旧版本兼容/回切，也没有安装新服务。R6源规格已声明该 DDL、Capability 发布用途和 Core 未覆盖列表，仍 `installable=false`。

## 相关验证与边界

默认 library 回归：admin-gateway49、Capability172、service-kit135通过，合计356；15个 ignored 单列，不视为已执行。

两个新增 ignored 方法显式使用拥有方本机PG18，在新建 nonce 库内执行并最终删除：

1. 正常生产 Capability RPC 方法（进程内，签名 metadata 真实验证）覆盖九类成功写、每类审计尾故障的业务表/证据回滚、应用拒绝、签名 request_id 和权限归因、实际改名、无变化重复、插件内存读侧与提交一致，以及提交后 reload 失败证据保留。
2. 真实 PG 源作用域与队列覆盖无签名上下文不造Staff、应用不符回滚、子任务不继承/作用域退出、发布失败退避、取消释放、同ID/字节重试及确认后不再重复。发布器受控，不是实际 JetStream 停启/总线整链。

相关三包严格 all-targets Clippy、格式/diff、Management2.4.0/192既有契约与R6源装配检查另记计划。本批未改 API DTO/权限集合，不重跑未受影响的 Web/Flutter 或全仓套件。初轮 SQLx UUID特性缺失、测试 import/字段及 Clippy 诊断已修复，不计通过。

## 尚未收口

- Online 会话/设备踢出的实际拥有方效果仍有旧 best-effort 生产路径。
- Media 对象 ACL 和对象清单仍明确未实现；孤儿清理的最新范围见下一节。
- Platform机器系统消息的 Writer 持久提交审计仍待；API accepted 不是持久化或客户端送达。
- 正常RPC→真实Broker→Platform聚合→客户端效果由最终统一初始化后整体验收，不用本批进程内/受控发布器测试代替。

因此本批仅完成 Capability 员工源事务审计，不能勾完 R4-F05/R4、R5/R6或全部计划。

## Media：原始 Staff 与逐资产源效果

正常 Core admin-gateway 媒资管理不再把员工转换为 TenantAdmin，也不再读取用于此路径的 Cell 私钥。`MediaCaller::staff` 只接受带原令牌的已验证 gateway Staff Context，逐跳转发同一断言，不延长期限、不丢失权限或重签。普通终端 User 媒资调用仍按原机制每次签发用户内部断言。

Media 生产签名层将 Staff 固定到 `kid=gateway`，清理/ACL 要求 `core:medias:write`，清单/桶读取要求 `core:medias:read`；均要求具体应用。有效的 User、System、TenantAdmin、其他路由权限或错误 Staff key id 都不能代用。Staff 也不能因为 actor ID 恰好等于文件 owner 而借普通用户读取/删除私有文件。支持公钥环输入，缺失或非法公钥仍拒绝启动。

孤儿清理沿用拥有方的逐行锁定、孤儿条件重验和领取事务：元数据 DELETE 后、外部存储调用前，在同一个尚未提交的 PG 事务写 `aggregate=media_asset_metadata/action=reclaim` 的源证据；外部删除成功后提交领取。证据尾失败不删文件；外部删除失败或领取取消则回滚元数据与证据，仍可重试。无元数据/存储配置返回不可用，而不是成功空列表。`dry_run=true` 在 Gateway 和直接 Media RPC 两处都拒绝，绝不执行删除。

这是**每个资产的元数据与证据**原子性，不是 PG 与文件系统/S3 的分布式事务，也不是整批原子。若对象已删除而 PG 提交失败或请求取消，下次仍依靠幂等对象删除重试；同批前一个资产已提交、后一个失败时，前者证据保留。已提交后缓存失效失败也不能抹去源事实。Gateway 不再按成功返回的 ID 列表补 best-effort effect，否则部分失败会丢失已提交证据。

Media 使用既有 PG 池与自己的服务生命周期启动共享 durable 发布器；消息 ID、重试、ACK 和清理边界与上方 Capability 相同，不增加容器。R6 源输入声明 Media 公钥环/审计发布权限以及最终 outbox DDL，仍不可安装。ACL 和对象清单保持明确 `not_implemented`，没有伪造空列表或成功；本批不是完成所有 Media 功能。

### 本地相关验证

- admin-gateway 默认与集成共118通过（49 library、69 integration）；Media74与service-kit136 library通过，共328。11个ignored另列，其中新增 Media 功能方法已显式运行，其余不追认执行。Media 的11个HTTP契约测试用受控下游；不是实际 Media gRPC 整链。
- 新增一个真实 PG18 + 生产签名层/Media gRPC + 实际本机文件功能方法通过，使用全新nonce库并清理。覆盖精确权限/应用隔离、直接 dry-run 拒绝、审计尾失败不删文件、领取取消释放、重复不追加、批次部分提交及存储故障后重试，核对实际行/文件/证据与秘密字段缺失。夹具元数据由仓储准备，不是上传流程、S3或真实Broker证明。
- 三个相关包 all-targets 严格Clippy、格式/diff、Management24契约、2.4.0/192源锁与Web生成类型检查、R6初始化源/装配26通过。仅更新旧私钥错误说明、Staff权限和dry-run描述，DTO/权限集合不变；没有运行未受影响Web视觉/Flutter套件。
- 首轮快照仍包含已删除的私钥错误说明，已按当前生产源更新并复验；新增缺配置断言初轮误用非Option错误码、Clippy发现测试clone/import问题均已修复，失败不计通过。

本批没有操作存量数据库、安装/替换运行制品、启动容器/VM或刷新用户页面。Online/Writer、Media ACL/清单及最终实际整链仍待，R4-F05保持部分。
