# MIG-05 凭证投影接收前置

`TenantProjection.UpsertCredentials` 仅接受已由平台公钥（`FLARE_PLATFORM_PUBLIC_KEY`）验证的 ProjectionWorker，
`token_use=control`、Core audience、签名租户与 app_id 必须精确匹配。员工、用户令牌、
错误 audience、通配 Worker、跨 app、无签名/伪造身份头都不能写入。

共享 `CredentialSnapshot` 验证规范身份、非零且适合 BIGINT 的版本、最多1024把 Key、
1 MiB 存储载荷、重复 Key ID、模式、32字节 hash/pepper 指纹、scope、到期与 CIDR。
材料在 Debug 和错误中脱敏；不保存 token/secret/pepper。

0010 仅增加本库的 `credential_projection_versions` 与 `credential_keys`；与 Social
库的 `flare_credential_*` 不混名。CAS 锁定应用版本行，在同一事务整份替换 Key。
空快照保留版本，旧/同版本不能恢复旧 Key。全局 key_id 冲突整笔回滚，不转移其他
应用的 Key。投影可先于租户到达，不创建或激活租户；未来 Platform 请求仍必须
检查已有运行租户、功能与资源策略。`GetVersions.credentials_version` 包括空快照
和只有凭证、尚无租户的应用，便于完整对账。

本地门禁：`make test-credential-projection` 创建随机独占 PostgreSQL 库，只应用0010，
执行实际接收器的回环 tonic gRPC 与实际仓储，再删除夹具。十一条真实 RPC 覆盖认证/
scope/伪造头/无效新快照不改状态；仓储覆盖版本乱序、空撤销、跨 app Key 冲突回滚、
64路并发最高版本获胜与他应用不变。普通 cargo 中该用例显式 ignored；由专用驱动
实际执行，不能把默认 ignored 计作通过。该门禁不启动整套 Core、Social、Console，
不是九容器或 Platform HTTP 验收。

上线硬前提：0010 的两版真实 Timescale 指纹与迁移回归通过；同步 schema-aware
回滚候选，不能回退到仅认识9条迁移的旧迁移器。当前没有 Control 凭证 outbox、
生命周期 API、网关装配或真实 Platform 业务路由，因此 MIG-05/CRED
仍未验收。Core 消息、会话、Storage 热路径未重构。

## 本地读取/缓存增量

仓储实现共享 `LocalCredentialReader`：单条本库主库MVCC查询读取绑定/材料/版本，
重新校验selector、规范app、模式、必需账号状态；坏行不暴露SQL/JSON详情。
共享Verifier缓存hash-only授予，1s/100ms正负TTL、4096容量、16冷读、64固定分片、
250ms查询/等待上限；命中重验摘要/pepper/app/scope/IP/到期，无过期值容错。
真实gRPC夹具使用随机生成Key在该adapter/verifier正向授权，错app拒绝；三种坏JSON
及default别名绑定拒绝。随后空撤销、等TTL、新Verifier、旧版本重放都不能复活Key。
缓存1s界限只覆盖本地投影提交后的缓存窗口，不覆盖Control投递延迟；此处仍无
HTTP Platform路由，不能称为CRED-02或九容器验收。接入时仍须独立运行租户门禁。

## Gateway 读取 adapter 的基础层归属

只读本库主库 adapter 移入 `flare-im-service-kit::credential_projection`，Capability
接收仓储委托同一个实现。API Gateway 无须导入 Capability writer，Core 不依赖
Social/Control 实现；SQL、MVCC与验证逻辑、0010账本不改。实际独占 PG/gRPC
十一条RPC、Key缓存吊销/重启/坏行及并发回归复跑通过；service-kit/capability lib
为281 passed、9 ignored（凭据用例由专用驱动实际执行，不算默认通过），严格
Clippy、fmt/diff通过。尚无Platform HTTP或新容器候选，MIG-05/06仍未完成。
