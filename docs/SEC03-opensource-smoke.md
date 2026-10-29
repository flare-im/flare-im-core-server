# SEC-03 开源栈独立容器验收

```sh
python3 scripts/smoke_opensource_container.py --dry-run
python3 scripts/smoke_opensource_container.py \
  --core-image localhost/flare/core:w14 \
  --migrate-image localhost/flare/core-migrate:w14
make test-opensource-runner
```

这是本地验收工具，不是部署器。需要 Podman、OpenSSL、Python3、已缓存且同版本的
Core/迁移镜像、基础设施镜像，以及同级 Core SDK。默认镜像选择对应本地 Wave14，
每次在启动前解析为不可变 image ID，写入证据；不 pull、不构建镜像、不操作远端。
宿主机8080/50050必须空闲，工具不会停止占用端口的进程。SDK Cargo 离线运行，
使用强制临时 `FLARE_SMOKE_DATA_ROOT` 与显式回环 WS/HTTP；不重定向 HOME，
不读取用户原有 SQLite，不覆写 `.dev-token-secret`。

服务器是真实 Core 监管容器，依赖随机独占 PostgreSQL/Redis/NATS/RustFS 容器。
新库通过真实 Core 迁移器全量建库、schema/checksum验收，不 baseline adopt；运行
账号仅 DML、无迁移账本写权限。每轮独立 Cell 密钥，NATS 专网仅 Core/NATS且使用
Core凭据。没有 Social、Control、Management、Console/SFU 或安装库依赖。
这不是 SaaS 九容器全栈：SDK 是宿主机进程，服务和基础设施是容器。

工具调用原 `smoke_opensource.sh` 的六个真实 SDK 示例。没有 GNU timeout/gtimeout
时使用 Python 标准库的每例300秒进程组看门狗，整套另有2100秒上限。超时是真失败，
不无限等待或遗留 Cargo/SDK 子进程。每例日志写专属0700目录，输出证据脱敏且0600。
结束后再次检查 Core 健康，并直接查本轮私有 PostgreSQL：E2EE 消息须真正落盘，
类型为20 Placeholder且 content/extra/extensions不含演示明文，不能仅以本地缓存
和“打印了成功”认定服务端加密证据。
四个发送示例先订阅匹配消息的事件、发送后在30秒内等 `Persisted` ACK；本地
入队和 `BrokerAccepted` 不视为成功，拒绝/失败/超时均返回非零。此前两个示例
在本地入队后即退出，六例退出码通过却只有两个发送者落盘，真实数据库门禁
检出并保留了失败证据。修复后必须有四个具名发送者真实落盘；不降低判据。

音视频通话是托管插件 `flare.call`，不属于开源 Core 验收：夹具只在真实启动日志
同时证明零 Hook 和零发现插件后运行示例，示例不探测任何插件。本工具不向 SDK
注入 Cell 私钥，也不关闭 Capability 验签。

失败/超时同样清理本轮专有名字且专有label的容器、匿名卷和网络。inspect失败不
当作“已不存在”；标签不符则不删除，cleanup_errors非空不能验收。临时env/PEM在
确认容器清理后删除；若引擎不可用则保留0600材料和精确owner以便安全收尾。
`logs/opensource-smoke/<owner>/`仅持久化已脱敏report、容器日志和各例日志，不复制
凭据/SQLite。报告包含镜像ID、实际源码sha256、结果与密钥未变/清理状态。

范围限制：六例是具名进程级smoke，并非SDK全部API/事件/断线/隔离的强断言测试。
其他API完整回归和SEC套件仍须按PLAN单独执行，不能用本工具给MIG-05/06或CRED
任务勾选。
