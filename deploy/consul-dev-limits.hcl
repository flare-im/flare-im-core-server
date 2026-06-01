# 本地 dev：避免多进程 discovery/心跳把单 agent 打到 429。
# 生产请按容量评估，勿直接 -1。
limits {
  http_max_conns_per_client = -1
}
