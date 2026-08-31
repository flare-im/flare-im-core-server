# flare-core-web-app 的 nginx 参考配置

web 端要代理四条通路。前三条常规，第四条有坑。

| 路径 | 后端 | 说明 |
|---|---|---|
| `/` | 静态产物 | SPA，`try_files` 回落 `index.html`，否则刷新子路由 404 |
| `/ws` | 接入网关 `60051` | 必须显式 Upgrade；`proxy_read_timeout 3600s` + `proxy_buffering off`，否则 IM 长连接被默认 60s 掐断 |
| `/api/` | HTTP 网关 `50050` | `proxy_pass` **带**结尾 `/` 剥掉前缀。SDK 会拼成 `/api/api/v1/...`，剥掉后端拿到 `/api/v1/...` |
| `/flare-media/` | 对象存储 `29000` | 见下 |

## 对象存储不能加路径前缀

S3 预签名的 Signature 覆盖完整 canonical URI。若用 `/storage/` 之类的前缀再由
nginx 剥掉，对象存储拿到的路径与签名时不一致，**必然 403**。所以按桶名直挂根
路径，且 `proxy_pass` 结尾**不带** `/`（原样透传）。`Host` 同理必须原样转发，
SignedHeaders 里含 host。另需放开 `client_max_body_size`（默认 1m，媒体必超）。

同源部署还顺带免了 CORS 预检。

## 服务端配套

预签名 URL 是给浏览器直接请求的，必须外部可达：

```
FLARE_S3_ENDPOINT=http://127.0.0.1:29000          # 服务自身访问对象存储
FLARE_S3_PUBLIC_ENDPOINT=https://im.example.com   # 只用于生成预签名 URL
```

两者分开是必要的：合成一个时只能二选一——填内网地址浏览器连不上，填公网地址
则服务启动时的桶检查要走公网 TLS。分离是安全的：SigV4 不签 scheme，预签名也
是纯离线计算。

## HTTPS

语音录制走 `getUserMedia`，**只在安全上下文可用**；明文 HTTP 下
`navigator.mediaDevices` 根本不存在，按钮点了毫无反应也无提示。前端的同源推导
会在 https 页面自动把 WebSocket 升到 `wss://`，无需额外配置。

证书优先用域名 + 受信 CA。只有 IP 时自签，SAN 必须写 `IP:`（写 `DNS:` 浏览器
用 IP 访问时不认），且此时对象存储直传会失败——`aws-sdk-s3` 的
`default-https-client` 用编译进二进制的 webpki 根证书，不认自签，只能靠
`FLARE_S3_PUBLIC_ENDPOINT` 让服务自身继续走内网明文。

### 只有 80 端口可用时

若 443 被安全组挡住（表现：TCP 能连、数据被静默丢弃，而服务器本机经公网 IP
访问却正常），可用 `stream` 的 `ssl_preread` 在 80 上同时承载明文与 TLS，
见 `port80-mux.conf`。这是权宜之计，能开 443 就开。
