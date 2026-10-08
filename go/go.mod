module github.com/houzch/speedmq-plugin/go

go 1.24

// 插件只依赖 SpeedMQ 的官方 SDK（第一方，非第三方）：
//   - pkg/sidecar：sidecar 线协议的插件侧服务端（分帧/握手/心跳/多路复用）
//   - pkg/plugin ：插件 API 契约（APIVersion、Capability 等常量）
// 除它之外，本示例只用标准库。
require github.com/houzch/speedmq v1.1.1-0.20261003100534-5b24a997e0e0
