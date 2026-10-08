# speedmq-plugin

SpeedMQ **外部进程插件（sidecar）** 的参考实现集合 —— 5 种语言，**零第三方依赖**，全部实测跑通。

> 用途：不改内核、不重编内核，用**任意语言**给 [SpeedMQ](https://github.com/houzch/speedmq) 扩展协议能力。
> 每个示例都完整走通：**握手 → 认证 → 内核语义桥（队列/发布/消费）→ 投递回推 → 结算 → 客户端字节流回显**。

[English](README.en.md) | 简体中文

---

## 这是什么 / 不是什么

| | |
| --- | --- |
| **是** | sidecar 线协议的**插件侧**参考实现（Python / Node.js / PHP / Java / Go），可直接当模板改 |
| **是** | Python / Node.js / PHP / Java 四份**从零实现了线协议**（分帧、握手、心跳、逻辑流、反向调用），因此也可以当协议规格读 |
| **是** | Go 一份基于**官方 SDK**（`pkg/sidecar`），只要实现三个方法 —— 演示"有 SDK 时能有多短" |
| **不是** | SpeedMQ 本体。插件要跑起来，内核侧必须按配置"声明"它（见下文） |
| **不是** | 官方内核插件。内核内置协议插件（AMQP 0-9-1 / MQTT）不走这套机制 |

---

## 目录结构

```
speedmq-plugin/
├── python/sidecar_plugin.py     # Python 3（仅标准库）
├── nodejs/index.js              # Node.js（仅标准库，无 npm 依赖）
├── php/sidecar_plugin.php       # PHP 7.4+（仅标准库，无 composer 依赖）
├── java/SidecarPlugin.java      # Java 11+（单文件，仅 JDK；自带极简 JSON）
├── go/main.go                   # Go 1.24+（基于官方 SDK pkg/sidecar，不手写线协议）
├── go/go.mod
├── LICENSE                      # Apache-2.0
└── NOTICE
```

---

## 快速开始（以 Python 为例）

下面是最短的**可跑通**路径：插件跑在宿主机，内核跑在容器里（内核来连宿主机）。
生产上"插件与内核同容器"的打包方式见 §打包进镜像。

### 1. 起插件（宿主机）

```bash
python3 python/sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
# 期望日志：sidecar 已启动 plugin=py-sidecar addr=0.0.0.0:19001 version=0.1.0
```

> 监听 `0.0.0.0` 而不是 `127.0.0.1`，否则容器里的内核连不上宿主机。

### 2. 内核侧：声明插件

在 SpeedMQ 配置文件（`speedmqd.json`）里加一段。注意 `address` 用
`host.docker.internal`（Docker Desktop 提供的"宿主机"域名），`spawn` 留空
（进程已经由你自己起好，内核只连不拉）：

> ⚠️ 下面的配置是**标准 JSON，不能带注释**（内核用 Go 的 `encoding/json` 解析）。

```json
{
  "users": {
    "guest": {
      "password": "guest",
      "tags": ["administrator"],
      "remote_access": true,
      "root": true
    }
  },
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://host.docker.internal:19001",
        "spawn": [],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `remote_access: true`：内核在容器里时，客户端与插件都来自**非回环**地址，默认账号会被拦下。
- `address` 是**内核去连你**的地址（内核是客户端）；`spawn: []` 表示只连不拉。
- `prefix` **必须非空**：内核据此把连接判给这个协议（见下文"工作原理"第 3 条）。
- `listeners` 是**对外端口，由内核打开**（客户端连的是内核，不是你）。

### 3. 起内核（容器）

```bash
docker run -d --name speedmq -p 5672:5672 -p 15672:15672 -p 19002:19002 \
  -v "$PWD/speedmqd.json:/etc/speedmq/speedmqd.json:ro" houzch/speedmq:1.1.01 \
  -config /etc/speedmq/speedmqd.json -log-level info
```

### 4. 验证

```bash
# 插件应已接入并处于 enabled
curl -u guest:guest http://127.0.0.1:15672/api/plugins

# 客户端连的是**内核**的端口 19002；首字节必须是协议前缀 "PY"
printf 'PYhello\n' | nc 127.0.0.1 19002
# 期望输出：PYhello
```

`-session-demo` 打开时，插件还会在每个连接上演示内核语义桥（认证 → 声明队列 → 发布 → 消费 → 结算），
日志会打印 `认证通过` / `session 演示完成` / `收到投递（session.deliver）`。

### 打包进镜像（可选，生产常用）

若要让内核自己把插件拉起来（`spawn`），插件可执行文件与运行时必须**在内核容器里**：

```dockerfile
FROM houzch/speedmq:1.1.01

USER root
# 运行插件需要该语言的运行时；alpine 基础镜像默认没有
RUN apk add --no-cache python3
COPY python/sidecar_plugin.py /opt/speedmq-plugin/python/sidecar_plugin.py
USER speedmq
```

对应配置改成进程内地址 + `spawn`：

```json
"sidecar": {
  "address": "tcp://127.0.0.1:19001",
  "spawn": ["python3", "/opt/speedmq-plugin/python/sidecar_plugin.py",
            "-addr", "0.0.0.0:19001", "-name", "py-sidecar"],
  "protocols": [{ "name": "pyecho", "prefix": "PY",
                  "listeners": [{ "name": "pyecho", "addr": ":19002" }] }]
}
```

> 也可以把插件做成**独立容器**：`spawn: []`、`address: "tcp://<服务名>:19001"`，
> 插件监听 `0.0.0.0`；同样记得把 `listeners` 的端口映射出来。

---

## 各语言一览

| 语言 | 运行要求 | 启动命令 | 文件 |
| --- | --- | --- | --- |
| Python | Python 3.8+ | `python3 sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo` | [`python/sidecar_plugin.py`](python/sidecar_plugin.py) |
| Node.js | Node 16+ | `node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo` | [`nodejs/index.js`](nodejs/index.js) |
| PHP | PHP 7.4+ | `php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo` | [`php/sidecar_plugin.php`](php/sidecar_plugin.php) |
| Java | JDK 11+ | `javac -encoding UTF-8 -d classes SidecarPlugin.java` 然后 `java -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo` | [`java/SidecarPlugin.java`](java/SidecarPlugin.java) |
| Go | Go 1.24+ | `go build -o go-sidecar .` 然后 `./go-sidecar -addr tcp://0.0.0.0:19041 -name go-sidecar -session-demo` | [`go/main.go`](go/main.go) |

五个示例的**协议行为完全一致**，差别只在"有没有官方 SDK"与各语言的并发模型：

| 语言 | 并发模型 | 需要特别注意 |
| --- | --- | --- |
| Python | 每连接一个读线程 + 每流一个处理线程 | 写帧要加锁；正向调用（`session.deliver`）必须丢到独立线程，否则等待反向调用应答会死锁 |
| Node.js | 单线程事件循环 + 异步队列 | 读到的 chunk 与帧边界无关，必须自己累积缓冲；不要在帧切分函数里 `await` |
| PHP | 单线程 + **可重入帧泵**（无多线程） | 反向调用期间要一边读帧一边分发，因此分发函数必须可重入；PHP 7.4 不支持 `mixed` 返回类型 |
| Java | 每连接一个读线程 + 每流一个处理线程 | JDK 无 JSON API（示例自带极简实现）；源码注释里也不能出现 `\uXXXX`；lambda 捕获需 effectively final |
| Go | goroutine（每条流一个）+ SDK 内部多路复用 | **用官方 SDK，不必手写分帧/握手/心跳**：只实现 `sidecar.Handler` 的三个方法（`Hello` / `Call` / `Open`） |

> **为什么只有 Go 示例这么短**：SpeedMQ 是 Go 项目，因此有官方 SDK
> [`pkg/sidecar`](https://github.com/houzch/speedmq/tree/master/pkg/sidecar)（服务端角色）。
> 其它语言没有 SDK，所以那四份示例各自实现了线协议 —— 也正因如此，把那四份**当协议规格读**最直观。
>
> Go 示例需要能取到 speedmq 模块（`go mod tidy` 会去拉 `github.com/houzch/speedmq`）。
> 离线/内网环境下，用 `go work` 或 `replace` 指向本地的 speedmq 克隆即可：
>
> ```
> go mod edit -replace github.com/houzch/speedmq=<本地 speedmq 目录>
> ```

---

## 工作原理（先建立正确心智模型）

```
客户端 ──► 内核监听端口 ──嗅探前缀──► 建流 ──► 你的插件进程（本机 TCP 服务端）
                                                │
                            反向调用 core.authenticate / session.*  ◄─┤
                            （插件 → 内核：队列 / 路由 / 权限 / 确认）   │
                                                    内核 ──投递回推──►│
```

要点（最容易搞错的三条）：

1. **你的插件进程是服务端**：它只监听一个**本机地址**，等**内核来连**（`sidecar.address`）。
2. **对外业务端口由内核开**：客户端连的是内核的端口（`protocols[].listeners`），字节由内核代理给你。
3. **`prefix` 必须非空**：接入层只按"嗅探前缀"决定连接归谁；`prefix` 为空表示**不参与嗅探**，
   这类连接**不会**被交给插件。因此即使是"专属端口"，也要给一个非空前缀（示例里当作协议的魔法头）。

---

## 线协议速查

**帧格式**（所有帧统一，网络字节序）：

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（含 kind 字节）；单帧上限 16 MiB。
```

**帧类型**：

| kind | 名称 | 方向 | 载荷 |
| --- | --- | --- | --- |
| 1 | Hello | 内核 → 插件 | JSON |
| 2 | HelloAck | 插件 → 内核 | JSON（`deny` 非空 = 拒绝接入） |
| 3 / 4 | Ping / Pong | 内核 → 插件 / 插件 → 内核 | 空（内核默认每 2s 一次，8s 内须回） |
| 5 / 6 | Call / Reply | 双向 | JSON（`reverse` 区分方向，见下） |
| 7 / 8 | Open / OpenAck | 内核 → 插件 / 插件 → 内核 | JSON（一条流 = 一条客户端连接） |
| 9 | Data | 双向 | `u32 BE 流号` + 原始字节（**不 base64**） |
| 10 | Close | 双向 | JSON |

**关键约定**：

- **握手**：内核先发 `Hello`，插件回 `HelloAck`；内核校验 `name`（须等于配置里的插件名）与
  `api_version`（当前 `v1`）。
- **两个 ID 空间**：`Call` 的 `reverse` 区分"内核 → 插件"（false）与"插件 → 内核"（true），
  两个方向各自从 1 自增，因此 `Reply` **必须回带 `reverse`**。
- **顺序**：`core.authenticate` → `session.open` → 其他 `session.*`。
  未认证就开会话会被拒（`ACCESS_REFUSED ... for user ''`）。
- **投递**：内核把消费投递以**正向调用** `session.deliver` 推给插件，插件处理后用
  `session.settle` 结算（`ack` / `requeue` / `reject`；投递编号全局唯一）。
- **不丢消息**：流结束或插件断开时，未结算的投递一律**回队**。

**反向调用方法**（插件 → 内核）：

| 方法 | 说明 |
| --- | --- |
| `core.authenticate` | 认证（必须先做）；参数 `{stream, mechanism, response}`，`response` 为 base64 字节 |
| `session.open` / `session.close` | 打开 / 释放某条流上的 vhost 会话 |
| `session.declare_exchange` / `session.delete_exchange` / `session.bind_exchange` / `session.unbind_exchange` | 交换机增删与绑定 |
| `session.declare_queue` / `session.delete_queue` / `session.bind_queue` / `session.unbind_queue` / `session.purge_queue` | 队列增删、绑定与清空 |
| `session.publish` / `session.get` / `session.consume` / `session.cancel` / `session.settle` | 发布、拉取、消费与结算 |
| `session.deliver`（**正向**） | 内核 → 插件的投递回推 |

---

## 写你自己的插件（清单）

1. 起一个本机 TCP/Unix 服务端（内核会来连）。
2. 按上面的帧格式收发；先处理 `Hello` → 回 `HelloAck`。
3. 收到 `Ping` 回 `Pong`。
4. 收到 `Open` → 回 `OpenAck` → 开始处理这条流；`Data` 是 `流号 + 原始字节`。
5. 要用内核语义（队列/发布/消费）就先 `core.authenticate`，再 `session.open`，然后按需调用。
6. 在配置里声明：`plugins.<插件名>.sidecar`（`address` / `spawn` / `restart` / `protocols`）。
7. 验证：`GET /api/plugins` 看状态；用真实客户端连 `listeners` 里的端口实测。

---

## 实测记录

示例不是"看起来能跑"——下面这些链路都在本机真实执行过（详见各语言文件头部注释与
[上游分语言文档](https://github.com/houzch/speedmq/tree/master/docs)）：

```
环境：Windows + Python 3.12 / Node 24 / PHP 7.4 / JDK 25 / Go 1.24
内核：SpeedMQ 1.1.01（Docker 容器），插件：宿主机进程（内核经 host.docker.internal 连接）

/api/plugins → <插件名> state=enabled
客户端          printf 'PYhello\n' | nc 127.0.0.1 <内核端口>   → 回显 PYhello
（Go 示例同理：printf 'GOhello\n' → 回显 GOhello）
插件日志        握手完成 → 认证通过 user=guest → session 演示完成
                → 流已打开 → 收到投递（session.deliver） body=hello from <语言> sidecar
```

---

## 常见问题

| 现象 | 原因与处理 |
| --- | --- |
| `/api/plugins` 里状态 `failed`，原因含"插件名不一致" | 配置里的插件名 ≠ `HelloAck.name` |
| 状态 `failed`，原因含"API 版本不匹配" | `HelloAck.api_version` ≠ 内核 `APIVersion`（当前 `v1`） |
| 状态 `down` | 插件进程崩了/连接断了；`restart=always` 会重连，`never` 需人工拉起 |
| 客户端连上端口后立刻断开 | `prefix` 为空或首字节不匹配前缀（见上文第 3 条要点） |
| `ACCESS_REFUSED ... for user ''` | 没先 `core.authenticate` |
| 反向调用报"流 N 尚未打开会话" | 顺序要 `core.authenticate` → `session.open` → 其他 |
| 内核在容器、插件在宿主机，连不上 | `address` 用 `tcp://host.docker.internal:<port>`，插件监听 `0.0.0.0` |
| 账号从别的机器连不上 | 用户默认只允许回环登录，需要 `remote_access: true`（仅本地开发用） |
| 内核容器起不来 / 状态停在 `created` | 宿主机 `5672` / `15672` 可能已被占用；换映射端口，如 `-p 15680:15672 -p 5673:5672` |

---

## 与上游的关系 / 进一步阅读

- **SpeedMQ 内核**：<https://github.com/houzch/speedmq> —— 线协议的权威定义在 `pkg/sidecar`。
- **插件 SDK**（Go）：`github.com/houzch/speedmq/pkg/sidecar`（插件侧服务端）+ `pkg/plugin`（插件契约）。
  本仓库的 [`go/main.go`](go/main.go) 就是用它写的。
- **分语言开发指南**（上游 `docs/`）：`plugin-development.md`（总览 + 协议规格）、
  `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`。
- **另一份 Go 参考实现**：上游测试工程里的 `test/integration/echosidecar`（与本仓库 `go/` 同源思路，
  区别是它带两个控制面方法 `stats` / `set_greeting`，用于内核侧的集成测试）。

---

## 贡献

- 修 bug / 提改进请说明：语言、运行时版本、复现步骤，以及期望行为。
- 新增一门语言时，请对齐现有示例的**行为**（同样的握手/认证/语义桥/结算），
  而不是只对齐代码结构。
- 提交前请确保你的示例在本机真实跑通（握手 + 认证 + 投递结算 + 字节流回显），
  并在文件中写清"需要什么运行时、怎么验证"。

---

## 许可

[Apache License 2.0](LICENSE)，另见 [NOTICE](NOTICE)。
