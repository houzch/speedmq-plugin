#!/usr/bin/env node
// SwiftMQ 外部进程插件（sidecar）示例 —— Node.js 实现（仅标准库，无 npm 依赖）。
//
// 它实现 sidecar 线协议的插件侧（对应 `pkg/sidecar` 里的 Server 角色）：
// 内核是客户端、本进程是服务端；只监听一个本机地址，等内核来连。
//
// 覆盖：
//   - 握手（Hello / HelloAck）
//   - 心跳（收到 Ping 回 Pong）
//   - 逻辑流：把客户端字节流原样回显
//   - --session-demo：在流上演示内核语义桥
//     （core.authenticate → session.open → declare_queue → publish → consume → 投递回推 → settle）
//
// 用法：
//   node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
//
// 配套文档：swiftmq/docs/plugin-development-nodejs.md
'use strict'

const net = require('net')

// ---- 线协议常量（与 pkg/sidecar/frame.go 一致）----
const KIND = {
  HELLO: 1,
  HELLO_ACK: 2,
  PING: 3,
  PONG: 4,
  CALL: 5,
  REPLY: 6,
  OPEN: 7,
  OPEN_ACK: 8,
  DATA: 9,
  CLOSE: 10,
}

const VERSION = '0.1.0'
const API_VERSION = 'v1' // 必须与内核 pkg/plugin.APIVersion 一致

function log(msg, fields = {}) {
  const extra = Object.entries(fields)
    .map(([k, v]) => `${k}=${v}`)
    .join(' ')
  console.log(extra ? `${msg} ${extra}` : msg)
}

/** SASL PLAIN 响应：\x00<user>\x00<password> */
function plainResponse(user, password) {
  return Buffer.concat([Buffer.from(`\x00${user}\x00`, 'utf8'), Buffer.from(password, 'utf8')])
}

/** 一条流：内核侧的一条客户端连接。读侧用异步队列，写侧直接发数据帧。 */
class Stream {
  constructor(id, send) {
    this.id = id
    this._send = send
    this._chunks = [] // 已到达但还没被消费的字节
    this._ended = false
    this._waiter = null // 等待下一块的 resolve
  }

  /** 由读循环调用：交付一段字节。 */
  push(data) {
    if (this._ended) return
    if (this._waiter) {
      const w = this._waiter
      this._waiter = null
      w({ value: data, done: false })
      return
    }
    this._chunks.push(data)
  }

  /** 由读循环调用：对端关闭了这条流。 */
  async end() {
    if (this._ended) return
    this._ended = true
    if (this._waiter) {
      const w = this._waiter
      this._waiter = null
      w({ value: undefined, done: true })
    }
  }

  /** 取下一块；流结束返回 null。 */
  read() {
    if (this._chunks.length > 0) {
      return Promise.resolve(this._chunks.shift())
    }
    if (this._ended) return Promise.resolve(null)
    return new Promise((resolve) => {
      this._waiter = resolve
    }).then((r) => (r.done ? null : r.value))
  }

  /** 写一段字节（内核会把它转发给客户端）。 */
  write(data) {
    this._send(KIND.DATA, Buffer.concat([u32(this.id), data]))
  }
}

function u32(n) {
  const b = Buffer.alloc(4)
  b.writeUInt32BE(n >>> 0, 0)
  return b
}

/** 一条来自内核的连接：帧解析 + 逻辑流 + 反向调用。 */
class Conn {
  constructor(socket, opts) {
    this.socket = socket
    this.opts = opts
    this.buf = Buffer.alloc(0)
    this.streams = new Map()
    this.pending = new Map() // 反向调用编号 → resolve
    this.nextId = 0
    this.closed = false
  }

  // ---------- 帧收发 ----------

  send(kind, payload) {
    if (this.closed) return
    const body = payload || Buffer.alloc(0)
    this.socket.write(Buffer.concat([u32(1 + body.length), Buffer.from([kind]), body]))
  }

  sendJSON(kind, obj) {
    this.send(kind, Buffer.from(JSON.stringify(obj), 'utf8'))
  }

  // ---------- 反向调用（插件 → 内核）----------

  call(method, params = null, timeoutMs = 10000) {
    const id = ++this.nextId
    const call = { id, method, reverse: true }
    if (params !== null) call.params = params
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id)
        reject(new Error(`反向调用 ${method} 超时`))
      }, timeoutMs)
      this.pending.set(id, (reply) => {
        clearTimeout(timer)
        if (!reply.ok) {
          reject(new Error(`反向调用 ${method} 失败: ${reply.error || ''}`))
          return
        }
        resolve(reply.data)
      })
      this.sendJSON(KIND.CALL, call)
    })
  }

  // ---------- 主循环 ----------

  start() {
    this.socket.on('data', (chunk) => {
      this.buf = Buffer.concat([this.buf, chunk])
      this.drain()
    })
    this.socket.on('error', (err) => log('socket 错误', { err: err.message }))
    this.socket.on('close', () => {
      this.closed = true
      log('内核连接已关闭', { plugin: this.opts.name })
    })
  }

  /** 按帧切分并处理已到达的字节。 */
  drain() {
    while (this.buf.length >= 5) {
      const len = this.buf.readUInt32BE(0)
      if (len === 0) {
        this.socket.destroy()
        return
      }
      if (this.buf.length < 4 + len) return // 帧还没收全
      const kind = this.buf.readUInt8(4)
      const payload = this.buf.subarray(5, 4 + len)
      this.buf = this.buf.subarray(4 + len)
      this.dispatch(kind, payload)
      if (this.closed) return
    }
  }

  dispatch(kind, payload) {
    switch (kind) {
      case KIND.HELLO:
        this.handleHello(payload)
        break
      case KIND.PING:
        this.send(KIND.PONG, null)
        break
      case KIND.CALL:
        // 内核 → 插件的正向调用（如 session.deliver）。异步处理，不阻塞读循环。
        this.handleForwardCall(payload)
        break
      case KIND.REPLY:
        this.handleReply(payload)
        break
      case KIND.OPEN:
        this.handleOpen(payload)
        break
      case KIND.DATA: {
        const id = payload.readUInt32BE(0)
        const s = this.streams.get(id)
        if (s) s.push(payload.subarray(4))
        break
      }
      case KIND.CLOSE: {
        const msg = JSON.parse(payload.toString('utf8') || '{}')
        const s = this.streams.get(msg.stream)
        if (s) s.end()
        break
      }
      default:
        log('收到未知帧类型，断开', { plugin: this.opts.name, kind })
        this.socket.destroy()
    }
  }

  handleHello(payload) {
    const hello = JSON.parse(payload.toString('utf8'))
    if (hello.plugin !== this.opts.name) {
      log('握手被拒：插件名不匹配', { want: this.opts.name, got: hello.plugin })
      this.socket.destroy()
      return
    }
    if (hello.api_version !== API_VERSION) {
      log('握手被拒：插件 API 版本不匹配', { want: API_VERSION, got: hello.api_version })
      this.socket.destroy()
      return
    }
    this.sendJSON(KIND.HELLO_ACK, {
      name: this.opts.name,
      version: VERSION,
      api_version: API_VERSION,
      capabilities: ['net.listen'],
      protocols: ['nodeecho'],
      methods: ['stats'],
    })
    log('握手完成', { plugin: this.opts.name, kernel: hello.kernel_version })
  }

  handleReply(payload) {
    const reply = JSON.parse(payload.toString('utf8'))
    // 只有反向应答（reverse=true）属于我们；正向应答由内核自己消费。
    if (!reply.reverse) return
    const waiter = this.pending.get(reply.id)
    if (waiter) {
      this.pending.delete(reply.id)
      waiter(reply)
    }
  }

  async handleForwardCall(payload) {
    const call = JSON.parse(payload.toString('utf8'))
    let reply = { id: call.id, ok: true }
    try {
      const data = await this.handleMethod(call.method, call.params || {})
      if (data !== undefined && data !== null) reply.data = data
    } catch (err) {
      log('正向调用失败', { method: call.method, err: err.message })
      reply = { id: call.id, ok: false, error: err.message }
    }
    this.sendJSON(KIND.REPLY, reply)
  }

  async handleMethod(method, params) {
    if (method === 'session.deliver') {
      const body = Buffer.from((params.message && params.message.body) || '', 'base64')
      log('收到投递（session.deliver）', {
        queue: params.queue,
        delivery_id: params.delivery_id,
        body: body.toString('utf8'),
      })
      // 结算：投递编号在整条连接上全局唯一，不需要流号。
      await this.call('session.settle', { delivery_id: params.delivery_id, action: 'ack' })
      return null
    }
    if (method === 'stats') return { streams: this.streams.size }
    throw new Error(`未知方法 ${method}`)
  }

  handleOpen(payload) {
    const meta = JSON.parse(payload.toString('utf8'))
    const id = meta.stream
    const stream = new Stream(id, (k, p) => this.send(k, p))
    this.streams.set(id, stream)
    // 先回 OpenAck 再交给处理逻辑：否则内核会一直以为流没打开。
    this.sendJSON(KIND.OPEN_ACK, { stream: id, ok: true })
    this.serveStream(stream, meta)
  }

  async serveStream(stream, meta) {
    try {
      if (this.opts.sessionDemo) await this.sessionDemo(stream.id)
      log('流已打开', {
        plugin: this.opts.name,
        stream: stream.id,
        remote: meta.remote,
        local: meta.local,
      })
      for (;;) {
        const chunk = await stream.read()
        if (chunk === null) break
        stream.write(chunk) // 原样回显
      }
    } catch (err) {
      log('流处理失败', { stream: stream.id, err: err.message })
    } finally {
      this.streams.delete(stream.id)
      // 处理逻辑返回即流结束：通知内核并回收本地状态。
      this.sendJSON(KIND.CLOSE, { stream: stream.id, reason: 'handler returned' })
    }
  }

  async sessionDemo(streamId) {
    // 第一步必须是 core.authenticate：连接的内核操作面在认证前没有身份，
    // 任何 session.open 都会被拒（ACCESS_REFUSED - ... for user ''）。
    const ident = await this.call('core.authenticate', {
      stream: streamId,
      mechanism: 'PLAIN',
      response: plainResponse(this.opts.user, this.opts.password).toString('base64'),
    })
    log('认证通过', { user: ident.user })

    await this.call('session.open', { stream: streamId, vhost: '/' })
    const q = await this.call('session.declare_queue', {
      stream: streamId,
      exclusive: true,
      auto_delete: true,
    })
    await this.call('session.publish', {
      stream: streamId,
      routing_key: q.name,
      message: {
        body: Buffer.from('hello from node sidecar', 'utf8').toString('base64'),
        properties: { content_type: 'text/plain' },
      },
    })
    await this.call('session.consume', { stream: streamId, queue: q.name, prefetch: 32 })
    log('session 演示完成', { queue: q.name })
  }
}

function parseArgs(argv) {
  const opts = {
    addr: '0.0.0.0:19011',
    name: 'node-sidecar',
    sessionDemo: false,
    user: 'guest',
    password: 'guest',
  }
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i]
    if (a === '--addr') opts.addr = argv[++i]
    else if (a === '--name') opts.name = argv[++i]
    else if (a === '--session-demo') opts.sessionDemo = true
    else if (a === '--user') opts.user = argv[++i]
    else if (a === '--password') opts.password = argv[++i]
  }
  return opts
}

function main() {
  const opts = parseArgs(process.argv)
  const idx = opts.addr.lastIndexOf(':')
  const host = opts.addr.slice(0, idx) || '0.0.0.0'
  const port = Number(opts.addr.slice(idx + 1))

  const server = net.createServer((socket) => {
    socket.setNoDelay(true)
    log('内核已接入', { plugin: opts.name, peer: `${socket.remoteAddress}:${socket.remotePort}` })
    new Conn(socket, opts).start()
  })
  server.on('error', (err) => {
    console.error(`监听失败: ${err.message}`)
    process.exit(1)
  })
  server.listen(port, host, () => {
    log('sidecar 已启动', { plugin: opts.name, addr: opts.addr, version: VERSION })
  })
}

main()
