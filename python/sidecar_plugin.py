#!/usr/bin/env python3
"""SwiftMQ 外部进程插件（sidecar）示例 —— Python 实现（仅标准库）。

它实现 sidecar 线协议的插件侧（对应 `pkg/sidecar` 里的 Server 角色）：
内核是客户端、本进程是服务端；只监听一个本机地址，等内核来连。

覆盖：
    - 握手（Hello / HelloAck，校验插件名与插件 API 版本）
    - 心跳（收到 Ping 回 Pong）
    - 逻辑流：把客户端字节流原样回显（ECHO）
    - 可选 -session-demo：在流上演示内核语义桥
      （session.open → declare_queue → publish → consume → 投递回推 → settle）

用法（通常由内核按配置 spawn，也可手动起）：
    python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo

配套文档：swiftmq/docs/plugin-development-python.md
"""

from __future__ import annotations

import argparse
import base64
import json
import queue
import socket
import struct
import threading

# ---- 线协议常量（与 pkg/sidecar/frame.go 一致）----
KIND_HELLO = 1
KIND_HELLO_ACK = 2
KIND_PING = 3
KIND_PONG = 4
KIND_CALL = 5
KIND_REPLY = 6
KIND_OPEN = 7
KIND_OPEN_ACK = 8
KIND_DATA = 9
KIND_CLOSE = 10

VERSION = "0.1.0"
API_VERSION = "v1"  # 必须与内核 pkg/plugin.APIVersion 一致

# 流的结束哨兵：读取线程用它唤醒流的处理协程
_STREAM_END = object()


def log(msg: str, **fields: object) -> None:
    """极简日志：写 stdout。由内核 spawn 时，这些输出会被转发进内核日志。"""
    if fields:
        extra = " ".join(f"{k}={v}" for k, v in fields.items())
        print(f"{msg} {extra}", flush=True)
    else:
        print(msg, flush=True)


def read_exact(sock: socket.socket, n: int) -> bytes:
    """读满 n 字节；对端关闭返回不足长度（由调用方判定）。"""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            break
        buf.extend(chunk)
    return bytes(buf)


def read_frame(sock: socket.socket) -> tuple[int, bytes] | None:
    """读一帧：(kind, payload)。连接结束返回 None。

    帧格式：`<len u32 BE><kind u8><payload>`，len = 1 + len(payload)。
    """
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    if length == 0:
        raise ValueError("帧长度字段为 0")
    payload = read_exact(sock, length - 1)
    if len(payload) < length - 1:
        return None
    return head[4], payload


class Conn:
    """一条来自内核的连接：帧读循环 + 逻辑流 + 反向调用。"""

    def __init__(self, sock: socket.socket, name: str, session_demo: bool, user: str, password: str):
        self.sock = sock
        self.name = name
        self.session_demo = session_demo
        self.user = user
        self.password = password
        # 写必须串行：心跳、控制面应答与数据块可能来自不同线程，交错写会污染整条连接。
        self._write_lock = threading.Lock()
        self._streams: dict[int, queue.Queue] = {}
        self._pending: dict[int, queue.Queue] = {}
        self._next_id = 0
        self._id_lock = threading.Lock()

    # ---------- 帧收发 ----------

    def send(self, kind: int, payload: bytes) -> None:
        frame = struct.pack(">I", 1 + len(payload)) + bytes([kind]) + payload
        with self._write_lock:
            self.sock.sendall(frame)

    def send_json(self, kind: int, obj: object) -> None:
        self.send(kind, json.dumps(obj, separators=(",", ":")).encode())

    # ---------- 反向调用（插件 → 内核）----------

    def call(self, method: str, params: object = None, timeout: float = 10.0) -> object:
        """发起一次反向调用并等待应答；内核明确拒绝时抛错。"""
        with self._id_lock:
            self._next_id += 1
            call_id = self._next_id
        waiter: queue.Queue = queue.Queue(maxsize=1)
        self._pending[call_id] = waiter
        call: dict[str, object] = {"id": call_id, "method": method, "reverse": True}
        if params is not None:
            call["params"] = params
        self.send_json(KIND_CALL, call)
        try:
            reply = waiter.get(timeout=timeout)
        except queue.Empty:
            raise RuntimeError(f"反向调用 {method} 超时") from None
        finally:
            self._pending.pop(call_id, None)
        if not reply.get("ok"):
            raise RuntimeError(f"反向调用 {method} 失败: {reply.get('error', '')}")
        return reply.get("data")

    # ---------- 主循环 ----------

    def serve(self) -> None:
        while True:
            frame = read_frame(self.sock)
            if frame is None:
                log("内核连接已关闭", plugin=self.name)
                return
            kind, payload = frame
            if kind == KIND_HELLO:
                if not self._handle_hello(payload):
                    return
            elif kind == KIND_PING:
                self.send(KIND_PONG, b"")
            elif kind == KIND_CALL:
                # 内核 → 插件的正向调用（如 session.deliver）。必须放到独立线程：
                # 处理里可能又要发起反向调用（settle），在读取线程里同步等待会死锁。
                threading.Thread(target=self._handle_forward_call, args=(payload,), daemon=True).start()
            elif kind == KIND_REPLY:
                self._handle_reply(payload)
            elif kind == KIND_OPEN:
                self._handle_open(payload)
            elif kind == KIND_DATA:
                (stream_id,) = struct.unpack(">I", payload[:4])
                q = self._streams.get(stream_id)
                if q is not None:
                    q.put(payload[4:])
            elif kind == KIND_CLOSE:
                msg = json.loads(payload or b"{}")
                self._end_stream(msg.get("stream"))
            else:
                log("收到未知帧类型，断开", plugin=self.name, kind=kind)
                return

    def _handle_hello(self, payload: bytes) -> bool:
        hello = json.loads(payload)
        if hello.get("plugin") != self.name:
            log("握手被拒：插件名不匹配", want=self.name, got=hello.get("plugin"))
            return False
        if hello.get("api_version") != API_VERSION:
            log("握手被拒：插件 API 版本不匹配", want=API_VERSION, got=hello.get("api_version"))
            return False
        self.send_json(KIND_HELLO_ACK, {
            "name": self.name,
            "version": VERSION,
            "api_version": API_VERSION,
            "capabilities": ["net.listen"],
            "protocols": ["pyecho"],
            "methods": ["stats"],
        })
        log("握手完成", plugin=self.name, kernel=hello.get("kernel_version"))
        return True

    def _handle_reply(self, payload: bytes) -> None:
        reply = json.loads(payload)
        # 只有反向应答（reverse=true）属于我们；正向应答由内核自己消费。
        if not reply.get("reverse"):
            return
        waiter = self._pending.get(reply.get("id"))
        if waiter is not None:
            waiter.put(reply)

    def _handle_forward_call(self, payload: bytes) -> None:
        call = json.loads(payload)
        call_id = call.get("id")
        try:
            data = self._dispatch(call.get("method", ""), call.get("params") or {})
            reply: dict[str, object] = {"id": call_id, "ok": True}
            if data is not None:
                reply["data"] = data
        except Exception as exc:  # noqa: BLE001 - 必须明确回错误，否则调用方会一直等
            log("正向调用失败", method=call.get("method"), err=str(exc))
            reply = {"id": call_id, "ok": False, "error": str(exc)}
        self.send_json(KIND_REPLY, reply)

    def _dispatch(self, method: str, params: dict) -> object:
        if method == "session.deliver":
            body = base64.b64decode(params.get("message", {}).get("body") or "")
            log("收到投递（session.deliver）", queue=params.get("queue"),
                delivery_id=params.get("delivery_id"), body=body.decode("utf-8", "replace"))
            # 结算：投递编号在整条连接上全局唯一，不需要流号。
            self.call("session.settle", {"delivery_id": params.get("delivery_id"), "action": "ack"})
            return None
        if method == "stats":
            return {"streams": len(self._streams)}
        raise RuntimeError(f"未知方法 {method}")

    # ---------- 逻辑流 ----------

    def _handle_open(self, payload: bytes) -> None:
        meta = json.loads(payload)
        stream_id = meta.get("stream")
        self._streams[stream_id] = queue.Queue()
        # 先回 OpenAck 再交给处理协程：否则内核会一直以为流没打开。
        self.send_json(KIND_OPEN_ACK, {"stream": stream_id, "ok": True})
        # 每条流一个处理协程，因此多条客户端连接可以并发。
        threading.Thread(target=self._serve_stream, args=(stream_id, meta), daemon=True).start()

    def _serve_stream(self, stream_id: int, meta: dict) -> None:
        try:
            if self.session_demo:
                self._session_demo(stream_id)
            log("流已打开", plugin=self.name, stream=stream_id,
                remote=meta.get("remote"), local=meta.get("local"))
            self._echo(stream_id)
        except Exception as exc:  # noqa: BLE001
            log("流处理失败", stream=stream_id, err=str(exc))
        finally:
            self._streams.pop(stream_id, None)
            # 处理协程返回即流结束：通知内核并回收本地状态。
            self.send_json(KIND_CLOSE, {"stream": stream_id, "reason": "handler returned"})

    def _end_stream(self, stream_id: int) -> None:
        q = self._streams.get(stream_id)
        if q is not None:
            q.put(_STREAM_END)

    def _echo(self, stream_id: int) -> None:
        q = self._streams[stream_id]
        while True:
            chunk = q.get()
            if chunk is _STREAM_END:
                return
            # 数据帧载荷 = 4 字节大端流号 + 原始字节（消息体不走 base64，省开销）。
            self.send(KIND_DATA, struct.pack(">I", stream_id) + chunk)

    def _session_demo(self, stream_id: int) -> None:
        """演示内核语义桥：与进程内协议插件走的是同一套 Session 语义。

        第一步必须是 **core.authenticate**：连接的内核操作面在认证前没有身份，
        任何 session.open 都会被拒（ACCESS_REFUSED - ... for user ''）。
        插件负责从自己的协议里取凭据，这里为演示直接写死为命令行参数。
        """
        plain = b"\x00" + self.user.encode() + b"\x00" + self.password.encode()
        ident = self.call("core.authenticate", {
            "stream": stream_id, "mechanism": "PLAIN", "response": base64.b64encode(plain).decode(),
        })
        log("认证通过", user=ident["user"])
        self.call("session.open", {"stream": stream_id, "vhost": "/"})
        q = self.call("session.declare_queue",
                      {"stream": stream_id, "exclusive": True, "auto_delete": True})
        self.call("session.publish", {
            "stream": stream_id,
            "routing_key": q["name"],
            "message": {
                "body": base64.b64encode(b"hello from python sidecar").decode(),
                "properties": {"content_type": "text/plain"},
            },
        })
        self.call("session.consume", {"stream": stream_id, "queue": q["name"], "prefetch": 32})
        log("session 演示完成", queue=q["name"])


def _serve_conn(conn: socket.socket, args: argparse.Namespace) -> None:
    try:
        Conn(conn, args.name, args.session_demo, args.user, args.password).serve()
    except Exception as exc:  # noqa: BLE001
        log("连接处理失败", plugin=args.name, err=str(exc))
    finally:
        try:
            conn.close()
        except OSError:
            pass


def main() -> int:
    parser = argparse.ArgumentParser(description="SwiftMQ 外部进程插件示例（Python）")
    parser.add_argument("-addr", default="0.0.0.0:19001", help="监听地址 host:port")
    parser.add_argument("-name", default="py-sidecar", help="插件名（须与内核配置一致）")
    parser.add_argument("-session-demo", action="store_true", help="每条流演示内核语义桥")
    parser.add_argument("-user", default="guest", help="session-demo 认证用的用户名")
    parser.add_argument("-password", default="guest", help="session-demo 认证用的口令")
    args = parser.parse_args()

    host, _, port = args.addr.rpartition(":")
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((host or "0.0.0.0", int(port)))
    srv.listen(8)
    log("sidecar 已启动", plugin=args.name, addr=args.addr, version=VERSION)

    try:
        while True:
            conn, addr = srv.accept()
            log("内核已接入", plugin=args.name, peer=addr)
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # 每条内核连接一个读循环；内核断线重连会建立新连接。
            threading.Thread(target=_serve_conn, args=(conn, args), daemon=True).start()
    except KeyboardInterrupt:
        return 0
    finally:
        srv.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
