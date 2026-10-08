// SpeedMQ 外部进程插件（sidecar）示例 —— Java 实现（仅标准库，无需 Maven/Gradle）。
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
// Java 标准库没有 JSON API，因此这里带了一个极简 JSON 读写（见文件末尾的 Json 类）；
// 真实项目请直接引入你惯用的 JSON 库（Gson / Jackson / …），那与本示例无关。
//
// 用法（JDK 11+ 可直接运行单文件源码）：
//   java SidecarPlugin.java --addr 0.0.0.0:19031 --name java-sidecar --session-demo
//
// 配套文档：speedmq/docs/plugin-development-java.md

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public final class SidecarPlugin {

    // ---- 线协议常量（与 pkg/sidecar/frame.go 一致）----
    static final int KIND_HELLO = 1;
    static final int KIND_HELLO_ACK = 2;
    static final int KIND_PING = 3;
    static final int KIND_PONG = 4;
    static final int KIND_CALL = 5;
    static final int KIND_REPLY = 6;
    static final int KIND_OPEN = 7;
    static final int KIND_OPEN_ACK = 8;
    static final int KIND_DATA = 9;
    static final int KIND_CLOSE = 10;

    static final String VERSION = "0.1.0";
    static final String API_VERSION = "v1"; // 必须与内核 pkg/plugin.APIVersion 一致

    /** 流的结束标记：读取线程用它唤醒流的处理线程。 */
    static final byte[] STREAM_END = new byte[0];

    public static void main(String[] args) throws Exception {
        String addr = "0.0.0.0:19031";
        String name = "java-sidecar";
        boolean sessionDemo = false;
        String user = "guest";
        String password = "guest";
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--addr" -> addr = args[++i];
                case "--name" -> name = args[++i];
                case "--session-demo" -> sessionDemo = true;
                case "--user" -> user = args[++i];
                case "--password" -> password = args[++i];
                default -> { }
            }
        }
        int colon = addr.lastIndexOf(':');
        String host = colon > 0 ? addr.substring(0, colon) : "0.0.0.0";
        int port = Integer.parseInt(addr.substring(colon + 1));

        Options opts = new Options(name, sessionDemo, user, password);
        try (ServerSocket server = new ServerSocket()) {
            server.setReuseAddress(true);
            server.bind(new InetSocketAddress(host, port));
            log("sidecar 已启动", "plugin", name, "addr", addr, "version", VERSION);
            while (true) {
                Socket socket = server.accept();
                socket.setTcpNoDelay(true);
                log("内核已接入", "plugin", name, "peer", socket.getRemoteSocketAddress().toString());
                // 每条内核连接一个读循环；内核断线重连会建立新连接。
                Thread t = new Thread(() -> {
                    try {
                        new Conn(socket, opts).serve();
                    } catch (Exception e) {
                        log("连接处理失败", "plugin", opts.name, "err", e.toString());
                    } finally {
                        try { socket.close(); } catch (IOException ignored) { /* 收尾尽力而为 */ }
                    }
                }, "sidecar-conn");
                t.setDaemon(true);
                t.start();
            }
        }
    }

    /** 极简日志：写 stdout。由内核 spawn 时，这些输出会被转发进内核日志。 */
    static void log(String msg, Object... kv) {
        StringBuilder sb = new StringBuilder(msg);
        for (int i = 0; i + 1 < kv.length; i += 2) {
            sb.append(' ').append(kv[i]).append('=').append(kv[i + 1]);
        }
        System.out.println(sb);
        System.out.flush();
    }

    /** SASL PLAIN 响应：以 NUL 分隔的「NUL + 用户名 + NUL + 口令」字节序列（即 \x00user\x00password）。 */
    static byte[] plainResponse(String user, String password) {
        byte[] u = user.getBytes(StandardCharsets.UTF_8);
        byte[] p = password.getBytes(StandardCharsets.UTF_8);
        byte[] out = new byte[u.length + p.length + 2];
        System.arraycopy(u, 0, out, 1, u.length);
        System.arraycopy(p, 0, out, u.length + 2, p.length);
        return out;
    }

    static final class Options {
        final String name;
        final boolean sessionDemo;
        final String user;
        final String password;

        Options(String name, boolean sessionDemo, String user, String password) {
            this.name = name;
            this.sessionDemo = sessionDemo;
            this.user = user;
            this.password = password;
        }
    }

    /** 一条流的接收侧：读取线程投递字节，处理线程取走；STREAM_END 表示对端关闭。 */
    static final class StreamState {
        final int id;
        final BlockingQueue<byte[]> chunks = new ArrayBlockingQueue<>(64);

        StreamState(int id) { this.id = id; }

        void push(byte[] data) { chunks.offer(data); }
        void end() {
            // 队列满时也要把结束标记塞进去，避免处理线程永远等不到收尾。
            while (!chunks.offer(STREAM_END)) {
                chunks.poll();
            }
        }
        byte[] take() throws InterruptedException { return chunks.take(); }
    }

    /** 一条来自内核的连接：帧收发 + 逻辑流 + 反向调用。 */
    static final class Conn {
        private final Socket socket;
        private final DataInputStream in;
        private final OutputStream out;
        private final Options opts;
        private final Object writeLock = new Object();
        private final Map<Integer, StreamState> streams = new ConcurrentHashMap<>();
        private final Map<Long, BlockingQueue<Object>> pending = new ConcurrentHashMap<>();
        private final AtomicLong nextId = new AtomicLong();

        Conn(Socket socket, Options opts) throws IOException {
            this.socket = socket;
            this.in = new DataInputStream(socket.getInputStream());
            this.out = socket.getOutputStream();
            this.opts = opts;
        }

        // ---------- 帧收发 ----------

        void send(int kind, byte[] payload) throws IOException {
            synchronized (writeLock) {
                out.write(int32(1 + payload.length));
                out.write(kind);
                out.write(payload);
                out.flush();
            }
        }

        void sendJson(int kind, Object obj) throws IOException {
            send(kind, Json.write(obj).getBytes(StandardCharsets.UTF_8));
        }

        static byte[] int32(int v) {
            return new byte[]{(byte) (v >>> 24), (byte) (v >>> 16), (byte) (v >>> 8), (byte) v};
        }

        static int readInt32(byte[] b, int off) {
            return ((b[off] & 0xff) << 24) | ((b[off + 1] & 0xff) << 16)
                    | ((b[off + 2] & 0xff) << 8) | (b[off + 3] & 0xff);
        }

        /** @return {kind, payload}；连接结束返回 null。 */
        private Object[] readFrame() throws IOException {
            int len;
            try {
                len = in.readInt();
            } catch (IOException eof) {
                return null;
            }
            if (len <= 0) return null;
            int kind = in.readUnsignedByte();
            byte[] payload = new byte[len - 1];
            in.readFully(payload);
            return new Object[]{kind, payload};
        }

        // ---------- 反向调用（插件 → 内核）----------

        Object call(String method, Map<String, Object> params, long timeoutMs) throws Exception {
            long id = nextId.incrementAndGet();
            BlockingQueue<Object> waiter = new ArrayBlockingQueue<>(1);
            pending.put(id, waiter);

            Map<String, Object> call = new LinkedHashMap<>();
            call.put("id", id);
            call.put("method", method);
            call.put("reverse", true);
            if (params != null) call.put("params", params);
            sendJson(KIND_CALL, call);

            Object got = waiter.poll(timeoutMs, TimeUnit.MILLISECONDS);
            pending.remove(id);
            if (got == null) throw new RuntimeException("反向调用 " + method + " 超时");
            if (!(got instanceof Map<?, ?> reply)) throw new RuntimeException("应答格式非法");
            if (!Boolean.TRUE.equals(reply.get("ok"))) {
                throw new RuntimeException("反向调用 " + method + " 失败: " + reply.get("error"));
            }
            return reply.get("data");
        }

        private void deliverReply(Map<?, ?> reply) {
            Object idObj = reply.get("id");
            if (!(idObj instanceof Number num)) return;
            BlockingQueue<Object> waiter = pending.get(num.longValue());
            if (waiter != null) waiter.offer(reply);
        }

        // ---------- 主循环 ----------

        void serve() throws Exception {
            while (true) {
                Object[] frame = readFrame();
                if (frame == null) {
                    log("内核连接已关闭", "plugin", opts.name);
                    return;
                }
                int kind = (Integer) frame[0];
                byte[] payload = (byte[]) frame[1];
                switch (kind) {
                    case KIND_HELLO -> {
                        if (!handleHello(payload)) return;
                    }
                    case KIND_PING -> send(KIND_PONG, new byte[0]);
                    case KIND_CALL -> {
                        // 内核 → 插件的正向调用（如 session.deliver）。
                        // 必须放到独立线程：处理里可能又要发起反向调用（settle），
                        // 在读取线程里同步等待会死锁。
                        final byte[] p = payload;
                        Thread t = new Thread(() -> handleForwardCall(p), "sidecar-call");
                        t.setDaemon(true);
                        t.start();
                    }
                    case KIND_REPLY -> {
                        Map<?, ?> reply = (Map<?, ?>) Json.parse(new String(payload, StandardCharsets.UTF_8));
                        // 只有反向应答（reverse=true）属于我们；正向应答由内核自己消费。
                        if (Boolean.TRUE.equals(reply.get("reverse"))) deliverReply(reply);
                    }
                    case KIND_OPEN -> handleOpen(payload);
                    case KIND_DATA -> {
                        int id = readInt32(payload, 0);
                        StreamState st = streams.get(id);
                        if (st != null) {
                            byte[] data = new byte[payload.length - 4];
                            System.arraycopy(payload, 4, data, 0, data.length);
                            st.push(data);
                        }
                    }
                    case KIND_CLOSE -> {
                        Map<?, ?> msg = (Map<?, ?>) Json.parse(new String(payload, StandardCharsets.UTF_8));
                        Object idObj = msg.get("stream");
                        if (idObj instanceof Number num) {
                            StreamState st = streams.get(num.intValue());
                            if (st != null) st.end();
                        }
                    }
                    default -> {
                        log("收到未知帧类型，断开", "plugin", opts.name, "kind", kind);
                        return;
                    }
                }
            }
        }

        private boolean handleHello(byte[] payload) throws IOException {
            Map<?, ?> hello = (Map<?, ?>) Json.parse(new String(payload, StandardCharsets.UTF_8));
            if (!opts.name.equals(hello.get("plugin"))) {
                log("握手被拒：插件名不匹配", "want", opts.name, "got", hello.get("plugin"));
                return false;
            }
            if (!API_VERSION.equals(hello.get("api_version"))) {
                log("握手被拒：插件 API 版本不匹配", "want", API_VERSION, "got", hello.get("api_version"));
                return false;
            }
            Map<String, Object> ack = new LinkedHashMap<>();
            ack.put("name", opts.name);
            ack.put("version", VERSION);
            ack.put("api_version", API_VERSION);
            ack.put("capabilities", List.of("net.listen"));
            ack.put("protocols", List.of("javaecho"));
            ack.put("methods", List.of("stats"));
            sendJson(KIND_HELLO_ACK, ack);
            log("握手完成", "plugin", opts.name, "kernel", hello.get("kernel_version"));
            return true;
        }

        private void handleForwardCall(byte[] payload) {
            Map<?, ?> call = (Map<?, ?>) Json.parse(new String(payload, StandardCharsets.UTF_8));
            long id = ((Number) call.get("id")).longValue();
            String method = String.valueOf(call.get("method"));
            Map<String, Object> reply = new LinkedHashMap<>();
            reply.put("id", id);
            try {
                Object data = handleMethod(method, asMap(call.get("params")));
                reply.put("ok", true);
                if (data != null) reply.put("data", data);
            } catch (Exception e) {
                log("正向调用失败", "method", method, "err", e.toString());
                reply.put("ok", false);
                reply.put("error", e.toString());
            }
            try {
                sendJson(KIND_REPLY, reply);
            } catch (IOException e) {
                log("回写应答失败", "err", e.toString());
            }
        }

        private Object handleMethod(String method, Map<String, Object> params) throws Exception {
            if ("session.deliver".equals(method)) {
                Map<String, Object> message = asMap(params.get("message"));
                String body = String.valueOf(message.get("body"));
                String text = new String(Base64.getDecoder().decode(body), StandardCharsets.UTF_8);
                log("收到投递（session.deliver）", "queue", params.get("queue"),
                        "delivery_id", params.get("delivery_id"), "body", text);
                // 结算：投递编号在整条连接上全局唯一，不需要流号。
                Map<String, Object> settle = new LinkedHashMap<>();
                settle.put("delivery_id", params.get("delivery_id"));
                settle.put("action", "ack");
                call("session.settle", settle, 10_000);
                return null;
            }
            if ("stats".equals(method)) {
                Map<String, Object> out = new LinkedHashMap<>();
                out.put("streams", streams.size());
                return out;
            }
            throw new RuntimeException("未知方法 " + method);
        }

        private void handleOpen(byte[] payload) throws IOException {
            Map<?, ?> meta = (Map<?, ?>) Json.parse(new String(payload, StandardCharsets.UTF_8));
            int id = ((Number) meta.get("stream")).intValue();
            StreamState st = new StreamState(id);
            streams.put(id, st);
            // 先回 OpenAck 再交给处理线程：否则内核会一直以为流没打开。
            Map<String, Object> ack = new LinkedHashMap<>();
            ack.put("stream", id);
            ack.put("ok", true);
            sendJson(KIND_OPEN_ACK, ack);
            Thread t = new Thread(() -> serveStream(st, meta), "sidecar-stream-" + id);
            t.setDaemon(true);
            t.start();
        }

        private void serveStream(StreamState st, Map<?, ?> meta) {
            try {
                if (opts.sessionDemo) sessionDemo(st.id);
                log("流已打开", "plugin", opts.name, "stream", st.id,
                        "remote", meta.get("remote"), "local", meta.get("local"));
                while (true) {
                    byte[] chunk = st.take();
                    if (chunk == STREAM_END) break;
                    // 数据帧载荷 = 4 字节大端流号 + 原始字节（消息体不走 base64，省开销）。
                    byte[] frame = new byte[4 + chunk.length];
                    System.arraycopy(int32(st.id), 0, frame, 0, 4);
                    System.arraycopy(chunk, 0, frame, 4, chunk.length);
                    send(KIND_DATA, frame);
                }
            } catch (Exception e) {
                log("流处理失败", "stream", st.id, "err", e.toString());
            } finally {
                streams.remove(st.id);
                // 处理线程返回即流结束：通知内核并回收本地状态。
                try {
                    Map<String, Object> close = new LinkedHashMap<>();
                    close.put("stream", st.id);
                    close.put("reason", "handler returned");
                    sendJson(KIND_CLOSE, close);
                } catch (IOException ignored) {
                    // 连接已断时无需求通知
                }
            }
        }

        /** 演示内核语义桥：与进程内协议插件走的是同一套 Session 语义。 */
        private void sessionDemo(int streamId) throws Exception {
            // 第一步必须是 core.authenticate：连接的内核操作面在认证前没有身份，
            // 任何 session.open 都会被拒（ACCESS_REFUSED - ... for user ''）。
            Map<String, Object> auth = new LinkedHashMap<>();
            auth.put("stream", streamId);
            auth.put("mechanism", "PLAIN");
            auth.put("response", Base64.getEncoder().encodeToString(plainResponse(opts.user, opts.password)));
            Object ident = call("core.authenticate", auth, 10_000);
            log("认证通过", "user", asMap(ident).get("user"));

            Map<String, Object> open = new LinkedHashMap<>();
            open.put("stream", streamId);
            open.put("vhost", "/");
            call("session.open", open, 10_000);

            Map<String, Object> declare = new LinkedHashMap<>();
            declare.put("stream", streamId);
            declare.put("exclusive", true);
            declare.put("auto_delete", true);
            Map<String, Object> q = asMap(call("session.declare_queue", declare, 10_000));
            String queue = String.valueOf(q.get("name"));

            Map<String, Object> message = new LinkedHashMap<>();
            message.put("body", Base64.getEncoder()
                    .encodeToString("hello from java sidecar".getBytes(StandardCharsets.UTF_8)));
            message.put("properties", Map.of("content_type", "text/plain"));
            Map<String, Object> publish = new LinkedHashMap<>();
            publish.put("stream", streamId);
            publish.put("routing_key", queue);
            publish.put("message", message);
            call("session.publish", publish, 10_000);

            Map<String, Object> consume = new LinkedHashMap<>();
            consume.put("stream", streamId);
            consume.put("queue", queue);
            consume.put("prefetch", 32);
            call("session.consume", consume, 10_000);
            log("session 演示完成", "queue", queue);
        }
    }

    @SuppressWarnings("unchecked")
    static Map<String, Object> asMap(Object o) {
        if (o instanceof Map<?, ?> m) return (Map<String, Object>) m;
        return new HashMap<>();
    }
}

/**
 * 极简 JSON 读写：只覆盖本示例用到的形态（对象/数组/字符串/数字/布尔/null）。
 *
 * 真实插件请直接使用成熟的 JSON 库；这里自带一份是为了让示例**零依赖**可运行。
 */
final class Json {

    static Object parse(String s) {
        Parser p = new Parser(s);
        p.skipWs();
        Object v = p.value();
        p.skipWs();
        return v;
    }

    static String write(Object o) {
        StringBuilder sb = new StringBuilder();
        writeTo(sb, o);
        return sb.toString();
    }

    private static void writeTo(StringBuilder sb, Object o) {
        if (o == null) {
            sb.append("null");
        } else if (o instanceof String s) {
            quote(sb, s);
        } else if (o instanceof Boolean || o instanceof Integer || o instanceof Long) {
            sb.append(o);
        } else if (o instanceof Number n) {
            sb.append(n);
        } else if (o instanceof Map<?, ?> m) {
            sb.append('{');
            boolean first = true;
            for (Map.Entry<?, ?> e : m.entrySet()) {
                if (!first) sb.append(',');
                first = false;
                quote(sb, String.valueOf(e.getKey()));
                sb.append(':');
                writeTo(sb, e.getValue());
            }
            sb.append('}');
        } else if (o instanceof Iterable<?> it) {
            sb.append('[');
            boolean first = true;
            for (Object e : it) {
                if (!first) sb.append(',');
                first = false;
                writeTo(sb, e);
            }
            sb.append(']');
        } else {
            quote(sb, String.valueOf(o));
        }
    }

    private static void quote(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }

    private static final class Parser {
        private final String s;
        private int i;

        Parser(String s) { this.s = s; }

        void skipWs() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        }

        Object value() {
            skipWs();
            char c = s.charAt(i);
            return switch (c) {
                case '{' -> object();
                case '[' -> array();
                case '"' -> string();
                case 't' -> literal("true", Boolean.TRUE);
                case 'f' -> literal("false", Boolean.FALSE);
                case 'n' -> literal("null", null);
                default -> number();
            };
        }

        private Object literal(String text, Object v) {
            if (!s.startsWith(text, i)) throw new IllegalArgumentException("非法字面量 @" + i);
            i += text.length();
            return v;
        }

        private Map<String, Object> object() {
            Map<String, Object> m = new LinkedHashMap<>();
            i++; // '{'
            skipWs();
            if (s.charAt(i) == '}') { i++; return m; }
            while (true) {
                skipWs();
                String k = string();
                skipWs();
                i++; // ':'
                m.put(k, value());
                skipWs();
                char c = s.charAt(i++);
                if (c == '}') return m;
                if (c != ',') throw new IllegalArgumentException("对象分隔符非法 @" + (i - 1));
            }
        }

        private List<Object> array() {
            List<Object> list = new ArrayList<>();
            i++; // '['
            skipWs();
            if (s.charAt(i) == ']') { i++; return list; }
            while (true) {
                list.add(value());
                skipWs();
                char c = s.charAt(i++);
                if (c == ']') return list;
                if (c != ',') throw new IllegalArgumentException("数组分隔符非法 @" + (i - 1));
            }
        }

        private String string() {
            i++; // '"'
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = s.charAt(i++);
                if (c == '"') return sb.toString();
                if (c != '\\') { sb.append(c); continue; }
                char e = s.charAt(i++);
                switch (e) {
                    case '"' -> sb.append('"');
                    case '\\' -> sb.append('\\');
                    case '/' -> sb.append('/');
                    case 'b' -> sb.append('\b');
                    case 'f' -> sb.append('\f');
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case 'u' -> {
                        sb.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                    }
                    default -> throw new IllegalArgumentException("非法转义 \\" + e);
                }
            }
        }

        private Object number() {
            int start = i;
            while (i < s.length() && "+-.eE0123456789".indexOf(s.charAt(i)) >= 0) i++;
            String text = s.substring(start, i);
            if (text.isEmpty()) throw new IllegalArgumentException("非法 JSON 值 @" + start);
            if (text.indexOf('.') < 0 && text.indexOf('e') < 0 && text.indexOf('E') < 0) {
                return Long.parseLong(text);
            }
            return Double.parseDouble(text);
        }
    }
}
