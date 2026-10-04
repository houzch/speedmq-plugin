<?php
// SwiftMQ 外部进程插件（sidecar）示例 —— PHP 实现（仅标准库，无 composer 依赖）。
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
// PHP 没有线程，因此这里用**可重入的帧泵**：一次反向调用会一边读帧一边分发，
// 直到等到自己的应答（期间收到的心跳/数据/正向调用照常处理）。见 callAndWait()。
//
// 用法：
//   php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
//
// 配套文档：swiftmq/docs/plugin-development-php.md

declare(strict_types=1);

// ---- 线协议常量（与 pkg/sidecar/frame.go 一致）----
const KIND_HELLO     = 1;
const KIND_HELLO_ACK = 2;
const KIND_PING      = 3;
const KIND_PONG      = 4;
const KIND_CALL      = 5;
const KIND_REPLY     = 6;
const KIND_OPEN      = 7;
const KIND_OPEN_ACK  = 8;
const KIND_DATA      = 9;
const KIND_CLOSE     = 10;

const VERSION     = '0.1.0';
const API_VERSION = 'v1'; // 必须与内核 pkg/plugin.APIVersion 一致

function log_line(string $msg, array $fields = []): void
{
    $extra = '';
    foreach ($fields as $k => $v) {
        $extra .= sprintf(' %s=%s', $k, is_scalar($v) ? (string)$v : json_encode($v));
    }
    fwrite(STDOUT, $msg . $extra . PHP_EOL);
}

/** SASL PLAIN 响应：\x00<user>\x00<password> */
function plain_response(string $user, string $password): string
{
    return "\x00" . $user . "\x00" . $password;
}

/** 读满 n 字节；对端关闭返回长度不足的串。 */
function read_exact($sock, int $n): string
{
    $buf = '';
    while (strlen($buf) < $n) {
        $chunk = fread($sock, $n - strlen($buf));
        if ($chunk === false || $chunk === '') {
            break;
        }
        $buf .= $chunk;
    }
    return $buf;
}

/** 一条来自内核的连接：帧收发 + 逻辑流 + 反向调用。 */
final class Conn
{
    /** @var resource */
    private $sock;
    private array $opts;
    /** @var array<int,bool> 已打开的流 */
    private array $streams = [];
    private int $nextId = 0;

    public function __construct($sock, array $opts)
    {
        $this->sock = $sock;
        $this->opts = $opts;
    }

    // ---------- 帧收发 ----------

    public function sendFrame(int $kind, string $payload = ''): void
    {
        // 长度字段 = 1 + payload 长度（包含 kind 字节），大端。
        fwrite($this->sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);
    }

    public function sendJson(int $kind, $obj): void
    {
        $this->sendFrame($kind, json_encode($obj, JSON_UNESCAPED_UNICODE));
    }

    /** @return array{0:int,1:string}|null */
    private function readFrame(): ?array
    {
        $head = read_exact($this->sock, 5);
        if (strlen($head) < 5) {
            return null;
        }
        $len = unpack('N', substr($head, 0, 4))[1];
        if ($len === 0) {
            return null;
        }
        $payload = read_exact($this->sock, $len - 1);
        if (strlen($payload) < $len - 1) {
            return null;
        }
        return [ord($head[4]), $payload];
    }

    // ---------- 反向调用（插件 → 内核）----------

    /**
     * 发起一次反向调用并等待应答；期间照常处理心跳/数据/正向调用（可重入帧泵）。
     *
     * @return mixed 应答的 data（无 data 时为 null）
     */
    public function callAndWait(string $method, $params = null)
    {
        $id = ++$this->nextId;
        $call = ['id' => $id, 'method' => $method, 'reverse' => true];
        if ($params !== null) {
            $call['params'] = $params;
        }
        $this->sendJson(KIND_CALL, $call);

        for (;;) {
            $frame = $this->readFrame();
            if ($frame === null) {
                throw new RuntimeException('内核连接已关闭');
            }
            [$kind, $payload] = $frame;
            if ($kind === KIND_REPLY) {
                $reply = json_decode($payload, true);
                if (!is_array($reply) || empty($reply['reverse']) || ($reply['id'] ?? 0) !== $id) {
                    continue; // 不是我们的应答（理论上不会出现）
                }
                if (empty($reply['ok'])) {
                    throw new RuntimeException((string)($reply['error'] ?? '反向调用失败'));
                }
                return $reply['data'] ?? null;
            }
            $this->dispatchOther($kind, $payload);
        }
    }

    // ---------- 主循环 ----------

    public function serve(): void
    {
        for (;;) {
            $frame = $this->readFrame();
            if ($frame === null) {
                log_line('内核连接已关闭', ['plugin' => $this->opts['name']]);
                return;
            }
            [$kind, $payload] = $frame;
            if ($kind === KIND_HELLO) {
                if (!$this->handleHello($payload)) {
                    return;
                }
                continue;
            }
            if ($kind === KIND_REPLY) {
                // 应答只可能在我们主动调用时被消费（见 callAndWait）；这里出现说明无人等待。
                continue;
            }
            $this->dispatchOther($kind, $payload);
        }
    }

    /** 处理非握手帧。callAndWait 的帧泵也走这里，因此可重入。 */
    private function dispatchOther(int $kind, string $payload): void
    {
        switch ($kind) {
            case KIND_PING:
                $this->sendFrame(KIND_PONG);
                break;

            case KIND_OPEN:
                $meta = json_decode($payload, true);
                $id = (int)$meta['stream'];
                $this->streams[$id] = true;
                // 先回 OpenAck 再开始服务：否则内核会一直以为流没打开。
                $this->sendJson(KIND_OPEN_ACK, ['stream' => $id, 'ok' => true]);
                if (!empty($this->opts['session_demo'])) {
                    try {
                        $this->sessionDemo($id);
                    } catch (Throwable $e) {
                        log_line('session 演示失败', ['stream' => $id, 'err' => $e->getMessage()]);
                    }
                }
                log_line('流已打开', [
                    'plugin' => $this->opts['name'],
                    'stream' => $id,
                    'remote' => $meta['remote'] ?? '',
                    'local'  => $meta['local'] ?? '',
                ]);
                break;

            case KIND_DATA:
                // 数据帧载荷 = 4 字节大端流号 + 原始字节。原样回显。
                $id = unpack('N', substr($payload, 0, 4))[1];
                if (isset($this->streams[$id])) {
                    $this->sendFrame(KIND_DATA, pack('N', $id) . substr($payload, 4));
                }
                break;

            case KIND_CLOSE:
                $msg = json_decode($payload, true);
                unset($this->streams[(int)($msg['stream'] ?? -1)]);
                break;

            case KIND_CALL:
                $this->handleForwardCall($payload);
                break;

            default:
                log_line('收到未知帧类型，断开', ['plugin' => $this->opts['name'], 'kind' => $kind]);
                exit(0);
        }
    }

    private function handleHello(string $payload): bool
    {
        $hello = json_decode($payload, true);
        if (($hello['plugin'] ?? null) !== $this->opts['name']) {
            log_line('握手被拒：插件名不匹配', ['want' => $this->opts['name'], 'got' => $hello['plugin'] ?? '']);
            return false;
        }
        if (($hello['api_version'] ?? null) !== API_VERSION) {
            log_line('握手被拒：插件 API 版本不匹配', ['want' => API_VERSION, 'got' => $hello['api_version'] ?? '']);
            return false;
        }
        $this->sendJson(KIND_HELLO_ACK, [
            'name'         => $this->opts['name'],
            'version'      => VERSION,
            'api_version'  => API_VERSION,
            'capabilities' => ['net.listen'],
            'protocols'    => ['phpecho'],
            'methods'      => ['stats'],
        ]);
        log_line('握手完成', ['plugin' => $this->opts['name'], 'kernel' => $hello['kernel_version'] ?? '']);
        return true;
    }

    /** 内核 → 插件的正向调用（如 session.deliver）：必须明确回一条应答。 */
    private function handleForwardCall(string $payload): void
    {
        $call = json_decode($payload, true);
        $id = (int)($call['id'] ?? 0);
        try {
            $data = $this->handleMethod((string)($call['method'] ?? ''), $call['params'] ?? []);
            $reply = ['id' => $id, 'ok' => true];
            if ($data !== null) {
                $reply['data'] = $data;
            }
        } catch (Throwable $e) {
            log_line('正向调用失败', ['method' => $call['method'] ?? '', 'err' => $e->getMessage()]);
            $reply = ['id' => $id, 'ok' => false, 'error' => $e->getMessage()];
        }
        $this->sendJson(KIND_REPLY, $reply);
    }

    /** @return mixed 方法结果（无结果为 null） */
    private function handleMethod(string $method, array $params)
    {
        if ($method === 'session.deliver') {
            $body = base64_decode((string)($params['message']['body'] ?? ''), true);
            log_line('收到投递（session.deliver）', [
                'queue'       => $params['queue'] ?? '',
                'delivery_id' => $params['delivery_id'] ?? 0,
                'body'        => $body === false ? '' : $body,
            ]);
            // 结算：投递编号在整条连接上全局唯一，不需要流号。
            $this->callAndWait('session.settle', [
                'delivery_id' => $params['delivery_id'],
                'action'      => 'ack',
            ]);
            return null;
        }
        if ($method === 'stats') {
            return ['streams' => count($this->streams)];
        }
        throw new RuntimeException("未知方法 {$method}");
    }

    /** 演示内核语义桥：与进程内协议插件走的是同一套 Session 语义。 */
    private function sessionDemo(int $streamId): void
    {
        // 第一步必须是 core.authenticate：连接的内核操作面在认证前没有身份，
        // 任何 session.open 都会被拒（ACCESS_REFUSED - ... for user ''）。
        $ident = $this->callAndWait('core.authenticate', [
            'stream'    => $streamId,
            'mechanism' => 'PLAIN',
            'response'  => base64_encode(plain_response($this->opts['user'], $this->opts['password'])),
        ]);
        log_line('认证通过', ['user' => $ident['user'] ?? '']);

        $this->callAndWait('session.open', ['stream' => $streamId, 'vhost' => '/']);
        $q = $this->callAndWait('session.declare_queue', [
            'stream'     => $streamId,
            'exclusive'  => true,
            'auto_delete' => true,
        ]);
        $this->callAndWait('session.publish', [
            'stream'      => $streamId,
            'routing_key' => $q['name'],
            'message'     => [
                'body'       => base64_encode('hello from php sidecar'),
                'properties' => ['content_type' => 'text/plain'],
            ],
        ]);
        $this->callAndWait('session.consume', [
            'stream'   => $streamId,
            'queue'    => $q['name'],
            'prefetch' => 32,
        ]);
        log_line('session 演示完成', ['queue' => $q['name']]);
    }
}

// ---- 命令行 ----

$opts = [
    'addr'         => '0.0.0.0:19021',
    'name'         => 'php-sidecar',
    'session_demo' => false,
    'user'         => 'guest',
    'password'     => 'guest',
];
for ($i = 1; $i < $argc; $i++) {
    switch ($argv[$i]) {
        case '--addr':     $opts['addr'] = $argv[++$i]; break;
        case '--name':     $opts['name'] = $argv[++$i]; break;
        case '--session-demo': $opts['session_demo'] = true; break;
        case '--user':     $opts['user'] = $argv[++$i]; break;
        case '--password': $opts['password'] = $argv[++$i]; break;
    }
}

$pos  = strrpos($opts['addr'], ':');
$host = substr($opts['addr'], 0, $pos) ?: '0.0.0.0';
$port = (int)substr($opts['addr'], $pos + 1);

$server = @stream_socket_server("tcp://{$host}:{$port}", $errno, $errstr);
if ($server === false) {
    fwrite(STDERR, "监听失败: {$errstr} ({$errno})" . PHP_EOL);
    exit(1);
}
log_line('sidecar 已启动', ['plugin' => $opts['name'], 'addr' => $opts['addr'], 'version' => VERSION]);

while (true) {
    $conn = @stream_socket_accept($server, -1);
    if ($conn === false) {
        continue;
    }
    stream_set_blocking($conn, true);
    log_line('内核已接入', ['plugin' => $opts['name'], 'peer' => (string)@stream_socket_get_name($conn, true)]);
    try {
        (new Conn($conn, $opts))->serve();
    } catch (Throwable $e) {
        log_line('连接处理失败', ['plugin' => $opts['name'], 'err' => $e->getMessage()]);
    }
    fclose($conn);
}
