# swiftmq-plugin

Reference implementations of SwiftMQ **external-process plugins (sidecar)** — five languages,
**zero third-party dependencies**, all verified end to end.

> Purpose: extend [SwiftMQ](https://github.com/houzch/swiftmq) with new protocol capabilities
> in **any language**, without forking or rebuilding the broker.
> Every example goes through the full loop: **handshake → authentication → core session bridge
> (queues/publish/consume) → delivery push-back → settle → client byte-stream echo**.

English | [简体中文](README.md)

---

## What this is / is not

| | |
| --- | --- |
| **Is** | Reference implementations of the sidecar wire protocol, plugin side (Python / Node.js / PHP / Java / Go) — copy them as templates |
| **Is** | Python / Node.js / PHP / Java implement the wire protocol **from scratch** (framing, handshake, heartbeat, streams, reverse calls) — read them as a protocol spec |
| **Is** | Go uses the **official SDK** (`pkg/sidecar`) and implements just three methods — showing how short it gets when an SDK exists |
| **Is not** | The broker itself. To run a plugin, the broker must *declare* it in its config (see below) |
| **Is not** | A built-in kernel plugin. SwiftMQ's built-in protocols (AMQP 0-9-1 / MQTT) do not use this mechanism |

---

## Layout

```
swiftmq-plugin/
├── python/sidecar_plugin.py     # Python 3 (stdlib only)
├── nodejs/index.js              # Node.js (stdlib only, no npm deps)
├── php/sidecar_plugin.php       # PHP 7.4+ (stdlib only, no composer deps)
├── java/SidecarPlugin.java      # Java 11+ (single file, JDK only; ships a tiny JSON reader)
├── go/main.go                   # Go 1.24+ (uses the official SDK pkg/sidecar; no hand-rolled wire code)
├── go/go.mod
├── LICENSE                      # Apache-2.0
└── NOTICE
```

---

## Quick start (Python)

The shortest **runnable** path: run the plugin on the host, run the broker in a container
(the broker dials back to the host). For "plugin in the same container as the broker",
see §Packaging into an image.

### 1. Start the plugin (on the host)

```bash
python3 python/sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
# expected: sidecar 已启动 plugin=py-sidecar addr=0.0.0.0:19001 version=0.1.0
```

> Listen on `0.0.0.0`, not `127.0.0.1`, otherwise the broker inside the container cannot reach you.

### 2. Declare the plugin on the broker side

Add this to SwiftMQ's config file (`swiftmqd.json`). Note `address` uses
`host.docker.internal` (the "host" hostname provided by Docker Desktop) and `spawn` is empty
(you already started the process; the broker only dials):

> ⚠️ The config below is **plain JSON and must not contain comments**
> (the broker parses it with Go's `encoding/json`).

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

- `remote_access: true`: with the broker in a container both clients and the plugin are
  **non-loopback**, so the default account would be refused.
- `address` is where the **broker dials you** (the broker is the client); `spawn: []` means dial only.
- `prefix` **must be non-empty**: that is how the broker claims connections for this protocol
  (see rule 3 under "How it works").
- `listeners` is the **public port, opened by the broker** (clients connect to the broker, not to you).

### 3. Start the broker (container)

```bash
docker run -d --name swiftmq -p 5672:5672 -p 15672:15672 -p 19002:19002 \
  -v "$PWD/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro" houzch/swiftmq:1.1.01 \
  -config /etc/swiftmq/swiftmqd.json -log-level info
```

### 4. Verify

```bash
# The plugin should be attached and enabled
curl -u guest:guest http://127.0.0.1:15672/api/plugins

# Clients connect to the BROKER's port 19002; the first bytes must be the "PY" prefix
printf 'PYhello\n' | nc 127.0.0.1 19002
# expected: PYhello
```

With `-session-demo`, each connection also exercises the kernel session bridge
(authenticate → declare queue → publish → consume → settle); the log shows
`认证通过` / `session 演示完成` / `收到投递（session.deliver）`.

### Packaging into an image (optional, common in production)

For the broker to spawn the plugin itself (`spawn`), both the executable **and its runtime** must
exist inside the broker container:

```dockerfile
FROM houzch/swiftmq:1.1.01

USER root
# the plugin needs its language runtime; the alpine base image has none by default
RUN apk add --no-cache python3
COPY python/sidecar_plugin.py /opt/swiftmq-plugin/python/sidecar_plugin.py
USER swiftmq
```

Then switch the config to a loopback address plus `spawn`:

```json
"sidecar": {
  "address": "tcp://127.0.0.1:19001",
  "spawn": ["python3", "/opt/swiftmq-plugin/python/sidecar_plugin.py",
            "-addr", "0.0.0.0:19001", "-name", "py-sidecar"],
  "protocols": [{ "name": "pyecho", "prefix": "PY",
                  "listeners": [{ "name": "pyecho", "addr": ":19002" }] }]
}
```

> Alternatively ship the plugin as its **own container**: `spawn: []`,
> `address: "tcp://<service-name>:19001"`, listening on `0.0.0.0` — and remember to publish the
> `listeners` ports as well.

---

## Languages at a glance

| Language | Requires | Run | File |
| --- | --- | --- | --- |
| Python | Python 3.8+ | `python3 sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo` | [`python/sidecar_plugin.py`](python/sidecar_plugin.py) |
| Node.js | Node 16+ | `node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo` | [`nodejs/index.js`](nodejs/index.js) |
| PHP | PHP 7.4+ | `php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo` | [`php/sidecar_plugin.php`](php/sidecar_plugin.php) |
| Java | JDK 11+ | `javac -encoding UTF-8 -d classes SidecarPlugin.java` then `java -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo` | [`java/SidecarPlugin.java`](java/SidecarPlugin.java) |
| Go | Go 1.24+ | `go build -o go-sidecar .` then `./go-sidecar -addr tcp://0.0.0.0:19041 -name go-sidecar -session-demo` | [`go/main.go`](go/main.go) |

All five behave **identically on the wire**; they differ in whether an official SDK exists and in
concurrency model and idioms:

| Language | Concurrency | Watch out for |
| --- | --- | --- |
| Python | one reader thread per connection, one handler thread per stream | serialize frame writes; handle forward calls (`session.deliver`) on a separate thread, otherwise waiting for a reverse-call reply deadlocks |
| Node.js | single-threaded event loop with async queues | a `data` chunk has nothing to do with frame boundaries — buffer it yourself; never `await` inside the frame-splitting function |
| PHP | single-threaded with a **re-entrant frame pump** (no threads) | a reverse call keeps reading frames while it waits, so the dispatcher must be re-entrant; PHP 7.4 has no `mixed` return type |
| Java | one reader thread per connection, one handler thread per stream | no JSON API in the JDK (example ships a tiny one); `\uXXXX` is processed even inside comments; lambdas need effectively-final captures |
| Go | goroutines (one per stream) + SDK-internal multiplexing | **uses the official SDK, so no framing/handshake/heartbeat code**: implement just the three `sidecar.Handler` methods (`Hello` / `Call` / `Open`) |

> **Why the Go example is so short**: SwiftMQ is a Go project, so it ships an official SDK —
> [`pkg/sidecar`](https://github.com/houzch/swiftmq/tree/master/pkg/sidecar) (the server role).
> The other languages have no SDK, which is why those four implement the wire protocol themselves —
> and precisely why they are the most readable *protocol spec*.
>
> The Go example needs the swiftmq module (`go mod tidy` fetches `github.com/houzch/swiftmq`).
> Offline or behind a proxy, point it at a local clone with `go work` or `replace`:
>
> ```
> go mod edit -replace github.com/houzch/swiftmq=<path-to-local-swiftmq>
> ```

---

## How it works

```
client ──► broker listener ──sniff prefix──► open stream ──► your plugin process (a local TCP server)
                                                                   │
                                    reverse call core.authenticate / session.*  ◄─┤
                                    (plugin → broker: queues / routing / auth / acks)
                                                         broker ──delivery push──►│
```

Three things people get wrong first:

1. **Your plugin process is the server**: it listens on a local address and the **broker dials it**
   (`sidecar.address`).
2. **The public port is opened by the broker**: clients connect to the broker
   (`protocols[].listeners`), and the broker proxies those bytes to you.
3. **`prefix` must be non-empty**: the ingress layer decides ownership purely by sniffing the first
   bytes. An empty `prefix` means "does not participate in sniffing", so such connections are
   **never** handed to you — even on a dedicated port. Give every protocol a non-empty prefix
   (examples treat it as the protocol's magic header).

---

## Wire protocol cheat sheet

**Frame layout** (all frames, network byte order):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) bytes    |
+--------+--------+------------------+
len = 1 + len(payload); max frame 16 MiB.
```

**Frame kinds**:

| kind | Name | Direction | Payload |
| --- | --- | --- | --- |
| 1 | Hello | broker → plugin | JSON |
| 2 | HelloAck | plugin → broker | JSON (`deny` non-empty = refuse) |
| 3 / 4 | Ping / Pong | broker → plugin / plugin → broker | empty (broker pings every 2s; reply within 8s) |
| 5 / 6 | Call / Reply | both ways | JSON (`reverse` tells the direction apart) |
| 7 / 8 | Open / OpenAck | broker → plugin / plugin → broker | JSON (one stream = one client connection) |
| 9 | Data | both ways | `u32 BE stream id` + raw bytes (no base64) |
| 10 | Close | both ways | JSON |

**Key rules**:

- **Handshake**: the broker sends `Hello`, you reply `HelloAck`. The broker checks `name`
  (must equal the configured plugin name) and `api_version` (currently `v1`).
- **Two ID spaces**: `Call.reverse` distinguishes broker → plugin (false) from plugin → broker (true);
  both start at 1, so a `Reply` **must echo `reverse`**.
- **Order**: `core.authenticate` → `session.open` → any other `session.*`.
  Opening a session before authenticating is refused (`ACCESS_REFUSED ... for user ''`).
- **Deliveries**: the broker pushes consumed messages to you via the **forward** call
  `session.deliver`; you settle with `session.settle` (`ack` / `requeue` / `reject`; delivery ids are
  unique per connection).
- **No message loss**: unsettled deliveries are re-queued when the stream or the plugin connection ends.

**Reverse calls** (plugin → broker):

| Method | Notes |
| --- | --- |
| `core.authenticate` | must be first; params `{stream, mechanism, response}` with `response` base64-encoded bytes |
| `session.open` / `session.close` | open/release a vhost session on a stream |
| `session.declare_exchange` / `session.delete_exchange` / `session.bind_exchange` / `session.unbind_exchange` | exchange management |
| `session.declare_queue` / `session.delete_queue` / `session.bind_queue` / `session.unbind_queue` / `session.purge_queue` | queue management |
| `session.publish` / `session.get` / `session.consume` / `session.cancel` / `session.settle` | publish, get, consume, settle |
| `session.deliver` (**forward**) | broker → plugin delivery push-back |

---

## Writing your own plugin (checklist)

1. Listen on a local TCP/Unix socket (the broker dials you).
2. Speak the frame format above; handle `Hello` → reply `HelloAck`.
3. Reply `Pong` to every `Ping`.
4. On `Open` → reply `OpenAck` → serve that stream; `Data` payload is `stream id + raw bytes`.
5. To use broker semantics (queues/publish/consume), call `core.authenticate` first, then
   `session.open`, then the rest.
6. Declare it in the broker config: `plugins.<name>.sidecar`
   (`address` / `spawn` / `restart` / `protocols`).
7. Verify: check `GET /api/plugins`, then exercise the `listeners` port with a real client.

---

## Verified, not "looks runnable"

These paths were actually executed on a developer machine (see the header comments in each file and the
[upstream per-language docs](https://github.com/houzch/swiftmq/tree/master/docs)):

```
Environment: Windows + Python 3.12 / Node 24 / PHP 7.4 / JDK 25 / Go 1.24
Broker: SwiftMQ 1.1.01 in Docker; plugin: host process (broker dials host.docker.internal)

/api/plugins → <name> state=enabled
client         printf 'PYhello\n' | nc 127.0.0.1 <broker port>   → echoes PYhello
(Go example likewise: printf 'GOhello\n' → echoes GOhello)
plugin log     握手完成 → 认证通过 user=guest → session 演示完成
               → 流已打开 → 收到投递（session.deliver） body=hello from <lang> sidecar
```

---

## FAQ

| Symptom | Cause / fix |
| --- | --- |
| `failed`, reason mentions "plugin name mismatch" | configured plugin name ≠ `HelloAck.name` |
| `failed`, reason mentions "API version mismatch" | `HelloAck.api_version` ≠ broker `APIVersion` (`v1`) |
| `down` | plugin process died / connection dropped; `restart=always` reconnects, `never` waits for you |
| Client connects then is dropped immediately | empty `prefix`, or the first bytes do not match it (see rule 3 above) |
| `ACCESS_REFUSED ... for user ''` | you did not call `core.authenticate` first |
| Reverse call says "stream N has no session" | order must be `core.authenticate` → `session.open` → rest |
| Broker in Docker, plugin on host: cannot connect | use `tcp://host.docker.internal:<port>` and listen on `0.0.0.0` |
| A user cannot connect from another machine | users only allow loopback by default; set `remote_access: true` (local development only) |
| Broker container will not start / stays in `created` | host ports `5672` / `15672` may already be in use; remap them, e.g. `-p 15680:15672 -p 5673:5672` |

---

## Relation to upstream / further reading

- **SwiftMQ broker**: <https://github.com/houzch/swiftmq> — the authoritative protocol definition lives in `pkg/sidecar`.
- **Plugin SDK** (Go): `github.com/houzch/swiftmq/pkg/sidecar` (plugin-side server) + `pkg/plugin` (plugin contract).
  This repo's [`go/main.go`](go/main.go) is written with it.
- **Per-language guides** (upstream `docs/`): `plugin-development.md` (overview + protocol spec),
  `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`.
- **Another Go reference implementation**: `test/integration/echosidecar` in the upstream repo
  (same idea as this repo's `go/`; it adds two control-plane methods `stats` / `set_greeting` for the
  broker's integration tests).

---

## Contributing

- For bug reports, include the language, runtime version, reproduction steps and expected behaviour.
- When adding a language, match the **behaviour** of the existing examples (same handshake /
  authentication / session bridge / settle), not merely the code structure.
- Before submitting, make sure your example really runs locally (handshake + auth + delivery/settle +
  byte-stream echo), and document its runtime requirements and how to verify it.

---

## License

[Apache License 2.0](LICENSE). See also [NOTICE](NOTICE).
