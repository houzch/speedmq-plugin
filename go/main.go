// Command sidecar-plugin 是 SwiftMQ 外部进程插件（sidecar）示例 —— **Go 实现**。
//
// 行为与同系列其它语言示例（python / nodejs / php / java）**一致**：
// 握手 → 认证 → 内核语义桥（声明队列 / 发布 / 消费 / 结算）→ 客户端字节流回显。
//
// 唯一的差别是：Go 有官方 SDK（github.com/houzch/swiftmq/pkg/sidecar），
// 分帧、握手、心跳、多路复用都由它实现，插件作者只需要实现 Handler 的三个方法；
// 其它语言没有 SDK，所以那几个示例各自手写了线协议（见同系列对应目录）。
//
// 用法（通常由内核按配置 spawn，也可手动起）：
//
//	go run . -addr tcp://0.0.0.0:19041 -name go-sidecar -session-demo
//
// 配套文档：swiftmq/docs/plugin-development.md（心智模型 / 配置字段 / 线协议总表）。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/houzch/swiftmq/pkg/plugin"
	"github.com/houzch/swiftmq/pkg/sidecar"
)

const (
	// version 是插件自报的版本。
	version = "0.1.0"
	// protocol 是本插件提供的协议名：内核配置里 protocols[].name 要与它一致。
	protocol = "goecho"
)

func main() {
	addr := flag.String("addr", "tcp://0.0.0.0:19041", "监听地址：tcp://host:port 或 unix:///path")
	name := flag.String("name", "go-sidecar", "插件名（须与内核配置一致）")
	sessionDemo := flag.Bool("session-demo", false, "每条流打开后演示内核语义桥（认证→声明队列→发布→消费→结算）")
	vhost := flag.String("vhost", "/", "session-demo 使用的 vhost")
	user := flag.String("user", "guest", "session-demo 认证用的用户名")
	password := flag.String("password", "guest", "session-demo 认证用的口令")
	flag.Parse()

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	h := &handler{
		name:        *name,
		log:         logger,
		sessionDemo: *sessionDemo,
		vhost:       *vhost,
		user:        *user,
		password:    *password,
	}

	srv, err := sidecar.NewServer(h, sidecar.ServerOptions{
		Address: *addr,
		Logger:  stdLogger{logger},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "sidecar 监听失败: %v\n", err)
		os.Exit(1)
	}
	logger.Printf("sidecar 已启动 plugin=%s addr=%s version=%s", *name, srv.Address(), version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "sidecar 服务退出: %v\n", err)
		os.Exit(1)
	}
}

// handler 实现 sidecar.Handler：握手 + 控制面方法 + 数据面回显。
//
// 三个方法都可能被并发调用（多条内核连接、每条连接多条流），共享状态要自己保证并发安全。
type handler struct {
	name        string
	log         *log.Logger
	sessionDemo bool
	vhost       string
	// user / password 只是演示用：真实插件应当从**自己的协议**里解析出凭据，
	// 再交给 core.authenticate 让内核校验（插件不接触口令库）。
	user     string
	password string

	// 计数用 atomic.Int64：它由编译器保证 8 字节对齐，在 32 位平台上也是安全的。
	streams atomic.Int64
}

// Hello 处理握手：任一校验失败都返回 error，内核会据此拒绝加载并隔离该插件。
func (h *handler) Hello(_ context.Context, hello sidecar.Hello) (sidecar.HelloAck, error) {
	if hello.ProtocolVersion != sidecar.ProtocolVersion {
		return sidecar.HelloAck{}, fmt.Errorf("不支持的线协议版本 %q（本插件为 %q）",
			hello.ProtocolVersion, sidecar.ProtocolVersion)
	}
	if hello.APIVersion != plugin.APIVersion {
		return sidecar.HelloAck{}, fmt.Errorf("不支持的插件 API 版本 %q（本插件实现 %q）",
			hello.APIVersion, plugin.APIVersion)
	}
	if hello.Plugin != h.name {
		return sidecar.HelloAck{}, fmt.Errorf("插件名不匹配：内核声明 %q，本插件是 %q", hello.Plugin, h.name)
	}
	h.log.Printf("握手完成 plugin=%s kernel=%s", h.name, hello.KernelVersion)
	return sidecar.HelloAck{
		Name:       h.name,
		Version:    version,
		APIVersion: plugin.APIVersion,
		// Capabilities 只用于管理面审计展示；真正的授权仍在插件进程内自治。
		Capabilities: []string{string(plugin.CapNetListen)},
		Protocols:    []string{protocol},
		Methods:      []string{"stats"},
	}, nil
}

// Call 处理控制面方法（含内核回推投递的正向调用 session.deliver）。
func (h *handler) Call(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case sidecar.MethodSessionDeliver:
		return h.handleDeliver(ctx, params)
	case "stats":
		return map[string]any{"name": h.name, "streams": h.streams.Load()}, nil
	default:
		// 未知方法必须明确报错：静默成功会让调用方以为生效了。
		return nil, fmt.Errorf("未知方法 %q", method)
	}
}

// Open 处理一条新流：先（可选）演示语义桥，然后把客户端字节原样回显，直到流结束。
func (h *handler) Open(ctx context.Context, stream *sidecar.Stream, meta sidecar.Open) error {
	h.streams.Add(1)
	defer h.streams.Add(-1)

	if h.sessionDemo {
		h.runSessionDemo(ctx, stream)
	}
	h.log.Printf("流已打开 plugin=%s stream=%d remote=%s local=%s", h.name, stream.ID(), meta.Remote, meta.Local)

	// 数据面就是"读多少写多少"：SDK 负责分帧（载荷是 4 字节大端流号 + 原始字节）与并发复用。
	_, err := io.Copy(stream, stream)
	if err != nil && ctx.Err() == nil {
		h.log.Printf("流读取结束 stream=%d err=%v", stream.ID(), err)
	}
	return nil
}

// runSessionDemo 在流上跑一遍内核语义桥：与进程内协议插件走的是同一套 Session 语义。
//
// 第一步必须是 core.authenticate：连接的内核操作面在认证前没有身份，
// 任何 session.open 都会被拒（内核报 ACCESS_REFUSED：access to vhost / refused for user，此时用户名为空）。
func (h *handler) runSessionDemo(ctx context.Context, stream *sidecar.Stream) {
	br, ok := sidecar.BridgeFromContext(ctx)
	if !ok {
		h.log.Printf("session 演示：ctx 中没有内核桥，跳过")
		return
	}
	sid := stream.ID()

	// SASL PLAIN 响应布局：\x00<user>\x00<password>；DTO 里的 []byte 会自动 base64 编码上线。
	plain := append([]byte("\x00"+h.user+"\x00"), []byte(h.password)...)
	var ident sidecar.AuthIdentityDTO
	if err := br.Call(ctx, sidecar.MethodCoreAuthenticate, sidecar.CoreAuthenticateParams{
		Stream: sid, Mechanism: "PLAIN", Response: plain,
	}, &ident); err != nil {
		h.log.Printf("session 演示：认证失败: %v", err)
		return
	}
	h.log.Printf("认证通过 user=%s", ident.User)

	if err := br.Call(ctx, sidecar.MethodSessionOpen,
		sidecar.SessionOpenParams{Stream: sid, VHost: h.vhost}, nil); err != nil {
		h.log.Printf("session 演示：打开 vhost %q 的会话失败: %v", h.vhost, err)
		return
	}
	// 独占 + 自动删除：演示用临时队列，流结束时会随会话一起被内核清理。
	var q sidecar.QueueInfoResult
	if err := br.Call(ctx, sidecar.MethodSessionDeclareQueue, sidecar.QueueDeclareParams{
		Stream: sid, Exclusive: true, AutoDelete: true,
	}, &q); err != nil {
		h.log.Printf("session 演示：声明队列失败: %v", err)
		return
	}
	var pub sidecar.PublishResultDTO
	if err := br.Call(ctx, sidecar.MethodSessionPublish, sidecar.PublishParams{
		Stream:     sid,
		RoutingKey: q.Name,
		Message: sidecar.MessageDTO{
			Body:       []byte("hello from go sidecar"),
			Properties: sidecar.PropertiesDTO{ContentType: "text/plain"},
		},
	}, &pub); err != nil {
		h.log.Printf("session 演示：发布失败: %v", err)
		return
	}
	var cons sidecar.ConsumeResult
	if err := br.Call(ctx, sidecar.MethodSessionConsume, sidecar.ConsumeParams{
		Stream: sid, Queue: q.Name, Prefetch: 32,
	}, &cons); err != nil {
		h.log.Printf("session 演示：注册消费者失败: %v", err)
		return
	}
	h.log.Printf("session 演示完成 queue=%s routed=%v tag=%s（等 session.deliver 回推后结算）",
		q.Name, pub.Routed, cons.Tag)
}

// handleDeliver 处理内核回推的投递（正向调用 session.deliver）：打印消息体并 ack 结算。
func (h *handler) handleDeliver(ctx context.Context, params json.RawMessage) (any, error) {
	var p sidecar.DeliverParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, fmt.Errorf("session.deliver 参数解析失败: %w", err)
	}
	h.log.Printf("收到投递（session.deliver） queue=%s delivery_id=%d body=%q",
		p.Queue, p.DeliveryID, string(p.Message.Body))

	br, ok := sidecar.BridgeFromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("ctx 中没有内核桥，无法结算")
	}
	// 结算不必带流号：投递编号在整条连接上全局唯一。
	if err := br.Call(ctx, sidecar.MethodSessionSettle, sidecar.SettleParams{
		DeliveryID: p.DeliveryID, Action: sidecar.SettleActionAck,
	}, nil); err != nil {
		return nil, fmt.Errorf("结算失败: %w", err)
	}
	return nil, nil
}

// stdLogger 把 sidecar 的最小日志接口接到标准库日志。
type stdLogger struct{ l *log.Logger }

func (s stdLogger) Info(msg string, args ...any) {
	s.l.Printf("%s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}

func (s stdLogger) Warn(msg string, args ...any) {
	s.l.Printf("WARN %s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}

// 让编译器确认接口实现（顺便当作接口文档）。
var (
	_ sidecar.Handler = (*handler)(nil)
	_ sidecar.Logger  = stdLogger{}
)
