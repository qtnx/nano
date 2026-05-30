package cluster

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lonng/nano/benchmark/testdata"
	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/scheduler"
	nanojson "github.com/lonng/nano/serialize/json"
	"github.com/lonng/nano/session"
	"github.com/valyala/fasthttp"
)

// The global scheduler can only be started once per process (its started/closed
// counters are one-shot), so share a single run across the tests that need
// tasks to actually execute and never Close it (the process exits anyway).
var schedOnce sync.Once

func ensureScheduler() { schedOnce.Do(func() { go scheduler.Sched() }) }

func useJSONSerializer() func() {
	old := env.Serializer
	env.Serializer = nanojson.NewSerializer()
	return func() { env.Serializer = old }
}

// --- H7: send() must never block once the buffer fills -----------------------

func TestAgentSendNonBlocking(t *testing.T) {
	log.SetLogger(&noopLogger{})
	// An agent whose write goroutine is NOT started never drains chSend.
	a := newAgent(newCountConn(), nil, nil)

	const producers = 64
	done := make(chan error, producers)
	for i := 0; i < producers; i++ {
		go func() { done <- a.Push("route", []byte("x")) }()
	}

	deadline := time.After(2 * time.Second)
	exceed := 0
	for i := 0; i < producers; i++ {
		select {
		case err := <-done:
			if err == ErrBufferExceed {
				exceed++
			}
		case <-deadline:
			t.Fatalf("Push blocked: %d/%d producers still pinned on a full chSend (H7)", producers-i, producers)
		}
	}
	if exceed == 0 {
		t.Fatal("expected some Push calls to report ErrBufferExceed on a full buffer (H7)")
	}
}

// --- H32: SSE Push must surface an error instead of silently dropping --------

func TestHTTPAgentPushErrors(t *testing.T) {
	log.SetLogger(&noopLogger{})
	ch := make(chan []byte, 1)
	a := NewHTTPAgent(1, nil, ch, nil, &fasthttp.RequestCtx{})

	if err := a.Push("r", map[string]interface{}{"a": 1}); err != nil {
		t.Fatalf("first push should succeed: %v", err)
	}
	// Channel (cap 1) is now full.
	if err := a.Push("r", map[string]interface{}{"a": 2}); err != ErrBufferExceed {
		t.Fatalf("expected ErrBufferExceed on a full SSE channel, got %v (H32)", err)
	}

	a.Close()
	if err := a.Push("r", map[string]interface{}{"a": 3}); err != ErrBrokenPipe {
		t.Fatalf("expected ErrBrokenPipe after Close, got %v (H32/H17)", err)
	}
}

// --- H33: message ids and SSE session ids must be unique ---------------------

func TestHTTPAgentNextMidUnique(t *testing.T) {
	a := NewHTTPAgent(1, nil, nil, nil, &fasthttp.RequestCtx{})
	seen := make(map[uint64]bool, 1000)
	for i := 0; i < 1000; i++ {
		m := a.nextMid()
		if seen[m] {
			t.Fatalf("duplicate message id %d (H33)", m)
		}
		seen[m] = true
	}
}

func TestGenerateSessionIDUnique(t *testing.T) {
	seen := make(map[string]bool, 2000)
	for i := 0; i < 2000; i++ {
		id := generateSessionID()
		if seen[id] {
			t.Fatalf("duplicate SSE session id %q (H33)", id)
		}
		seen[id] = true
		if _, err := strconv.ParseInt(id, 10, 64); err != nil {
			t.Fatalf("SSE session id %q is not a base-10 int64: %v", id, err)
		}
	}
}

// --- H16/H34: concurrent /api responses route per request; ctx not touched ---

func TestHTTPResponseMidRoutesPerRequest(t *testing.T) {
	log.SetLogger(&noopLogger{})
	defer useJSONSerializer()()

	a := NewHTTPAgent(1, nil, nil, nil, &fasthttp.RequestCtx{})
	c1, c2 := &fasthttp.RequestCtx{}, &fasthttp.RequestCtx{}
	a.AttackHttpRequestCtx(10, c1)
	a.AttackHttpRequestCtx(20, c2)

	// Respond out of order; each response must land on its own request.
	if err := a.ResponseMid(20, []byte(`"twenty"`)); err != nil {
		t.Fatalf("ResponseMid(20): %v", err)
	}
	if err := a.ResponseMid(10, []byte(`"ten"`)); err != nil {
		t.Fatalf("ResponseMid(10): %v", err)
	}

	o1 := a.GetFastHttpContextObserve(10)
	o2 := a.GetFastHttpContextObserve(20)
	if string(o1.body) != `"ten"` {
		t.Fatalf("mid 10 got %q, want \"ten\" (H34 cross-wire)", o1.body)
	}
	if string(o2.body) != `"twenty"` {
		t.Fatalf("mid 20 got %q, want \"twenty\" (H34 cross-wire)", o2.body)
	}

	// H16: ResponseMid must NOT write the fasthttp ctx from this (non-owner)
	// goroutine; the owning /api goroutine writes it after the done signal.
	if len(c1.Response.Body()) != 0 || len(c2.Response.Body()) != 0 {
		t.Fatal("ResponseMid wrote the RequestCtx body from a non-owner goroutine (H16)")
	}
}

// --- H17/M31: Close is idempotent, synchronized, and signals the SSE stream --

func TestHTTPAgentCloseIdempotentSignalsSSE(t *testing.T) {
	log.SetLogger(&noopLogger{})
	a := NewHTTPAgent(1, nil, make(chan []byte, 1), nil, &fasthttp.RequestCtx{})

	// An in-flight waiter must be woken (not left blocking) on close.
	a.AttackHttpRequestCtx(7, &fasthttp.RequestCtx{})
	o := a.GetFastHttpContextObserve(7)

	a.Close()
	select {
	case <-o.done:
	default:
		t.Fatal("Close did not wake the in-flight request waiter (H17)")
	}
	select {
	case <-a.sseDone:
	default:
		t.Fatal("Close did not signal the SSE stream to stop (M31)")
	}
	// Idempotent: a second Close must not panic (double close-of-closed).
	a.Close()
}

// --- H19: a stale stream's unregister must not tear down its replacement -----

func TestSSEUnregisterChannelIdentity(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	chA := make(chan []byte, 1)
	chB := make(chan []byte, 1)
	n.registerSSEClient("555", chA)
	n.registerSSEClient("555", chB) // reconnect replaces the stream

	// The OLD stream's defer runs with the OLD channel; it must be a no-op.
	n.unregisterSSEClient("555", chA)
	if n.sseClient("555") != chB {
		t.Fatal("stale unregister removed the replacement live stream (H19)")
	}

	// The live stream's own unregister does delete it.
	n.unregisterSSEClient("555", chB)
	if n.sseClient("555") != nil {
		t.Fatal("live stream's unregister did not remove the mapping")
	}
}

// --- H18: a client-supplied SSE id must not hijack a non-SSE session ---------

func TestHandleSSERejectsNonSSECollision(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()

	// Pre-store a non-SSE (acceptor) session under id 777.
	ac := &acceptor{sid: 777}
	s := session.NewWithID(ac, 777)
	n.storeSession(s)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/sse")
	ctx.Request.Header.Set("X-SSE-SessionID", "777")
	n.handleSSE(ctx)

	if ctx.Response.StatusCode() != fasthttp.StatusConflict {
		t.Fatalf("expected 409 for SSE id colliding with a non-SSE session, got %d (H18)", ctx.Response.StatusCode())
	}
	// The colliding session must not have been replaced.
	if _, ok := n.findSession(777).NetworkEntity().(*acceptor); !ok {
		t.Fatal("SSE handler overwrote a non-SSE session's entity (H18)")
	}
}

// --- H26: HandshakeAck must require a prior validated Handshake ---------------

func TestHandshakeAckRequiresHandshake(t *testing.T) {
	log.SetLogger(&noopLogger{})
	cache()
	h := newTestNode().handler

	// HandshakeAck as the first packet must be rejected.
	a := newAgent(newCountConn(), nil, h.remoteProcess)
	if err := h.processPacket(a, &packet.Packet{Type: packet.HandshakeAck}); err == nil {
		t.Fatal("HandshakeAck accepted before a validated Handshake (H26)")
	}
	if a.status() == statusWorking {
		t.Fatal("agent reached statusWorking without a validated handshake (H26)")
	}

	// The proper sequence must succeed.
	b := newAgent(newCountConn(), nil, h.remoteProcess)
	if err := h.processPacket(b, &packet.Packet{Type: packet.Handshake}); err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	if b.status() != statusHandshake {
		t.Fatalf("after Handshake status=%d, want statusHandshake", b.status())
	}
	if err := h.processPacket(b, &packet.Packet{Type: packet.HandshakeAck}); err != nil {
		t.Fatalf("HandshakeAck after Handshake: %v", err)
	}
	if b.status() != statusWorking {
		t.Fatalf("after HandshakeAck status=%d, want statusWorking", b.status())
	}
}

// --- M16: the session-id push must be a valid nano message frame -------------

func TestEncodeSessionIdIsValidMessage(t *testing.T) {
	log.SetLogger(&noopLogger{})
	raw := encodeSessionId(42)
	if raw == nil {
		t.Fatal("encodeSessionId returned nil")
	}
	dec := codec.NewDecoder()
	packets, err := dec.Decode(raw)
	if err != nil || len(packets) != 1 {
		t.Fatalf("packet decode: err=%v packets=%d", err, len(packets))
	}
	msg, err := message.Decode(packets[0].Data)
	if err != nil {
		t.Fatalf("message.Decode rejected the session-id frame (M16): %v", err)
	}
	if msg.Route != "onSessionId" {
		t.Fatalf("session-id message route=%q, want onSessionId", msg.Route)
	}
}

// --- M7: remoteProcess must report routing/delivery failures -----------------

func TestRemoteProcessReturnsError(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	s := newAgent(newCountConn(), nil, n.handler.remoteProcess).session

	if err := n.handler.remoteProcess(s, &message.Message{Type: message.Request, Route: "noDotRoute"}, false); err == nil {
		t.Fatal("expected error for an invalid (dotless) route (M7)")
	}
	if err := n.handler.remoteProcess(s, &message.Message{Type: message.Request, Route: "Unknown.Method"}, false); err == nil {
		t.Fatal("expected error for an unregistered service (M7)")
	}
}

// --- L9: a malformed ServiceAddr must error, not panic, on startup -----------

func TestInitNodeRejectsMalformedServiceAddr(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := &Node{
		Options:     Options{AdvertiseAddr: "127.0.0.1:9000"}, // forces initNode past the singleton check
		ServiceAddr: "localhost",                              // no ":port" -> old code panicked on [1]
	}
	n.handler = NewHandler(n, nil)
	n.cluster = newCluster(n)
	if err := n.initNode(); err == nil {
		t.Fatal("expected an error for a malformed ServiceAddr, got nil (L9)")
	}
}

// --- M36: convertFastHTTPToHTTP must preserve body and content metadata ------

func TestConvertFastHTTPPopulatesBody(t *testing.T) {
	body := `{"hello":"world"}`
	ctx := newAPIPost(body, "1")
	r := convertFastHTTPToHTTP(ctx)

	if r.Body == nil {
		t.Fatal("request body not populated (M36)")
	}
	got, _ := io.ReadAll(r.Body)
	if string(got) != body {
		t.Fatalf("body=%q, want %q (M36)", got, body)
	}
	if r.ContentLength != int64(len(body)) {
		t.Fatalf("ContentLength=%d, want %d (M36)", r.ContentLength, len(body))
	}
	if r.RequestURI == "" {
		t.Fatal("RequestURI not set (M36)")
	}
}

// --- H20/M35/H16: a local request runs, decodes JSON, and responds 200 -------

type HTTPEchoComp struct{ component.Base }

func (c *HTTPEchoComp) Echo(s *session.Session, ping *testdata.Ping) error {
	return s.Response(&testdata.Pong{Content: ping.Content})
}

func TestHTTPLocalRequestResponse(t *testing.T) {
	log.SetLogger(&noopLogger{})
	defer useJSONSerializer()()
	ensureScheduler()

	n := newTestNode()
	if err := n.handler.register(&HTTPEchoComp{}, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	ch := make(chan []byte, 4)
	a := NewHTTPAgent(4242, nil, ch, n.handler.remoteProcess, &fasthttp.RequestCtx{})
	n.storeSession(a.session)
	n.registerSSEClient("4242", ch)

	ctx := newAPIPost(`{"route":"HTTPEchoComp.Echo","data":{"content":"hi"},"type":0}`, "4242")
	done := make(chan struct{})
	go func() { n.handleHTTPRequest(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleHTTPRequest did not complete (H20)")
	}

	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("status=%d, want 200 (H20/M35)", ctx.Response.StatusCode())
	}
	if got := string(ctx.Response.Body()); got != `{"content":"hi"}` && got != `{"Content":"hi"}` {
		t.Fatalf("response body=%q, want the echoed content (M35/H16)", got)
	}
}

// --- H20: a local notify is acknowledged immediately and never blocks --------

type HTTPNotifyComp struct {
	component.Base
	got chan struct{}
}

func (c *HTTPNotifyComp) Do(s *session.Session, _ []byte) error {
	select {
	case c.got <- struct{}{}:
	default:
	}
	return nil
}

func TestHTTPLocalNotifyReturns200Immediately(t *testing.T) {
	log.SetLogger(&noopLogger{})
	ensureScheduler()

	n := newTestNode()
	comp := &HTTPNotifyComp{got: make(chan struct{}, 1)}
	if err := n.handler.register(comp, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	ch := make(chan []byte, 4)
	a := NewHTTPAgent(5252, nil, ch, n.handler.remoteProcess, &fasthttp.RequestCtx{})
	n.storeSession(a.session)
	n.registerSSEClient("5252", ch)

	ctx := newAPIPost(`{"route":"HTTPNotifyComp.Do","data":{},"type":1}`, "5252")
	start := time.Now()
	n.handleHTTPRequest(ctx)
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("notify blocked %v before returning (H20)", d)
	}
	if ctx.Response.StatusCode() != fasthttp.StatusOK {
		t.Fatalf("notify status=%d, want 200 (H20)", ctx.Response.StatusCode())
	}
	// The handler must actually run; if the scheduler task blocked (old unread
	// responseChan), this never fires.
	select {
	case <-comp.got:
	case <-time.After(2 * time.Second):
		t.Fatal("notify handler did not run; scheduler task blocked (H20)")
	}
}

// --- M31: a backend-initiated CloseSession stops the SSE stream + session ----

func TestCloseSessionStopsSSEStream(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	ch := make(chan []byte, 4)
	a := NewHTTPAgent(888, nil, ch, n.handler.remoteProcess, &fasthttp.RequestCtx{})
	n.storeSession(a.session)
	n.registerSSEClient("888", ch)

	if _, err := n.CloseSession(context.Background(), &clusterpb.CloseSessionRequest{SessionId: 888}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
	select {
	case <-a.sseDone:
	case <-time.After(time.Second):
		t.Fatal("CloseSession did not stop the SSE stream (M31)")
	}
	if n.findSession(888) != nil {
		t.Fatal("CloseSession did not remove the session")
	}
}

// --- H25: master register/unregister must mutate local state despite a -------
//          failed peer fan-out.

func newMasterCluster(t *testing.T) (*cluster, *rpcClient) {
	t.Helper()
	node := &Node{Options: Options{IsMaster: true}, ServiceAddr: "127.0.0.1:9000"}
	c := newCluster(node)
	t.Cleanup(c.stopHeartbeatChecker)
	node.cluster = c
	node.handler = NewHandler(node, nil)
	rc := newRPCClient()
	c.setRpcClient(rc)
	return c, rc
}

func TestRegisterBestEffortOnPeerFailure(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, rc := newMasterCluster(t)
	c.addMember(memberInfo("127.0.0.1:9001", "peer"), 0, 0)
	rc.closePool() // every getConnPool now errors -> peer fan-out must be best-effort

	resp, err := c.Register(context.Background(), &clusterpb.RegisterRequest{MemberInfo: memberInfo("127.0.0.1:9002", "svc")})
	if err != nil {
		t.Fatalf("Register aborted on an unreachable peer (H25): %v", err)
	}
	if resp == nil {
		t.Fatal("nil register response")
	}
	if !c.isKnownAddr("127.0.0.1:9002") {
		t.Fatal("new member not added to the local registry despite peer fan-out failure (H25)")
	}
}

func TestUnregisterBestEffortOnPeerFailure(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, rc := newMasterCluster(t)
	c.addMember(memberInfo("127.0.0.1:9002", "svc"), 0, 0)  // the departing member
	c.addMember(memberInfo("127.0.0.1:9001", "peer"), 0, 0) // a peer to notify
	rc.closePool()

	if _, err := c.Unregister(context.Background(), &clusterpb.UnregisterRequest{ServiceAddr: "127.0.0.1:9002"}); err != nil {
		t.Fatalf("Unregister aborted on an unreachable peer (H25): %v", err)
	}
	if c.isKnownAddr("127.0.0.1:9002") {
		t.Fatal("member not removed from the local registry despite peer fan-out failure (H25)")
	}
}

// --- H11: the client read loop must refresh a read deadline ------------------

type readDeadlineConn struct {
	remote net.Addr
	set    chan struct{}
	once   sync.Once
}

func (c *readDeadlineConn) Read(b []byte) (int, error)  { return 0, io.EOF }
func (c *readDeadlineConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *readDeadlineConn) Close() error                { return nil }
func (c *readDeadlineConn) LocalAddr() net.Addr         { return c.remote }
func (c *readDeadlineConn) RemoteAddr() net.Addr        { return c.remote }
func (c *readDeadlineConn) SetDeadline(time.Time) error { return nil }
func (c *readDeadlineConn) SetReadDeadline(t time.Time) error {
	if !t.IsZero() {
		c.once.Do(func() { close(c.set) })
	}
	return nil
}
func (c *readDeadlineConn) SetWriteDeadline(time.Time) error { return nil }

func TestReadLoopSetsReadDeadline(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	conn := &readDeadlineConn{remote: ipAddr{"203.0.113.5:1234"}, set: make(chan struct{})}

	done := make(chan struct{})
	go func() { n.handler.handle(conn); close(done) }()

	select {
	case <-conn.set:
	case <-time.After(2 * time.Second):
		t.Fatal("read loop did not set a read deadline (H11)")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handle did not return after EOF")
	}
}
