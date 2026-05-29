package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// --- H1: sharded dispatch wiring in localProcess ----------------------------

// dispatchProbe routes a dispatched handler invocation to the test hook
// installed for the running test. It is an atomic.Value so the shard worker
// goroutines and the test goroutine never race on the function pointer.
var dispatchProbe atomic.Value // func(*session.Session, []byte)

type DispatchComp struct{ component.Base }

func (c *DispatchComp) Handle(s *session.Session, data []byte) error {
	if v := dispatchProbe.Load(); v != nil {
		v.(func(*session.Session, []byte))(s, data)
	}
	return nil
}

// shardOf replicates scheduler.mix (splitmix64 finalizer) + modulo so the test
// can pick session ids that deterministically land on distinct shards.
func shardOf(key uint64, n int) int {
	key ^= key >> 30
	key *= 0xbf58476d1ce4e5b9
	key ^= key >> 27
	key *= 0x94d049bb133111eb
	key ^= key >> 31
	return int(key % uint64(n))
}

func firstHandler(h *LocalHandler) (string, *component.Handler) {
	for route, handler := range h.localHandlers {
		return route, handler
	}
	return "", nil
}

func TestShardedDispatchPreservesOrderAndConcurrency(t *testing.T) {
	log.SetLogger(&noopLogger{})
	const nShards = 8
	scheduler.EnableSharded(nShards)
	if !scheduler.Sharded() {
		t.Fatal("expected sharded mode after EnableSharded")
	}

	n := newTestNode()
	if err := n.handler.register(&DispatchComp{}, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	route, handler := firstHandler(n.handler)
	if handler == nil {
		t.Fatal("no handler registered")
	}

	// --- same session id preserves FIFO order (one shard, one worker) -------
	const sid1 = int64(1)
	const total = 50
	var mu sync.Mutex
	got := make([]int, 0, total)
	setProbe := func(fn func(*session.Session, []byte)) { dispatchProbe.Store(fn) }
	setProbe(func(_ *session.Session, data []byte) {
		v, _ := strconv.Atoi(string(data))
		mu.Lock()
		got = append(got, v)
		mu.Unlock()
	})

	s1 := session.NewWithID(nil, sid1)
	for i := 0; i < total; i++ {
		msg := &message.Message{Type: message.Notify, Route: route, Data: []byte(strconv.Itoa(i))}
		n.handler.localProcess(handler, 0, s1, msg, nil)
	}
	if !waitFor(func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == total }, 3*time.Second) {
		mu.Lock()
		l := len(got)
		mu.Unlock()
		t.Fatalf("only %d/%d tasks ran for one session", l, total)
	}
	mu.Lock()
	for i := 0; i < total; i++ {
		if got[i] != i {
			order := append([]int(nil), got...)
			mu.Unlock()
			t.Fatalf("per-session order broken at index %d: %v", i, order)
		}
	}
	mu.Unlock()

	// --- two different session ids run concurrently (distinct shards) -------
	idA := int64(1)
	idB := int64(0)
	for cand := int64(2); cand < 100000; cand++ {
		if shardOf(uint64(cand), nShards) != shardOf(uint64(idA), nShards) {
			idB = cand
			break
		}
	}
	if idB == 0 {
		t.Fatal("could not find two session ids on distinct shards")
	}

	blockA := make(chan struct{})
	startedA := make(chan struct{})
	doneB := make(chan struct{})
	setProbe(func(s *session.Session, _ []byte) {
		switch s.ID() {
		case idA:
			close(startedA)
			<-blockA // pin this shard's worker
		case idB:
			close(doneB)
		}
	})

	sA := session.NewWithID(nil, idA)
	sB := session.NewWithID(nil, idB)
	n.handler.localProcess(handler, 0, sA, &message.Message{Type: message.Notify, Route: route, Data: []byte("A")}, nil)
	<-startedA // A's shard worker is now occupied
	n.handler.localProcess(handler, 0, sB, &message.Message{Type: message.Notify, Route: route, Data: []byte("B")}, nil)

	select {
	case <-doneB:
		// B ran while A is blocked => distinct sessions are not head-of-line
		// blocked by one another (concurrent across shards).
	case <-time.After(3 * time.Second):
		close(blockA)
		t.Fatal("session B did not run while session A was blocked (no cross-session concurrency)")
	}
	close(blockA) // release A
}

// --- M27: outbound []byte payloads are copied at the enqueue boundary -------

func TestPushCopiesBytePayload(t *testing.T) {
	log.SetLogger(&noopLogger{})
	// The write goroutine is NOT started, so chSend is never drained and we can
	// inspect the enqueued payload directly.
	a := newAgent(newCountConn(), nil, nil)

	orig := []byte("hello")
	if err := a.Push("r", orig); err != nil {
		t.Fatalf("Push: %v", err)
	}
	pm := <-a.chSend
	queued, ok := pm.payload.([]byte)
	if !ok {
		t.Fatalf("payload type %T, want []byte", pm.payload)
	}
	orig[0] = 'X' // caller reuses/mutates its buffer
	if string(queued) != "hello" {
		t.Fatalf("enqueued Push payload corrupted by caller mutation: %q (M27)", queued)
	}
}

func TestResponseMidCopiesBytePayload(t *testing.T) {
	log.SetLogger(&noopLogger{})
	a := newAgent(newCountConn(), nil, nil)

	orig := []byte("world")
	if err := a.ResponseMid(7, orig); err != nil {
		t.Fatalf("ResponseMid: %v", err)
	}
	pm := <-a.chSend
	queued := pm.payload.([]byte)
	orig[0] = 'Z'
	if string(queued) != "world" {
		t.Fatalf("enqueued Response payload corrupted by caller mutation: %q (M27)", queued)
	}

	// Non-[]byte payloads must pass through unchanged (no double copy).
	type box struct{ N int }
	in := &box{N: 5}
	if err := a.Push("r", in); err != nil {
		t.Fatalf("Push struct: %v", err)
	}
	pm2 := <-a.chSend
	if pm2.payload.(*box) != in {
		t.Fatal("non-[]byte payload was copied/replaced (M27 should only touch []byte)")
	}
}

// --- H11: node-global connection cap ----------------------------------------

func TestGlobalConnectionCapRejects(t *testing.T) {
	log.SetLogger(&noopLogger{})
	old := env.MaxConnections
	env.MaxConnections = 1
	defer func() { env.MaxConnections = old }()

	n := newTestNode()

	// First connection (distinct IP so the per-IP cap is irrelevant): accepted.
	c1 := newAddrConn(ipAddr{"203.0.113.1:1111"})
	done1 := make(chan struct{})
	go func() { n.handler.handle(c1); close(done1) }()
	if !waitFor(func() bool { return n.sessionLen() == 1 }, 2*time.Second) {
		t.Fatal("first connection was not accepted")
	}

	// Second connection (different IP): over the global cap -> rejected.
	c2 := newAddrConn(ipAddr{"198.51.100.2:2222"})
	done2 := make(chan struct{})
	go func() { n.handler.handle(c2); close(done2) }()
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("over-cap connection handler did not return")
	}
	if n.sessionLen() != 1 {
		t.Fatalf("global connection cap breached: %d sessions stored (want 1)", n.sessionLen())
	}

	// Disconnect releases the slot.
	c1.Close()
	<-done1
	if !waitFor(func() bool { return n.sessionLen() == 0 }, 2*time.Second) {
		t.Fatal("session not reclaimed after disconnect")
	}
	if got := atomic.LoadInt64(&n.acceptedConns); got != 0 {
		t.Fatalf("acceptedConns leaked: %d (want 0)", got)
	}
}

// --- H9 / M11: cluster auth interceptors ------------------------------------

func TestClusterAuthInterceptorEnforcesToken(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := &Node{}
	called := false
	handler := func(_ context.Context, _ interface{}) (interface{}, error) {
		called = true
		return "ok", nil
	}
	info := &grpc.UnaryServerInfo{FullMethod: "/clusterpb.Member/Ping"}

	old := env.ClusterAuthToken
	defer func() { env.ClusterAuthToken = old }()

	// (1) Pass-through when no token is configured.
	env.ClusterAuthToken = ""
	called = false
	resp, err := n.authUnaryInterceptor(context.Background(), nil, info, handler)
	if err != nil || resp != "ok" || !called {
		t.Fatalf("no-token pass-through failed: resp=%v err=%v called=%v", resp, err, called)
	}

	env.ClusterAuthToken = "s3cret"

	// (2) Missing metadata -> Unauthenticated, handler not run.
	called = false
	if _, err := n.authUnaryInterceptor(context.Background(), nil, info, handler); status.Code(err) != codes.Unauthenticated || called {
		t.Fatalf("missing-token: code=%v called=%v (want Unauthenticated, false)", status.Code(err), called)
	}

	// (3) Wrong token -> Unauthenticated, handler not run.
	badCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "nope"))
	called = false
	if _, err := n.authUnaryInterceptor(badCtx, nil, info, handler); status.Code(err) != codes.Unauthenticated || called {
		t.Fatalf("bad-token: code=%v called=%v (want Unauthenticated, false)", status.Code(err), called)
	}

	// (4) Correct token -> handler runs.
	okCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "s3cret"))
	called = false
	resp, err = n.authUnaryInterceptor(okCtx, nil, info, handler)
	if err != nil || resp != "ok" || !called {
		t.Fatalf("correct-token rejected: resp=%v err=%v called=%v", resp, err, called)
	}
}

func TestClusterAuthClientInterceptorAttachesToken(t *testing.T) {
	log.SetLogger(&noopLogger{})
	old := env.ClusterAuthToken
	defer func() { env.ClusterAuthToken = old }()

	var seen []string
	invoker := func(ctx context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			seen = md.Get("authorization")
		} else {
			seen = nil
		}
		return nil
	}

	// Token set -> attached on outgoing metadata.
	env.ClusterAuthToken = "tok"
	seen = nil
	if err := clusterAuthClientInterceptor(context.Background(), "/m", nil, nil, nil, invoker); err != nil {
		t.Fatalf("interceptor err: %v", err)
	}
	if len(seen) != 1 || seen[0] != "tok" {
		t.Fatalf("client interceptor did not attach the token: %v", seen)
	}

	// Token unset -> nothing attached (pass-through).
	env.ClusterAuthToken = ""
	seen = []string{"sentinel"}
	if err := clusterAuthClientInterceptor(context.Background(), "/m", nil, nil, nil, invoker); err != nil {
		t.Fatalf("interceptor err: %v", err)
	}
	if len(seen) != 0 {
		t.Fatalf("client interceptor attached metadata when no token set: %v", seen)
	}
}

// --- M35: HTTP responses are JSON-encoded, independent of env.Serializer ----

type poisonSerializer struct{}

func (poisonSerializer) Marshal(interface{}) ([]byte, error) {
	return nil, errors.New("poison serializer Marshal must not be used for HTTP responses (M35)")
}
func (poisonSerializer) Unmarshal([]byte, interface{}) error { return errors.New("poison") }

func TestHTTPResponseSerializesJSONNotGlobal(t *testing.T) {
	log.SetLogger(&noopLogger{})
	old := env.Serializer
	env.Serializer = poisonSerializer{}
	defer func() { env.Serializer = old }()

	a := NewHTTPAgent(1, nil, nil, nil, &fasthttp.RequestCtx{})
	a.AttackHttpRequestCtx(5, &fasthttp.RequestCtx{})

	// A non-[]byte value must be JSON-serialized via the HTTP JSON serializer,
	// NOT message.Serialize/env.Serializer (which here would error).
	if err := a.ResponseMid(5, map[string]interface{}{"content": "pong"}); err != nil {
		t.Fatalf("ResponseMid should JSON-serialize regardless of global serializer: %v", err)
	}
	o := a.GetFastHttpContextObserve(5)
	if o.contentType != "application/json" {
		t.Fatalf("content type = %q, want application/json (M35)", o.contentType)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(o.body, &decoded); err != nil {
		t.Fatalf("response body is not JSON: %v (body=%s)", err, o.body)
	}
	if decoded["content"] != "pong" {
		t.Fatalf("decoded body = %v", decoded)
	}

	// A []byte payload is already-encoded JSON and must pass through unchanged.
	a.AttackHttpRequestCtx(6, &fasthttp.RequestCtx{})
	if err := a.ResponseMid(6, []byte(`{"x":1}`)); err != nil {
		t.Fatalf("ResponseMid([]byte): %v", err)
	}
	if got := string(a.GetFastHttpContextObserve(6).body); got != `{"x":1}` {
		t.Fatalf("[]byte passthrough corrupted: %q (M35)", got)
	}
}

// --- H34: response correlation is request-scoped (explicit mid), not lastMid -

func TestHTTPResponseMidIsRequestScoped(t *testing.T) {
	log.SetLogger(&noopLogger{})

	a := NewHTTPAgent(1, nil, nil, nil, &fasthttp.RequestCtx{})
	c1, c2 := &fasthttp.RequestCtx{}, &fasthttp.RequestCtx{}
	a.AttackHttpRequestCtx(100, c1)
	a.AttackHttpRequestCtx(200, c2)

	// Poison the shared lastMid: a request-scoped implementation must ignore it
	// and route strictly by the explicit mid (H34).
	a.lastMid = 999999

	if err := a.ResponseMid(200, []byte(`"b"`)); err != nil {
		t.Fatalf("ResponseMid(200): %v", err)
	}
	if err := a.ResponseMid(100, []byte(`"a"`)); err != nil {
		t.Fatalf("ResponseMid(100): %v", err)
	}

	if got := string(a.GetFastHttpContextObserve(100).body); got != `"a"` {
		t.Fatalf("mid 100 got %q, want \"a\" (H34 cross-wire)", got)
	}
	if got := string(a.GetFastHttpContextObserve(200).body); got != `"b"` {
		t.Fatalf("mid 200 got %q, want \"b\" (H34 cross-wire)", got)
	}
	if a.GetFastHttpContextObserve(999999) != nil {
		t.Fatal("shared lastMid leaked into the per-request correlation map (H34)")
	}
}

// H34: a bare session.Response (no explicit mid) must route to the in-flight
// request bound to the calling goroutine, so concurrent requests sharing one
// agent cannot cross-wire even when the formerly shared lastMid field is
// hammered by both. localProcess wraps each handler call in runWithRequestMid.
func TestHTTPResponseRoutesPerGoroutineMid(t *testing.T) {
	log.SetLogger(&noopLogger{})

	a := NewHTTPAgent(1, nil, nil, nil, &fasthttp.RequestCtx{})
	a.AttackHttpRequestCtx(10, &fasthttp.RequestCtx{})
	a.AttackHttpRequestCtx(20, &fasthttp.RequestCtx{})

	// Deterministically force the cross-wire window. The shared lastMid is
	// driven 10 -> 20: goroutine A binds mid 10 first, then goroutine B binds
	// mid 20, and only then is A allowed to reply. A correct, goroutine-scoped
	// Response still replies to mid 10; the old shared-lastMid implementation
	// would read 20 and cross-wire onto B's request.
	aBound := make(chan struct{})
	bBound := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		a.runWithRequestMid(10, func() {
			close(aBound) // lastMid stored as 10
			<-bBound      // B has since stored lastMid = 20
			if err := a.Response([]byte(`"ten"`)); err != nil {
				t.Errorf("Response(mid=10): %v", err)
			}
		})
	}()
	go func() {
		defer wg.Done()
		<-aBound // ensure lastMid was 10 before we overwrite it with 20
		a.runWithRequestMid(20, func() {
			close(bBound)
			if err := a.Response([]byte(`"twenty"`)); err != nil {
				t.Errorf("Response(mid=20): %v", err)
			}
		})
	}()
	wg.Wait()

	if got := string(a.GetFastHttpContextObserve(10).body); got != `"ten"` {
		t.Fatalf("mid 10 body = %q, want \"ten\" (H34 cross-wire)", got)
	}
	if got := string(a.GetFastHttpContextObserve(20).body); got != `"twenty"` {
		t.Fatalf("mid 20 body = %q, want \"twenty\" (H34 cross-wire)", got)
	}
}

// H34: a bare session.Response from a goroutine with NO in-flight request bound
// (e.g. an async handler replying after its synchronous call returned) must NOT
// fall back to the shared lastMid. A later concurrent /api request can reassign
// lastMid, so the fallback would cross-wire this response onto that request.
// Response must reject, forcing async handlers onto the captured-mid
// ResponseMID path (session.LastMid() at entry + session.ResponseMID).
func TestHTTPResponseRejectsWhenNoRequestBound(t *testing.T) {
	log.SetLogger(&noopLogger{})

	a := NewHTTPAgent(1, nil, nil, nil, &fasthttp.RequestCtx{})
	a.AttackHttpRequestCtx(42, &fasthttp.RequestCtx{})
	// Simulate a later concurrent request having reassigned the shared field.
	a.lastMid = 42

	if err := a.Response([]byte(`"leak"`)); err != errNoRequestBound {
		t.Fatalf("Response from unbound goroutine = %v, want errNoRequestBound (H34)", err)
	}
	o := a.GetFastHttpContextObserve(42)
	if o == nil {
		t.Fatal("attached request 42 missing")
	}
	if len(o.body) != 0 {
		t.Fatalf("unbound Response cross-wired onto the shared lastMid request: body=%q (H34)", o.body)
	}
}

// M35: a []byte response payload that is NOT already valid JSON must be encoded
// through the HTTP JSON serializer rather than written raw; otherwise the
// application/json response carries invalid JSON. (Valid-JSON []byte still
// passes through unchanged; see TestHTTPResponseSerializesJSONNotGlobal.)
func TestHTTPResponseMidEncodesNonJSONBytes(t *testing.T) {
	log.SetLogger(&noopLogger{})

	a := NewHTTPAgent(1, nil, nil, nil, &fasthttp.RequestCtx{})
	a.AttackHttpRequestCtx(9, &fasthttp.RequestCtx{})

	if err := a.ResponseMid(9, []byte("ok")); err != nil {
		t.Fatalf("ResponseMid([]byte non-JSON): %v", err)
	}
	o := a.GetFastHttpContextObserve(9)
	if o.contentType != "application/json" {
		t.Fatalf("content type = %q, want application/json (M35)", o.contentType)
	}
	if string(o.body) == "ok" {
		t.Fatalf("non-JSON []byte passed through raw; must be JSON-encoded (M35): %q", o.body)
	}
	if !json.Valid(o.body) {
		t.Fatalf("application/json response body is not valid JSON (M35): %q", o.body)
	}
	want, err := httpJSONSerializer.Marshal([]byte("ok"))
	if err != nil {
		t.Fatalf("reference marshal: %v", err)
	}
	if string(o.body) != string(want) {
		t.Fatalf("body = %q, want HTTP JSON serializer encoding %q (M35)", o.body, want)
	}
}

// --- H35: bounded per-write deadline around SSE Flush -----------------------

type sseFakeConn struct {
	writeErr    error
	deadlineSet bool
}

func (c *sseFakeConn) Write(b []byte) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(b), nil
}
func (c *sseFakeConn) Read([]byte) (int, error)        { return 0, io.EOF }
func (c *sseFakeConn) Close() error                    { return nil }
func (c *sseFakeConn) LocalAddr() net.Addr             { return dummyAddr{} }
func (c *sseFakeConn) RemoteAddr() net.Addr            { return dummyAddr{} }
func (c *sseFakeConn) SetDeadline(time.Time) error     { return nil }
func (c *sseFakeConn) SetReadDeadline(time.Time) error { return nil }
func (c *sseFakeConn) SetWriteDeadline(time.Time) error {
	c.deadlineSet = true
	return nil
}

func TestFlushSSEBoundsWriteAndPropagatesError(t *testing.T) {
	// Success path: sets a write deadline, flushes, returns nil.
	okConn := &sseFakeConn{}
	w := bufio.NewWriter(okConn)
	_, _ = w.WriteString("data: x\n\n")
	if err := flushSSE(okConn, w, sseWriteTimeout); err != nil {
		t.Fatalf("flushSSE success path returned error: %v", err)
	}
	if !okConn.deadlineSet {
		t.Fatal("flushSSE did not set a write deadline (H35)")
	}

	// Stalled/failing write: the error is propagated so the caller can return
	// and let the unregister defer reclaim the stream.
	errConn := &sseFakeConn{writeErr: errors.New("i/o timeout")}
	w2 := bufio.NewWriter(errConn)
	_, _ = w2.WriteString("data: y\n\n")
	if err := flushSSE(errConn, w2, sseWriteTimeout); err == nil {
		t.Fatal("flushSSE must propagate a write error so the goroutine unwinds (H35)")
	}
	if !errConn.deadlineSet {
		t.Fatal("flushSSE did not set a write deadline before flushing (H35)")
	}

	// nil conn / zero timeout: still flushes, no deadline, no panic.
	okConn2 := &sseFakeConn{}
	w3 := bufio.NewWriter(okConn2)
	_, _ = w3.WriteString("z")
	if err := flushSSE(nil, w3, 0); err != nil {
		t.Fatalf("flushSSE(nil, ...) returned error: %v", err)
	}
	if okConn2.deadlineSet {
		t.Fatal("flushSSE set a deadline despite a nil conn")
	}
}

// --- M30: the WebSocket route is gated by the IsWebsocket option ------------

func TestWSRouteGatedByIsWebsocket(t *testing.T) {
	log.SetLogger(&noopLogger{})
	oldWS := env.WSPath
	env.WSPath = "ws"
	defer func() { env.WSPath = oldWS }()

	n := newTestNode()

	// Not opted into WS: the upgrade route must 404.
	n.IsWebsocket = false
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/ws")
	n.handleFastHTTP(ctx)
	if code := ctx.Response.StatusCode(); code != fasthttp.StatusNotFound {
		t.Fatalf("WS route with IsWebsocket=false: status %d, want 404 (M30)", code)
	}

	// Opted in: the route is reachable (a bad upgrade fails with !=404).
	n.IsWebsocket = true
	ctx2 := &fasthttp.RequestCtx{}
	ctx2.Request.Header.SetMethod("GET")
	ctx2.Request.SetRequestURI("/ws")
	n.handleFastHTTP(ctx2)
	if code := ctx2.Response.StatusCode(); code == fasthttp.StatusNotFound {
		t.Fatalf("WS route with IsWebsocket=true must not 404 (route gated off) (M30)")
	}
}

// --- rank-3 perf: empty session state skips json.Marshal but keeps the wire ---

func TestRemoteEmptyStateWireContract(t *testing.T) {
	// The fast path substitutes emptyStateJSON for json.Marshal(empty map);
	// it must be byte-identical so the wire contract / receiver decode is
	// unchanged.
	want, err := json.Marshal(map[string]interface{}{})
	if err != nil {
		t.Fatalf("marshal empty map: %v", err)
	}
	if string(emptyStateJSON) != string(want) {
		t.Fatalf("emptyStateJSON = %q, want %q (wire contract changed)", emptyStateJSON, want)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(emptyStateJSON, &decoded); err != nil {
		t.Fatalf("emptyStateJSON not decodable by receiver: %v", err)
	}
	if len(decoded) != 0 {
		t.Fatalf("emptyStateJSON decoded to non-empty map: %v", decoded)
	}
}