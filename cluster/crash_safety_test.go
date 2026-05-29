package cluster

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/session"
	"github.com/valyala/fasthttp"
)

// --- test logger ---------------------------------------------------------

// captureLogger records every log line emitted through the package logger so
// tests can assert on hot-path noise (C5) and that the gRPC serve goroutine no
// longer escalates a transient error to a process-killing Fatal (H22).
type captureLogger struct {
	mu          sync.Mutex
	buf         bytes.Buffer
	fatalCalled bool
}

func (l *captureLogger) write(s string) {
	l.mu.Lock()
	l.buf.WriteString(s)
	l.buf.WriteByte('\n')
	l.mu.Unlock()
}

func (l *captureLogger) Debug(v ...interface{})                 { l.write(fmt.Sprint(v...)) }
func (l *captureLogger) Println(v ...interface{})               { l.write(fmt.Sprint(v...)) }
func (l *captureLogger) Infof(format string, v ...interface{})  { l.write(fmt.Sprintf(format, v...)) }
func (l *captureLogger) Error(v ...interface{})                 { l.write(fmt.Sprint(v...)) }
func (l *captureLogger) Errorf(format string, v ...interface{}) { l.write(fmt.Sprintf(format, v...)) }
func (l *captureLogger) Warn(v ...interface{})                  { l.write(fmt.Sprint(v...)) }
func (l *captureLogger) Fatal(v ...interface{}) {
	l.mu.Lock()
	l.fatalCalled = true
	l.mu.Unlock()
	l.write(fmt.Sprint(v...))
}
func (l *captureLogger) Fatalf(format string, v ...interface{}) {
	l.mu.Lock()
	l.fatalCalled = true
	l.mu.Unlock()
	l.write(fmt.Sprintf(format, v...))
}

func (l *captureLogger) contains(s string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Contains(l.buf.String(), s)
}

func (l *captureLogger) fatal() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.fatalCalled
}

// installCapture swaps in a capture logger and returns it plus a restore func.
func installCapture(t *testing.T) (*captureLogger, func()) {
	t.Helper()
	cl := &captureLogger{}
	log.SetLogger(cl)
	return cl, func() { log.SetLogger(&noopLogger{}) }
}

// noopLogger discards everything; used to restore a quiet logger after capture.
type noopLogger struct{}

func (noopLogger) Debug(...interface{})          {}
func (noopLogger) Println(...interface{})        {}
func (noopLogger) Infof(string, ...interface{})  {}
func (noopLogger) Error(...interface{})          {}
func (noopLogger) Errorf(string, ...interface{}) {}
func (noopLogger) Warn(...interface{})           {}
func (noopLogger) Fatal(...interface{})          {}
func (noopLogger) Fatalf(string, ...interface{}) {}

// freeAddr returns a currently-free loopback address.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freeAddr: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

const leakProbePath = "/__cluster_leak_probe__"

var leakRegisterOnce sync.Once

func registerLeakProbe() {
	leakRegisterOnce.Do(func() {
		http.HandleFunc(leakProbePath, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("LEAK"))
		})
	})
}

func newFastHTTPGet(path string) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI(path)
	return ctx
}

// --- C1: a panic while processing one packet must not crash the process -----

func TestProcessPacketRecoversPanic(t *testing.T) {
	oldValidator := env.HandshakeValidator
	env.HandshakeValidator = func(*session.Session, []byte) error { panic("boom: simulated malformed handshake") }
	defer func() { env.HandshakeValidator = oldValidator }()

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	h := NewHandler(nil, nil)
	a := newAgent(c1, nil, nil)

	// Without recover() this call panics and unwinds into the read goroutine,
	// terminating the whole process. With recover() it must return an error.
	err := h.processPacket(a, &packet.Packet{Type: packet.Handshake, Data: []byte{}})
	if err == nil {
		t.Fatal("expected an error after recovering the handshake panic, got nil")
	}
}

// --- C2: concurrent Close must not double-close chDie -----------------------

func TestAgentConcurrentCloseNoPanic(t *testing.T) {
	// Hammer Close from many goroutines released simultaneously, repeated over
	// several fresh agents to reliably expose the non-atomic check-then-close
	// window. A double close(a.chDie) panics "close of closed channel" and
	// crashes the test binary.
	for iter := 0; iter < 100; iter++ {
		c1, c2 := net.Pipe()
		a := newAgent(c1, nil, nil)

		const closers = 32
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := 0; i < closers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				_ = a.Close()
			}()
		}
		close(start) // release all closers at once
		wg.Wait()

		if a.status() != statusClosed {
			t.Fatalf("agent should be closed, got status %d", a.status())
		}
		_ = c1.Close()
		_ = c2.Close()
	}
}

// --- C3: lastAt must be written atomically (run under -race) ----------------

func TestAgentLastAtNoRace(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	h := NewHandler(nil, nil)
	a := newAgent(c1, nil, nil)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			// processPacket writes a.lastAt on every packet.
			_ = h.processPacket(a, &packet.Packet{Type: packet.Heartbeat})
		}
		close(done)
	}()

	for i := 0; i < 2000; i++ {
		// Mirrors the heartbeat-timeout reader in agent.write.
		_ = atomic.LoadInt64(&a.lastAt)
	}
	<-done
}

// --- C4: the write goroutine must never self-deadlock on its own buffer -----

// countConn is a net.Conn whose Write always succeeds instantly and counts the
// number of frames flushed. With the pre-fix design the single write goroutine
// both feeds and drains an internal 16-slot chWrite channel; under continuous
// producer pressure that channel saturates and the goroutine blocks forever on
// its own send, so the flushed-frame count stalls. With a direct conn.Write the
// count climbs without bound.
type countConn struct {
	n      int64
	closed chan struct{}
	once   sync.Once
}

func newCountConn() *countConn { return &countConn{closed: make(chan struct{})} }

func (c *countConn) Write(b []byte) (int, error) {
	atomic.AddInt64(&c.n, 1)
	return len(b), nil
}
func (c *countConn) count() int64 { return atomic.LoadInt64(&c.n) }
func (c *countConn) Read(b []byte) (int, error) {
	<-c.closed
	return 0, fmt.Errorf("closed")
}
func (c *countConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}
func (c *countConn) LocalAddr() net.Addr              { return dummyAddr{} }
func (c *countConn) RemoteAddr() net.Addr             { return dummyAddr{} }
func (c *countConn) SetDeadline(time.Time) error      { return nil }
func (c *countConn) SetReadDeadline(time.Time) error  { return nil }
func (c *countConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "pipe" }
func (dummyAddr) String() string  { return "ctrl" }

func TestWriteGoroutineNoSelfDeadlock(t *testing.T) {
	// Suppress stdout so the write loop spins fast enough to exercise the
	// scheduler many times.
	_, restore := installCapture(t)
	defer restore()

	conn := newCountConn()
	a := newAgent(conn, nil, nil)
	writeDone := make(chan struct{})
	go func() { a.write(); close(writeDone) }()

	stop := make(chan struct{})
	producerDone := make(chan struct{})
	go func() {
		defer close(producerDone)
		for {
			select {
			case <-stop:
				return
			default:
				// send is non-blocking (H7): ErrBufferExceed means the buffer is
				// momentarily full while the writer drains — retry. Any other
				// error means chSend is closed (write goroutine exited), so stop.
				if err := a.send(pendingMessage{typ: message.Push, route: "x", payload: []byte("p")}); err != nil {
					if err == ErrBufferExceed {
						continue
					}
					return
				}
			}
		}
	}()

	// A healthy writer flushes frames directly to the connection without bound.
	// A wedged writer stalls far below this target.
	const target = int64(20000)
	deadline := time.After(4 * time.Second)
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-deadline:
			close(stop)
			t.Fatalf("write goroutine wedged: only %d frames flushed (want >= %d) — chWrite self-deadlock", conn.count(), target)
		case <-tick.C:
			if conn.count() >= target {
				// Stop the producer and wait for it to finish sending BEFORE
				// closing the agent, so the write goroutine's close(chSend) can
				// never race a concurrent a.send.
				close(stop)
				<-producerDone
				a.Close()      // unblock and exit the write goroutine
				<-writeDone    // join it so no goroutine outlives the test
				return
			}
		}
	}
}

// --- C5: hot-path message handling must not log unconditionally -------------

type C5Comp struct{ component.Base }

func (c *C5Comp) Echo(s *session.Session, data []byte) error { return nil }

func TestLocalProcessNoHotPathLog(t *testing.T) {
	cl, restore := installCapture(t)
	defer restore()

	oldDebug := env.Debug
	env.Debug = false
	defer func() { env.Debug = oldDebug }()

	h := NewHandler(nil, nil)
	if err := h.register(&C5Comp{}, nil); err != nil {
		t.Fatalf("register: %v", err)
	}

	var route string
	var handler *component.Handler
	for r, hd := range h.localHandlers {
		route, handler = r, hd
		break
	}
	if handler == nil {
		t.Fatal("no handler registered")
	}

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	a := newAgent(c1, nil, nil)

	msg := &message.Message{Type: message.Notify, Route: route, Data: []byte("payload")}
	h.localProcess(handler, 0, a.session, msg, nil)

	for _, noisy := range []string{"Push task", "Schedule task", "Local process task completed"} {
		if cl.contains(noisy) {
			t.Fatalf("hot-path log emitted with Debug off: %q", noisy)
		}
	}
}

func TestHTTPAgentNoHotPathLog(t *testing.T) {
	cl, restore := installCapture(t)
	defer restore()

	oldDebug := env.Debug
	env.Debug = false
	defer func() { env.Debug = oldDebug }()

	sseChan := make(chan []byte, 4)
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	a := newAgent(c1, nil, nil)

	h := &httpAgent{
		session:               a.session,
		sseChan:               sseChan,
		messageIDMapToRequest: map[uint64]*fastHttpContextObserve{},
	}

	_ = h.Push("route", []byte(`{"k":"v"}`))

	// Reach the ResponseMid success path (where the leftover "ss ptr" debug
	// line lives) by registering a context for the message id first.
	h.AttackHttpRequestCtx(123, &fasthttp.RequestCtx{})
	_ = h.ResponseMid(123, []byte(`{"k":"v"}`))

	for _, noisy := range []string{
		"[HTTP Agent] Push event",
		"[HTTP Agent] SSE event sent",
		"[HTTP Agent] ResponseMid",
		"ss ptr after insert",
	} {
		if cl.contains(noisy) {
			t.Fatalf("hot-path HTTP-agent log emitted with Debug off: %q", noisy)
		}
	}
}

// --- C7: nil MemberInfo in membership RPC handlers must not crash -----------

func TestHeartbeatNilMemberInfo(t *testing.T) {
	c := newCluster(&Node{})
	_, err := c.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{})
	if err == nil {
		t.Fatal("expected error for Heartbeat with nil MemberInfo, got nil")
	}
}

func TestNewMemberNilMemberInfo(t *testing.T) {
	n := &Node{}
	n.handler = NewHandler(n, nil)
	n.cluster = newCluster(n)
	_, err := n.NewMember(context.Background(), &clusterpb.NewMemberRequest{})
	if err == nil {
		t.Fatal("expected error for NewMember with nil MemberInfo, got nil")
	}
}

// --- H30: the public client fallback must not expose the global mux ---------

func TestClientFallbackDoesNotLeakDefaultMux(t *testing.T) {
	registerLeakProbe()

	n := &Node{}
	ctx := newFastHTTPGet(leakProbePath)
	n.handleFastHTTP(ctx)

	if ctx.Response.StatusCode() == http.StatusOK && strings.Contains(string(ctx.Response.Body()), "LEAK") {
		t.Fatal("client listener fallback served a handler from http.DefaultServeMux")
	}
}

// --- H31: a client-listener bind failure must surface from Startup ----------

func TestStartupReturnsClientBindError(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer busy.Close()

	n := &Node{
		Options: Options{
			ClientAddr: busy.Addr().String(),
			Components: &component.Components{},
		},
		ServiceAddr: "127.0.0.1:0",
	}

	if err := n.Startup(); err == nil {
		n.Shutdown()
		t.Fatal("expected Startup to fail when ClientAddr is already in use, got nil")
	}
}

// --- H21: Shutdown must release the client listener -------------------------

func TestShutdownReleasesClientListener(t *testing.T) {
	addr := freeAddr(t)
	n := &Node{
		Options: Options{
			ClientAddr: addr,
			Components: &component.Components{},
		},
		ServiceAddr: "127.0.0.1:0",
	}
	if err := n.Startup(); err != nil {
		t.Fatalf("Startup: %v", err)
	}
	// Ensure the listener is actually bound before we try to shut it down.
	time.Sleep(150 * time.Millisecond)
	n.Shutdown()
	// Give shutdown a moment to release the socket.
	time.Sleep(150 * time.Millisecond)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("client listener was not released by Shutdown: %v", err)
	}
	_ = ln.Close()
}
