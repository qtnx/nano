package cluster

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/fasthttp/websocket"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/metrics"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

func TestHandshakeAdvertisesEffectiveHeartbeatTimeout(t *testing.T) {
	tests := []struct {
		name               string
		heartbeat          time.Duration
		heartbeatTimeout   time.Duration
		wantTimeoutSeconds float64
	}{
		{"default", 30 * time.Second, 60 * time.Second, 60},
		{"subsecond normalized", 500 * time.Millisecond, time.Second, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := handshakeResponsePacket(tt.heartbeat, tt.heartbeatTimeout)
			if err != nil {
				t.Fatal(err)
			}
			packets, err := codec.NewDecoder().Decode(data)
			if err != nil {
				t.Fatal(err)
			}
			if len(packets) != 1 || packets[0].Type != packet.Handshake {
				t.Fatalf("handshake packets = %#v", packets)
			}

			var response struct {
				Sys map[string]interface{} `json:"sys"`
			}
			if err := json.Unmarshal(packets[0].Data, &response); err != nil {
				t.Fatal(err)
			}
			if got, ok := response.Sys["heartbeat_timeout"].(float64); !ok || got != tt.wantTimeoutSeconds {
				t.Fatalf("heartbeat_timeout = %#v, want integer seconds %v", response.Sys["heartbeat_timeout"], tt.wantTimeoutSeconds)
			}
		})
	}
}

func TestAgentHeartbeatTimeoutOwnsTerminalReason(t *testing.T) {
	server, client := net.Pipe()
	defer client.Close()
	a := newAgent(server, nil, nil)
	a.heartbeat = 5 * time.Millisecond
	a.heartbeatTimeout = 5 * time.Millisecond
	a.lastAt = time.Now().Add(-time.Second).UnixNano()

	go a.write()
	select {
	case <-a.chDie:
	case <-time.After(time.Second):
		t.Fatal("heartbeat timeout did not close the agent")
	}
	if got := a.terminalCloseReason(); got != metrics.ConnectionCloseHeartbeatTimeout {
		t.Fatalf("terminal close reason = %q, want %q", got, metrics.ConnectionCloseHeartbeatTimeout)
	}
}

func TestAgentCompetingClosesCountOnce(t *testing.T) {
	metrics.ConnectionClosed.Reset()
	server, client := net.Pipe()
	defer client.Close()
	a := newAgent(server, nil, nil)

	reasons := []string{metrics.ConnectionCloseApplication, metrics.ConnectionCloseHeartbeatTimeout}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(reason string) {
			defer wg.Done()
			<-start
			_ = a.closeWithReason(reason)
		}(reasons[i%len(reasons)])
	}
	close(start)
	wg.Wait()

	application := testutil.ToFloat64(metrics.ConnectionClosed.WithLabelValues(metrics.ConnectionCloseApplication))
	heartbeat := testutil.ToFloat64(metrics.ConnectionClosed.WithLabelValues(metrics.ConnectionCloseHeartbeatTimeout))
	if application+heartbeat != 1 {
		t.Fatalf("distinct close reason total = %v, want 1", application+heartbeat)
	}
	if (application == 1) == (heartbeat == 1) {
		t.Fatalf("winner labels application=%v heartbeat=%v, want exactly one", application, heartbeat)
	}
}

func TestCloseReasonClassification(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"client EOF", closeReasonForReadError(io.EOF), metrics.ConnectionCloseClientEOF},
		{"client close", closeReasonForReadError(&websocket.CloseError{Code: websocket.CloseNormalClosure}), metrics.ConnectionCloseClientClose},
		{"read timeout", closeReasonForReadError(timeoutReadError{}), metrics.ConnectionCloseHeartbeatTimeout},
		{"write timeout", closeReasonForWriteError(timeoutReadError{}), metrics.ConnectionCloseWriteTimeout},
		{"write unknown", closeReasonForWriteError(errors.New("broken writer")), metrics.ConnectionCloseUnknown},
		{"handshake rejected", closeReasonForPacketError(&packet.Packet{Type: packet.Handshake}, &handshakeRejectedError{err: errors.New("denied")}), metrics.ConnectionCloseHandshakeRejected},
		{"handshake write timeout", closeReasonForPacketError(&packet.Packet{Type: packet.Handshake}, timeoutReadError{}), metrics.ConnectionCloseWriteTimeout},
		{"protocol error", closeReasonForPacketError(&packet.Packet{Type: packet.Data}, errors.New("bad packet")), metrics.ConnectionCloseProtocolError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("reason = %q, want %q", tt.got, tt.want)
			}
		})
	}
}

type timeoutConn struct {
	remote   net.Addr
	closed   chan struct{}
	deadline chan time.Time
	once     sync.Once
}

func newTimeoutConn(remote net.Addr) *timeoutConn {
	return &timeoutConn{
		remote:   remote,
		closed:   make(chan struct{}),
		deadline: make(chan time.Time, 1),
	}
}

func (c *timeoutConn) Read([]byte) (int, error) {
	select {
	case <-c.deadline:
		return 0, timeoutReadError{}
	case <-c.closed:
		return 0, io.EOF
	}
}

func (c *timeoutConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *timeoutConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}
func (c *timeoutConn) LocalAddr() net.Addr         { return c.remote }
func (c *timeoutConn) RemoteAddr() net.Addr        { return c.remote }
func (c *timeoutConn) SetDeadline(time.Time) error { return nil }
func (c *timeoutConn) SetReadDeadline(deadline time.Time) error {
	select {
	case c.deadline <- deadline:
	default:
	}
	return nil
}
func (c *timeoutConn) SetWriteDeadline(time.Time) error { return nil }

type timeoutReadError struct{}

func (timeoutReadError) Error() string   { return "read timeout" }
func (timeoutReadError) Timeout() bool   { return true }
func (timeoutReadError) Temporary() bool { return true }

func connectionDurationSampleCount(t *testing.T) uint64 {
	t.Helper()
	var metric dto.Metric
	if err := metrics.ConnectionDuration.Write(&metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetHistogram().GetSampleCount()
}

func TestHandlerHeartbeatTimeoutLifecycle(t *testing.T) {
	metrics.ConnectionClosed.Reset()
	metrics.CurrentConnections.Set(0)
	n := newTestNode()
	conn := newTimeoutConn(ipAddr{"203.0.113.8:5555"})
	done := make(chan struct{})
	go func() {
		n.handler.handle(conn)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("handler did not return after heartbeat timeout")
	}
	if got := n.sessionLen(); got != 0 {
		t.Fatalf("session count after timeout = %d, want 0", got)
	}
	if got := testutil.ToFloat64(metrics.ConnectionClosed.WithLabelValues(metrics.ConnectionCloseHeartbeatTimeout)); got != 1 {
		t.Fatalf("heartbeat timeout close count = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.CurrentConnections); got != 0 {
		t.Fatalf("current connections after timeout = %v, want 0", got)
	}
}

type writeErrorConn struct {
	remote net.Addr
	err    error
}

func (c *writeErrorConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *writeErrorConn) Write([]byte) (int, error)        { return 0, c.err }
func (c *writeErrorConn) Close() error                     { return nil }
func (c *writeErrorConn) LocalAddr() net.Addr              { return c.remote }
func (c *writeErrorConn) RemoteAddr() net.Addr             { return c.remote }
func (c *writeErrorConn) SetDeadline(time.Time) error      { return nil }
func (c *writeErrorConn) SetReadDeadline(time.Time) error  { return nil }
func (c *writeErrorConn) SetWriteDeadline(time.Time) error { return nil }

func TestHandshakeWriteFailureIsNotHandshakeRejected(t *testing.T) {
	oldValidator := env.HandshakeValidator
	t.Cleanup(func() { env.HandshakeValidator = oldValidator })
	h := NewHandler(nil, nil)
	conn := &writeErrorConn{remote: ipAddr{"203.0.113.9:5555"}, err: timeoutReadError{}}
	a := newAgent(conn, nil, nil)

	env.HandshakeValidator = func(*session.Session, []byte) error { return nil }
	err := h.processPacket(a, &packet.Packet{Type: packet.Handshake})
	if got := closeReasonForPacketError(&packet.Packet{Type: packet.Handshake}, err); got != metrics.ConnectionCloseWriteTimeout {
		t.Fatalf("accepted handshake write reason = %q, want %q", got, metrics.ConnectionCloseWriteTimeout)
	}

	env.HandshakeValidator = func(*session.Session, []byte) error { return errors.New("denied") }
	err = h.processPacket(a, &packet.Packet{Type: packet.Handshake})
	if got := closeReasonForPacketError(&packet.Packet{Type: packet.Handshake}, err); got != metrics.ConnectionCloseHandshakeRejected {
		t.Fatalf("validator rejection reason = %q, want %q", got, metrics.ConnectionCloseHandshakeRejected)
	}
}

func TestSchedulerFullReentrantCloseDoesNotBlockTransportTeardown(t *testing.T) {
	go scheduler.Sched()

	metrics.ConnectionClosed.Reset()
	server, client := net.Pipe()
	defer client.Close()
	a := newAgent(server, nil, nil)
	panicObserved := make(chan struct{})
	session.Lifetime.OnClosed(func(s *session.Session) {
		if s == a.session {
			close(panicObserved)
			panic("test lifecycle callback panic")
		}
	})
	entered := make(chan struct{})
	closeNow := make(chan struct{})
	closed := make(chan struct{})
	scheduler.PushTask(func() {
		close(entered)
		<-closeNow
		_ = a.closeWithReason(metrics.ConnectionCloseApplication)
		close(closed)
	})
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("scheduler did not begin blocking task")
	}

	for {
		if err := scheduler.TryPushTask(func() {}); errors.Is(err, scheduler.ErrSchedulerBacklog) {
			break
		}
	}
	close(closeNow)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("reentrant Close blocked behind a full scheduler queue")
	}
	if got := a.status(); got != statusClosed {
		t.Fatalf("agent status = %d, want closed", got)
	}
	select {
	case <-panicObserved:
	case <-time.After(time.Second):
		t.Fatal("scheduler fallback did not invoke the close callback")
	}
	if got := testutil.ToFloat64(metrics.ConnectionClosed.WithLabelValues(metrics.ConnectionCloseApplication)); got != 1 {
		t.Fatalf("application close count = %v, want 1", got)
	}
}

func TestConnectionLimitCloseReasonLeavesAcceptedLifecycleUntouched(t *testing.T) {
	tests := []struct {
		name  string
		setup func(*Node)
	}{
		{
			name: "per IP",
			setup: func(n *Node) {
				n.LimitConnectPerIp = 1
				n.connectionCount["203.0.113.10"] = 1
			},
		},
		{
			name: "global",
			setup: func(n *Node) {
				env.MaxConnections = 1
				n.acceptedConns = 1
			},
		},
	}
	oldMaxConnections := env.MaxConnections
	defer func() { env.MaxConnections = oldMaxConnections }()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metrics.ConnectionClosed.Reset()
			metrics.CurrentConnections.Set(0)
			metrics.ConnectionsPerIP.Reset()
			beforeDuration := connectionDurationSampleCount(t)
			n := newTestNode()
			tt.setup(n)

			n.handler.handle(newAddrConn(ipAddr{"203.0.113.10:5555"}))

			if got := testutil.ToFloat64(metrics.ConnectionClosed.WithLabelValues(metrics.ConnectionCloseConnectionLimit)); got != 1 {
				t.Fatalf("connection limit close count = %v, want 1", got)
			}
			if got := testutil.ToFloat64(metrics.CurrentConnections); got != 0 {
				t.Fatalf("current connections = %v, want 0", got)
			}
			if got := connectionDurationSampleCount(t); got != beforeDuration {
				t.Fatalf("connection duration samples = %d, want %d", got, beforeDuration)
			}
			if got := testutil.CollectAndCount(metrics.ConnectionsPerIP); got != 0 {
				t.Fatalf("connections per IP series = %d, want 0", got)
			}
			if got := n.sessionLen(); got != 0 {
				t.Fatalf("sessions = %d, want 0", got)
			}
		})
	}
}
