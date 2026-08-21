package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/lonng/nano/cluster"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
)

type EchoComponent struct{ component.Base }

func (e *EchoComponent) Echo(s *session.Session, data []byte) error { return s.Response(data) }

func TestRunRealNanoWebSocket(t *testing.T) {
	url, stop := startNanoNode(t)
	defer stop()

	result := Run(context.Background(), Config{
		URL:            url,
		Connections:    3,
		Duration:       40 * time.Millisecond,
		ConnectTimeout: time.Second,
		RequestRoute:   "Echo.Echo",
		RequestJSON:    json.RawMessage(`{"value":"ok"}`),
		RequestEvery:   5 * time.Millisecond,
	})
	if result.Attempted != 3 || result.Connected != 3 || result.Failed != 0 {
		t.Fatalf("connections attempted=%d connected=%d failed=%d result=%+v", result.Attempted, result.Connected, result.Failed, result)
	}
	if result.ActiveAtEnd != 0 || result.PeakActive != 3 {
		t.Fatalf("active_at_end=%d peak_active=%d", result.ActiveAtEnd, result.PeakActive)
	}
	if result.ConnectedClientSeconds <= 0 || result.SetupMS.P50 <= 0 || result.SetupMS.P95 <= 0 || result.SetupMS.P99 <= 0 {
		t.Fatalf("unexpected timings: %+v", result)
	}
	if result.RequestMS.Count == 0 {
		t.Fatalf("request round-trips = 0")
	}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("wsload result=%s", data)
}

func TestRunInvalidEndpointCountsFailure(t *testing.T) {
	result := Run(context.Background(), Config{
		URL:            "ws://127.0.0.1:1/ws",
		Connections:    1,
		Duration:       time.Millisecond,
		ConnectTimeout: 20 * time.Millisecond,
	})
	if result.Attempted != 1 || result.Connected != 0 || result.Failed != 1 || result.Errors[errorDial] != 1 {
		t.Fatalf("invalid endpoint result: %+v", result)
	}
}

func TestRunServerDisconnectCountedOnce(t *testing.T) {
	url, stop := startProtocolServer(t, true, nil)
	defer stop()

	result := Run(context.Background(), Config{
		URL: url, Connections: 1, Duration: time.Second, ConnectTimeout: time.Second,
	})
	if result.Connected != 1 || result.UnexpectedDisconnects != 1 || result.ActiveAtEnd != 0 {
		t.Fatalf("disconnect result: %+v", result)
	}
}

func TestRunCancellationClosesEveryClient(t *testing.T) {
	connected := make(chan struct{}, 4)
	url, stop := startProtocolServer(t, false, connected)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resultCh := make(chan Result, 1)
	go func() {
		resultCh <- Run(ctx, Config{URL: url, Connections: 2, Duration: time.Second, ConnectTimeout: time.Second})
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-connected:
		case <-time.After(time.Second):
			t.Fatal("client did not complete Nano handshake")
		}
	}
	cancel()
	result := <-resultCh
	if result.Connected != 2 || result.ActiveAtEnd != 0 || result.UnexpectedDisconnects != 0 {
		t.Fatalf("cancellation result: %+v", result)
	}
	for i := 0; i < 2; i++ {
		select {
		case <-connected:
		case <-time.After(time.Second):
			t.Fatal("server did not observe client close")
		}
	}
}
type zeroSampler struct{}

func (zeroSampler) Uint64n(uint64) uint64 { return 0 }

func TestReservoirReplacesEarlySampleAfterCap(t *testing.T) {
	s := newStatsWithSource(Config{URL: "ws://example.test/ws"}, zeroSampler{})
	for i := 0; i < maxLatencySamples; i++ {
		s.addRequest(time.Duration(i+1) * time.Millisecond)
	}
	const lateSample = 999999
	s.addRequest(lateSample * time.Millisecond)

	if s.request.seen != maxLatencySamples+1 {
		t.Fatalf("request observations = %d, want %d", s.request.seen, maxLatencySamples+1)
	}
	if s.request.values[0] != lateSample {
		t.Fatalf("reservoir first sample = %v, want late sample %d", s.request.values[0], lateSample)
	}
	result := s.result(time.Unix(1, 0), time.Unix(2, 0))
	if result.RequestMS.Count != maxLatencySamples+1 {
		t.Fatalf("request count = %d, want all %d observations", result.RequestMS.Count, maxLatencySamples+1)
	}
}

func TestStatsPercentilesAreDeterministic(t *testing.T) {
	s := newStats(Config{URL: "ws://example.test/ws", Connections: 5})
	for _, sample := range []time.Duration{1, 2, 3, 4, 5} {
		s.addSetup(sample * time.Millisecond)
	}
	got := s.result(time.Unix(1, 0), time.Unix(2, 0))
	if got.SetupMS.P50 != 3 || got.SetupMS.P95 != 5 || got.SetupMS.P99 != 5 {
		t.Fatalf("setup percentiles = %+v", got.SetupMS)
	}
}

func TestJSONResultSchema(t *testing.T) {
	result := Result{Attempted: 1, Connected: 1, Errors: map[string]int{}, Parameters: Parameters{URL: "ws://example.test/ws"}}
	data, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"parameters", "attempted", "connected", "failed", "peak_active", "active_at_end", "unexpected_disconnects", "connected_client_seconds", "setup_ms", "request_ms", "errors", "started_at", "ended_at", "duration_ms"} {
		if _, ok := got[key]; !ok {
			t.Errorf("missing JSON key %q: %s", key, data)
		}
	}
}

func startNanoNode(t *testing.T) (string, func()) {
	t.Helper()
	go scheduler.Sched()
	serviceAddr := freeAddr(t)
	clientAddr := freeAddr(t)
	components := &component.Components{}
	components.Register(&EchoComponent{}, component.WithName("Echo"))
	node := &cluster.Node{ServiceAddr: serviceAddr, Options: cluster.Options{
		IsMaster: true, ClientAddr: clientAddr, IsWebsocket: true, Components: components,
	}}
	if err := node.Startup(); err != nil {
		t.Fatal(err)
	}
	return "ws://" + clientAddr + "/", func() { node.Shutdown() }
}

func freeAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func startProtocolServer(t *testing.T, disconnect bool, connected chan<- struct{}) (string, func()) {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
	handshaken := false
	defer func() {
		_ = conn.Close()
		if handshaken && connected != nil {
			connected <- struct{}{}
		}
	}()
	_, frame, err := conn.ReadMessage()
		if err != nil {
			return
		}
		packets, err := codec.NewDecoder().Decode(frame)
		if err != nil || len(packets) != 1 || packets[0].Type != packet.Handshake {
			return
		}
		handshake, _ := codec.Encode(packet.Handshake, nil)
		if err := conn.WriteMessage(websocket.BinaryMessage, handshake); err != nil {
			return
		}
		_, frame, err = conn.ReadMessage()
		if err != nil {
			return
		}
		packets, err = codec.NewDecoder().Decode(frame)
		if err != nil || len(packets) != 1 || packets[0].Type != packet.HandshakeAck {
			return
		}
	handshaken = true
		if connected != nil {
			connected <- struct{}{}
		}
		if disconnect {
			kick, _ := codec.Encode(packet.Kick, nil)
			_ = conn.WriteMessage(websocket.BinaryMessage, kick)
			return
		}
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				return
			}
		}
	}))
	return "ws" + server.URL[len("http"):], server.Close
}

var _ = message.Request
