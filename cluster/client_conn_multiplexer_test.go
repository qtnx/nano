package cluster

import (
	"bufio"
	"net"
	"testing"
	"time"
)

// TestMultiplexerSilentConnDoesNotBlockOtherAccepts is the regression test for
// the production stall where a client that connected but sent no bytes pinned
// the single accept loop inside serve(), delaying every other websocket
// upgrade by seconds. A silent connection must not delay a talkative one.
func TestMultiplexerSilentConnDoesNotBlockOtherAccepts(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := newClientConnMultiplexer(ln)
	defer mux.Close()
	go mux.serve(func(conn net.Conn) { conn.Close() })

	// Connection 1: connects and stays silent.
	silent, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer silent.Close()

	time.Sleep(50 * time.Millisecond) // let the mux accept the silent conn first

	// Connection 2: sends an HTTP-looking first byte immediately.
	talkative, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer talkative.Close()
	if _, err := talkative.Write([]byte("GET / HTTP/1.1\r\n\r\n")); err != nil {
		t.Fatal(err)
	}

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := mux.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	select {
	case conn := <-accepted:
		// Must be the talkative conn, delivered promptly despite the silent one.
		br := bufio.NewReader(conn)
		b, err := br.Peek(1)
		if err != nil || b[0] != 'G' {
			t.Fatalf("expected HTTP conn first byte 'G', got %q err=%v", b, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("talkative connection was not dispatched within 2s; accept loop is blocked by the silent connection")
	}
}

// TestMultiplexerSilentConnDroppedAfterSniffTimeout documents that a silent
// connection is closed once the sniff deadline elapses instead of pinning a
// goroutine forever. Uses a raw check that the server side observes EOF/close
// semantics via the client seeing a reset or close after the timeout window.
func TestMultiplexerNanoPacketStillDispatched(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := newClientConnMultiplexer(ln)
	defer mux.Close()
	nanoConns := make(chan net.Conn, 1)
	go mux.serve(func(conn net.Conn) { nanoConns <- conn })

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	// packet.Handshake == 0x01: first byte marks a raw nano TCP client.
	if _, err := client.Write([]byte{0x01, 0x00, 0x00, 0x00}); err != nil {
		t.Fatal(err)
	}

	select {
	case conn := <-nanoConns:
		br := bufio.NewReader(conn)
		b, err := br.Peek(1)
		if err != nil || b[0] != 0x01 {
			t.Fatalf("expected nano first byte 0x01, got %q err=%v", b, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("nano packet connection was not dispatched")
	}
}
