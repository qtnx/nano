package cluster

import (
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fasthttp/websocket"
)

func TestWSConnCloseNilSafe(t *testing.T) {
	tests := []struct {
		name string
		conn *wsConn
	}{
		{name: "nil receiver"},
		{name: "nil underlying websocket", conn: &wsConn{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.conn.Close(); err != ErrBrokenPipe {
				t.Fatalf("Close() error = %v, want %v", err, ErrBrokenPipe)
			}
		})
	}
}

func TestWSConnCloseIsIdempotent(t *testing.T) {
	fake := &fakeWebsocketConn{}
	conn := &wsConn{conn: fake}

	if err := conn.Close(); err != nil {
		t.Fatalf("first Close() error = %v, want nil", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("second Close() error = %v, want nil", err)
	}
	if fake.closeCalls != 1 {
		t.Fatalf("underlying Close calls = %d, want 1", fake.closeCalls)
	}
	if fake.writeControlCalls != 1 {
		t.Fatalf("WriteControl calls = %d, want 1", fake.writeControlCalls)
	}
}

func TestWSConnCloseAttemptsUnderlyingCloseWhenCloseFrameFails(t *testing.T) {
	closeErr := errors.New("close failed")
	fake := &fakeWebsocketConn{writeControlErr: errors.New("control failed"), closeErr: closeErr}
	conn := &wsConn{conn: fake}

	if err := conn.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}
	if fake.closeCalls != 1 {
		t.Fatalf("underlying Close calls = %d, want 1", fake.closeCalls)
	}
}

func TestWSConnCloseConcurrentWithWriteDoesNotPanic(t *testing.T) {
	fake := &fakeWebsocketConn{}
	conn := &wsConn{conn: fake}

	const goroutines = 64
	panicCh := make(chan interface{}, goroutines)
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { panicCh <- recover() }()
			if i%2 == 0 {
				_ = conn.Close()
				return
			}
			_, _ = conn.Write([]byte("x"))
		}(i)
	}
	wg.Wait()
	close(panicCh)

	for p := range panicCh {
		if p != nil {
			t.Fatalf("Close/Write panicked: %v", p)
		}
	}
}

type fakeWebsocketConn struct {
	mu                sync.Mutex
	closed            bool
	closeCalls        int
	writeCalls        int
	writeControlCalls int
	writeControlErr   error
	closeErr          error
}

func (f *fakeWebsocketConn) NextReader() (int, io.Reader, error) {
	return websocket.BinaryMessage, strings.NewReader(""), nil
}

func (f *fakeWebsocketConn) WriteMessage(int, []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrBrokenPipe
	}
	f.writeCalls++
	return nil
}

func (f *fakeWebsocketConn) WriteControl(int, []byte, time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.writeControlCalls++
	return f.writeControlErr
}

func (f *fakeWebsocketConn) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeCalls++
	f.closed = true
	return f.closeErr
}

func (f *fakeWebsocketConn) LocalAddr() net.Addr  { return ipAddr{s: "local:1"} }
func (f *fakeWebsocketConn) RemoteAddr() net.Addr { return ipAddr{s: "remote:1"} }

func (f *fakeWebsocketConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeWebsocketConn) SetWriteDeadline(time.Time) error { return nil }
