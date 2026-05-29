package cluster

import (
	"net"
	"sync"
	"testing"

	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/session"
	"github.com/valyala/fasthttp"
)

func newHTTPAgentForTest() *httpAgent {
	var req fasthttp.Request
	var ctx fasthttp.RequestCtx
	req.SetRequestURI("/api")
	ctx.Init(&req, &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 12345}, nil)

	return NewHTTPAgent(
		123,
		nil,
		make(chan []byte, 16),
		func(_ *session.Session, _ *message.Message, _ bool) {},
		&ctx,
	)
}

func requireNoPanic(t *testing.T, fn func() error) error {
	t.Helper()
	var err error
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic: %v", r)
		}
	}()
	err = fn()
	return err
}

func TestHTTPAgentMethodsReturnErrBrokenPipeAfterClose(t *testing.T) {
	agent := newHTTPAgentForTest()
	if err := agent.Close(); err != nil {
		t.Fatal(err)
	}
	if err := agent.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "RPC",
			call: func() error { return agent.RPC("Service.Method", nil) },
		},
		{
			name: "Push",
			call: func() error { return agent.Push("Service.Event", []byte(`{"ok":true}`)) },
		},
		{
			name: "Response",
			call: func() error { return agent.Response(nil) },
		},
		{
			name: "ResponseMid",
			call: func() error { return agent.ResponseMid(1, nil) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := requireNoPanic(t, tt.call); err != ErrBrokenPipe {
				t.Fatalf("error = %v, want %v", err, ErrBrokenPipe)
			}
		})
	}
}

func TestHTTPAgentConcurrentCloseAndMethodsRaceFree(t *testing.T) {
	agent := newHTTPAgentForTest()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				_ = requireNoPanic(t, func() error { return agent.RPC("Service.Method", []byte("payload")) })
				_ = requireNoPanic(t, func() error { return agent.Push("Service.Event", []byte(`{"ok":true}`)) })
				_ = requireNoPanic(t, func() error { return agent.ResponseMid(uint64(j+1), []byte(`{"ok":true}`)) })
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			_ = agent.Close()
		}
	}()

	wg.Wait()
}

func TestHTTPAgentOpenPathStillWorks(t *testing.T) {
	var (
		called bool
		got    *message.Message
	)
	agent := newHTTPAgentForTest()
	agent.rpcHandler = func(_ *session.Session, msg *message.Message, noCopy bool) {
		called = true
		got = msg
		if !noCopy {
			t.Fatal("RPC noCopy = false, want true")
		}
	}

	if err := agent.RPC("Service.Method", []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("RPC handler was not called")
	}
	if got == nil || got.Route != "Service.Method" || got.Type != message.Notify || string(got.Data) != "payload" {
		t.Fatalf("unexpected RPC message: %+v", got)
	}

	if err := agent.Push("Service.Event", []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case data := <-agent.sseChan:
		if string(data) != `{"body":{"ok":true},"route":"Service.Event"}` {
			t.Fatalf("unexpected SSE data: %s", data)
		}
	default:
		t.Fatal("Push did not enqueue SSE data")
	}
}
