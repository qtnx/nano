package cluster

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lonng/nano/internal/env"
	"github.com/valyala/fasthttp"
)

func TestDecreaseConnectionConcurrentAccess(t *testing.T) {
	n := &Node{
		Options: Options{
			LimitConnectPerIp: 1000000,
		},
		connectionCount: map[string]uint{},
	}

	const (
		workers    = 8
		iterations = 50
	)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				if err := n.increaseConnection("127.0.0.1"); err != nil {
					t.Errorf("increaseConnection returned error: %v", err)
					return
				}
				n.decreaseConnection("127.0.0.1")
			}
		}()
	}
	wg.Wait()

	if got := n.connectionCount["127.0.0.1"]; got != 0 {
		t.Fatalf("connection count = %d, want 0", got)
	}
}

func TestGenerateSessionIDIsNumericAndNotTimestamp(t *testing.T) {
	const count = 64
	ids := make(map[string]struct{}, count)

	for i := 0; i < count; i++ {
		before := time.Now().UnixNano()
		id := generateSessionID()
		after := time.Now().UnixNano()
		if _, err := strconv.ParseInt(id, 10, 64); err != nil {
			t.Fatalf("session ID %q is not numeric int64: %v", id, err)
		}
		idValue, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if idValue >= before && idValue <= after {
			t.Fatalf("session ID %q exposes current UnixNano timestamp", id)
		}
		if _, exists := ids[id]; exists {
			t.Fatalf("duplicate session ID generated: %s", id)
		}
		ids[id] = struct{}{}
	}
}

func TestDefaultCORSHeadersAndPreflightBehavior(t *testing.T) {
	var req fasthttp.Request
	var ctx fasthttp.RequestCtx
	req.Header.SetMethod(fasthttp.MethodOptions)
	req.SetRequestURI("/api")
	ctx.Init(&req, nil, nil)

	applyDefaultCORSHeaders(&ctx)
	if isPreflightRequest(&ctx) {
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	if got := ctx.Response.StatusCode(); got != fasthttp.StatusOK {
		t.Fatalf("status = %d, want %d", got, fasthttp.StatusOK)
	}
	if got := string(ctx.Response.Header.Peek("Access-Control-Allow-Origin")); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q, want *", got)
	}
	if got := string(ctx.Response.Header.Peek("Access-Control-Allow-Headers")); got != "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, X-SSE-SessionID" {
		t.Fatalf("Access-Control-Allow-Headers = %q", got)
	}
}

func TestSSESessionIDFromRequestAcceptsCurrentSources(t *testing.T) {
	tests := []struct {
		name  string
		setup func(req *fasthttp.Request)
	}{
		{
			name: "header",
			setup: func(req *fasthttp.Request) {
				req.Header.Set("X-SSE-SessionID", "123")
			},
		},
		{
			name: "query",
			setup: func(req *fasthttp.Request) {
				req.SetRequestURI("/sse?sse_sessionID=123")
			},
		},
		{
			name: "cookie",
			setup: func(req *fasthttp.Request) {
				req.Header.SetCookie("sse_sessionID", "123")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req fasthttp.Request
			var ctx fasthttp.RequestCtx
			req.Header.SetMethod(fasthttp.MethodGet)
			req.SetRequestURI("/sse")
			tt.setup(&req)
			ctx.Init(&req, nil, nil)

			if got := sseSessionIDFromRequest(&ctx); got != "123" {
				t.Fatalf("session source %s = %q, want 123", tt.name, got)
			}
		})
	}
}

func TestSetSSESessionCookieKeepsNumericCookieBehavior(t *testing.T) {
	var req fasthttp.Request
	var ctx fasthttp.RequestCtx
	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI("/sse")
	ctx.Init(&req, nil, nil)

	setSSESessionCookie(&ctx, generateSessionID())

	cookie := string(ctx.Response.Header.PeekCookie("sse_sessionID"))
	sessionID := strings.TrimPrefix(strings.SplitN(cookie, ";", 2)[0], "sse_sessionID=")
	if sessionID == "" {
		t.Fatal("sse_sessionID cookie was not set")
	}
	if _, err := strconv.ParseInt(sessionID, 10, 64); err != nil {
		t.Fatalf("sse_sessionID cookie %q is not numeric int64: %v", sessionID, err)
	}
	if !strings.Contains(cookie, "HttpOnly") {
		t.Fatalf("sse_sessionID cookie missing HttpOnly: %q", cookie)
	}
	if !strings.Contains(cookie, "secure") {
		t.Fatalf("sse_sessionID cookie missing secure flag: %q", cookie)
	}
}

func TestDefaultCheckOriginAllowsWebSocketOrigin(t *testing.T) {
	req, err := http.NewRequest(http.MethodGet, "http://example.com/nano", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "http://evil.example")

	if !env.CheckOrigin(req) {
		t.Fatal("default CheckOrigin rejected origin; want current open default")
	}
}
