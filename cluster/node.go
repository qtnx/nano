// Copyright (c) nano Authors. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cluster

import (
	"bufio"
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fasthttp/websocket"
	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/metrics"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/scheduler"
	nanojson "github.com/lonng/nano/serialize/json"
	"github.com/lonng/nano/session"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (

	// ErrLimitConnection the connection of the ip is reach the limit
	ErrLimitConnection = errors.New("reach the limit of connection")

	// httpJSONSerializer decodes HTTP /api request bodies, which are always
	// JSON, independent of the cluster-wide binary serializer (M35).
	httpJSONSerializer = nanojson.NewSerializer()

	// clusterAuthMetadataKey is the gRPC metadata key that carries the shared
	// cluster auth token on inter-node RPCs (H9).
	clusterAuthMetadataKey = "authorization"
)

// Options contains some configurations for current node
type Options struct {
	Pipeline           pipeline.Pipeline
	IsMaster           bool
	AdvertiseAddr      string
	RetryInterval      time.Duration
	ClientAddr         string
	Components         *component.Components
	Label              string
	IsWebsocket        bool
	TSLCertificate     string
	TSLKey             string
	UnregisterCallback func(Member, func())
	RemoteServiceRoute CustomerRemoteServiceRoute
	ForceHostname      bool
	LimitConnectPerIp  uint
	OpenPrometheus     bool
	PrometheusAddr     string
}

// Node represents a node in nano cluster, which will contain a group of services.
// All services will register to cluster, and messages will be forwarded to the node
// which provides the respective service.
type Node struct {
	Options            // current node options
	ServiceAddr string // current server service address (RPC)

	cluster   *cluster
	handler   *LocalHandler
	server    *grpc.Server
	rpcClient *rpcClient

	mu              sync.RWMutex
	sessions        map[int64]*session.Session
	sseClients      map[string]chan []byte
	connectionCount map[string]uint
	// acceptedConns is the node-global count of currently accepted client
	// connections, enforced against env.MaxConnections independently of the
	// per-IP cap (H11). Accessed atomically.
	acceptedConns int64

	once          sync.Once
	keepaliveExit chan struct{}

	// HTTP server
	httpServer *http.Server

	// clientServer is the public client (fasthttp) listener; retained so that
	// Shutdown can stop it and release the port.
	clientServer *fasthttp.Server
	// metricsServer is the private Prometheus HTTP server, if enabled.
	metricsServer *http.Server
}

func (n *Node) Startup() error {
	if n.ServiceAddr == "" {
		return errors.New("service address cannot be empty in master node")
	}
	n.sessions = map[int64]*session.Session{}
	n.connectionCount = map[string]uint{}
	n.cluster = newCluster(n)
	n.handler = NewHandler(n, n.Pipeline)
	n.sseClients = map[string]chan []byte{}
	components := n.Components.List()
	for _, c := range components {
		err := n.handler.register(c.Comp, c.Opts)
		if err != nil {
			return err
		}
	}

	// Opt into the sharded task dispatcher when configured so distinct sessions
	// run concurrently while a single session stays ordered (H1). EnableSharded
	// is idempotent, so repeated node startups in one process enable it once.
	if env.SchedulerShards > 0 {
		scheduler.EnableSharded(env.SchedulerShards)
	}

	cache()
	if err := n.initNode(); err != nil {
		return err
	}

	// Initialize all components
	for _, c := range components {
		c.Comp.Init()
	}
	for _, c := range components {
		c.Comp.AfterInit()
	}

	// Start the server to handle connections and HTTP requests
	if n.ClientAddr != "" {
		log.Println(fmt.Sprintf("Listen and serve on %s", n.ClientAddr))
		if err := n.listenAndServe(); err != nil {
			return err
		}
	}

	// Expose Prometheus metrics on a private, node-owned server. Bind errors
	// are returned to the caller instead of crashing the process, and the
	// endpoint is never routed through the public client listener.
	if n.OpenPrometheus {
		if err := n.startPrometheus(); err != nil {
			return err
		}
	}

	return nil
}

// listenAndServe starts the server to handle both TCP and HTTP requests
func (n *Node) listenAndServe() error {
	log.Println(fmt.Sprintf("Listening on %s", n.ClientAddr))

	// Start the fasthttp server
	server := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			// Set CORS headers to accept all
			ctx.Response.Header.Set("Access-Control-Allow-Origin", "*")
			ctx.Response.Header.Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
			ctx.Response.Header.Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, X-SSE-SessionID")

			// Handle preflight requests
			if string(ctx.Method()) == "OPTIONS" {
				ctx.SetStatusCode(fasthttp.StatusOK)
				return
			}

			// Call the original handler
			n.handleFastHTTP(ctx)
		},
		// Bound how long a slow client may take to send its request headers/body
		// and how long an idle keep-alive connection is held, to limit slowloris
		// and idle-connection FD pinning (H35). WriteTimeout is intentionally
		// left unset because /sse responses are long-lived streams.
		ReadTimeout: 30 * time.Second,
		IdleTimeout: 75 * time.Second,
	}

	// Bind synchronously so a failure (invalid/in-use address) is reported to
	// the caller instead of being swallowed inside a goroutine.
	ln, err := net.Listen("tcp", n.ClientAddr)
	if err != nil {
		return err
	}
	n.clientServer = server

	// Honor the configured TLS material so WithTSLConfig actually serves
	// HTTPS/WSS instead of being silently ignored (M30). The serve loop runs in
	// a goroutine only after a successful synchronous bind.
	serveTLS := n.TSLCertificate != "" && n.TSLKey != ""
	go func() {
		var serveErr error
		if serveTLS {
			serveErr = server.ServeTLS(ln, n.TSLCertificate, n.TSLKey)
		} else {
			serveErr = server.Serve(ln)
		}
		if serveErr != nil {
			log.Println(fmt.Sprintf("Error serving fasthttp server: %v", serveErr))
		}
	}()
	return nil
}

// handleFastHTTP is the request handler for fasthttp
func (n *Node) handleFastHTTP(ctx *fasthttp.RequestCtx) {
	log.Debugf("Received request on %s", string(ctx.Path()))
	switch string(ctx.Path()) {
	case "/api":
		n.handleHTTPRequest(ctx)
	case "/sse":
		n.handleSSE(ctx)
	case "/health":
		ctx.SetStatusCode(fasthttp.StatusOK)
	case "/" + strings.TrimPrefix(env.WSPath, "/"):
		// Gate the WebSocket upgrade route on the IsWebsocket option so a node
		// that did not opt into WS does not expose an upgrade endpoint (M30).
		if !n.IsWebsocket {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			return
		}
		log.Debug("Handling WebSocket request")
		n.listenAndServeWS(ctx)
	default:
		// Unmatched routes must not be forwarded to http.DefaultServeMux: that
		// would expose internal handlers (e.g. /metrics) on the public client
		// listener.
		ctx.SetStatusCode(fasthttp.StatusNotFound)
	}
}

func convertFastHTTPToHTTP(ctx *fasthttp.RequestCtx) *http.Request {
	header := make(http.Header)
	ctx.Request.Header.VisitAll(func(key, value []byte) {
		// Append rather than overwrite so repeated headers (e.g. multiple
		// Cookie / X-Forwarded-For values) are preserved for middleware (M36).
		header.Add(string(key), string(value))
	})

	scheme := "http"
	if ctx.IsTLS() {
		scheme = "https"
	}

	// Copy the body so middleware can read the same payload the handler will
	// dispatch: ctx.PostBody() aliases buffers fasthttp reuses after the
	// handler returns (M36).
	postBody := ctx.PostBody()
	req := &http.Request{
		Method:        string(ctx.Method()),
		URL:           &url.URL{Scheme: scheme, Host: string(ctx.Host()), Path: string(ctx.Path()), RawQuery: string(ctx.QueryArgs().QueryString())},
		Proto:         "HTTP/1.1",
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        header,
		Host:          string(ctx.Host()),
		RemoteAddr:    ctx.RemoteAddr().String(),
		RequestURI:    string(ctx.RequestURI()),
		ContentLength: int64(len(postBody)),
	}
	if len(postBody) > 0 {
		b := make([]byte, len(postBody))
		copy(b, postBody)
		req.Body = io.NopCloser(bytes.NewReader(b))
	}
	return req
}

// handleHTTPRequest handles incoming HTTP requests from clients
func (n *Node) handleHTTPRequest(ctx *fasthttp.RequestCtx) {
	if !ctx.IsPost() {
		ctx.Error("Only POST method is allowed", fasthttp.StatusMethodNotAllowed)
		return
	}

	body := ctx.PostBody()

	var request struct {
		Route string          `json:"route"`
		Data  json.RawMessage `json:"data"`
		Type  int             `json:"type,omitempty"`
	}
	if err := json.Unmarshal(body, &request); err != nil {
		ctx.Error("Invalid JSON", fasthttp.StatusBadRequest)
		return
	}

	sid := ctx.Request.Header.Cookie("sse_sessionID")
	if len(sid) == 0 {
		sid = ctx.Request.Header.Peek("X-SSE-SessionID")
		if len(sid) == 0 {
			log.Infof("[Nano] Session ID cookie not found")
			ctx.Error("Session ID cookie not found", fasthttp.StatusUnauthorized)
			return
		}
	}

	if n.sseClient(string(sid)) == nil {
		log.Infof("[Nano] Session ID not found")
		ctx.Error("Session ID not found", fasthttp.StatusUnauthorized)
		return
	}

	sidInt, err := strconv.ParseInt(string(sid), 10, 64)
	if err != nil {
		log.Errorf("[Nano] Invalid session ID: %v", err)
		ctx.Error("Invalid session ID", fasthttp.StatusBadRequest)
		return
	}

	// The session must already exist (created by the SSE handler). A new
	// stand-alone httpAgent here would have no SSE channel, so Push could never
	// reach the client; reject instead of creating an orphaned session.
	existingSession := n.findSession(sidInt)
	if existingSession == nil {
		log.Infof("[Nano] session not found for %d", sidInt)
		ctx.Error("Session ID not found", fasthttp.StatusUnauthorized)
		return
	}
	agent, ok := existingSession.NetworkEntity().(*httpAgent)
	if !ok {
		ctx.Error("Session is not an HTTP session", fasthttp.StatusBadRequest)
		return
	}

	var msgType message.Type
	switch request.Type {
	case 0:
		msgType = message.Request
	case 1:
		msgType = message.Notify
	default:
		log.Errorf("[Nano] Invalid message type: %d", request.Type)
		ctx.Error("Invalid message type", fasthttp.StatusBadRequest)
		return
	}

	// Per-agent monotonic message id; wall-clock nanoseconds collided under
	// concurrency and clobbered another in-flight request's context (H33).
	messageID := agent.nextMid()
	agent.AttackHttpRequestCtx(messageID, ctx)
	defer agent.RemoveHttpRequestCtx(messageID)

	// validate authenticate
	if env.MiddlewareHttp != nil {
		if err := env.MiddlewareHttp(agent.session, convertFastHTTPToHTTP(ctx)); err != nil {
			log.Infof("[Nano] Invalidate authenticate middleware")
			ctx.Error("Invalidate authenticate", fasthttp.StatusUnauthorized)
			return
		}
	}

	rawData, _ := request.Data.MarshalJSON()
	msg := &message.Message{
		Type:  msgType,
		Route: request.Route,
		Data:  rawData,
		ID:    messageID,
	}

	handler, found := n.handler.localHandlers[request.Route]
	if found {
		log.Debugf("[Nano] Found local handler for route: %s", request.Route)
		// HTTP bodies are JSON; decode with the JSON serializer regardless of
		// the cluster-wide (possibly binary) serializer (M35).
		n.handler.localProcess(handler, messageID, agent.session, msg, httpJSONSerializer)
	} else {
		log.Debugf("[Nano] No local handler found for route: %s", request.Route)
		if err := n.handler.remoteProcess(agent.session, msg, false); err != nil {
			// Surface routing/delivery failures instead of waiting for the
			// request to time out (M7).
			log.Errorf("[Nano] remote process route %s failed: %v", request.Route, err)
			ctx.Error("Service unavailable", fasthttp.StatusBadGateway)
			return
		}
	}

	// A notify expects no response: acknowledge immediately instead of pinning
	// the request goroutine on the response-wait loop (H20).
	if msgType == message.Notify {
		ctx.SetStatusCode(fasthttp.StatusOK)
		return
	}

	// A request waits for the response (the local handler's session.Response or
	// the remote node's HandleResponse) to be recorded against this message id.
	httpObserve := agent.GetFastHttpContextObserve(messageID)
	if httpObserve != nil {
		n.waitForHTTPResponse(ctx, httpObserve, messageID, remoteRPCTimeout)
	}
}

// waitForHTTPResponse blocks until the in-flight request's observe is signalled
// (its done channel closed when the response is recorded) or the timeout
// elapses, writing the appropriate response to ctx. It replaces a 1ms busy-poll
// that re-locked the agent mutex on every wakeup (M37). The response body is
// written here, on the request-owning goroutine, so the fasthttp RequestCtx is
// never mutated from the scheduler/RPC goroutine that produced it (H16).
func (n *Node) waitForHTTPResponse(ctx *fasthttp.RequestCtx, observe *fastHttpContextObserve, messageID uint64, timeout time.Duration) {
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-timer.C:
		log.Infof("[Nano] Request timeout")
		ctx.Error("Request timeout", fasthttp.StatusRequestTimeout)
	case <-observe.done:
		if observe.status() == HttpObserveError {
			log.Infof("[Nano] Request error %v", messageID)
			ctx.Error("Request error", fasthttp.StatusInternalServerError)
			return
		}
		if observe.contentType != "" {
			ctx.SetContentType(observe.contentType)
		}
		ctx.SetBody(observe.body)
		ctx.SetStatusCode(fasthttp.StatusOK)
		log.Debugf("[Nano] Request success %v", messageID)
	}
}

// sseWriteTimeout bounds a single SSE Flush so a stalled client cannot pin the
// stream goroutine/FD forever. It is applied per write and refreshed before
// each event/keepalive, so a healthy long-lived stream is unaffected (H35).
const sseWriteTimeout = 10 * time.Second

// flushSSE sets a bounded write deadline on the underlying connection (when
// available) and flushes the buffered SSE writer. A stalled write then fails
// with a timeout instead of blocking forever, so the caller can return and let
// the unregister defer reclaim the stream (H35).
func flushSSE(conn net.Conn, w *bufio.Writer, timeout time.Duration) error {
	if conn != nil && timeout > 0 {
		_ = conn.SetWriteDeadline(time.Now().Add(timeout))
	}
	return w.Flush()
}

// handleSSE handles Server-Sent Events to push events to clients
func (n *Node) handleSSE(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/event-stream")
	ctx.Response.Header.Set("Cache-Control", "no-cache")
	ctx.Response.Header.Set("Connection", "keep-alive")
	ctx.Response.Header.Set("Transfer-Encoding", "chunked")
	ctx.SetStatusCode(fasthttp.StatusOK)
	log.Debugf("SSE connection established")

	// Resolve the session id from cookie/header/query; clientSupplied records
	// whether the client chose it (vs. a server-generated one).
	var sessionID string
	clientSupplied := true
	cookie := ctx.Request.Header.Cookie("sse_sessionID")
	if len(cookie) == 0 {
		sessionID = string(ctx.Request.Header.Peek("X-SSE-SessionID"))
		if sessionID == "" {
			sessionID = string(ctx.QueryArgs().Peek("sse_sessionID"))
		}
	} else {
		sessionID = string(cookie)
	}

	if len(sessionID) == 0 {
		clientSupplied = false
		sessionID = generateSessionID()
		c := fasthttp.AcquireCookie()
		c.SetKey("sse_sessionID")
		c.SetValue(sessionID)
		c.SetPath("/")
		c.SetMaxAge(3600) // 1 hour
		c.SetHTTPOnly(true)
		// Only mark the cookie Secure when the listener actually serves TLS;
		// on plain HTTP a Secure cookie is never sent back by the browser and
		// the handshake silently breaks (L11).
		c.SetSecure(n.TSLCertificate != "" && n.TSLKey != "")
		ctx.Response.Header.SetCookie(c)
		fasthttp.ReleaseCookie(c)
	}

	sidInt, err := strconv.ParseInt(sessionID, 10, 64)
	if err != nil {
		log.Errorf("Invalid session ID: %v", err)
		ctx.Error("Invalid session ID", fasthttp.StatusBadRequest)
		return
	}

	// A client must not bind an SSE stream onto a session id owned by a
	// non-SSE (TCP/WS) session: that would replace another user's network
	// entity before any application auth (H18).
	if clientSupplied {
		if existing := n.findSession(sidInt); existing != nil {
			if _, ok := existing.NetworkEntity().(*httpAgent); !ok {
				log.Errorf("[Nano] SSE session id %d collides with a non-SSE session", sidInt)
				ctx.Error("Session ID conflict", fasthttp.StatusConflict)
				return
			}
		}
	}

	// Create the agent (and its sseDone signal) before the stream writer so the
	// writer goroutine — which fasthttp runs after this handler returns — can
	// capture it.
	sseEventChan := make(chan []byte, 2<<7) // buffer size 256
	agent := NewHTTPAgent(sidInt, nil, sseEventChan, n.handler.remoteProcess, ctx)
	if env.MiddlewareHttp != nil {
		if err := env.MiddlewareHttp(agent.session, convertFastHTTPToHTTP(ctx)); err != nil {
			ctx.Error("Invalidate authenticate", fasthttp.StatusUnauthorized)
			return
		}
	}
	// Apply the same connection caps as the TCP/WS path so long-lived SSE
	// streams are bounded too (H11): per-IP first, then the node-global cap.
	// The matching decrements run in the stream-writer defer below.
	remoteAddr := ctx.RemoteIP().String()
	if err := n.increaseConnection(remoteAddr); err != nil {
		ctx.Error("connection limit reached", fasthttp.StatusTooManyRequests)
		return
	}
	if env.MaxConnections > 0 {
		if atomic.AddInt64(&n.acceptedConns, 1) > int64(env.MaxConnections) {
			atomic.AddInt64(&n.acceptedConns, -1)
			n.decreaseConnection(remoteAddr)
			metrics.ServerClosedConnections.Inc()
			ctx.Error("server overloaded", fasthttp.StatusServiceUnavailable)
			return
		}
	}
	// Atomically publish the new SSE session + channel and capture any HTTP/SSE
	// session it displaces (a client reconnecting / a second tab reusing the same
	// id). The displaced session is closed after the lock so its
	// Lifetime.OnClosed cleanup runs and its old stream goroutine stops — a
	// plain overwrite would leak both on a normal EventSource reconnect. The
	// stale-channel guard in unregisterSSEClient keeps the old stream's defer
	// from tearing down this replacement (H19).
	n.mu.Lock()
	var displaced *httpAgent
	if old := n.sessions[sidInt]; old != nil {
		displaced, _ = old.NetworkEntity().(*httpAgent)
	}
	n.sessions[sidInt] = agent.session
	n.sseClients[sessionID] = sseEventChan
	n.mu.Unlock()
	if displaced != nil {
		displaced.Close()
	}

	// Capture the underlying connection before returning so the stream-writer
	// goroutine (run by fasthttp after this handler returns) can bound each
	// Flush with a write deadline (H35).
	conn := ctx.Conn()
	ctx.SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		fmt.Fprintf(w, "data: {\"route\": \"onConnected\", \"body\": {\"sse_sessionID\": \"%s\"}}\n\n", sessionID)
		_ = flushSSE(conn, w, sseWriteTimeout)

		// Keep the connection open with a ticker
		ticker := time.NewTicker(1 * time.Second)
		defer func() {
			ticker.Stop()
			// Remove only this stream's mapping/session; a reconnect that
			// replaced it under the same id must survive (H19).
			n.unregisterSSEClient(sessionID, sseEventChan)
			// Release this stream's connection-cap accounting exactly once (H11).
			n.decreaseConnection(remoteAddr)
			if env.MaxConnections > 0 {
				atomic.AddInt64(&n.acceptedConns, -1)
			}
			log.Debugf("SSE connection closed: %s", sessionID)
		}()

		for {
			select {
			case <-ctx.Done():
				log.Debugf("SSE connection closed by context done: %s", sessionID)
				return
			case <-agent.sseDone:
				// Backend-initiated close (CloseSession) signalled this stream
				// to stop, so the goroutine/entry don't leak (M31).
				log.Debugf("SSE connection closed by backend: %s", sessionID)
				return
			case event := <-sseEventChan:
				log.Debugf("Send SSE event: %s", event)
				fmt.Fprintf(w, "data: %s\n\n", event)
				if err := flushSSE(conn, w, sseWriteTimeout); err != nil {
					log.Errorf("Error flushing SSE event: %v", err)
					return
				}
			case <-ticker.C:
				// Send a keep-alive comment; a Flush error here reliably
				// detects a disconnected client within the tick interval so the
				// unregister defer runs (H35).
				fmt.Fprintf(w, ": keep-alive\n\n")
				if err := flushSSE(conn, w, sseWriteTimeout); err != nil {
					log.Errorf("Error flushing keep-alive: %v", err)
					return
				}
			}
		}
	}))

	log.Debugf("SSE stream established: %s", sessionID)
}

func (n *Node) registerSSEClient(sessionID string, eventChan chan []byte) {
	log.Debugf("Register SSE client: %s", sessionID)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sseClients[sessionID] = eventChan
}

// sseClient returns the SSE event channel for a session id, reading the map
// under the lock so concurrent register/unregister cannot trigger a fatal
// concurrent map access (H14).
func (n *Node) sseClient(sessionID string) chan []byte {
	n.mu.RLock()
	ch := n.sseClients[sessionID]
	n.mu.RUnlock()
	return ch
}

// unregisterSSEClient removes the SSE mapping and session for sessionID, but
// only when the registered channel still matches ch — a reconnect that replaced
// this stream under the same id must not be torn down (H19). It deletes the
// session and closes the entity too, so a stream exit fully reclaims state; it
// is the single teardown path shared with backend-initiated closes (M31).
func (n *Node) unregisterSSEClient(sessionID string, ch chan []byte) {
	log.Debugf("Unregister SSE client: %s", sessionID)
	n.mu.Lock()
	cur, ok := n.sseClients[sessionID]
	if ok && ch != nil && cur != ch {
		// A newer stream replaced this one; leave it and its session intact.
		n.mu.Unlock()
		return
	}
	delete(n.sseClients, sessionID)
	sidInt, _ := strconv.ParseInt(sessionID, 10, 64)
	s := n.sessions[sidInt]
	delete(n.sessions, sidInt)
	n.mu.Unlock()

	if s != nil {
		if ne := s.NetworkEntity(); ne != nil {
			ne.Close()
		}
	}
}

func generateSessionID() string {
	// Wall-clock nanoseconds collide for concurrent connections and are not
	// monotonic; use a crypto-random positive int64 so SSE ids are unique and
	// unguessable (H33). The id must remain a base-10 int64 string because it
	// doubles as the nano session id.
	var b [8]byte
	if _, err := crand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	id := int64(binary.BigEndian.Uint64(b[:]) >> 1) // clear sign bit -> positive
	return strconv.FormatInt(id, 10)
}

// listenAndServeWS handles WebSocket connections using fasthttp
func (n *Node) listenAndServeWS(ctx *fasthttp.RequestCtx) {
	upgrader := websocket.FastHTTPUpgrader{
		CheckOrigin: func(ctx *fasthttp.RequestCtx) bool {
			// Convert fasthttp.RequestCtx to http.Request
			r := &http.Request{
				Method:     string(ctx.Method()),
				URL:        &url.URL{Scheme: "http", Host: string(ctx.Host()), Path: string(ctx.Path())},
				Proto:      "HTTP/1.1",
				ProtoMajor: 1,
				ProtoMinor: 1,
				Header:     make(http.Header),
				Host:       string(ctx.Host()),
				RemoteAddr: ctx.RemoteAddr().String(),
			}
			ctx.Request.Header.VisitAll(func(key, value []byte) {
				r.Header.Add(string(key), string(value))
			})
			ok := env.CheckOrigin(r)
			if env.Debug {
				log.Debugf("CheckOrigin ok: %v", ok)
			}
			return ok
		},
	}

	err := upgrader.Upgrade(ctx, func(conn *websocket.Conn) {
		if conn == nil {
			log.Error("Upgrade error: conn is nil")
			return
		}
		n.handler.handleWS(conn)
	})
	if err != nil {
		log.Errorf("Upgrade error: %v", err)
		return
	}
}

// (removed dead listenAndServeWSTLS: it had no caller, routed WS through the
// global http.DefaultServeMux, and called log.Fatal on a bind error. TLS is now
// served by listenAndServe via fasthttp ServeTLS (M30).)

func (n *Node) Handler() *LocalHandler {
	return n.handler
}

func (n *Node) initNode() error {
	// Current node is not master server and does not contains master
	// address, so running in singleton mode
	if !n.IsMaster && n.AdvertiseAddr == "" {
		return nil
	}

	// Parse with net.SplitHostPort so a malformed address returns a startup
	// error instead of panicking on a missing index, and an IPv6 hostport such
	// as "[::1]:9000" is handled correctly (L9).
	_, port, err := net.SplitHostPort(n.ServiceAddr)
	if err != nil {
		return fmt.Errorf("cluster: invalid service address %q: %w", n.ServiceAddr, err)
	}
	if port == "" {
		return errors.New("invalid service address")
	}

	var listenAddr string
	if !n.ForceHostname {
		// This is a hack for docker container and kubernetes
		listenAddr = net.JoinHostPort("0.0.0.0", port)
	} else {
		listenAddr = n.ServiceAddr
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	// Initialize the gRPC server and register service
	// Chain the recovery interceptor (outermost, so it also covers a panic in
	// auth) with the shared-token auth interceptor (H9). With no token set the
	// auth interceptor is a pass-through.
	n.server = grpc.NewServer(grpc.ChainUnaryInterceptor(recoveryUnaryInterceptor, n.authUnaryInterceptor))

	// H9: a cluster gRPC server with no token accepts any peer that can reach
	// the port (register fake backends, close arbitrary sessions). Warn loudly
	// once at startup unless the operator explicitly acknowledged insecure mode.
	if env.ClusterAuthToken == "" && !env.InsecureCluster {
		log.Warn("cluster: SECURITY: gRPC server is running WITHOUT authentication (env.ClusterAuthToken is empty); any peer reaching this port can register backends or close sessions. Set env.ClusterAuthToken, or set env.InsecureCluster=true to acknowledge.")
	}
	n.rpcClient = newRPCClient()
	clusterpb.RegisterMemberServer(n.server, n)

	go n.runGRPCServer(listener)

	n.cluster.setRpcClient(n.rpcClient)
	if n.IsMaster {
		clusterpb.RegisterMasterServer(n.server, n.cluster)
		member := &Member{
			isMaster: true,
			memberInfo: &clusterpb.MemberInfo{
				Label:       n.Label,
				ServiceAddr: n.ServiceAddr,
				Services:    n.handler.LocalService(),
			},
		}
		n.cluster.addLocalMember(member)
	} else {
		pool, err := n.rpcClient.getConnPool(n.AdvertiseAddr)
		if err != nil {
			return err
		}
		client := clusterpb.NewMasterClient(pool.Get())
		request := &clusterpb.RegisterRequest{
			MemberInfo: &clusterpb.MemberInfo{
				Label:       n.Label,
				ServiceAddr: n.ServiceAddr,
				Services:    n.handler.LocalService(),
			},
		}
		for {
			// Bound each attempt and abort the retry loop on shutdown: the
			// previous context.Background() + unconditional sleep could hang
			// startup indefinitely during a master partition (H23).
			ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
			resp, err := client.Register(ctx, request)
			cancel()
			if err == nil {
				n.handler.initRemoteService(resp.Members)
				n.cluster.initMembers(resp.Members)
				break
			}
			log.Println("Register current node to cluster failed", err, "and will retry in", n.RetryInterval.String())
			select {
			case <-env.Die:
				return fmt.Errorf("cluster: registration aborted during shutdown: %w", err)
			case <-time.After(n.RetryInterval):
			}
		}
		n.once.Do(n.keepalive)
	}
	return nil
}

// Shutdown gracefully shuts down the node and the HTTP server
func (n *Node) Shutdown() {
	// Call `BeforeShutdown` hooks in reverse order
	components := n.Components.List()
	length := len(components)
	for i := length - 1; i >= 0; i-- {
		components[i].Comp.BeforeShutdown()
	}

	// Call `Shutdown` hooks in reverse order
	for i := length - 1; i >= 0; i-- {
		components[i].Comp.Shutdown()
	}

	// Close sendHeartbeat
	if n.keepaliveExit != nil {
		close(n.keepaliveExit)
	}
	// Stop the master heartbeat-checker goroutine so it no longer mutates stale
	// cluster state after shutdown (M13). Idempotent / no-op when not started.
	if n.cluster != nil {
		n.cluster.stopHeartbeatChecker()
	}
	if !n.IsMaster && n.AdvertiseAddr != "" {
		pool, err := n.rpcClient.getConnPool(n.AdvertiseAddr)
		if err != nil {
			log.Println("Retrieve master address error", err)
			goto EXIT
		}
		client := clusterpb.NewMasterClient(pool.Get())
		request := &clusterpb.UnregisterRequest{
			ServiceAddr: n.ServiceAddr,
		}
		// Bound shutdown unregister so a stalled master cannot hang shutdown
		// indefinitely (H23).
		ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
		_, err = client.Unregister(ctx, request)
		cancel()
		if err != nil {
			log.Println("Unregister current node failed", err)
			goto EXIT
		}
	}

EXIT:
	if n.server != nil {
		n.server.GracefulStop()
	}

	// Shutdown HTTP server if running
	if n.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := n.httpServer.Shutdown(ctx); err != nil {
			log.Println(fmt.Sprintf("HTTP server Shutdown Error: %v", err))
		}
	}

	// Stop the public client listener so its port is released on restart.
	if n.clientServer != nil {
		if err := n.clientServer.Shutdown(); err != nil {
			log.Println(fmt.Sprintf("Client server Shutdown Error: %v", err))
		}
	}

	// Stop the Prometheus metrics server if it was started.
	if n.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := n.metricsServer.Shutdown(ctx); err != nil {
			log.Println(fmt.Sprintf("Metrics server Shutdown Error: %v", err))
		}
	}

	// Close outbound gRPC client pools so their ClientConns/goroutines are
	// reclaimed on in-process shutdown/restart (M8).
	if n.rpcClient != nil {
		n.rpcClient.closePool()
	}
}

func (n *Node) storeSession(s *session.Session) {
	n.mu.Lock()
	n.sessions[s.ID()] = s
	log.Debugf("session %d stored", s.ID())
	n.mu.Unlock()
}

func (n *Node) findSession(sid int64) *session.Session {
	n.mu.RLock()
	s := n.sessions[sid]
	n.mu.RUnlock()
	return s
}

// deleteSession removes a session by id under the lock. Used by the client
// read-loop cleanup so a disconnect doesn't leak its n.sessions entry (H13).
func (n *Node) deleteSession(sid int64) {
	n.mu.Lock()
	delete(n.sessions, sid)
	n.mu.Unlock()
}

// applyClientState restores the client-supplied uid and user data onto a
// session. Shared by the found / created / raced paths of findOrCreateSession.
func applyClientState(s *session.Session, clientUid int64, clientUserData []byte) error {
	s.SetClientUid(clientUid)
	var userData map[string]interface{}
	if err := json.Unmarshal(clientUserData, &userData); err != nil {
		return err
	}
	s.Restore(userData)
	return nil
}

func (n *Node) findOrCreateSession(sid, clientUid int64, gateAddr string, clientUserData []byte, jsonPayload bool) (*session.Session, error) {
	n.mu.RLock()
	s, found := n.sessions[sid]
	n.mu.RUnlock()
	if found {
		if env.Debug {
			log.Println("Found session ")
		}
		if err := applyClientState(s, clientUid, clientUserData); err != nil {
			return nil, err
		}
		return s, nil
	}

	if env.Debug {
		log.Println("Not found session ")
	}

	// Only dial gates that are registered cluster members (or this node
	// itself). A malformed/hostile member RPC must not be able to make this
	// node dial an arbitrary address or forge a session for one (M9).
	if gateAddr != n.ServiceAddr && (n.cluster == nil || !n.cluster.isKnownAddr(gateAddr)) {
		return nil, fmt.Errorf("cluster: refusing to create session for unknown gate address %q", gateAddr)
	}

	conns, err := n.rpcClient.getConnPool(gateAddr)
	if err != nil {
		return nil, err
	}
	ac := &acceptor{
		sid:         sid,
		gateClient:  clusterpb.NewMemberClient(conns.Get()),
		rpcHandler:  n.handler.remoteProcess,
		gateAddr:    gateAddr,
		jsonPayload: jsonPayload,
	}
	s = session.New(ac)
	ac.session = s
	if err := applyClientState(s, clientUid, clientUserData); err != nil {
		return nil, err
	}

	// Double-check under the write lock: a concurrent first-seen RPC for the
	// same sid may have created and stored a session already. Reuse it so every
	// caller shares one Session per sid instead of splitting state across two
	// objects (M10).
	n.mu.Lock()
	if existing, ok := n.sessions[sid]; ok {
		n.mu.Unlock()
		if err := applyClientState(existing, clientUid, clientUserData); err != nil {
			return nil, err
		}
		return existing, nil
	}
	n.sessions[sid] = s
	n.mu.Unlock()
	return s, nil
}

// / increaseConnection prevent too many connections from the same ip
// / maybe cheat or ddos
func (n *Node) increaseConnection(ipAddress string) error {
	if n.LimitConnectPerIp > 0 {
		// Check and increment the per-IP count atomically under the lock so a
		// rejection rolls back cleanly and never races a concurrent connect /
		// disconnect (H15).
		n.mu.Lock()
		if n.connectionCount[ipAddress] >= n.LimitConnectPerIp {
			n.mu.Unlock()
			log.Warn(fmt.Sprintf("The connection of ip %s is reach the limit %v", ipAddress, n.LimitConnectPerIp))
			return ErrLimitConnection
		}
		n.connectionCount[ipAddress]++
		n.mu.Unlock()
	}
	// Only count accepted connections so a rejected one doesn't leak a gauge
	// child that is never decremented (M18); reclamation happens in
	// DecConnectionsPerIP when the per-IP count drains to zero (H29).
	metrics.IncConnectionsPerIP(ipAddress)
	return nil
}

func (n *Node) decreaseConnection(ipAddress string) {
	metrics.DecConnectionsPerIP(ipAddress)

	if n.LimitConnectPerIp > 0 {
		// Read and mutate the count only under the lock (H15) and drop the key
		// once it reaches zero so IP churn cannot grow the map unboundedly
		// (M28).
		n.mu.Lock()
		if c, ok := n.connectionCount[ipAddress]; ok {
			if c <= 1 {
				delete(n.connectionCount, ipAddress)
			} else {
				n.connectionCount[ipAddress] = c - 1
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) Ping(_ context.Context, _ *clusterpb.PingRequest) (*clusterpb.PingResponse, error) {
	return &clusterpb.PingResponse{
		Msg: "pong",
	}, nil
}

func (n *Node) HandleRequest(_ context.Context, req *clusterpb.RequestMessage) (*clusterpb.MemberHandleResponse, error) {
	if env.Debug {
		log.Debug("[Node] Start handle HandleRequest", req.String())
	}
	handler, found := n.handler.localHandlers[req.Route]

	if !found {
		return nil, fmt.Errorf("service not found in current node: %v", req.Route)
	}
	s, err := n.findOrCreateSession(req.SessionId, req.ClientUid, req.GateAddr, req.ClientUserData, req.JsonPayload)
	if err != nil {
		return nil, err
	}
	if env.Debug {
		log.Println("HandleRequest: ", req.Route, req.Id, req.SessionId, fmt.Sprintf("New session id: %v", s.ID()), fmt.Sprintf("ClientUid: %v", s.ClientUid()), fmt.Sprintf("ClientUserData: %v", s.State()))
	}
	msg := &message.Message{
		Type:  message.Request,
		ID:    req.Id,
		Route: req.Route,
		Data:  req.Data,
	}
	if req.JsonPayload {
		n.handler.localProcess(handler, req.Id, s, msg, httpJSONSerializer)
	} else {
		n.handler.localProcess(handler, req.Id, s, msg, nil)
	}
	if env.Debug {
		log.Debug("[Node] End handle HandleRequest", req.String())
	}
	return &clusterpb.MemberHandleResponse{}, nil
}

func (n *Node) HandleResponse(_ context.Context, req *clusterpb.ResponseMessage) (*clusterpb.MemberHandleResponse, error) {
	s := n.findSession(req.SessionId)
	if s == nil {
		return &clusterpb.MemberHandleResponse{}, fmt.Errorf("session not found: %v", req.SessionId)
	}
	return &clusterpb.MemberHandleResponse{}, s.ResponseMID(req.Id, req.Data)
}

func (n *Node) HandleNotify(_ context.Context, req *clusterpb.NotifyMessage) (*clusterpb.MemberHandleResponse, error) {
	if env.Debug {
		log.Debug("[Node] Start handle HandleNotify", req.String())
	}
	handler, found := n.handler.localHandlers[req.Route]
	if !found {
		return nil, fmt.Errorf("service not found in current node: %v", req.Route)
	}
	s, err := n.findOrCreateSession(req.SessionId, req.ClientUid, req.GateAddr, req.ClientUserData, req.JsonPayload)
	if err != nil {
		return nil, err
	}
	if env.Debug {
		log.Println("[Node] HandleNotify:", req.Route, req.SessionId, fmt.Sprintf("session id: %v", s.ID()))
	}
	msg := &message.Message{
		Type:  message.Notify,
		Route: req.Route,
		Data:  req.Data,
	}
	if req.JsonPayload {
		n.handler.localProcess(handler, 0, s, msg, httpJSONSerializer)
	} else {
		n.handler.localProcess(handler, 0, s, msg, nil)
	}
	return &clusterpb.MemberHandleResponse{}, nil
}

func (n *Node) HandlePush(_ context.Context, req *clusterpb.PushMessage) (*clusterpb.MemberHandleResponse, error) {
	s := n.findSession(req.SessionId)
	if s == nil {
		return &clusterpb.MemberHandleResponse{}, fmt.Errorf("session not found: %v", req.SessionId)
	}
	return &clusterpb.MemberHandleResponse{}, s.Push(req.Route, req.Data)
}

func (n *Node) NewMember(_ context.Context, req *clusterpb.NewMemberRequest) (*clusterpb.NewMemberResponse, error) {
	if req == nil || req.MemberInfo == nil || req.MemberInfo.ServiceAddr == "" {
		return nil, ErrInvalidRegisterReq
	}
	// Update membership and install routes under one c.mu critical section so a
	// concurrent same-address DelMember cannot interleave a route/member split on
	// this peer (issue #7, review round 4). replaceRemoteService also drops stale
	// services on a same-address rejoin (fix 2/7). c.mu -> h.mu order is safe.
	n.cluster.mu.Lock()
	n.cluster.addMemberLocked(req.MemberInfo)
	n.handler.replaceRemoteService(req.MemberInfo)
	n.cluster.mu.Unlock()
	return &clusterpb.NewMemberResponse{}, nil
}

func (n *Node) DelMember(_ context.Context, req *clusterpb.DelMemberRequest) (*clusterpb.DelMemberResponse, error) {
	if req == nil || req.ServiceAddr == "" {
		return nil, ErrInvalidRegisterReq
	}
	log.Println("[Node] DelMember member", req.String())
	// Remove routes and membership under one c.mu critical section so a
	// concurrent same-address NewMember cannot interleave a route/member split on
	// this peer (issue #7, review round 4). Pool teardown / pending-delete drop
	// stay outside the lock.
	n.cluster.mu.Lock()
	n.handler.delMember(req.ServiceAddr)
	n.cluster.removeMemberLocked(req.ServiceAddr)
	n.cluster.mu.Unlock()
	n.cluster.releaseDepartedPeer(req.ServiceAddr)
	// Purge any acceptor sessions homed on the departed gate so they don't leak
	// in n.sessions when the gate vanishes before sending per-session
	// SessionClosed RPCs (H24).
	n.purgeGateSessions(req.ServiceAddr)
	return &clusterpb.DelMemberResponse{}, nil
}

// purgeGateSessions removes every acceptor session whose owning gate matches
// gateAddr and runs its lifetime-close hooks. It deliberately does not call the
// acceptor's Close (which would issue a CloseSession RPC back to the now-gone
// gate); it only reclaims local state.
func (n *Node) purgeGateSessions(gateAddr string) {
	n.mu.Lock()
	var closed []*session.Session
	for sid, s := range n.sessions {
		if ac, ok := s.NetworkEntity().(*acceptor); ok && ac.gateAddr == gateAddr {
			delete(n.sessions, sid)
			closed = append(closed, s)
		}
	}
	n.mu.Unlock()
	for _, s := range closed {
		sc := s
		scheduler.PushTask(func() { session.Lifetime.Close(sc) })
	}
}

// SessionClosed implements the MemberServer interface.
//
// M11 (owner authorization): a session is only reclaimed when the caller is its
// owning gate. When req.GateAddr is set and does not match the session's owning
// acceptor.gateAddr, the request is ignored so a non-owning peer cannot drop
// another gate's session. An empty GateAddr keeps legacy behavior; note a
// shared-token peer could still spoof GateAddr — full per-gate identity needs
// mTLS client certificates.
func (n *Node) SessionClosed(_ context.Context, req *clusterpb.SessionClosedRequest) (*clusterpb.SessionClosedResponse, error) {
	n.mu.Lock()
	s, found := n.sessions[req.SessionId]
	if found && req.GateAddr != "" {
		if ac, ok := s.NetworkEntity().(*acceptor); ok && ac.gateAddr != req.GateAddr {
			n.mu.Unlock()
			return &clusterpb.SessionClosedResponse{}, nil
		}
	}
	delete(n.sessions, req.SessionId)
	n.mu.Unlock()
	if found {
		scheduler.PushTask(func() { session.Lifetime.Close(s) })
	}
	return &clusterpb.SessionClosedResponse{}, nil
}

// CloseSession implements the MemberServer interface.
//
// M11: same caveat as SessionClosed — no caller/gate identity is available on
// the request, so ownership is not verified before closing. See SessionClosed.
func (n *Node) CloseSession(_ context.Context, req *clusterpb.CloseSessionRequest) (*clusterpb.CloseSessionResponse, error) {
	n.mu.Lock()
	s, found := n.sessions[req.SessionId]
	delete(n.sessions, req.SessionId)
	n.mu.Unlock()
	if found {
		s.Close()
	}
	return &clusterpb.CloseSessionResponse{}, nil
}

// PingNodes ping other nodes in the cluster
func (n *Node) PingNodes(nodeLabels []string) (lives []string, dies []string, err error) {
	if n.cluster == nil {
		return nil, nil, fmt.Errorf("Node is not initialized")
	}

	return n.cluster.pingNodes(nodeLabels)
}

// ticker send heartbeat register info to master
func (n *Node) keepalive() {
	if n.keepaliveExit == nil {
		n.keepaliveExit = make(chan struct{})
	}
	if n.AdvertiseAddr == "" || n.IsMaster {
		return
	}
	heartbeat := func() {
		pool, err := n.rpcClient.getConnPool(n.AdvertiseAddr)
		if err != nil {
			log.Println("rpcClient master conn", err)
			return
		}
		masterCli := clusterpb.NewMasterClient(pool.Get())
		ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
		defer cancel()
		if _, err := masterCli.Heartbeat(ctx, &clusterpb.HeartbeatRequest{
			MemberInfo: &clusterpb.MemberInfo{
				Label:       n.Label,
				ServiceAddr: n.ServiceAddr,
				Services:    n.handler.LocalService(),
			},
		}); err != nil {
			log.Println("Member send heartbeat error", err)
		}
	}
	go func() {
		ticker := time.NewTicker(env.Heartbeat)
		for {
			select {
			case <-ticker.C:
				heartbeat()
			case <-n.keepaliveExit:
				log.Println("Exit member node heartbeat ")
				ticker.Stop()
				return
			}
		}
	}()
}

// recoveryUnaryInterceptor converts a panic in any gRPC handler into a
// codes.Internal error instead of letting it crash the node process.
func recoveryUnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			method := ""
			if info != nil {
				method = info.FullMethod
			}
			log.Error(fmt.Sprintf("cluster: recovered panic in gRPC handler %s: %v", method, r))
			err = status.Errorf(codes.Internal, "internal server error")
		}
	}()
	return handler(ctx, req)
}

// authUnaryInterceptor enforces the shared-token check on inter-node cluster
// RPCs when env.ClusterAuthToken is configured: the incoming `authorization`
// metadata must carry a matching token, otherwise the call is rejected with
// codes.Unauthenticated. With no token configured it is a pass-through
// (insecure mode, warned about at startup) (H9).
func (n *Node) authUnaryInterceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	token := env.ClusterAuthToken
	if token == "" {
		return handler(ctx, req)
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "missing cluster auth metadata")
	}
	vals := md.Get(clusterAuthMetadataKey)
	if len(vals) == 0 || vals[0] != token {
		return nil, status.Error(codes.Unauthenticated, "invalid cluster auth token")
	}
	return handler(ctx, req)
}

// runGRPCServer serves the gRPC listener. A post-startup Serve error is logged
// rather than escalated to log.Fatalf, which would call os.Exit from this
// background goroutine and bypass node/component shutdown.
func (n *Node) runGRPCServer(listener net.Listener) {
	if err := n.server.Serve(listener); err != nil {
		log.Errorf("cluster: gRPC server stopped serving: %v", err)
	}
}

// startPrometheus exposes the metrics endpoint on a private mux and a
// node-owned HTTP server bound to a configurable address (default :2112). The
// listener is bound synchronously so bind errors surface to the caller, and
// the server is retained for shutdown.
func (n *Node) startPrometheus() error {
	addr := n.PrometheusAddr
	if addr == "" {
		addr = ":2112"
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	server := &http.Server{Addr: addr, Handler: mux}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	n.metricsServer = server

	go func() {
		if err := server.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Errorf("cluster: prometheus metrics server stopped: %v", err)
		}
	}()
	return nil
}

// serializeHTTPJSON marshals an outbound payload with the HTTP JSON serializer,
// passing a raw []byte through unchanged (matching message.Serialize semantics)
// so an already-encoded payload is not double-encoded (M35).
func serializeHTTPJSON(v interface{}) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	return httpJSONSerializer.Marshal(v)
}
