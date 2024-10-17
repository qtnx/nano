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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
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
	"github.com/lonng/nano/session"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
	"google.golang.org/grpc"
)

var (

	// ErrLimitConnection the connection of the ip is reach the limit
	ErrLimitConnection = errors.New("reach the limit of connection")
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

	once          sync.Once
	keepaliveExit chan struct{}

	// HTTP server
	httpServer *http.Server
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
		go n.listenAndServe()
	}

	// Expose Prometheus metrics endpoint
	go func() {
		if n.OpenPrometheus {
			http.Handle("/metrics", promhttp.Handler())
			err := http.ListenAndServe(":2112", nil)
			if err != nil {
				log.Fatalf("Error starting Prometheus HTTP server: %v", err)
			}
		}
	}()

	return nil
}

// listenAndServe starts the server to handle both TCP and HTTP requests
func (n *Node) listenAndServe() {
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
	}

	if err := server.ListenAndServe(n.ClientAddr); err != nil {
		log.Println(fmt.Sprintf("Error starting fasthttp server: %v", err))
	}
}

// handleFastHTTP is the request handler for fasthttp
func (n *Node) handleFastHTTP(ctx *fasthttp.RequestCtx) {
	log.Println(fmt.Sprintf("Received request on %s", string(ctx.Path())))
	switch string(ctx.Path()) {
	case "/api":
		n.handleHTTPRequest(ctx)
	case "/sse":
		n.handleSSE(ctx)
	case "/health":
		ctx.SetStatusCode(fasthttp.StatusOK)
	case "/" + strings.TrimPrefix(env.WSPath, "/"):
		log.Println("Handling WebSocket request")
		n.listenAndServeWS(ctx)
	default:
		// Use default http.Handler for unmatched routes
		// Adapt net/http.DefaultServeMux to fasthttp.RequestHandler
		fasthttpadaptor.NewFastHTTPHandler(http.DefaultServeMux)(ctx)
	}
}

func convertFastHTTPToHTTP(ctx *fasthttp.RequestCtx) *http.Request {
	header := make(http.Header)
	ctx.Request.Header.VisitAll(func(key, value []byte) {
		header[string(key)] = []string{string(value)}
	})
	return &http.Request{
		Method:     string(ctx.Method()),
		URL:        &url.URL{Scheme: "http", Host: string(ctx.Host()), Path: string(ctx.Path()), RawQuery: string(ctx.QueryArgs().QueryString())},
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     header,
		Host:       string(ctx.Host()),
		RemoteAddr: ctx.RemoteAddr().String(),
	}
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

	var msgType message.Type

	if request.Type == 0 {
		msgType = message.Request
	} else if request.Type == 1 {
		msgType = message.Notify
	} else {
		log.Errorf("[Nano] Invalid message type: %d", request.Type)
		ctx.Error("Invalid message type", fasthttp.StatusBadRequest)
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

	ch := n.sseClients[string(sid)]
	if ch == nil {
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

	log.Infof("[Nano] Received request %v with session ID: %d", request, sidInt)

	existingSession := n.findSession(sidInt)
	var agent *httpAgent
	if existingSession == nil {
		log.Infof("[Nano] session not found for %d, create new", sidInt)
		agent = NewHTTPAgent(sidInt, nil, ch, n.handler.remoteProcess, ctx)
		n.storeSession(agent.session)
	} else {
		agent = existingSession.NetworkEntity().(*httpAgent)
	}

	// validate authen
	if env.MiddlewareHttp != nil {
		if err := env.MiddlewareHttp(agent.session, convertFastHTTPToHTTP(ctx)); err != nil {
			log.Infof("[Nano] Invalidate authenticate middleware")
			ctx.Error("Invalidate authenticate", fasthttp.StatusUnauthorized)
			return
		}
	}

	msg := &message.Message{
		Type:  msgType,
		Route: request.Route,
		Data:  request.Data,
	}

	handler, found := n.handler.localHandlers[request.Route]
	var responseChan chan []byte
	if !found {
		n.handler.remoteProcess(agent.session, msg, false)
		if msgType == message.Notify {
			// if notify, just send a response to client
			ctx.SetStatusCode(fasthttp.StatusOK)
		} else if msgType == message.Request {
			// if request, attach response chan to agent to wait for service handle
			// and send a response to client
			// flow: node -[gRPC.HandleRequest]-> service
			//      service -[gRPC.HandleResponse]-> agent
			//      agent -[responseChan]-> client
			responseChan = make(chan []byte)
			agent.AttachResponseChan(responseChan)
		}
	} else {
		responseChan = make(chan []byte)
		n.handler.localProcess(handler, 0, agent.session, msg, responseChan)
	}

	if responseChan != nil {
		// Wait for response or timeout
		select {
		case response := <-responseChan:
			ctx.SetContentType("application/json")
			log.Infof("ss ptr after insert %v", agent.session)
			ctx.SetBody(response)
		case <-time.After(10 * time.Second):
			log.Infof("[Nano] Request timeout")
			ctx.Error("Request timeout", fasthttp.StatusRequestTimeout)
		}
	}
}

// handleSSE handles Server-Sent Events to push events to clients
func (n *Node) handleSSE(ctx *fasthttp.RequestCtx) {
	ctx.SetContentType("text/event-stream")
	ctx.Response.Header.Set("Cache-Control", "no-cache")
	ctx.Response.Header.Set("Connection", "keep-alive")
	ctx.Response.Header.Set("Transfer-Encoding", "chunked")
	ctx.SetStatusCode(fasthttp.StatusOK)
	log.Infof("SSE connection established")

	// Check if session ID cookie exists
	var sessionID string
	cookie := ctx.Request.Header.Cookie("sse_sessionID")
	if len(cookie) == 0 {
		// Try to get sessionID from "X-SSE-SessionID" header
		sessionIDHeader := ctx.Request.Header.Peek("X-SSE-SessionID")
		sessionID = string(sessionIDHeader)
		// if sessionIDHeader is not exist, get from search params
		if sessionID == "" {
			sessionID = string(ctx.QueryArgs().Peek("sse_sessionID"))
		}
	} else {
		sessionID = string(cookie)
	}

	if len(sessionID) == 0 {
		// Generate a new session ID if cookie doesn't exist
		sessionID = generateSessionID()
		// Set the session ID cookie for the client
		c := fasthttp.AcquireCookie()
		c.SetKey("sse_sessionID")
		c.SetValue(sessionID)
		c.SetPath("/")
		c.SetMaxAge(3600) // 1 hour
		c.SetHTTPOnly(true)
		c.SetSecure(true)
		ctx.Response.Header.SetCookie(c)
		fasthttp.ReleaseCookie(c)
	}

	// Create a channel for sending events
	eventChan := make(chan []byte, 100) // Increased buffer size to 100
	sidInt, err := strconv.ParseInt(sessionID, 10, 64)
	if err != nil {
		log.Errorf("Invalid session ID: %v", err)
		ctx.Error("Invalid session ID", fasthttp.StatusBadRequest)
		return
	}

	httpAgent := NewHTTPAgent(sidInt, nil, eventChan, n.handler.remoteProcess, ctx)
	if env.MiddlewareHttp != nil {
		// validate authen
		if err := env.MiddlewareHttp(httpAgent.session, convertFastHTTPToHTTP(ctx)); err != nil {
			ctx.Error("Invalidate authenticate", fasthttp.StatusUnauthorized)
			return
		}
	}
	n.storeSession(httpAgent.session)

	// Register the client's event channel
	n.registerSSEClient(sessionID, eventChan)

	ctx.SetBodyStreamWriter(fasthttp.StreamWriter(func(w *bufio.Writer) {
		fmt.Fprintf(w, "data: {\"route\": \"onConnected\", \"body\": {\"sse_sessionID\": \"%s\"}}\n\n", sessionID)
		w.Flush()

		// Keep the connection open with a ticker
		ticker := time.NewTicker(1 * time.Second)
		defer func() {
			ticker.Stop()
			n.unregisterSSEClient(sessionID)
			log.Infof("SSE connection closed: %s", sessionID)
		}()

		for {
			select {
			case <-ctx.Done():
				log.Infof("SSE connection closed by context done: %s", sessionID)
				return
			case event := <-eventChan:
				log.Infof("Send SSE event: %s", event)
				fmt.Fprintf(w, "data: %s\n\n", event)
				if err := w.Flush(); err != nil {
					log.Errorf("Error flushing SSE event: %v", err)
					return
				}
			case <-ticker.C:
				// Send a keep-alive comment to prevent connection timeout
				fmt.Fprintf(w, ": keep-alive\n\n")
				if err := w.Flush(); err != nil {
					log.Errorf("Error flushing keep-alive: %v", err)
					return
				}
			}
		}
	}))

}

func (n *Node) registerSSEClient(sessionID string, eventChan chan []byte) {
	log.Infof("Register SSE client: %s", sessionID)
	n.mu.Lock()
	defer n.mu.Unlock()
	n.sseClients[sessionID] = eventChan
}

func (n *Node) unregisterSSEClient(sessionID string) {
	log.Infof("Unregister SSE client: %s", sessionID)
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.sseClients, sessionID)
}

func generateSessionID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
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
			log.Println(fmt.Sprintf("CheckOrigin ok: %v", ok))
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

func (n *Node) listenAndServeWSTLS() {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin:     env.CheckOrigin,
	}

	http.HandleFunc("/"+strings.TrimPrefix(env.WSPath, "/"), func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println(fmt.Sprintf("Upgrade failure, URI=%s, Error=%s", r.RequestURI, err.Error()))
			return
		}

		n.handler.handleWS(conn)
	})

	if err := http.ListenAndServeTLS(n.ClientAddr, n.TSLCertificate, n.TSLKey, nil); err != nil {
		log.Fatal(err.Error())
	}
}

func (n *Node) Handler() *LocalHandler {
	return n.handler
}

func (n *Node) initNode() error {
	// Current node is not master server and does not contains master
	// address, so running in singleton mode
	if !n.IsMaster && n.AdvertiseAddr == "" {
		return nil
	}

	port := strings.Split(n.ServiceAddr, ":")[1]
	if port == "" {
		return errors.New("invalid service address")
	}

	var listenAddr string
	if !n.ForceHostname {
		// This is a hack for docker container and kubernetes
		listenAddr = fmt.Sprintf("0.0.0.0:%s", port)
	} else {
		listenAddr = n.ServiceAddr
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}

	// Initialize the gRPC server and register service
	n.server = grpc.NewServer()
	n.rpcClient = newRPCClient()
	clusterpb.RegisterMemberServer(n.server, n)

	go func() {
		err := n.server.Serve(listener)
		if err != nil {
			log.Fatalf("Start current node failed: %v", err)
		}
	}()

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
		n.cluster.members = append(n.cluster.members, member)
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
			resp, err := client.Register(context.Background(), request)
			if err == nil {
				n.handler.initRemoteService(resp.Members)
				n.cluster.initMembers(resp.Members)
				break
			}
			log.Println("Register current node to cluster failed", err, "and will retry in", n.RetryInterval.String())
			time.Sleep(n.RetryInterval)
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
		_, err = client.Unregister(context.Background(), request)
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
}

func (n *Node) storeSession(s *session.Session) {
	n.mu.Lock()
	n.sessions[s.ID()] = s
	log.Infof("session %d stored", s.ID())
	n.mu.Unlock()
}

func (n *Node) findSession(sid int64) *session.Session {
	n.mu.RLock()
	s := n.sessions[sid]
	n.mu.RUnlock()
	return s
}

func (n *Node) findOrCreateSession(sid, clientUid int64, gateAddr string, clientUserData []byte) (*session.Session, error) {
	n.mu.RLock()
	s, found := n.sessions[sid]
	n.mu.RUnlock()
	if !found {
		if env.Debug {
			log.Println("Not found session ")
		}
		conns, err := n.rpcClient.getConnPool(gateAddr)
		if err != nil {
			return nil, err
		}
		ac := &acceptor{
			sid:        sid,
			gateClient: clusterpb.NewMemberClient(conns.Get()),
			rpcHandler: n.handler.remoteProcess,
			gateAddr:   gateAddr,
		}
		s = session.New(ac)
		s.SetClientUid(clientUid)

		var userData map[string]interface{}
		if err := json.Unmarshal(clientUserData, &userData); err != nil {
			return nil, err
		}
		s.Restore(userData)

		ac.session = s
		n.mu.Lock()
		n.sessions[sid] = s
		n.mu.Unlock()
	} else {
		if env.Debug {
			log.Println("Found session ")
		}
		s.SetClientUid(clientUid)

		var userData map[string]interface{}
		if err := json.Unmarshal(clientUserData, &userData); err != nil {
			return nil, err
		}
		s.Restore(userData)
	}
	return s, nil
}

// / increaseConnection prevent too many connections from the same ip
// / maybe cheat or ddos
func (n *Node) increaseConnection(ipAddress string) error {
	metrics.ConnectionsPerIP.WithLabelValues(ipAddress).Inc()
	if n.LimitConnectPerIp > 0 {
		n.mu.Lock()
		defer n.mu.Unlock()
		if _, ok := n.connectionCount[ipAddress]; !ok {
			n.connectionCount[ipAddress] = 0
		}
		n.connectionCount[ipAddress]++
		log.Println(fmt.Sprintf("IncreaseConnection of ip %s the connect remain  %v", ipAddress, n.connectionCount[ipAddress]))

		if n.connectionCount[ipAddress] > n.LimitConnectPerIp {
			log.Warn(fmt.Sprintf("The connection of ip %s is reach the limit %v", ipAddress, n.LimitConnectPerIp))
			n.connectionCount[ipAddress]--
			return ErrLimitConnection
		}
	}

	return nil
}

func (n *Node) decreaseConnection(ipAddress string) {
	metrics.ConnectionsPerIP.WithLabelValues(ipAddress).Dec()

	if n.LimitConnectPerIp > 0 {
		if _, ok := n.connectionCount[ipAddress]; ok {
			if n.connectionCount[ipAddress] > 0 {
				n.mu.Lock()
				defer n.mu.Unlock()
				n.connectionCount[ipAddress]--
				log.Println(fmt.Sprintf("DecreaseConnection of ip %s the connect remain  %v", ipAddress, n.connectionCount[ipAddress]))

			}
		}
	}
}

func (n *Node) Ping(_ context.Context, _ *clusterpb.PingRequest) (*clusterpb.PingResponse, error) {
	return &clusterpb.PingResponse{
		Msg: "pong",
	}, nil
}

func (n *Node) HandleRequest(_ context.Context, req *clusterpb.RequestMessage) (*clusterpb.MemberHandleResponse, error) {
	log.Debug("[Node] Start handle HandleRequest", req.String())
	handler, found := n.handler.localHandlers[req.Route]

	if !found {
		return nil, fmt.Errorf("service not found in current node: %v", req.Route)
	}
	s, err := n.findOrCreateSession(req.SessionId, req.ClientUid, req.GateAddr, req.ClientUserData)
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
	n.handler.localProcess(handler, req.Id, s, msg, nil)
	log.Debug("[Node] End handle HandleRequest", req.String())
	return &clusterpb.MemberHandleResponse{}, nil
}

func (n *Node) HandleNotify(_ context.Context, req *clusterpb.NotifyMessage) (*clusterpb.MemberHandleResponse, error) {
	log.Debug("[Node] Start handle HandleNotify", req.String())
	handler, found := n.handler.localHandlers[req.Route]
	if !found {
		return nil, fmt.Errorf("service not found in current node: %v", req.Route)
	}
	s, err := n.findOrCreateSession(req.SessionId, req.ClientUid, req.GateAddr, req.ClientUserData)
	if err != nil {
		return nil, err
	}
	log.Println("[Node] HandleRequest old: ", req.Route, req.SessionId, fmt.Sprintf("New session id: %v", s.ID()))
	msg := &message.Message{
		Type:  message.Notify,
		Route: req.Route,
		Data:  req.Data,
	}
	n.handler.localProcess(handler, 0, s, msg, nil)
	return &clusterpb.MemberHandleResponse{}, nil
}

func (n *Node) HandlePush(_ context.Context, req *clusterpb.PushMessage) (*clusterpb.MemberHandleResponse, error) {
	s := n.findSession(req.SessionId)
	if s == nil {
		return &clusterpb.MemberHandleResponse{}, fmt.Errorf("session not found: %v", req.SessionId)
	}
	return &clusterpb.MemberHandleResponse{}, s.Push(req.Route, req.Data)
}

func (n *Node) HandleResponse(_ context.Context, req *clusterpb.ResponseMessage) (*clusterpb.MemberHandleResponse, error) {
	s := n.findSession(req.SessionId)
	if s == nil {
		return &clusterpb.MemberHandleResponse{}, fmt.Errorf("session not found: %v", req.SessionId)
	}
	return &clusterpb.MemberHandleResponse{}, s.ResponseMID(req.Id, req.Data)
}

func (n *Node) NewMember(_ context.Context, req *clusterpb.NewMemberRequest) (*clusterpb.NewMemberResponse, error) {
	n.handler.addRemoteService(req.MemberInfo)
	n.cluster.addMember(req.MemberInfo)
	return &clusterpb.NewMemberResponse{}, nil
}

func (n *Node) DelMember(_ context.Context, req *clusterpb.DelMemberRequest) (*clusterpb.DelMemberResponse, error) {
	log.Println("DelMember member", req.String())
	n.handler.delMember(req.ServiceAddr)
	n.cluster.delMember(req.ServiceAddr)
	return &clusterpb.DelMemberResponse{}, nil
}

// SessionClosed implements the MemberServer interface
func (n *Node) SessionClosed(_ context.Context, req *clusterpb.SessionClosedRequest) (*clusterpb.SessionClosedResponse, error) {
	n.mu.Lock()
	s, found := n.sessions[req.SessionId]
	delete(n.sessions, req.SessionId)
	n.mu.Unlock()
	if found {
		scheduler.PushTask(func() { session.Lifetime.Close(s) })
	}
	return &clusterpb.SessionClosedResponse{}, nil
}

// CloseSession implements the MemberServer interface
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
		if _, err := masterCli.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{
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
