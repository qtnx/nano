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
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fasthttp/websocket"
	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/codec"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/internal/packet"
	"github.com/lonng/nano/metrics"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/serialize"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
)

var (
	// cached serialized data
	hrd []byte // handshake response data
	hbd []byte // heartbeat packet data

	// emptyStateJSON is the canonical JSON encoding of an empty session state.
	// Reusing it avoids a json.Marshal call + allocation on every remote
	// request whose session carries no state, while keeping the wire contract
	// identical to json.Marshal(map{}) (rank-3 perf).
	emptyStateJSON = []byte("{}")
)

// rpcHandler dispatches a message to a remote member. It returns a non-nil
// error when the message could not be routed/delivered so HTTP and RPC callers
// can surface the failure instead of silently succeeding (M7).
type rpcHandler func(session *session.Session, msg *message.Message, noCopy bool) error

// remoteRPCTimeout bounds a single forwarded request/notify so a slow or
// partitioned backend cannot pin the calling (client read / scheduler)
// goroutine indefinitely (H5).
const remoteRPCTimeout = 10 * time.Second

// CustomerRemoteServiceRoute customer remote service route
type CustomerRemoteServiceRoute func(service string, session *session.Session, members []*clusterpb.MemberInfo) *clusterpb.MemberInfo

func cache() {
	hrdata := map[string]interface{}{
		"code": 200,
		"sys": map[string]interface{}{
			"heartbeat":  env.Heartbeat.Seconds(),
			"servertime": time.Now().UTC().Unix(),
		},
	}
	if dict, ok := message.GetDictionary(); ok {
		hrdata = map[string]interface{}{
			"code": 200,
			"sys": map[string]interface{}{
				"heartbeat":  env.Heartbeat.Seconds(),
				"servertime": time.Now().UTC().Unix(),
				"dict":       dict,
			},
		}
	}
	// data, err := json.Marshal(map[string]interface{}{
	// 	"code": 200,
	// 	"sys": map[string]float64{
	// 		"heartbeat": env.Heartbeat.Seconds(),
	// 	},
	// })
	data, err := json.Marshal(hrdata)
	if err != nil {
		panic(err)
	}

	hrd, err = codec.Encode(packet.Handshake, data)
	if err != nil {
		panic(err)
	}

	hbd, err = codec.Encode(packet.Heartbeat, nil)
	if err != nil {
		panic(err)
	}
}

func encodeSessionId(sessionId int64) []byte {
	msg := map[string]interface{}{
		"sid": sessionId,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		log.Error("Encode session id error", err)
		return nil
	}
	// Wrap the payload in a valid nano message frame (Push) before the packet
	// frame. Clients decode a Data packet body via message.Decode, which
	// rejects a bare JSON blob (its first byte would be parsed as a message
	// flag and fail type validation); emitting a proper Push lets clients
	// parse the session id (M16).
	em, err := message.Encode(&message.Message{
		Type:  message.Push,
		Route: "onSessionId",
		Data:  data,
	})
	if err != nil {
		log.Error("Encode session id error", err)
		return nil
	}
	dat, err := codec.Encode(packet.Data, em)
	if err != nil {
		log.Error("Encode session id error", err)
		return nil
	}
	return dat
}

type LocalHandler struct {
	localServices map[string]*component.Service // all registered service
	localHandlers map[string]*component.Handler // all handler method

	mu             sync.RWMutex
	remoteServices map[string][]*clusterpb.MemberInfo

	pipeline    pipeline.Pipeline
	currentNode *Node
}

func NewHandler(currentNode *Node, pipeline pipeline.Pipeline) *LocalHandler {
	h := &LocalHandler{
		localServices:  make(map[string]*component.Service),
		localHandlers:  make(map[string]*component.Handler),
		remoteServices: map[string][]*clusterpb.MemberInfo{},
		pipeline:       pipeline,
		currentNode:    currentNode,
	}

	return h
}

func (h *LocalHandler) register(comp component.Component, opts []component.Option) error {
	s := component.NewService(comp, opts)

	if _, ok := h.localServices[s.Name]; ok {
		return fmt.Errorf("handler: service already defined: %s", s.Name)
	}

	if err := s.ExtractHandler(); err != nil {
		return err
	}

	// register all localHandlers
	h.localServices[s.Name] = s
	for name, handler := range s.Handlers {
		n := fmt.Sprintf("%s.%s", s.Name, name)
		log.Println("Register local handler", n)
		h.localHandlers[n] = handler
		// Mark the route as known so its Prometheus histogram series is
		// recorded under its real name; unregistered (client-fabricated)
		// routes collapse to a bounded sentinel label (H28).
		metrics.RegisterRoute(n)
	}
	return nil
}

func (h *LocalHandler) initRemoteService(members []*clusterpb.MemberInfo) {
	for _, m := range members {
		h.addRemoteService(m)
	}
}

func (h *LocalHandler) addRemoteService(member *clusterpb.MemberInfo) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, s := range member.Services {
		// Purge any stale entry for this service address before re-adding so a
		// node restart/redeploy cannot accumulate duplicate or dangling routes
		// (M12). Routing therefore stays idempotent across rejoins.
		var filtered []*clusterpb.MemberInfo
		for _, m := range h.remoteServices[s] {
			if m.ServiceAddr != member.ServiceAddr {
				filtered = append(filtered, m)
			}
		}
		log.Println("Register remote service", s)
		h.remoteServices[s] = append(filtered, member)
	}
}

func (h *LocalHandler) delMember(addr string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for name, members := range h.remoteServices {
		for i, maddr := range members {
			if addr == maddr.ServiceAddr {
				if i >= len(members)-1 {
					members = members[:i]
				} else {
					members = append(members[:i], members[i+1:]...)
				}
			}
		}
		if len(members) == 0 {
			delete(h.remoteServices, name)
		} else {
			h.remoteServices[name] = members
		}
	}
}

func (h *LocalHandler) LocalService() []string {
	var result []string
	for service := range h.localServices {
		result = append(result, service)
	}
	sort.Strings(result)
	return result
}

func (h *LocalHandler) RemoteService() []string {
	h.mu.RLock()
	defer h.mu.RUnlock()

	var result []string
	for service := range h.remoteServices {
		result = append(result, service)
	}
	sort.Strings(result)
	return result
}

func (h *LocalHandler) handle(conn net.Conn) {
	// A panic while servicing one connection must never take down the read
	// goroutine (and with it the whole process). Recover here so the deferred
	// cleanup below still runs and the server keeps serving everyone else.
	defer func() {
		if r := recover(); r != nil {
			log.Error(fmt.Sprintf("cluster: recovered panic in client read loop: %v", r))
		}
	}()
	if conn == nil {
		log.Error("handle: conn is nil")
		return
	}

	// create a client agent and startup write gorontine
	agent := newAgent(conn, h.pipeline, h.remoteProcess)
	if agent == nil {
		log.Error("handle: agent is nil after newAgent")
		return
	}

	remoteAddr := agent.RemoteAddrWithoutPortStr()
	if remoteAddr == "" {
		log.Error("handle: RemoteAddrWithoutPortStr returned empty string")
		_ = conn.Close()
		return
	}

	// Enforce the per-IP connection limit BEFORE storing the session or
	// starting goroutines so a rejected connection leaks neither an n.sessions
	// entry nor the upgraded connection (M18).
	if err := h.currentNode.increaseConnection(remoteAddr); err != nil {
		log.Errorf("handle: increaseConnection error: %v", err)
		_ = conn.Close()
		return
	}

	// Enforce the node-global connection cap (independent of the per-IP cap)
	// BEFORE storing the session so a rejected connection leaks neither an
	// n.sessions entry nor goroutines (H11). The matching decrement runs in the
	// cleanup defer below, which is registered only on this accepted path.
	if env.MaxConnections > 0 {
		if atomic.AddInt64(&h.currentNode.acceptedConns, 1) > int64(env.MaxConnections) {
			atomic.AddInt64(&h.currentNode.acceptedConns, -1)
			h.currentNode.decreaseConnection(remoteAddr)
			metrics.ServerClosedConnections.Inc()
			log.Warn(fmt.Sprintf("handle: global connection cap %d reached, rejecting %s", env.MaxConnections, remoteAddr))
			_ = conn.Close()
			return
		}
	}

	h.currentNode.storeSession(agent.session)

	// startup write goroutine
	go agent.write()

	if env.Debug {
		log.Debug(fmt.Sprintf("New session established: %s", agent.String()))
	}

	// Increment the total and current connections metrics
	metrics.TotalConnections.Inc()

	// Record the start time of the connection
	startTime := time.Now()
	metrics.CurrentConnections.Inc()

	// guarantee agent related resource be destroyed
	defer func() {
		log.Debug("Closing session")
		sid := agent.session.ID()
		uid := agent.session.UID()
		h.currentNode.decreaseConnection(remoteAddr)
		if env.MaxConnections > 0 {
			atomic.AddInt64(&h.currentNode.acceptedConns, -1)
		}

		// Reclaim local state first so a slow or partitioned peer cannot pin
		// this read goroutine (and the agent/write goroutines) during the
		// close fan-out (H6). The per-session SessionClosed notifications are
		// then issued asynchronously, each bounded by a timeout (H5).
		h.currentNode.deleteSession(sid)
		agent.Close()
		if env.Debug {
			log.Println(
				fmt.Sprintf("Session read goroutine exit, SessionID=%d, UID=%d", sid, uid),
			)
		}

		// Observe the connection duration
		duration := time.Since(startTime).Seconds()
		metrics.ConnectionDuration.Observe(duration)
		metrics.CurrentConnections.Dec()

		if members := h.currentNode.cluster.remoteAddrs(); len(members) > 0 {
			go h.notifySessionClosed(sid, members)
		}
	}()

	// read loop
	buf := make([]byte, 2048)
	for {
		// Refresh the read deadline so a connection that opens then stalls
		// (slowloris) is closed instead of pinning a goroutine/FD outside the
		// heartbeat path (H11). The window matches the write goroutine's
		// heartbeat-timeout policy (2x the interval).
		if env.Heartbeat > 0 {
			_ = conn.SetReadDeadline(time.Now().Add(2 * env.Heartbeat))
		}
		n, err := conn.Read(buf)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Warn(fmt.Sprintf("Read message error: %s, session will be closed immediately", err.Error()))
			}
			return
		}

		// TODO(warning): decoder use slice for performance, packet data should be copy before next Decode
		packets, err := agent.decoder.Decode(buf[:n])
		if err != nil {
			log.Println(err.Error())

			// process packets decoded
			for _, p := range packets {
				if err := h.processPacket(agent, p); err != nil {
					log.Println(err.Error())
					return
				}
			}
			return
		}

		// process all packets
		for _, p := range packets {
			if err := h.processPacket(agent, p); err != nil {
				log.Println(err.Error())
				return
			}
		}
	}
	//
	//packetChan := make(chan *packet.Packet, 100) // Buffered channel để xử lý gói tin
	//errChan := make(chan error, 1)
	//
	//go func() {
	//	buf := make([]byte, 2048)
	//	for {
	//		n, err := conn.Read(buf)
	//		if err != nil {
	//			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
	//				log.Warn(fmt.Sprintf("Read message error: %s, session will be closed immediately", err.Error()))
	//			}
	//			errChan <- err
	//			return
	//		}
	//
	//		if n > len(buf) {
	//			log.Warn("Read more data than buffer size, truncating")
	//			n = len(buf)
	//		}
	//
	//		dataCopy := make([]byte, n)
	//		copy(dataCopy, buf[:n])
	//
	//		packets, err := agent.decoder.Decode(dataCopy)
	//		if err != nil {
	//			log.Println("Decode error:", err.Error())
	//			continue
	//		}
	//
	//		for _, p := range packets {
	//			packetChan <- p
	//		}
	//	}
	//}()
	//
	//for {
	//	select {
	//	case p := <-packetChan:
	//		if err := h.processPacket(agent, p); err != nil {
	//			log.Println("Process packet error:", err.Error())
	//			return
	//		}
	//	case err := <-errChan:
	//		log.Println("Connection error:", err.Error())
	//		return
	//	}
	//}
}

// notifySessionClosed informs every remote member that the given session has
// disconnected. It runs off the read goroutine (H6) and bounds each RPC with a
// timeout so one slow or partitioned peer cannot stall the fan-out (H5).
func (h *LocalHandler) notifySessionClosed(sid int64, members []string) {
	request := &clusterpb.SessionClosedRequest{SessionId: sid}
	for _, remote := range members {
		pool, err := h.currentNode.rpcClient.getConnPool(remote)
		if err != nil {
			log.Println("Cannot retrieve connection pool for address", remote, err)
			continue
		}
		client := clusterpb.NewMemberClient(pool.Get())
		ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
		_, err = client.SessionClosed(ctx, request)
		cancel()
		if err != nil {
			log.Error("Cannot closed session in remote address", remote, err)
			continue
		}
		if env.Debug {
			log.Debug("Notify remote server success", remote)
		}
	}
}

func (h *LocalHandler) processPacket(agent *agent, p *packet.Packet) (err error) {
	// A single malformed packet must never crash the read goroutine. Recover
	// and surface a normal error so the caller closes only this connection.
	defer func() {
		if r := recover(); r != nil {
			log.Error(fmt.Sprintf("cluster: recovered panic while processing packet (type=%d): %v", p.Type, r))
			err = ErrBrokenPipe
		}
	}()

	switch p.Type {
	case packet.Handshake:

		agent.lastMid = 1
		if err := env.HandshakeValidator(agent.session, p.Data); err != nil {
			return err
		}

		if _, err := agent.conn.Write(hrd); err != nil {
			return err
		}

		agent.setStatus(statusHandshake)
		if env.Debug {
			log.Println(fmt.Sprintf("Session handshake Id=%d, Remote=%s", agent.session.ID(), agent.conn.RemoteAddr()))
		}

	case packet.HandshakeAck:
		// Only a connection that completed a validated Handshake may transition
		// to working. Accepting HandshakeAck from any other state lets a client
		// reach statusWorking (and send Data) without ever passing
		// env.HandshakeValidator (H26).
		if agent.status() != statusHandshake {
			return fmt.Errorf("receive HandshakeAck before a validated handshake, session will be closed immediately, remote=%s",
				agent.conn.RemoteAddr().String())
		}
		agent.setStatus(statusWorking)
		if env.Debug {
			log.Println(fmt.Sprintf("Receive handshake ACK Id=%d, Remote=%s", agent.session.ID(), agent.conn.RemoteAddr()))
		}
		go func() {
			// notify session id
			if _, err := agent.conn.Write(encodeSessionId(agent.session.ID())); err != nil {
				log.Println(err)
			}
		}()

	case packet.Data:
		if agent.status() < statusWorking {
			return fmt.Errorf("receive data on socket which not yet ACK, session will be closed immediately, remote=%s",
				agent.conn.RemoteAddr().String())
		}

		msg, err := message.Decode(p.Data)
		if err != nil {
			return err
		}
		h.processMessage(agent, msg)

	case packet.Heartbeat:
		// expected
	}

	agent.touch()
	return nil
}

func (h *LocalHandler) findMembers(service string) []*clusterpb.MemberInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	src := h.remoteServices[service]
	if len(src) == 0 {
		return nil
	}
	// Return a copy: routing callers iterate the result without the lock while
	// register/unregister mutate the backing array (H4).
	members := make([]*clusterpb.MemberInfo, len(src))
	copy(members, src)
	return members
}

func (h *LocalHandler) remoteProcess(
	session *session.Session,
	msg *message.Message,
	noCopy bool,
) error {
	log.Debugf("[RemoteProcess] request process remoteProcess ssid: %v msg: %v msgId: %d", session.ID(), msg, session.ID())
	index := strings.LastIndex(msg.Route, ".")
	if index < 0 {
		log.Println(fmt.Sprintf("nano/handler: invalid route %s", msg.Route))
		return fmt.Errorf("nano/handler: invalid route %s", msg.Route)
	}

	service := msg.Route[:index]
	members := h.findMembers(service)
	if len(members) == 0 {
		log.Println(fmt.Sprintf("nano/handler: %s not found(forgot registered?)", msg.Route))
		return fmt.Errorf("nano/handler: %s not found(forgot registered?)", msg.Route)
	}

	// Select a remote service address
	// 1. if exist customer remote service route ,use it, otherwise use default strategy
	// 2. Use the service address directly if the router contains binding item
	// 3. Select a remote service address randomly and bind to router
	var remoteAddr string
	if h.currentNode.Options.RemoteServiceRoute != nil {
		if addr, found := session.Router().Find(service); found {
			remoteAddr = addr
		} else {
			member := h.currentNode.Options.RemoteServiceRoute(service, session, members)
			if member == nil {
				log.Println(fmt.Sprintf("customize remoteServiceRoute handler: %s is not found", msg.Route))
				return fmt.Errorf("nano/handler: customize remoteServiceRoute handler: %s is not found", msg.Route)
			}
			remoteAddr = member.ServiceAddr
			session.Router().Bind(service, remoteAddr)
		}
	} else {
		if addr, found := session.Router().Find(service); found {
			remoteAddr = addr
		} else {
			remoteAddr = members[rand.Intn(len(members))].ServiceAddr
			session.Router().Bind(service, remoteAddr)
		}
	}
	pool, err := h.currentNode.rpcClient.getConnPool(remoteAddr)
	if err != nil {
		log.Println(err)
		return err
	}
	data := msg.Data
	if !noCopy && len(msg.Data) > 0 {
		data = make([]byte, len(msg.Data))
		copy(data, msg.Data)
	}

	// Retrieve gate address and session id
	gateAddr := h.currentNode.ServiceAddr
	sessionId := session.ID()
	switch v := session.NetworkEntity().(type) {
	case *acceptor:
		gateAddr = v.gateAddr
		sessionId = v.sid
	}
	// Marshal the session state at most once per request, and skip the
	// json.Marshal allocation entirely when the state is empty (the common
	// stateless case). The wire contract is unchanged: an empty state still
	// sends "{}" so the receiver's applyClientState decodes an empty map, and a
	// marshal failure still yields nil exactly as before (rank-3 perf).
	sessionUserData := emptyStateJSON
	if state := session.State(); len(state) > 0 {
		sessionUserData, _ = json.Marshal(state)
	}

	client := clusterpb.NewMemberClient(pool.Get())

	// Bound the forwarded RPC: context.Background() never times out, so a slow
	// or partitioned backend would otherwise pin the calling (client read /
	// scheduler) goroutine and head-of-line block that client (H5).
	ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
	defer cancel()

	// Start timing before making the RPC call
	startTime := time.Now()

	switch msg.Type {
	case message.Request:
		request := &clusterpb.RequestMessage{
			GateAddr:       gateAddr,
			SessionId:      sessionId,
			ClientUid:      session.ClientUid(),
			ClientUserData: sessionUserData,
			Id:             msg.ID,
			Route:          msg.Route,
			Data:           data,
		}
		log.Debugf("[RemoteProcess] start request remoteProcess ssid: %v msg: %v msgId: %d, request %v", session.ID(), msg, session.ID(), request)
		_, err = client.HandleRequest(ctx, request)
		log.Debugf("[RemoteProcess] request process msg %v completed", msg)

	case message.Notify:
		request := &clusterpb.NotifyMessage{
			GateAddr:       gateAddr,
			SessionId:      sessionId,
			ClientUid:      session.ClientUid(),
			ClientUserData: sessionUserData,
			Route:          msg.Route,
			Data:           data,
		}
		_, err = client.HandleNotify(ctx, request)
	}

	// Record the duration after the RPC call under a bounded route label so a
	// client cannot mint unbounded histogram series by varying the method
	// suffix of a known service (H28).
	duration := time.Since(startTime).Seconds()
	metrics.ObserveRouteRequestDuration(msg.Route, msg.Type.String(), "remote", duration)

	if err != nil {
		log.Error(fmt.Sprintf("Process remote message (%d:%s) error: %+v", msg.ID, msg.Route, err))
		return err
	}
	log.Debugf("[RemoteProcess] request process remoteProcess ssid: %v msg: %v msgId: %d COMPLETED", session.ID(), msg, session.ID())
	return nil
}

func (h *LocalHandler) processMessage(agent *agent, msg *message.Message) {
	var lastMid uint64
	switch msg.Type {
	case message.Request:
		lastMid = msg.ID
	case message.Notify:
		lastMid = 0
	default:
		log.Println("Invalid message type: " + msg.Type.String())
		return
	}

	handler, found := h.localHandlers[msg.Route]
	if !found {
		if err := h.remoteProcess(agent.session, msg, false); err != nil {
			log.Errorf("nano/handler: remote process route %s failed: %v", msg.Route, err)
		}
	} else {
		h.localProcess(handler, lastMid, agent.session, msg, nil)
	}
}

func (h *LocalHandler) handleWS(conn *websocket.Conn) {
	c, err := newWSConn(conn)
	if err != nil {
		log.Println(err)
		// The upgrader handed FD ownership to this handler; close the upgraded
		// connection on the error path so a bad/slow first frame doesn't leak
		// it (H27).
		_ = conn.Close()
		return
	}
	h.handle(c)
}

func (h *LocalHandler) localProcess(
	handler *component.Handler,
	lastMid uint64,
	session *session.Session,
	msg *message.Message,
	serializer serialize.Serializer,
) {
	// HTTP requests carry a JSON body while the cluster-wide env.Serializer may
	// be binary (protobuf). Callers pass an explicit serializer for the inbound
	// payload; nil falls back to the global serializer (M35).
	if serializer == nil {
		serializer = env.Serializer
	}

	if pipe := h.pipeline; pipe != nil {
		err := pipe.Inbound().Process(session, msg)
		if err != nil {
			log.Println("Pipeline process failed: " + err.Error())
			return
		}
	}

	payload := msg.Data
	var data interface{}
	if handler.IsRawArg {
		// Copy the raw payload before the task (dispatched on a scheduler) can
		// capture it: msg.Data aliases the decoder's internal buffer, which is
		// reused/compacted on the next Decode and would corrupt a raw-arg
		// handler reading it asynchronously (H10).
		if len(payload) > 0 {
			cp := make([]byte, len(payload))
			copy(cp, payload)
			payload = cp
		}
		data = payload
	} else {
		data = reflect.New(handler.Type.Elem()).Interface()
		if err := serializer.Unmarshal(payload, data); err != nil {
			log.Println(fmt.Sprintf("Deserialize to %T failed: %+v (%v)", data, err, payload))
			return
		}
	}

	if env.Debug {
		log.Println(fmt.Sprintf("SID %d, UID=%d, Message={%s}, Data=%+v ClientUid=%d", session.ID(), session.UID(), msg.String(), data, session.ClientUid()))
	}

	args := []reflect.Value{handler.Receiver, reflect.ValueOf(session), reflect.ValueOf(data)}

	// Start timing before processing the request
	startTime := time.Now()

	task := func() {
		// Bind the request mid before invoking the handler. For agent/acceptor
		// (one connection == one session, processed sequentially) writing the
		// field is safe; for httpAgent (one session shared by concurrent /api
		// requests) the mid is bound to this goroutine for the synchronous call
		// so a handler's session.Response routes to THIS request rather than a
		// racing one that overwrote a shared field (H34).
		var result []reflect.Value
		switch v := session.NetworkEntity().(type) {
		case *agent:
			v.lastMid = lastMid
			result = handler.Method.Func.Call(args)
		case *acceptor:
			v.lastMid = lastMid
			result = handler.Method.Func.Call(args)
		case *httpAgent:
			v.runWithRequestMid(lastMid, func() {
				result = handler.Method.Func.Call(args)
			})
		default:
			result = handler.Method.Func.Call(args)
		}
		if len(result) > 0 {
			if err := result[0].Interface(); err != nil {
				log.Println(fmt.Sprintf("Service %s error: %+v", msg.Route, err))
				return
			}
		}

		// Record the duration after processing under a bounded route label so a
		// client cannot mint unbounded histogram series (H28).
		duration := time.Since(startTime).Seconds()
		metrics.ObserveRouteRequestDuration(msg.Route, msg.Type.String(), "local", duration)
	}

	index := strings.LastIndex(msg.Route, ".")
	if index < 0 {
		log.Println(fmt.Sprintf("nano/handler: invalid route %s", msg.Route))
		return
	}

	// Dispatch the message to the appropriate scheduler
	service := msg.Route[:index]
	if s, found := h.localServices[service]; found && s.SchedName != "" {
		sched := session.Value(s.SchedName)
		if sched == nil {
			log.Println(fmt.Sprintf("nano/handler: cannot find `scheduler.LocalScheduler` by %s", s.SchedName))
			return
		}

		local, ok := sched.(scheduler.LocalScheduler)
		if !ok {
			log.Println(fmt.Sprintf("nano/handler: Type %T does not implement the `scheduler.LocalScheduler` interface", sched))
			return
		}
		local.Schedule(task)
	} else if scheduler.Sharded() {
		// Default path under sharding: route by session id so distinct sessions
		// run concurrently while a single session stays strictly ordered. On
		// backlog, shed the task (overload) instead of blocking the producer (H1).
		if err := scheduler.PushTaskOnShard(uint64(session.ID()), task); err == scheduler.ErrSchedulerBacklog {
			log.Errorf("nano/handler: scheduler shard backlog full, dropping task route=%s sid=%d", msg.Route, session.ID())
		}
	} else {
		scheduler.PushTask(task)
	}
}
