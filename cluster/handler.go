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
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
)

var (
	// cached serialized data
	hrd []byte // handshake response data
	hbd []byte // heartbeat packet data
)

type rpcHandler func(session *session.Session, msg *message.Message, noCopy bool)

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
	dat, err := codec.Encode(packet.Data, data)
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
		log.Println("Register remote service", s)
		h.remoteServices[s] = append(h.remoteServices[s], member)
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

	h.currentNode.storeSession(agent.session)

	remoteAddr := agent.RemoteAddrWithoutPortStr()
	if remoteAddr == "" {
		log.Error("handle: RemoteAddrWithoutPortStr returned empty string")
		return
	}

	err := h.currentNode.increaseConnection(remoteAddr)
	if err != nil {
		log.Errorf("handle: increaseConnection error: %v", err)
		return
	}
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
		log.Println("Closing session")
		request := &clusterpb.SessionClosedRequest{
			SessionId: agent.session.ID(),
		}
		h.currentNode.decreaseConnection(agent.RemoteAddrWithoutPortStr())

		members := h.currentNode.cluster.remoteAddrs()
		for _, remote := range members {
			log.Debug("Notify remote server", remote)
			pool, err := h.currentNode.rpcClient.getConnPool(remote)
			if err != nil {
				log.Println("Cannot retrieve connection pool for address", remote, err)
				continue
			}
			client := clusterpb.NewMemberClient(pool.Get())
			_, err = client.SessionClosed(context.Background(), request)
			if err != nil {
				log.Error("Cannot closed session in remote address", remote, err)
				continue
			}
			if env.Debug {
				log.Debug("Notify remote server success", remote)
			}
		}

		agent.Close()
		if env.Debug {
			log.Println(
				fmt.Sprintf("Session read goroutine exit, SessionID=%d, UID=%d", agent.session.ID(), agent.session.UID()),
			)
		}

		// Observe the connection duration
		duration := time.Since(startTime).Seconds()
		metrics.ConnectionDuration.Observe(duration)
		metrics.CurrentConnections.Dec()
	}()

	// read loop
	buf := make([]byte, 2048)
	for {
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

func (h *LocalHandler) processPacket(agent *agent, p *packet.Packet) error {
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

	agent.lastAt = time.Now().Unix()
	return nil
}

func (h *LocalHandler) findMembers(service string) []*clusterpb.MemberInfo {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.remoteServices[service]
}

func (h *LocalHandler) remoteProcess(
	session *session.Session,
	msg *message.Message,
	noCopy bool,
) {
	log.Debugf("[RemoteProcess] request process remoteProcess ssid: %v msg: %v msgId: %d", session.ID(), msg, session.ID())
	index := strings.LastIndex(msg.Route, ".")
	if index < 0 {
		log.Println(fmt.Sprintf("nano/handler: invalid route %s", msg.Route))
		return
	}

	service := msg.Route[:index]
	members := h.findMembers(service)
	if len(members) == 0 {
		log.Println(fmt.Sprintf("nano/handler: %s not found(forgot registered?)", msg.Route))
		return
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
				return
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
		return
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
	sessionUserData, _ := json.Marshal(session.State())

	client := clusterpb.NewMemberClient(pool.Get())

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
		_, err = client.HandleRequest(context.Background(), request)
		log.Debugf("[RemoteProcess] request process msg %v completed", msg)
		if err != nil {
			log.Errorf("[RemoteProcess] request process remoteProcess ssid: %v msg: %v msgId: %d Error: %v", session.ID(), msg, session.ID(), err)
		}

	case message.Notify:
		request := &clusterpb.NotifyMessage{
			GateAddr:       gateAddr,
			SessionId:      sessionId,
			ClientUid:      session.ClientUid(),
			ClientUserData: sessionUserData,
			Route:          msg.Route,
			Data:           data,
		}
		_, err = client.HandleNotify(context.Background(), request)
		if err != nil {
			log.Errorf("[RemoteProcess] request process (notify) remoteProcess ssid: %v msg: %v msgId: %d Error: %v", session.ID(), msg, session.ID(), err)
		}
	}

	// Record the duration after the RPC call
	duration := time.Since(startTime).Seconds()
	metrics.RouteRequestDuration.WithLabelValues(msg.Route, msg.Type.String(), "remote").Observe(duration)

	if err != nil {
		log.Error(fmt.Sprintf("Process remote message (%d:%s) error: %+v", msg.ID, msg.Route, err))
	}
	log.Debugf("[RemoteProcess] request process remoteProcess ssid: %v msg: %v msgId: %d COMPLETED", session.ID(), msg, session.ID())
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
		h.remoteProcess(agent.session, msg, false)
	} else {
		h.localProcess(handler, lastMid, agent.session, msg, nil)
	}
}

func (h *LocalHandler) handleWS(conn *websocket.Conn) {
	c, err := newWSConn(conn)
	if err != nil {
		log.Println(err)
		return
	}
	h.handle(c)
}

func (h *LocalHandler) localProcess(
	handler *component.Handler,
	lastMid uint64,
	session *session.Session,
	msg *message.Message,
	responseChan chan<- []byte,
) {
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
		data = payload
	} else {
		data = reflect.New(handler.Type.Elem()).Interface()
		err := env.Serializer.Unmarshal(payload, data)
		if err != nil {
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

		// Set the last message ID
		switch v := session.NetworkEntity().(type) {
		case *agent:
			v.lastMid = lastMid
		case *acceptor:
			v.lastMid = lastMid
		case *httpAgent:
			v.lastMid = lastMid
		}

		result := handler.Method.Func.Call(args)
		log.Infof("Local process task completed: %v", msg.Route)
		if len(result) > 0 {
			if err := result[0].Interface(); err != nil {
				log.Println(fmt.Sprintf("Service %s error: %+v", msg.Route, err))
				if responseChan != nil {
					responseChan <- []byte(fmt.Sprintf(`{"error": "%v"}`, err))
				}
				return
			}
		}

		// Record the duration after processing
		duration := time.Since(startTime).Seconds()
		metrics.RouteRequestDuration.WithLabelValues(msg.Route, msg.Type.String(), "local").Observe(duration)

		if responseChan != nil {
			// Assuming the last return value is the response
			if len(result) > 1 {
				response, err := json.Marshal(result[len(result)-1].Interface())
				if err != nil {
					log.Println(fmt.Sprintf("Failed to marshal response: %v", err))
					responseChan <- []byte(`{"error": "Internal server error"}`)
					return
				}
				responseChan <- response
			} else {
				responseChan <- []byte(`{"status": "ok"}`)
			}
		}
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
		log.Infof("Schedule task: %v", task)
		local.Schedule(task)
	} else {
		log.Infof("Push task: %v", task)
		scheduler.PushTask(task)
	}
}
