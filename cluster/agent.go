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
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

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

const (
	agentWriteBacklog = 16
)

var (
	// ErrBrokenPipe represents the low-level connection has broken.
	ErrBrokenPipe = errors.New("broken low-level pipe")
	// ErrBufferExceed indicates that the current session buffer is full and
	// can not receive more data.
	ErrBufferExceed = errors.New("session send buffer exceed")
)

type (
	// Agent corresponding a user, used for store raw conn information
	agent struct {
		// regular agent member
		session  *session.Session    // session
		conn     net.Conn            // low-level conn fd
		lastMid  uint64              // last message id
		state    int32               // current agent state
		chDie    chan struct{}       // wait for close
		chSend   chan pendingMessage // push message queue
		lastAt   int64               // last heartbeat unix time stamp
		decoder  *codec.Decoder      // binary decoder
		pipeline pipeline.Pipeline

		rpcHandler rpcHandler
		srv        reflect.Value // cached session reflect.Value
	}

	pendingMessage struct {
		typ     message.Type // message type
		route   string       // message route(push)
		mid     uint64       // response message id(response)
		payload interface{}  // payload
	}
)

// Create new agent instance
func newAgent(conn net.Conn, pipeline pipeline.Pipeline, rpcHandler rpcHandler) *agent {
	a := &agent{
		conn:       conn,
		state:      statusStart,
		chDie:      make(chan struct{}),
		lastAt:     time.Now().Unix(),
		chSend:     make(chan pendingMessage, agentWriteBacklog),
		decoder:    codec.NewDecoder(),
		pipeline:   pipeline,
		rpcHandler: rpcHandler,
	}

	// binding session
	s := session.New(a)
	a.session = s
	a.srv = reflect.ValueOf(s)

	return a
}

func (a *agent) send(m pendingMessage) (err error) {
	defer func() {
		if e := recover(); e != nil {
			log.Error(fmt.Sprintf("[SessionClosed] Agent send message error: %v", e))
			err = ErrBrokenPipe
		}
	}()
	a.chSend <- m
	return
}

// LastMid implements the session.NetworkEntity interface
func (a *agent) LastMid() uint64 {
	return a.lastMid
}

// Push, implementation for session.NetworkEntity interface
func (a *agent) Push(route string, v interface{}) error {
	if a.status() == statusClosed {
		log.Error("[SessionClosed] Push message to closed session")
		return ErrBrokenPipe
	}

	if len(a.chSend) >= agentWriteBacklog {
		return ErrBufferExceed
	}

	if env.Debug {
		switch d := v.(type) {
		case []byte:
			log.Println(fmt.Sprintf("Type=Push, ID=%d, UID=%d, Route=%s, Data=%dbytes",
				a.session.ID(), a.session.UID(), route, len(d)))
		default:
			log.Println(fmt.Sprintf("Type=Push, ID=%d, UID=%d, Route=%s, Data=%+v",
				a.session.ID(), a.session.UID(), route, v))
		}
	}

	return a.send(pendingMessage{typ: message.Push, route: route, payload: v})
}

// RPC, implementation for session.NetworkEntity interface
func (a *agent) RPC(route string, v interface{}) error {
	if a.status() == statusClosed {
		log.Error("[SessionClosed] RPC message to closed session")
		return ErrBrokenPipe
	}

	// TODO: buffer
	data, err := message.Serialize(v)
	if err != nil {
		return err
	}
	msg := &message.Message{
		Type:  message.Notify,
		Route: route,
		Data:  data,
	}
	a.rpcHandler(a.session, msg, true)
	return nil
}

// Response, implementation for session.NetworkEntity interface
// Response message to session
func (a *agent) Response(v interface{}) error {
	return a.ResponseMid(a.lastMid, v)
}

// ResponseMid, implementation for session.NetworkEntity interface
// Response message to session
func (a *agent) ResponseMid(mid uint64, v interface{}) error {
	if a.status() == statusClosed {
		log.Error("[SessionClosed] Response message to closed session")
		return ErrBrokenPipe
	}

	if mid <= 0 {
		return ErrSessionOnNotify
	}

	if len(a.chSend) >= agentWriteBacklog {
		return ErrBufferExceed
	}

	if env.Debug {
		switch d := v.(type) {
		case []byte:
			log.Debug(fmt.Sprintf("Type=Response, ID=%d, UID=%d, MID=%d, Data=%dbytes",
				a.session.ID(), a.session.UID(), mid, len(d)))
		default:
			log.Debug(fmt.Sprintf("Type=Response, ID=%d, UID=%d, MID=%d, Data=%+v",
				a.session.ID(), a.session.UID(), mid, v))
		}
	}

	return a.send(pendingMessage{typ: message.Response, mid: mid, payload: v})
}

// Close, implementation for session.NetworkEntity interface
// Close closes the agent, clean inner state and close low-level connection.
// Any blocked Read or Write operations will be unblocked and return errors.
func (a *agent) Close() error {
	if a.status() == statusClosed {
		return ErrCloseClosedSession
	}
	a.setStatus(statusClosed)

	if env.Debug {
		log.Println(fmt.Sprintf("Session closed, ID=%d, UID=%d, IP=%s",
			a.session.ID(), a.session.UID(), a.conn.RemoteAddr()))
	}

	// prevent closing closed channel
	select {
	case <-a.chDie:
		// expect
	default:
		close(a.chDie)
		scheduler.PushTask(func() { session.Lifetime.Close(a.session) })
	}
	metrics.AgentClose.Inc()
	return a.conn.Close()
}

// RemoteAddr implementation for session.NetworkEntity interface
// returns the remote network address.
func (a *agent) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}

// RemoteAddrStr return remote address string
func (a *agent) RemoteAddrStr() string {
	return a.conn.RemoteAddr().String()
}

// RemoteAddrWithoutPortStr return remote address without port string
func (a *agent) RemoteAddrWithoutPortStr() string {

	address := a.conn.RemoteAddr().String()
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		host = address
	}

	host = strings.Trim(host, "[]")

	ip := net.ParseIP(host)
	if ip == nil {
		return ""
	}

	return ip.String()
}

// String, implementation for Stringer interface
func (a *agent) String() string {
	return fmt.Sprintf("Remote=%s, LastTime=%d", a.conn.RemoteAddr().String(), atomic.LoadInt64(&a.lastAt))
}

func (a *agent) status() int32 {
	return atomic.LoadInt32(&a.state)
}

func (a *agent) setStatus(state int32) {
	atomic.StoreInt32(&a.state, state)
}

func (a *agent) write() {
	ticker := time.NewTicker(env.Heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			deadline := time.Now().Add(-2 * env.Heartbeat).Unix()
			if atomic.LoadInt64(&a.lastAt) < deadline {
				log.Println("Session heartbeat timeout")
				return
			}
			if err := a.writeMessage(hbd); err != nil {
				return
			}

		case data := <-a.chSend:
			if err := a.handleSendData(data); err != nil {
				return
			}

		case <-a.chDie:
			return

		case <-env.Die:
			return
		}
	}
}

func (a *agent) writeMessage(msg []byte) error {
	_, err := a.conn.Write(msg)
	return err
}

func (a *agent) handleSendData(data pendingMessage) error {
	payload, err := message.Serialize(data.payload)
	if err != nil {
		switch data.typ {
		case message.Push:
			log.Println(fmt.Sprintf("Push: %s error: %s", data.route, err.Error()))
		case message.Response:
			log.Println(fmt.Sprintf("Response message(id: %d) error: %s", data.mid, err.Error()))
		default:
			// expect
		}
		return err
	}

	if data.route == "" {
		log.Println("empty route")
	}

	// construct message and encode
	m := &message.Message{
		Type:  data.typ,
		Data:  payload,
		Route: data.route,
		ID:    data.mid,
	}

	//log.Debug(fmt.Sprintf("Send message, Type=%d, ID=%d, Route=%s, Data=%+v",
	//	m.Type, m.ID, m.Route, m.Data))

	if pipe := a.pipeline; pipe != nil {
		err := pipe.Outbound().Process(a.session, m)
		if err != nil {
			log.Println("broken pipeline", err.Error())
			return err
		}
	}

	em, err := m.Encode()
	if err != nil {
		log.Println(err.Error())
		return err
	}

	// packet encode
	p, err := codec.Encode(packet.Data, em)
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("push encoded t chWrite")
	return a.writeMessage(p)
}

// OriginalSid get original session id
func (a *agent) OriginalSid() int64 {
	return 0
}
