package cluster

import (
	"context"
	"net"
	"sync"
	"sync/atomic"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/mock"
	"github.com/lonng/nano/session"
)

type acceptor struct {
	sid                     int64
	gateClient              clusterpb.MemberClient
	session                 *session.Session
	lastMid                 uint64
	rpcHandler              rpcHandler
	gateAddr                string
	jsonPayload             bool
	reqMids                 sync.Map
	strictRequestMidBinding atomic.Bool
}

// Push implements the session.NetworkEntity interface
func (a *acceptor) Push(route string, v interface{}) error {
	// TODO: buffer
	data, err := a.serialize(v)
	if err != nil {
		return err
	}
	request := &clusterpb.PushMessage{
		SessionId: a.sid,
		Route:     route,
		Data:      data,
	}
	ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
	defer cancel()
	_, err = a.gateClient.HandlePush(ctx, request)
	return err
}

// RPC implements the session.NetworkEntity interface
func (a *acceptor) RPC(route string, v interface{}) error {
	// TODO: buffer
	data, err := a.serialize(v)
	if err != nil {
		return err
	}
	msg := &message.Message{
		Type:  message.Notify,
		Route: route,
		Data:  data,
	}
	return a.rpcHandler(a.session, msg, true)
}

// RPC with response
func (a *acceptor) RPCWithResponse(route string, v interface{}) ([]byte, error) {

	// data, err := message.Serialize(v)
	// if err != nil {
	// 	return nil, err
	// }
	// msg := &message.Message{
	// 	Type:  message.Request,
	// 	Route: route,
	// 	Data:  data,
	// }
	return nil, nil
}

// LastMid implements the session.NetworkEntity interface
func (a *acceptor) LastMid() uint64 {
	if raw, ok := a.reqMids.Load(goID()); ok {
		return raw.(uint64)
	}
	return atomic.LoadUint64(&a.lastMid)
}

func (a *acceptor) setLastMid(mid uint64) {
	atomic.StoreUint64(&a.lastMid, mid)
}

func (a *acceptor) setStrictRequestMidBinding(enabled bool) {
	a.strictRequestMidBinding.Store(enabled)
}

func (a *acceptor) runWithRequestMid(mid uint64, fn func()) {
	a.setLastMid(mid)
	gid := goID()
	a.reqMids.Store(gid, mid)
	defer a.reqMids.Delete(gid)
	fn()
}

// Response implements the session.NetworkEntity interface
func (a *acceptor) Response(v interface{}) error {
	if raw, ok := a.reqMids.Load(goID()); ok {
		return a.ResponseMid(raw.(uint64), v)
	}
	if a.strictRequestMidBinding.Load() {
		return errNoRequestBound
	}
	return a.ResponseMid(atomic.LoadUint64(&a.lastMid), v)
}

// ResponseMid implements the session.NetworkEntity interface
func (a *acceptor) ResponseMid(mid uint64, v interface{}) error {
	if mid <= 0 {
		return ErrSessionOnNotify
	}

	log.Debugf("[Acceptor] ResponseMid: %d", mid)
	// TODO: buffer
	data, err := a.serialize(v)
	if err != nil {
		return err
	}
	request := &clusterpb.ResponseMessage{
		SessionId: a.sid,
		Id:        mid,
		Data:      data,
	}

	ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
	defer cancel()
	_, err = a.gateClient.HandleResponse(ctx, request)
	if err != nil {
		log.Errorf("[Acceptor] Failed to response message: %v", err)
	}
	return err
}

// OriginalSid get original session id
func (a *acceptor) OriginalSid() int64 {
	return a.sid
}

// Close implements the session.NetworkEntity interface
func (a *acceptor) Close() error {
	// TODO: buffer
	request := &clusterpb.CloseSessionRequest{
		SessionId: a.sid,
	}
	ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
	defer cancel()
	_, err := a.gateClient.CloseSession(ctx, request)
	return err
}

// RemoteAddr implements the session.NetworkEntity interface
func (*acceptor) RemoteAddr() net.Addr {
	return mock.NetAddr{}
}

// serialize encodes an outbound payload for this remote session. HTTP/SSE-backed
// sessions (jsonPayload) use the JSON codec so a binary cluster-wide serializer
// does not produce a body the HTTP client cannot read (M35); a raw []byte is
// passed through unchanged in both paths.
func (a *acceptor) serialize(v interface{}) ([]byte, error) {
	if a.jsonPayload {
		return serializeHTTPJSON(v)
	}
	return message.Serialize(v)
}
