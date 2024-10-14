package cluster

import (
	"encoding/json"
	"net"

	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/session"
	"github.com/valyala/fasthttp"
)

type httpAgent struct {
	sseChan    chan []byte
	rpcHandler rpcHandler
	session    *session.Session
	httpCtx    *fasthttp.RequestCtx
}

func NewHTTPAgent(
	sseChan chan []byte,
	rpcHandler rpcHandler,
	httpCtx *fasthttp.RequestCtx,
) *httpAgent {

	a := &httpAgent{
		sseChan:    sseChan,
		rpcHandler: rpcHandler,
		httpCtx:    httpCtx,
	}

	s := session.New(a)
	a.session = s

	return a

}

// Close implements session.NetworkEntity.
func (h *httpAgent) Close() error {
	return nil
}

// LastMid implements session.NetworkEntity.
func (h *httpAgent) LastMid() uint64 {
	return 0
}

// OriginalSid implements session.NetworkEntity.
func (h *httpAgent) OriginalSid() int64 {
	return 0
}

// Push implements session.NetworkEntity.
func (h *httpAgent) Push(route string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	log.Infof("[HTTP Agent] Push event: %s", data)

	// Use a select statement with a default case to avoid blocking
	select {
	case h.sseChan <- data:
		// Data sent successfully
		log.Infof("[HTTP Agent] SSE event sent: %s", data)
	default:
		// Channel is full, log a warning
		log.Infof("[HTTP Agent] SSE channel is full, dropping event: %s", data)
	}
	return nil
}

// RPC implements session.NetworkEntity.
func (h *httpAgent) RPC(route string, v interface{}) error {
	data, err := message.Serialize(v)
	if err != nil {
		return err
	}
	msg := &message.Message{
		Type:  message.Notify,
		Route: route,
		Data:  data,
	}
	log.Infof("[HTTP Agent] RPC event: %s", data)
	h.rpcHandler(h.session, msg, true)
	return nil
}

// RemoteAddr implements session.NetworkEntity.
func (h *httpAgent) RemoteAddr() net.Addr {
	return h.httpCtx.RemoteAddr()
}

// Response implements session.NetworkEntity.
func (h *httpAgent) Response(v interface{}) error {
	data, err := message.Serialize(v)
	if err != nil {
		return err
	}
	h.httpCtx.SetBody(data)
	return nil
}

// ResponseMid implements session.NetworkEntity.
func (h *httpAgent) ResponseMid(mid uint64, v interface{}) error {
	data, err := message.Serialize(v)
	if err != nil {
		return err
	}
	h.httpCtx.SetBody(data)
	return nil
}
