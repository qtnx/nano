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
	sseChan      chan []byte
	rpcHandler   rpcHandler
	session      *session.Session
	httpCtx      *fasthttp.RequestCtx
	responseChan chan []byte
}

func NewHTTPAgent(
	sid int64,
	s *session.Session,
	sseChan chan []byte,
	rpcHandler rpcHandler,
	httpCtx *fasthttp.RequestCtx,
) *httpAgent {

	a := &httpAgent{
		sseChan:      sseChan,
		rpcHandler:   rpcHandler,
		httpCtx:      httpCtx,
		responseChan: nil,
	}

	if s == nil {
		s = session.NewWithID(a, sid)
	}
	a.session = s

	return a

}

func (h *httpAgent) AttachResponseChan(responseChan chan []byte) {
	h.session.NetworkEntity().(*httpAgent).responseChan = responseChan
	h.responseChan = responseChan
}

func (h *httpAgent) DeAttachResponseChan() {
	h.responseChan = nil
	h.session.NetworkEntity().(*httpAgent).responseChan = nil
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
	log.Infof("[HTTP Agent] Raw Push event to route: %s, data: %v", route, v)
	var body interface{}

	// Check if v is already JSON
	switch data := v.(type) {
	case string:
		if err := json.Unmarshal([]byte(data), &body); err != nil {
			// If it's not valid JSON, use v as is
			body = v
		}
	case []byte:
		if err := json.Unmarshal(data, &body); err != nil {
			// If it's not valid JSON, use v as is
			body = v
		}
	default:
		body = v
	}

	data, err := json.Marshal(
		map[string]interface{}{
			"route": route,
			"body":  body,
		},
	)
	if err != nil {
		log.Errorf("[HTTP Agent] Failed to marshal event: %v error: %v", v, err)
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
	return h.ResponseMid(0, v)
}

// ResponseMid implements session.NetworkEntity.
func (h *httpAgent) ResponseMid(mid uint64, v interface{}) error {
	log.Infof("[HTTP Agent] ResponseMid: %v", v)
	data, err := message.Serialize(v)
	if err != nil {
		log.Errorf("[HTTP Agent] Failed to serialize response: %v error: %v", v, err)
		return err
	}
	if h.responseChan != nil {
		log.Infof("[HTTP Agent] response chan found set to httpCtx: %s", data)
		h.responseChan <- data
	} else {
		log.Infof("[HTTP Agent] response chan not found set to httpCtx: %s", data)
		h.httpCtx.SetBody(data)
	}
	return nil
}
