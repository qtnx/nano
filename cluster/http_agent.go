package cluster

import (
	"encoding/json"
	"net"
	"sync"

	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/session"
	"github.com/valyala/fasthttp"
)

type HttpObserveStatus uint

const (
	HttpObserveSuccess HttpObserveStatus = iota
	HttpObserveError   HttpObserveStatus = 1
	HttpObserveWaiting
)

type fastHttpContextObserve struct {
	context       *fasthttp.RequestCtx
	observeStatus HttpObserveStatus
}

type httpAgent struct {
	sseChan               chan []byte
	rpcHandler            rpcHandler
	session               *session.Session
	httpCtx               *fasthttp.RequestCtx
	messageIDMapToRequest map[uint64]*fastHttpContextObserve
	responseChan          chan []byte
	mu                    sync.Mutex
}

func NewHTTPAgent(
	sid int64,
	s *session.Session,
	sseChan chan []byte,
	rpcHandler rpcHandler,
	httpCtx *fasthttp.RequestCtx,
) *httpAgent {
	a := &httpAgent{
		sseChan:               sseChan,
		rpcHandler:            rpcHandler,
		httpCtx:               httpCtx,
		messageIDMapToRequest: make(map[uint64]*fastHttpContextObserve),
	}

	if s == nil {
		s = session.NewWithID(a, sid)
	}
	a.session = s

	return a
}

func (h *httpAgent) AttackHttpRequestCtx(mid uint64, httpCtx *fasthttp.RequestCtx) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messageIDMapToRequest[mid] = &fastHttpContextObserve{
		context:       httpCtx,
		observeStatus: HttpObserveWaiting,
	}
}

func (h *httpAgent) RemoveHttpRequestCtx(mid uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, exists := h.messageIDMapToRequest[mid]; exists {
		delete(h.messageIDMapToRequest, mid)
	}
}

func (h *httpAgent) GetFastHttpContextObserve(mid uint64) *fastHttpContextObserve {

	h.mu.Lock()
	defer h.mu.Unlock()
	if ctxObserve, exists := h.messageIDMapToRequest[mid]; exists {
		return ctxObserve
	}
	return nil
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
	h.mu.Lock()
	defer h.mu.Unlock()

	log.Infof("[HTTP Agent] ResponseMid: %v", mid)
	data, err := message.Serialize(v)
	if err != nil {
		log.Errorf("[HTTP Agent] Failed to serialize response: %v error: %v", v, err)
		return err
	}

	ctx, exists := h.messageIDMapToRequest[mid]
	if !exists {
		log.Infof("[HTTP Agent] No context found for messageID: %d, response might have timed out", mid)
		return nil
	}

	log.Infof("ss ptr after insert %v", h.session.ID())
	ctx.context.SetContentType("application/json")
	ctx.context.SetBody(data)
	ctx.context.SetStatusCode(fasthttp.StatusOK)

	h.messageIDMapToRequest[mid].observeStatus = HttpObserveSuccess

	//
	//log.Infof("[HTTP Agent] Setting response directly to context for messageID: %d", mid)
	//ctx.SetContentType("application/json")
	//ctx.SetBody(data)
	//delete(h.messageIDMapToRequest, mid)

	return nil
}
