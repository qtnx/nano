package cluster

import (
	"encoding/json"
	"net"
	"sync"
	"sync/atomic"

	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	nanojson "github.com/lonng/nano/serialize/json"
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
	// body and contentType hold the serialized response produced by a
	// scheduler/RPC goroutine. The owning /api request goroutine (the only
	// goroutine allowed to touch the fasthttp RequestCtx) reads them after it
	// observes done closed and writes the ctx itself, so the ctx is never
	// mutated from a non-owner goroutine (H16).
	body        []byte
	contentType string
	// done is closed exactly once when observeStatus reaches a terminal value,
	// so the HTTP request handler can wait on a signal instead of busy-polling
	// the status under the agent mutex (M37).
	done chan struct{}
	once sync.Once
}

// setStatus records a terminal observe status and signals any waiter exactly
// once. Callers must hold the owning httpAgent's mutex.
func (o *fastHttpContextObserve) setStatus(status HttpObserveStatus) {
	o.observeStatus = status
	if o.done != nil {
		o.once.Do(func() { close(o.done) })
	}
}

// status returns the recorded observe status. It is safe to call after the
// done channel has been observed closed: setStatus writes the status before
// closing done, and the channel close establishes the happens-before edge.
func (o *fastHttpContextObserve) status() HttpObserveStatus {
	return o.observeStatus
}

type httpAgent struct {
	sseChan               chan []byte
	rpcHandler            rpcHandler
	session               *session.Session
	httpCtx               *fasthttp.RequestCtx
	messageIDMapToRequest map[uint64]*fastHttpContextObserve
	lastMid               uint64
	midCounter            uint64        // monotonic per-agent message-id source (H33)
	closed                bool          // set by Close; rejects new attaches (H17)
	sseDone               chan struct{} // closed by Close to stop the SSE stream goroutine (M31)
	mu                    sync.Mutex
}

// nextMid returns a process-unique-per-agent message id. The previous wall
// clock (time.Now().UnixNano()) source collided when two requests observed the
// same timestamp, clobbering one in-flight request's context (H33).
func (h *httpAgent) nextMid() uint64 {
	return atomic.AddUint64(&h.midCounter, 1)
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
		sseDone:               make(chan struct{}),
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
	// Reject new attaches once closed so a request racing teardown does not
	// repopulate a map Close is tearing down (H17).
	if h.closed || h.messageIDMapToRequest == nil {
		return
	}
	h.messageIDMapToRequest[mid] = &fastHttpContextObserve{
		context:       httpCtx,
		observeStatus: HttpObserveWaiting,
		done:          make(chan struct{}),
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

// Close implements session.NetworkEntity. It is idempotent and synchronized so
// it can race in-flight /api requests and SSE teardown without
// nil-dereferencing shared state (H17). It marks the agent closed, wakes any
// in-flight request waiters, and releases the SSE channel, but it does not nil
// the session/ctx/map that concurrent lock-holding accessors still read.
func (h *httpAgent) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	// Wake any in-flight request waiters so they fail fast instead of blocking
	// until their timeout.
	for _, o := range h.messageIDMapToRequest {
		o.setStatus(HttpObserveError)
	}
	h.sseChan = nil
	// Signal the SSE stream-writer goroutine (if any) to stop so a
	// backend-initiated close (CloseSession) doesn't leak the stream/goroutine
	// (M31). Guarded by the closed flag above, so it closes exactly once.
	if h.sseDone != nil {
		close(h.sseDone)
	}
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
	if env.Debug {
		log.Infof("[HTTP Agent] Push event: %s", data)
	}

	// Snapshot the channel under the lock so a concurrent Close (which nils it)
	// cannot race this read (H17).
	h.mu.Lock()
	ch, closed := h.sseChan, h.closed
	h.mu.Unlock()
	if closed || ch == nil {
		return ErrBrokenPipe
	}

	select {
	case ch <- data:
		if env.Debug {
			log.Infof("[HTTP Agent] SSE event sent: %s", data)
		}
		return nil
	default:
		// Surface an explicit error when the SSE buffer is full instead of
		// silently dropping the event while reporting success, so the
		// application sees the loss (matches agent.Push semantics) (H32).
		log.Warn("[HTTP Agent] SSE channel full, dropping event for route ", route)
		return ErrBufferExceed
	}
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
	if env.Debug {
		log.Infof("[HTTP Agent] RPC event: %s", data)
	}
	return h.rpcHandler(h.session, msg, true)
}

// RemoteAddr implements session.NetworkEntity.
func (h *httpAgent) RemoteAddr() net.Addr {
	return h.httpCtx.RemoteAddr()
}

// Response implements session.NetworkEntity.
func (h *httpAgent) Response(v interface{}) error {
	return h.ResponseMid(h.lastMid, v)
}

// ResponseMid implements session.NetworkEntity.
func (h *httpAgent) ResponseMid(mid uint64, v interface{}) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if env.Debug {
		log.Infof("[HTTP Agent] ResponseMid: %v", mid)
	}
	data, err := message.Serialize(v)
	if err != nil {
		log.Errorf("[HTTP Agent] Failed to serialize response: %v error: %v", v, err)
		return err
	}

	ctx, exists := h.messageIDMapToRequest[mid]
	if !exists {
		if env.Debug {
			log.Infof("[HTTP Agent] No context found for messageID: %d, response might have timed out", mid)
		}
		return nil
	}

	// Publish the serialized body and let the owning /api request goroutine
	// write the fasthttp RequestCtx (H16): fasthttp forbids touching a
	// RequestCtx from a non-owner goroutine, and this method runs on the
	// scheduler/RPC goroutine. The content type reflects the actual serializer
	// rather than always claiming JSON (M35).
	ctx.body = data
	ctx.contentType = httpContentType()
	ctx.setStatus(HttpObserveSuccess)

	return nil
}

// httpContentType reports the MIME type matching the active serializer so HTTP
// responses are labelled by their actual encoding instead of always claiming
// JSON (M35).
func httpContentType() string {
	if _, ok := env.Serializer.(*nanojson.Serializer); ok {
		return "application/json"
	}
	return "application/octet-stream"
}