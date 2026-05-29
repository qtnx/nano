package cluster

import (
	"encoding/json"
	"errors"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/session"
	"github.com/valyala/fasthttp"
)

// errNoRequestBound is returned by httpAgent.Response when no in-flight /api
// request is bound to the calling goroutine. A bare session.Response is only
// well-defined inside the synchronous handler call (runWithRequestMid binds the
// mid); an async handler replying from a goroutine it spawned must capture
// session.LastMid() at entry and reply with session.ResponseMID. Falling back
// to the shared lastMid would let a later concurrent /api request cross-wire
// this response (H34).
var errNoRequestBound = errors.New("nano/cluster: no in-flight HTTP request bound to goroutine; capture session.LastMid() and use session.ResponseMID")

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
	reqMids               sync.Map      // goroutine id -> in-flight request mid, for Response routing (H34)
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

// goID returns the calling goroutine's id by parsing the runtime stack header
// ("goroutine N [..."). Go exposes no public goroutine-local storage, so this
// scopes an in-flight request's mid to the goroutine running its handler (see
// runWithRequestMid). The cost is paid once per /api request/response.
func goID() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = b[len("goroutine "):]
	i := 0
	for i < len(b) && b[i] >= '0' && b[i] <= '9' {
		i++
	}
	id, _ := strconv.ParseUint(string(b[:i]), 10, 64)
	return id
}

// runWithRequestMid binds mid to the calling goroutine for the duration of the
// synchronous handler call fn and records it as the last-set mid. A handler's
// session.Response (which carries no explicit mid) then resolves to THIS
// request's mid, so concurrent /api requests sharing one agent cannot
// cross-wire their responses via a shared field (H34).
func (h *httpAgent) runWithRequestMid(mid uint64, fn func()) {
	atomic.StoreUint64(&h.lastMid, mid)
	gid := goID()
	h.reqMids.Store(gid, mid)
	defer h.reqMids.Delete(gid)
	fn()
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
	// Return the mid bound to the calling goroutine while inside a handler so an
	// async handler can capture session.LastMid() at entry and later reply with
	// session.ResponseMID; fall back to the last-set mid otherwise (H34).
	if raw, ok := h.reqMids.Load(goID()); ok {
		return raw.(uint64)
	}
	return atomic.LoadUint64(&h.lastMid)
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
	// Route by the request mid bound to this goroutine (set by localProcess for
	// the duration of the handler call) so concurrent /api requests sharing one
	// agent cannot cross-wire their responses (H34). A bare Response is only
	// well-defined inside the synchronous handler call; an async handler that
	// replies from a goroutine it spawned must capture session.LastMid() at
	// entry and reply with session.ResponseMID(mid, v). Falling back to the
	// shared lastMid here would let a later concurrent /api request reassign it
	// and cross-wire (or drop) this response, so reject instead (H34).
	if raw, ok := h.reqMids.Load(goID()); ok {
		return h.ResponseMid(raw.(uint64), v)
	}
	return errNoRequestBound
}

// ResponseMid implements session.NetworkEntity.
func (h *httpAgent) ResponseMid(mid uint64, v interface{}) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if env.Debug {
		log.Infof("[HTTP Agent] ResponseMid: %v", mid)
	}
	// HTTP responses are JSON end-to-end, independent of the cluster-wide
	// (possibly binary) serializer: serialize with the HTTP JSON serializer
	// here rather than message.Serialize/env.Serializer (M35). A []byte payload
	// that is ALREADY valid JSON passes through unchanged so a handler returning
	// pre-encoded JSON is not double-encoded (e.g. base64); any other value
	// (including a []byte that is not valid JSON) is run through the HTTP JSON
	// serializer so an application/json response is always valid JSON (M35).
	var data []byte
	if b, ok := v.([]byte); ok && json.Valid(b) {
		data = b
	} else {
		var err error
		data, err = httpJSONSerializer.Marshal(v)
		if err != nil {
			log.Errorf("[HTTP Agent] Failed to serialize response: %v error: %v", v, err)
			return err
		}
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
	// scheduler/RPC goroutine. The body is JSON-encoded via the HTTP JSON
	// serializer, so the content type is always application/json (M35).
	ctx.body = data
	ctx.contentType = "application/json"
	ctx.setStatus(HttpObserveSuccess)

	return nil
}