package cluster

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/pipeline"
	"github.com/lonng/nano/scheduler"
	"github.com/lonng/nano/session"
)

type ConcurrentRouteComp struct {
	component.Base
	fn func(*session.Session, []byte) error
}

func (c *ConcurrentRouteComp) Allow(s *session.Session, data []byte) error {
	return c.fn(s, data)
}

func (c *ConcurrentRouteComp) FIFO(s *session.Session, data []byte) error {
	return c.fn(s, data)
}

type MixedLaneComp struct {
	component.Base
	allow  func(*session.Session, []byte) error
	first  func(*session.Session, []byte) error
	second func(*session.Session, []byte) error
}

func (c *MixedLaneComp) Allow(s *session.Session, data []byte) error {
	return c.allow(s, data)
}

func (c *MixedLaneComp) First(s *session.Session, data []byte) error {
	return c.first(s, data)
}

func (c *MixedLaneComp) Second(s *session.Session, data []byte) error {
	return c.second(s, data)
}

type ResponseRouteComp struct {
	component.Base
	one func(*session.Session, []byte) error
	two func(*session.Session, []byte) error
}

func (c *ResponseRouteComp) One(s *session.Session, data []byte) error {
	return c.one(s, data)
}

func (c *ResponseRouteComp) Two(s *session.Session, data []byte) error {
	return c.two(s, data)
}

type NotifySerialComp struct {
	component.Base
	fn func(*session.Session, []byte) error
}

func (c *NotifySerialComp) Handle(s *session.Session, data []byte) error {
	return c.fn(s, data)
}

func registeredHandler(t *testing.T, h *LocalHandler, route string) *component.Handler {
	t.Helper()
	handler := h.localHandlers[route]
	if handler == nil {
		t.Fatalf("handler %s not registered", route)
	}
	return handler
}

func TestProcessMessageRunsInboundPipelineOnceForLocalHandler(t *testing.T) {
	log.SetLogger(&noopLogger{})
	ensureScheduler()
	scheduler.DisableConcurrentRoutes()

	pipe := pipeline.New()
	var calls atomic.Int32
	pipe.Inbound().PushBack(func(_ *session.Session, _ *message.Message) error {
		calls.Add(1)
		return nil
	})

	done := make(chan struct{})
	comp := &NotifySerialComp{}
	comp.fn = func(_ *session.Session, _ []byte) error {
		close(done)
		return nil
	}
	n := newTestNode()
	n.handler = NewHandler(n, pipe)
	if err := n.handler.register(comp, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	a := newAgent(newCountConn(), pipe, nil)

	n.handler.processMessage(a, &message.Message{Type: message.Notify, Route: "NotifySerialComp.Handle", Data: []byte("x")})
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not run")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("inbound pipeline calls = %d, want 1", got)
	}
}
func TestConcurrentRouteRequestsSameSessionRunConcurrently(t *testing.T) {
	log.SetLogger(&noopLogger{})
	ensureScheduler()
	scheduler.DisableConcurrentRoutes()
	scheduler.EnableConcurrentRequests(2)
	defer scheduler.DisableConcurrentRoutes()

	started1 := make(chan struct{})
	started2 := make(chan struct{})
	release := make(chan struct{})
	comp := &ConcurrentRouteComp{}
	comp.fn = func(_ *session.Session, data []byte) error {
		switch string(data) {
		case "1":
			close(started1)
		case "2":
			close(started2)
		}
		<-release
		return nil
	}

	n := newTestNode()
	n.handler.setConcurrentRoutePolicy([]string{"ConcurrentRouteComp.Allow"}, false)
	if err := n.handler.register(comp, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	handler := registeredHandler(t, n.handler, "ConcurrentRouteComp.Allow")
	s := session.NewWithID(nil, 1001)

	n.handler.localProcess(handler, 1, s, &message.Message{Type: message.Request, ID: 1, Route: "ConcurrentRouteComp.Allow", Data: []byte("1")}, nil)
	n.handler.localProcess(handler, 2, s, &message.Message{Type: message.Request, ID: 2, Route: "ConcurrentRouteComp.Allow", Data: []byte("2")}, nil)

	if !waitFor(func() bool {
		select {
		case <-started1:
		default:
			return false
		}
		select {
		case <-started2:
			return true
		default:
			return false
		}
	}, 3*time.Second) {
		close(release)
		t.Fatal("allowlisted same-session requests did not both start before release")
	}
	close(release)
}

func TestNonAllowlistedRequestsRemainFIFOUnderShardedScheduler(t *testing.T) {
	log.SetLogger(&noopLogger{})
	ensureScheduler()
	scheduler.DisableConcurrentRoutes()
	scheduler.EnableConcurrentRequests(2)
	defer scheduler.DisableConcurrentRoutes()
	scheduler.EnableSharded(4)
	defer scheduler.DisableSharded()

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	comp := &ConcurrentRouteComp{}
	comp.fn = func(_ *session.Session, data []byte) error {
		switch string(data) {
		case "first":
			close(firstStarted)
			<-releaseFirst
		case "second":
			close(secondStarted)
		}
		return nil
	}

	n := newTestNode()
	n.handler.setConcurrentRoutePolicy([]string{"ConcurrentRouteComp.Allow"}, false)
	if err := n.handler.register(comp, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	handler := registeredHandler(t, n.handler, "ConcurrentRouteComp.FIFO")
	s := session.NewWithID(nil, 1002)

	n.handler.localProcess(handler, 1, s, &message.Message{Type: message.Request, ID: 1, Route: "ConcurrentRouteComp.FIFO", Data: []byte("first")}, nil)
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first FIFO request did not start")
	}
	n.handler.localProcess(handler, 2, s, &message.Message{Type: message.Request, ID: 2, Route: "ConcurrentRouteComp.FIFO", Data: []byte("second")}, nil)
	select {
	case <-secondStarted:
		close(releaseFirst)
		t.Fatal("second same-session non-allowlisted request started before first completed")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("second FIFO request did not run after first completed")
	}
}

func TestConcurrentRouteRunsBesideBlockedFIFOLane(t *testing.T) {
	log.SetLogger(&noopLogger{})
	ensureScheduler()
	scheduler.DisableConcurrentRoutes()
	scheduler.EnableConcurrentRequests(2)
	defer scheduler.DisableConcurrentRoutes()
	scheduler.EnableSharded(4)
	defer scheduler.DisableSharded()

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	allowStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	comp := &MixedLaneComp{}
	comp.first = func(_ *session.Session, _ []byte) error {
		close(firstStarted)
		<-releaseFirst
		return nil
	}
	comp.second = func(_ *session.Session, _ []byte) error {
		close(secondStarted)
		return nil
	}
	comp.allow = func(_ *session.Session, _ []byte) error {
		close(allowStarted)
		return nil
	}

	n := newTestNode()
	n.handler.setConcurrentRoutePolicy([]string{"MixedLaneComp.Allow"}, false)
	if err := n.handler.register(comp, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	first := registeredHandler(t, n.handler, "MixedLaneComp.First")
	second := registeredHandler(t, n.handler, "MixedLaneComp.Second")
	allow := registeredHandler(t, n.handler, "MixedLaneComp.Allow")
	s := session.NewWithID(nil, 1003)

	n.handler.localProcess(first, 1, s, &message.Message{Type: message.Request, ID: 1, Route: "MixedLaneComp.First", Data: []byte("first")}, nil)
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first FIFO request did not start")
	}
	n.handler.localProcess(second, 2, s, &message.Message{Type: message.Request, ID: 2, Route: "MixedLaneComp.Second", Data: []byte("second")}, nil)
	n.handler.localProcess(allow, 3, s, &message.Message{Type: message.Request, ID: 3, Route: "MixedLaneComp.Allow", Data: []byte("allow")}, nil)

	select {
	case <-allowStarted:
	case <-time.After(3 * time.Second):
		close(releaseFirst)
		t.Fatal("allowlisted request did not run while FIFO lane was blocked")
	}
	select {
	case <-secondStarted:
		close(releaseFirst)
		t.Fatal("second FIFO request bypassed first FIFO request")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("second FIFO request did not run after first completed")
	}
}

func TestConcurrentAgentResponsesUseBoundRequestMid(t *testing.T) {
	log.SetLogger(&noopLogger{})
	ensureScheduler()
	scheduler.DisableConcurrentRoutes()
	scheduler.EnableConcurrentRequests(2)
	defer scheduler.DisableConcurrentRoutes()

	oneStarted := make(chan struct{})
	twoStarted := make(chan struct{})
	release := make(chan struct{})
	comp := &ResponseRouteComp{}
	comp.one = func(s *session.Session, _ []byte) error {
		close(oneStarted)
		<-twoStarted
		<-release
		return s.Response([]byte("one"))
	}
	comp.two = func(s *session.Session, _ []byte) error {
		<-oneStarted
		close(twoStarted)
		<-release
		return s.Response([]byte("two"))
	}

	n := newTestNode()
	n.handler.setConcurrentRoutePolicy([]string{"ResponseRouteComp.One", "ResponseRouteComp.Two"}, false)
	if err := n.handler.register(comp, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	one := registeredHandler(t, n.handler, "ResponseRouteComp.One")
	two := registeredHandler(t, n.handler, "ResponseRouteComp.Two")
	a := newAgent(newCountConn(), nil, nil)
	s := a.session

	n.handler.localProcess(one, 11, s, &message.Message{Type: message.Request, ID: 11, Route: "ResponseRouteComp.One", Data: []byte("one")}, nil)
	n.handler.localProcess(two, 22, s, &message.Message{Type: message.Request, ID: 22, Route: "ResponseRouteComp.Two", Data: []byte("two")}, nil)
	select {
	case <-twoStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("response handlers did not both start")
	}
	close(release)

	got := map[uint64]string{}
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case m := <-a.chSend:
			if m.typ != message.Response {
				t.Fatalf("queued message type = %v, want response", m.typ)
			}
			b, ok := m.payload.([]byte)
			if !ok {
				t.Fatalf("response payload type = %T, want []byte", m.payload)
			}
			got[m.mid] = string(b)
		case <-deadline:
			t.Fatalf("timed out waiting for responses, got %v", got)
		}
	}
	if got[11] != "one" || got[22] != "two" {
		t.Fatalf("responses routed to wrong mids: %v", got)
	}
}

func TestAgentAndAcceptorResponseRejectUnboundWhenConcurrentRoutesEnabled(t *testing.T) {
	log.SetLogger(&noopLogger{})
	scheduler.DisableConcurrentRoutes()
	acNotify := &acceptor{}
	acNotify.setLastMid(0)
	if err := acNotify.Response([]byte("notify")); err != ErrSessionOnNotify {
		t.Fatalf("acceptor notify Response = %v, want ErrSessionOnNotify", err)
	}
	if err := acNotify.ResponseMid(0, []byte("notify")); err != ErrSessionOnNotify {
		t.Fatalf("acceptor ResponseMid(0) = %v, want ErrSessionOnNotify", err)
	}

	a := newAgent(newCountConn(), nil, nil)
	a.setStrictRequestMidBinding(true)
	a.setLastMid(99)
	if err := a.Response([]byte("agent")); err != errNoRequestBound {
		t.Fatalf("agent Response without bound mid = %v, want errNoRequestBound", err)
	}

	ac := &acceptor{}
	ac.setStrictRequestMidBinding(true)
	ac.setLastMid(99)
	if err := ac.Response([]byte("acceptor")); err != errNoRequestBound {
		t.Fatalf("acceptor Response without bound mid = %v, want errNoRequestBound", err)
	}
}

func TestNotifySerializedWhenAllRequestsConcurrent(t *testing.T) {
	log.SetLogger(&noopLogger{})
	ensureScheduler()
	scheduler.DisableConcurrentRoutes()
	scheduler.EnableConcurrentRequests(2)
	defer scheduler.DisableConcurrentRoutes()
	scheduler.EnableSharded(4)
	defer scheduler.DisableSharded()

	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var once sync.Once
	comp := &NotifySerialComp{}
	comp.fn = func(_ *session.Session, _ []byte) error {
		first := false
		once.Do(func() { first = true })
		if first {
			close(firstStarted)
			<-releaseFirst
			return nil
		}
		close(secondStarted)
		return nil
	}

	n := newTestNode()
	n.handler.setConcurrentRoutePolicy(nil, true)
	if err := n.handler.register(comp, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	handler := registeredHandler(t, n.handler, "NotifySerialComp.Handle")
	s := session.NewWithID(nil, 1004)

	n.handler.localProcess(handler, 0, s, &message.Message{Type: message.Notify, Route: "NotifySerialComp.Handle", Data: []byte("first")}, nil)
	select {
	case <-firstStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("first notify did not start")
	}
	n.handler.localProcess(handler, 0, s, &message.Message{Type: message.Notify, Route: "NotifySerialComp.Handle", Data: []byte("second")}, nil)
	select {
	case <-secondStarted:
		close(releaseFirst)
		t.Fatal("second notify started while first notify was blocked")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	select {
	case <-secondStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("second notify did not run after first completed")
	}
}
