package cluster

import (
	"testing"

	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/session"
)

// A Request whose route cannot be forwarded (no member hosts the service)
// must invoke the configured RemoteRouteMissHandler so the embedding app can
// answer the client instead of letting the request burn its full timeout.
// A Notify has no reply channel and must never invoke it.
func TestProcessMessageRemoteForwardFailureInvokesMissHandler(t *testing.T) {
	var gotRoute string
	var gotErr error
	calls := 0
	node := &Node{Options: Options{RemoteRouteMissHandler: func(s *session.Session, msg *message.Message, err error) {
		calls++
		gotRoute = msg.Route
		gotErr = err
	}}}
	h := NewHandler(node, nil)
	a := newAgent(newCountConn(), nil, nil)

	h.processMessage(a, &message.Message{Type: message.Request, ID: 7, Route: "MapService.RemoveShield"})
	if calls != 1 || gotRoute != "MapService.RemoveShield" || gotErr == nil {
		t.Fatalf("request miss: calls=%d route=%q err=%v, want exactly one callback with the failed route", calls, gotRoute, gotErr)
	}

	h.processMessage(a, &message.Message{Type: message.Notify, Route: "MapService.RemoveShield"})
	if calls != 1 {
		t.Fatalf("notify invoked miss handler (calls=%d); Notify has no reply channel", calls)
	}
}

// Without a configured handler the failed forward must stay a logged no-op.
func TestProcessMessageRemoteForwardFailureWithoutHandlerIsNoop(t *testing.T) {
	h := NewHandler(&Node{}, nil)
	a := newAgent(newCountConn(), nil, nil)
	h.processMessage(a, &message.Message{Type: message.Request, ID: 9, Route: "MapService.RemoveShield"})
}
