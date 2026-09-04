package cluster

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/internal/message"
	"github.com/lonng/nano/session"
)

type decodeProbeArg struct {
	Militaries [][]int `json:"militaries"`
}

// DecodeProbeComp hosts one typed-argument route. A payload that cannot be
// decoded into decodeProbeArg must never reach Handle. Handle deliberately
// does not answer the session: the decodable control cases below run on the
// shared scheduler, and an acceptor-backed Response would dial the fake gate
// and wedge the scheduler worker for every later test in the package.
type DecodeProbeComp struct {
	component.Base
	calls atomic.Int32
}

func (c *DecodeProbeComp) Handle(_ *session.Session, _ *decodeProbeArg) error {
	c.calls.Add(1)
	return nil
}

// Locally hosted route: an undecodable Request payload must be surfaced to
// the RemoteRouteMissHandler as ErrRequestDecode so the embedding application
// can answer the client. A Notify never invokes it, and a decodable payload
// never invokes it.
func TestProcessMessageLocalDecodeFailureInvokesMissHandler(t *testing.T) {
	log.SetLogger(&noopLogger{})
	defer useJSONSerializer()()
	ensureScheduler()

	var gotRoute string
	var gotMid uint64
	var gotErr error
	calls := 0
	n := newTestNode()
	n.Options.RemoteRouteMissHandler = func(_ *session.Session, mid uint64, route string, err error) {
		calls++
		gotMid = mid
		gotRoute = route
		gotErr = err
	}
	n.handler = NewHandler(n, nil)
	comp := &DecodeProbeComp{}
	if err := n.handler.register(comp, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	a := newAgent(newCountConn(), nil, nil)

	bad := []byte(`{"militaries":[{"military_id":4,"amount":192}]}`)
	n.handler.processMessage(a, &message.Message{Type: message.Request, ID: 5, Route: "DecodeProbeComp.Handle", Data: bad})
	if calls != 1 || gotMid != 5 || gotRoute != "DecodeProbeComp.Handle" {
		t.Fatalf("decode failure: calls=%d mid=%d route=%q, want exactly one callback for the failed request", calls, gotMid, gotRoute)
	}
	if !errors.Is(gotErr, ErrRequestDecode) || !strings.Contains(gotErr.Error(), ErrRequestDecodeMarker) {
		t.Fatalf("err=%v, want ErrRequestDecode carrying %q", gotErr, ErrRequestDecodeMarker)
	}
	if got := comp.calls.Load(); got != 0 {
		t.Fatalf("handler ran %d times on an undecodable payload", got)
	}

	n.handler.processMessage(a, &message.Message{Type: message.Notify, Route: "DecodeProbeComp.Handle", Data: bad})
	if calls != 1 {
		t.Fatalf("notify invoked miss handler (calls=%d); Notify has no reply channel", calls)
	}

	n.handler.processMessage(a, &message.Message{Type: message.Request, ID: 6, Route: "DecodeProbeComp.Handle", Data: []byte(`{"militaries":[[1]]}`)})
	if calls != 1 {
		t.Fatalf("decodable payload invoked miss handler (calls=%d)", calls)
	}
}

// Remote member: HandleRequest must return the decode failure as its gRPC
// error so the accepting gateway's RemoteRouteMissHandler fires, instead of
// acknowledging a request nothing will ever answer.
func TestHandleRequestReturnsDecodeFailure(t *testing.T) {
	log.SetLogger(&noopLogger{})
	defer useJSONSerializer()()
	ensureScheduler()

	n := newTestNode()
	n.ServiceAddr = "gate"
	n.rpcClient = newRPCClient()
	n.cluster.setRpcClient(n.rpcClient)
	if err := n.handler.register(&DecodeProbeComp{}, nil); err != nil {
		t.Fatalf("register: %v", err)
	}
	req := &clusterpb.RequestMessage{
		GateAddr:       "gate",
		SessionId:      11,
		Id:             3,
		Route:          "DecodeProbeComp.Handle",
		Data:           []byte(`{"militaries":[{"military_id":4,"amount":192}]}`),
		ClientUserData: []byte("{}"),
	}
	_, err := n.HandleRequest(context.Background(), req)
	if !errors.Is(err, ErrRequestDecode) {
		t.Fatalf("HandleRequest err=%v, want ErrRequestDecode", err)
	}

	req.Data = []byte(`{"militaries":[[1]]}`)
	if _, err := n.HandleRequest(context.Background(), req); err != nil {
		t.Fatalf("HandleRequest on decodable payload err=%v, want nil", err)
	}
}
