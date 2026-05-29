package cluster

import (
	"context"
	"testing"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/session"
)

// M11: SessionClosed must only reclaim a session for its owning gate. A
// non-owning gate (different GateAddr) must not be able to drop it.
func TestSessionClosedOwnerCheck(t *testing.T) {
	n := &Node{sessions: map[int64]*session.Session{}}
	ac := &acceptor{sid: 42, gateAddr: "gateA:1000"}
	s := session.New(ac)
	ac.session = s
	n.sessions[s.ID()] = s

	// A non-owning gate must NOT remove the session.
	if _, err := n.SessionClosed(context.Background(), &clusterpb.SessionClosedRequest{
		SessionId: s.ID(), GateAddr: "gateB:2000",
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	n.mu.RLock()
	_, still := n.sessions[s.ID()]
	n.mu.RUnlock()
	if !still {
		t.Fatal("M11: a non-owning gate was able to drop the session")
	}

	// The owning gate removes it.
	if _, err := n.SessionClosed(context.Background(), &clusterpb.SessionClosedRequest{
		SessionId: s.ID(), GateAddr: "gateA:1000",
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	n.mu.RLock()
	_, gone := n.sessions[s.ID()]
	n.mu.RUnlock()
	if gone {
		t.Fatal("M11: the owning gate failed to remove the session")
	}
}

// M11: an empty GateAddr (legacy caller) keeps backward-compatible behavior
// (reclaim by id).
func TestSessionClosedLegacyEmptyGate(t *testing.T) {
	n := &Node{sessions: map[int64]*session.Session{}}
	ac := &acceptor{sid: 7, gateAddr: "gateA:1000"}
	s := session.New(ac)
	ac.session = s
	n.sessions[s.ID()] = s

	if _, err := n.SessionClosed(context.Background(), &clusterpb.SessionClosedRequest{
		SessionId: s.ID(), // GateAddr left empty
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	n.mu.RLock()
	_, present := n.sessions[s.ID()]
	n.mu.RUnlock()
	if present {
		t.Fatal("M11: empty GateAddr should keep legacy reclaim-by-id behavior (session not deleted)")
	}
}

// M35: a JSON-payload remote session must encode outbound payloads as JSON
// (not via the cluster-wide binary serializer), and a raw []byte must pass
// through unchanged.
func TestAcceptorSerializeJSON(t *testing.T) {
	ac := &acceptor{jsonPayload: true}

	data, err := ac.serialize(map[string]int{"a": 1})
	if err != nil {
		t.Fatalf("serialize struct: %v", err)
	}
	if string(data) != `{"a":1}` {
		t.Fatalf("M35: want JSON `{\"a\":1}`, got %q", data)
	}

	raw := []byte("already-encoded")
	got, err := ac.serialize(raw)
	if err != nil {
		t.Fatalf("serialize []byte: %v", err)
	}
	if string(got) != "already-encoded" {
		t.Fatalf("M35: raw []byte must pass through, got %q", got)
	}
}

// M35: serializeHTTPJSON must not double-encode a raw []byte.
func TestSerializeHTTPJSONBytePassthrough(t *testing.T) {
	got, err := serializeHTTPJSON([]byte("{\"x\":1}"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"x":1}` {
		t.Fatalf("want passthrough, got %q", got)
	}
}
