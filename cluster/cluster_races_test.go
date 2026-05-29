package cluster

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/log"
	"github.com/lonng/nano/metrics"
	"github.com/lonng/nano/session"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/valyala/fasthttp"
)

// --- shared test scaffolding ------------------------------------------------

// ipAddr is a net.Addr returning a caller-chosen string, used to drive the
// per-IP accounting / IPv6 parsing tests.
type ipAddr struct{ s string }

func (a ipAddr) Network() string { return "tcp" }
func (a ipAddr) String() string  { return a.s }

// addrConn is a net.Conn whose Read blocks until Close, whose Write is a no-op,
// and whose RemoteAddr returns a caller-chosen address. It lets handle() run a
// read loop with a deterministic, controllable remote IP.
type addrConn struct {
	remote net.Addr
	closed chan struct{}
	once   sync.Once
}

func newAddrConn(remote net.Addr) *addrConn {
	return &addrConn{remote: remote, closed: make(chan struct{})}
}

func (c *addrConn) Read(b []byte) (int, error)  { <-c.closed; return 0, io.EOF }
func (c *addrConn) Write(b []byte) (int, error) { return len(b), nil }
func (c *addrConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}
func (c *addrConn) LocalAddr() net.Addr              { return c.remote }
func (c *addrConn) RemoteAddr() net.Addr             { return c.remote }
func (c *addrConn) SetDeadline(time.Time) error      { return nil }
func (c *addrConn) SetReadDeadline(time.Time) error  { return nil }
func (c *addrConn) SetWriteDeadline(time.Time) error { return nil }

// newTestNode builds a minimal, non-master Node with all maps and the handler /
// cluster wired up, but no listeners or gRPC server.
func newTestNode() *Node {
	n := &Node{
		sessions:        map[int64]*session.Session{},
		sseClients:      map[string]chan []byte{},
		connectionCount: map[string]uint{},
	}
	n.handler = NewHandler(n, nil)
	n.cluster = newCluster(n) // non-master => no heartbeat ticker
	return n
}

func (n *Node) sessionLen() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.sessions)
}

func (n *Node) connCount(ip string) (uint, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	c, ok := n.connectionCount[ip]
	return c, ok
}

func memberInfo(addr string, services ...string) *clusterpb.MemberInfo {
	return &clusterpb.MemberInfo{ServiceAddr: addr, Services: services}
}

// --- H4: member-slice races + routing reads a live slice --------------------

func TestFindMembersReturnsCopy(t *testing.T) {
	h := NewHandler(&Node{}, nil)
	h.addRemoteService(memberInfo("svc-a:1", "Svc"))

	first := h.findMembers("Svc")
	if len(first) != 1 {
		t.Fatalf("expected 1 member, got %d", len(first))
	}
	// Mutating the returned slice must not corrupt the handler's internal state.
	first[0] = memberInfo("tampered:9", "Svc")

	second := h.findMembers("Svc")
	if len(second) != 1 || second[0].ServiceAddr != "svc-a:1" {
		t.Fatalf("findMembers returned a live internal slice; got %+v", second)
	}
}

func TestFindMembersNoRace(t *testing.T) {
	log.SetLogger(&noopLogger{})
	h := NewHandler(&Node{}, nil)
	h.addRemoteService(memberInfo("svc-a:1", "Svc"))

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Readers: fetch and iterate the member list.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for _, m := range h.findMembers("Svc") {
						_ = m.ServiceAddr
					}
				}
			}
		}()
	}
	// Writers: churn the routing table for the same service.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			addr := "svc-" + string(rune('A'+id)) + ":1"
			for {
				select {
				case <-stop:
					return
				default:
					h.addRemoteService(memberInfo(addr, "Svc"))
					h.delMember(addr)
				}
			}
		}(i)
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestClusterMembersNoRace(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c := newCluster(&Node{})

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = c.remoteAddrs()
				_, _, _ = c.pingNodes(nil)
			}
		}
	}()
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			addr := "node-" + string(rune('A'+id)) + ":1"
			for {
				select {
				case <-stop:
					return
				default:
					c.addMember(memberInfo(addr))
					c.delMember(addr)
				}
			}
		}(i)
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// --- H13: local sessions must be removed on disconnect ----------------------

func TestHandleDeletesSessionOnDisconnect(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()

	conn := newAddrConn(ipAddr{"203.0.113.7:5555"})
	done := make(chan struct{})
	go func() {
		n.handler.handle(conn)
		close(done)
	}()

	// Wait for the session to be stored.
	if !waitFor(func() bool { return n.sessionLen() == 1 }, 2*time.Second) {
		t.Fatal("session was never stored")
	}

	// Disconnect: the read loop returns and the cleanup defer must delete it.
	conn.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handle did not return after disconnect")
	}

	if n.sessionLen() != 0 {
		t.Fatalf("session leaked after disconnect: n.sessions still has %d entries", n.sessionLen())
	}
}

// --- H14: sseClients map must not be read without the lock ------------------

func TestSSEClientsMapNoRace(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()

	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Writer: register/unregister a real sse client id under n.mu.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				ch := make(chan []byte, 1)
				n.registerSSEClient("999", ch)
				n.unregisterSSEClient("999", ch)
			}
		}
	}()

	// Readers: POST /api with an unregistered session id; the handler reads the
	// sseClients map and returns Unauthorized immediately (ch == nil), so it
	// never enters the response wait loop.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					ctx := newAPIPost(`{"route":"x","data":{}}`, "111")
					n.handleHTTPRequest(ctx)
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// --- H15 / M28: connectionCount races + zero-key retention ------------------

func TestConnectionCountNoRace(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	n.LimitConnectPerIp = 1 << 20 // high so increments are never rejected

	const ip = "198.51.100.4"
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if n.increaseConnection(ip) == nil {
						n.decreaseConnection(ip)
					}
				}
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

func TestDecreaseConnectionDeletesZeroKey(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	n.LimitConnectPerIp = 4

	const ip = "198.51.100.9"
	if err := n.increaseConnection(ip); err != nil {
		t.Fatalf("increaseConnection: %v", err)
	}
	n.decreaseConnection(ip)

	if _, ok := n.connCount(ip); ok {
		t.Fatalf("connectionCount retained a zero-count key for %s", ip)
	}
}

// --- H24 / M3: departing gate purges acceptor sessions and conn pool --------

func TestDelMemberPurgesGateSessionsAndPool(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	n.ServiceAddr = "self:1"
	n.rpcClient = newRPCClient()
	n.cluster.setRpcClient(n.rpcClient)

	const gate = "gate-x:1"

	// A remote acceptor session homed on the gate.
	ac := &acceptor{sid: 42, gateAddr: gate}
	s := session.New(ac)
	ac.session = s
	n.storeSession(s)

	// An outbound conn pool to the gate.
	if _, err := n.rpcClient.createConnPool(gate); err != nil {
		t.Fatalf("createConnPool: %v", err)
	}
	n.rpcClient.RLock()
	_, hadPool := n.rpcClient.pools[gate]
	n.rpcClient.RUnlock()
	if !hadPool {
		t.Fatal("expected a pool to exist before DelMember")
	}

	if _, err := n.DelMember(context.Background(), &clusterpb.DelMemberRequest{ServiceAddr: gate}); err != nil {
		t.Fatalf("DelMember: %v", err)
	}

	if n.sessionLen() != 0 {
		t.Fatalf("acceptor session for departed gate %s was not purged", gate)
	}
	n.rpcClient.RLock()
	_, stillPooled := n.rpcClient.pools[gate]
	n.rpcClient.RUnlock()
	if stillPooled {
		t.Fatalf("conn pool for departed gate %s was not closed/removed", gate)
	}
}

// --- H29: per-IP gauge child reclaimed when count drains to zero ------------

func TestConnectionsPerIPGaugeReclaimed(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()

	const ip = "192.0.2.123"
	base := testutil.CollectAndCount(metrics.ConnectionsPerIP)

	if err := n.increaseConnection(ip); err != nil {
		t.Fatalf("increaseConnection: %v", err)
	}
	n.decreaseConnection(ip)

	if got := testutil.CollectAndCount(metrics.ConnectionsPerIP); got != base {
		t.Fatalf("per-IP gauge child leaked: series count %d != baseline %d", got, base)
	}
}

// --- M8: gRPC client pools closed on Shutdown -------------------------------

func TestShutdownClosesConnPools(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	n.Components = &component.Components{}
	n.rpcClient = newRPCClient()
	n.cluster.setRpcClient(n.rpcClient)
	if _, err := n.rpcClient.createConnPool("peer:1"); err != nil {
		t.Fatalf("createConnPool: %v", err)
	}

	n.Shutdown()

	n.rpcClient.RLock()
	closed := n.rpcClient.isClosed
	n.rpcClient.RUnlock()
	if !closed {
		t.Fatal("Shutdown did not close the gRPC client pools")
	}
}

// --- M9: unknown gate address must not be dialed / forged -------------------

func TestFindOrCreateSessionRejectsUnknownGate(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	n.ServiceAddr = "self:1"
	n.rpcClient = newRPCClient()
	n.cluster.setRpcClient(n.rpcClient)
	n.cluster.addMember(memberInfo("known-gate:1"))

	_, err := n.findOrCreateSession(7, 0, "evil.example.com:1", []byte("{}"))
	if err == nil {
		t.Fatal("expected findOrCreateSession to reject an unknown gate address")
	}
	if n.sessionLen() != 0 {
		t.Fatal("a session was forged for an unknown gate address")
	}
}

// --- M10: concurrent first-seen RPCs must share one session per sid ---------

func TestFindOrCreateSessionNoDuplicate(t *testing.T) {
	log.SetLogger(&noopLogger{})
	for iter := 0; iter < 50; iter++ {
		n := newTestNode()
		n.ServiceAddr = "self:1"
		n.rpcClient = newRPCClient()
		n.cluster.setRpcClient(n.rpcClient)
		const gate = "127.0.0.1:1"
		n.cluster.addMember(memberInfo(gate))

		const sid = int64(123)
		const workers = 32
		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make([]*session.Session, workers)
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-start
				s, err := n.findOrCreateSession(sid, 0, gate, []byte("{}"))
				if err != nil {
					t.Errorf("findOrCreateSession: %v", err)
					return
				}
				results[idx] = s
			}(i)
		}
		close(start)
		wg.Wait()

		canonical := n.findSession(sid)
		for i, s := range results {
			if s != nil && s != canonical {
				t.Fatalf("iter %d worker %d got a non-canonical session (%p != %p): duplicate sessions created", iter, i, s, canonical)
			}
		}
	}
}

// --- M12: remote service routing must be idempotent on re-register ----------

func TestAddRemoteServiceIdempotent(t *testing.T) {
	h := NewHandler(&Node{}, nil)
	info := memberInfo("svc-a:1", "Svc")

	h.addRemoteService(info)
	h.addRemoteService(info) // simulate a node restart / re-register

	members := h.findMembers("Svc")
	if len(members) != 1 {
		t.Fatalf("re-registration duplicated routing entries: got %d, want 1", len(members))
	}
}

// --- M14: empty ServiceAddr registrations must be rejected ------------------

func TestRegisterRejectsEmptyServiceAddr(t *testing.T) {
	c := newCluster(&Node{})
	_, err := c.Register(context.Background(), &clusterpb.RegisterRequest{
		MemberInfo: &clusterpb.MemberInfo{ServiceAddr: ""},
	})
	if err == nil {
		t.Fatal("expected Register to reject an empty ServiceAddr")
	}
}

// --- M17: IPv6 remote addresses must be parsed correctly --------------------

func TestRemoteAddrWithoutPortIPv6(t *testing.T) {
	a := newAgent(newAddrConn(ipAddr{"[2001:db8::1]:1234"}), nil, nil)
	defer a.conn.Close()

	if got := a.RemoteAddrWithoutPortStr(); got != "2001:db8::1" {
		t.Fatalf("IPv6 host mis-parsed: got %q, want %q", got, "2001:db8::1")
	}
}

// --- M18: a per-IP-limit rejection must not leak the session ----------------

func TestHandleRejectionCleansUp(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	n.LimitConnectPerIp = 1

	const ip = "9.9.9.9:1"

	// First connection: accepted, stays in the read loop.
	c1 := newAddrConn(ipAddr{ip})
	done1 := make(chan struct{})
	go func() { n.handler.handle(c1); close(done1) }()
	if !waitFor(func() bool { c, ok := n.connCount("9.9.9.9"); return ok && c == 1 }, 2*time.Second) {
		t.Fatal("first connection was never accounted")
	}

	// Second connection from the same IP: over the limit -> rejected.
	c2 := newAddrConn(ipAddr{ip})
	done2 := make(chan struct{})
	go func() { n.handler.handle(c2); close(done2) }()
	select {
	case <-done2:
	case <-time.After(2 * time.Second):
		t.Fatal("rejected connection handler did not return")
	}

	if n.sessionLen() != 1 {
		t.Fatalf("rejected over-limit connection leaked a session: n.sessions has %d entries (want 1)", n.sessionLen())
	}

	c1.Close()
	<-done1
}

// --- helpers ---------------------------------------------------------------

func newAPIPost(body, sid string) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI("/api")
	ctx.Request.SetBodyString(body)
	ctx.Request.Header.SetCookie("sse_sessionID", sid)
	return ctx
}

func waitFor(cond func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}
