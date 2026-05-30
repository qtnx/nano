// Copyright (c) nano Authors. All Rights Reserved.
//
// Regression tests added during the issue #7 review round. They cover the
// peer-side route replacement, the pending-delete cancel/rejoin race, the
// per-heartbeat retry batch cap, the ledger bound, and the cleanup paths that
// the first cut left untested.

package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/internal/log"
)

// pendingPeerCount returns the number of peers holding queued deletes.
func pendingPeerCount(c *cluster) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pendingDeletes)
}

// pendingHas reports whether deletedAddr is queued for peerAddr.
func pendingHas(c *cluster, peerAddr, deletedAddr string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.pendingDeletes[peerAddr][deletedAddr]
	return ok
}

// pairDelSpy records notifyDelMember calls and can fail specific (peer,deleted)
// pairs, so partial-flush behavior can be driven deterministically.
type pairDelSpy struct {
	mu    sync.Mutex
	calls []delCall
	fail  map[delCall]bool
}

func (s *pairDelSpy) notify(peer, deleted string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, delCall{peer, deleted})
	if s.fail[delCall{peer, deleted}] {
		return errors.New("unreachable")
	}
	return nil
}

func (s *pairDelSpy) count(peer, deleted string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if c.peer == peer && c.deleted == deleted {
			n++
		}
	}
	return n
}

// A1. The peer-side NewMember receive path must clean-replace routes, so a
// same-address rejoin that drops a service purges the stale route on the peer
// too (issue #7 fix 2/7 end-to-end; the original cut only fixed the master).
func TestPeerNewMemberReplacesStaleServiceRoutes(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()

	// Seed the peer's routing table as if it had learned backend:1 with two
	// services.
	n.handler.addRemoteService(memberInfo("backend:1", "Map", "Chat"))
	if !routable(n.cluster, "Map", "backend:1") || !routable(n.cluster, "Chat", "backend:1") {
		t.Fatal("setup: backend:1 should be routable for Map and Chat")
	}

	// backend:1 rolls and re-registers at the same address with only Map; the
	// master fans a NewMember to this peer.
	if _, err := n.NewMember(context.Background(), &clusterpb.NewMemberRequest{
		MemberInfo: memberInfo("backend:1", "Map"),
	}); err != nil {
		t.Fatalf("NewMember: %v", err)
	}
	if got := routableCount(n.cluster, "Map", "backend:1"); got != 1 {
		t.Fatalf("Map should remain routable exactly once after peer NewMember, got %d", got)
	}
	if routable(n.cluster, "Chat", "backend:1") {
		t.Fatal("stale Chat route survived a peer NewMember that dropped the service (fix 2/7 not end-to-end)")
	}
}

// A2. A rejoin that ADDS a service must make the new service routable (the
// replacement installs the full current list, not only prunes).
func TestRejoinAddsServiceRoutable(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)

	if _, err := c.Register(context.Background(), &clusterpb.RegisterRequest{MemberInfo: memberInfo("127.0.0.1:9001", "Map")}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := c.Register(context.Background(), &clusterpb.RegisterRequest{MemberInfo: memberInfo("127.0.0.1:9001", "Map", "Chat")}); err != nil {
		t.Fatalf("rejoin: %v", err)
	}
	if routableCount(c, "Map", "127.0.0.1:9001") != 1 || routableCount(c, "Chat", "127.0.0.1:9001") != 1 {
		t.Fatalf("after rejoin-adds-service expected Map=1 Chat=1, got Map=%d Chat=%d",
			routableCount(c, "Map", "127.0.0.1:9001"), routableCount(c, "Chat", "127.0.0.1:9001"))
	}
}

// 3a. A queued delete whose target re-registered must NOT be sent on flush, and
// must be dropped from the ledger — telling a peer to drop a live rejoined pod
// would blackhole it.
func TestFlushSkipsRejoinedMember(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	spy := &delSpy{failPeers: map[string]bool{}}
	c.notifyDelMember = spy.notify

	// A delete for 9002 is queued for peer 9001, then 9002 comes back as a live
	// member (addMember does not cancel the queue, isolating the flush guard).
	c.recordPendingDelete("127.0.0.1:9001", "127.0.0.1:9002")
	c.addMember(memberInfo("127.0.0.1:9002", "Map"))

	c.flushPendingDeletes("127.0.0.1:9001")

	if spy.sent("127.0.0.1:9001", "127.0.0.1:9002") {
		t.Fatal("flush told a peer to drop a re-registered (live) member")
	}
	if pendingCount(c, "127.0.0.1:9001") != 0 {
		t.Fatal("stale delete for a re-registered member was not dropped from the ledger")
	}
}

// 3b. Re-registering an address cancels any delete still queued for it across
// all peers (so a later peer heartbeat does not drop the rejoined member).
func TestCancelPendingDeleteOnReRegister(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	c.notifyDelMember = (&delSpy{failPeers: map[string]bool{}}).notify

	c.recordPendingDelete("127.0.0.1:9001", "127.0.0.1:9002")
	if pendingCount(c, "127.0.0.1:9001") != 1 {
		t.Fatal("setup: expected a queued delete")
	}

	if _, err := c.Register(context.Background(), &clusterpb.RegisterRequest{MemberInfo: memberInfo("127.0.0.1:9002", "Map")}); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	if pendingCount(c, "127.0.0.1:9001") != 0 {
		t.Fatal("re-registering 9002 did not cancel the queued delete for it")
	}
}

// 4. A partial flush failure re-queues only the failed target; the succeeded
// one is cleared.
func TestFlushPartialFailureRequeuesOnlyFailed(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	spy := &pairDelSpy{fail: map[delCall]bool{{"127.0.0.1:9001", "dead:b"}: true}}
	c.notifyDelMember = spy.notify

	c.recordPendingDelete("127.0.0.1:9001", "dead:a")
	c.recordPendingDelete("127.0.0.1:9001", "dead:b")

	c.flushPendingDeletes("127.0.0.1:9001")

	if spy.count("127.0.0.1:9001", "dead:a") != 1 || spy.count("127.0.0.1:9001", "dead:b") != 1 {
		t.Fatalf("both targets should be attempted once: a=%d b=%d",
			spy.count("127.0.0.1:9001", "dead:a"), spy.count("127.0.0.1:9001", "dead:b"))
	}
	if pendingHas(c, "127.0.0.1:9001", "dead:a") {
		t.Fatal("succeeded delete (dead:a) was wrongly re-queued")
	}
	if !pendingHas(c, "127.0.0.1:9001", "dead:b") {
		t.Fatal("failed delete (dead:b) was not re-queued for retry")
	}
}

// 6. A single heartbeat flush is capped so a large backlog cannot pin the
// handler on synchronous RPCs; the remainder stays queued.
func TestFlushBatchCapBoundsRetriesPerHeartbeat(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	spy := &delSpy{failPeers: map[string]bool{}}
	c.notifyDelMember = spy.notify

	const extra = 10
	for i := 0; i < maxDeleteRetriesPerHeartbeat+extra; i++ {
		c.recordPendingDelete("127.0.0.1:9001", fmt.Sprintf("dead:%d", i))
	}

	c.flushPendingDeletes("127.0.0.1:9001")

	if got := spy.count(); got != maxDeleteRetriesPerHeartbeat {
		t.Fatalf("flush sent %d deletes, want capped at %d", got, maxDeleteRetriesPerHeartbeat)
	}
	if got := pendingCount(c, "127.0.0.1:9001"); got != extra {
		t.Fatalf("expected %d deletes left queued after a capped flush, got %d", extra, got)
	}
}

// 4b. The peer-key ledger is bounded; once full, further new peers are dropped
// (logged, not silent) but existing peers still accept more deletes.
func TestRecordPendingDeleteBound(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)

	for i := 0; i < maxPendingDeletePeers+5; i++ {
		c.recordPendingDelete(fmt.Sprintf("peer:%d", i), "dead:x")
	}
	if got := pendingPeerCount(c); got != maxPendingDeletePeers {
		t.Fatalf("ledger peer count = %d, want capped at %d", got, maxPendingDeletePeers)
	}
	// An already-tracked peer can still queue more deletes.
	c.recordPendingDelete("peer:0", "dead:y")
	if pendingCount(c, "peer:0") != 2 {
		t.Fatal("an existing peer should still accept additional queued deletes at the bound")
	}
}

// 5. Concurrent heartbeats from the same unknown member must not duplicate the
// member or its routes (scan+append+route-install are atomic under c.mu).
func TestConcurrentUnknownHeartbeatsSingleMember(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	c.notifyDelMember = (&delSpy{failPeers: map[string]bool{}}).notify

	const n = 16
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			_, _ = c.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{
				MemberInfo: memberInfo("127.0.0.1:9003", "World"),
			})
		}()
	}
	close(start)
	wg.Wait()

	if got := memberCount(c); got != 1 {
		t.Fatalf("concurrent unknown heartbeats produced %d members, want 1 (duplicate append)", got)
	}
	if got := routableCount(c, "World", "127.0.0.1:9003"); got != 1 {
		t.Fatalf("World routable %d times for 9003, want exactly 1", got)
	}
}

// 8. The received-DelMember path (cluster.delMember) must also drop queued
// retries destined for the departing peer, not only master Unregister.
func TestClusterDelMemberDropsPendingDeletes(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)

	c.recordPendingDelete("127.0.0.1:9001", "127.0.0.1:9002")
	if pendingCount(c, "127.0.0.1:9001") != 1 {
		t.Fatal("setup: expected a queued delete for 9001")
	}

	c.delMember("127.0.0.1:9001")

	if pendingCount(c, "127.0.0.1:9001") != 0 {
		t.Fatal("cluster.delMember did not drop queued retries for the removed peer")
	}
}

// 3c (strengthens issue7 test 3). Unregister must remove the departing member's
// route from the local table and close its conn pool, not only notify peers.
func TestUnregisterClearsRouteAndPool(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, rc := newMasterCluster(t)
	c.notifyDelMember = (&delSpy{failPeers: map[string]bool{}}).notify

	if _, err := c.Register(context.Background(), &clusterpb.RegisterRequest{MemberInfo: memberInfo("127.0.0.1:9002", "Map")}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := rc.createConnPool("127.0.0.1:9002"); err != nil {
		t.Fatalf("createConnPool: %v", err)
	}
	if !routable(c, "Map", "127.0.0.1:9002") {
		t.Fatal("setup: 9002 should be routable for Map")
	}

	if _, err := c.Unregister(context.Background(), &clusterpb.UnregisterRequest{ServiceAddr: "127.0.0.1:9002"}); err != nil {
		t.Fatalf("unregister: %v", err)
	}

	if routable(c, "Map", "127.0.0.1:9002") {
		t.Fatal("departing member's route was not purged from the local table")
	}
	rc.RLock()
	_, pooled := rc.pools["127.0.0.1:9002"]
	rc.RUnlock()
	if pooled {
		t.Fatal("departing member's conn pool was not closed/removed")
	}
}

// warnCaptureLogger counts Warn calls (matching noopLogger's method set) so a
// test can assert the bound-overflow warning fires instead of a silent drop.
type warnCaptureLogger struct {
	mu    sync.Mutex
	warns int
}

func (l *warnCaptureLogger) Warn(...interface{})         { l.mu.Lock(); l.warns++; l.mu.Unlock() }
func (l *warnCaptureLogger) warnCount() int              { l.mu.Lock(); defer l.mu.Unlock(); return l.warns }
func (*warnCaptureLogger) Debug(...interface{})          {}
func (*warnCaptureLogger) Println(...interface{})        {}
func (*warnCaptureLogger) Infof(string, ...interface{})  {}
func (*warnCaptureLogger) Error(...interface{})          {}
func (*warnCaptureLogger) Errorf(string, ...interface{}) {}
func (*warnCaptureLogger) Fatal(...interface{})          {}
func (*warnCaptureLogger) Fatalf(string, ...interface{}) {}

// 4c. Overflowing the ledger bound must log a warning, not drop the repair
// silently (issue #7 review round 1 changed silent-drop -> warn).
func TestRecordPendingDeleteBoundLogsWarning(t *testing.T) {
	capLog := &warnCaptureLogger{}
	log.SetLogger(capLog)
	defer log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)

	for i := 0; i < maxPendingDeletePeers; i++ {
		c.recordPendingDelete(fmt.Sprintf("peer:%d", i), "dead:x")
	}
	if capLog.warnCount() != 0 {
		t.Fatalf("no warning expected before the bound is exceeded, got %d", capLog.warnCount())
	}
	// One more distinct peer overflows the bound -> must warn.
	c.recordPendingDelete("overflow-peer", "dead:x")
	if capLog.warnCount() == 0 {
		t.Fatal("recordPendingDelete dropped a retry at the bound without logging a warning")
	}
}

// 5b. Stress the unknown-heartbeat vs Unregister interleaving for the same
// address. Invariant after every round: if the address is not a known member,
// it must not be routable (a removed member's route must never be reinstalled by
// a racing heartbeat). Run under -race to exercise the c.mu critical section.
func TestUnknownHeartbeatRaceUnregisterNoStaleRoute(t *testing.T) {
	log.SetLogger(&noopLogger{})
	const addr = "127.0.0.1:9007"
	for iter := 0; iter < 200; iter++ {
		c, _ := newMasterCluster(t)
		c.notifyDelMember = (&delSpy{failPeers: map[string]bool{}}).notify

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, _ = c.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{MemberInfo: memberInfo(addr, "World")})
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _ = c.Unregister(context.Background(), &clusterpb.UnregisterRequest{ServiceAddr: addr})
		}()
		close(start)
		wg.Wait()

		if !c.isKnownAddr(addr) && routable(c, "World", addr) {
			t.Fatalf("iter %d: %s routable but not a member -> stale route reinstalled after removal", iter, addr)
		}
	}
}

// 5c. Stress the same-address Register vs Unregister interleaving (a rolling
// rollover: old pod unregistering while the new pod registers). Invariant after
// every round: membership and the local route table agree — the address is a
// known member iff it is routable for its service. The pre-fix non-atomic
// version could leave a route for a removed member (issue #7, review round 3).
func TestRegisterUnregisterRaceMembershipRouteConsistent(t *testing.T) {
	log.SetLogger(&noopLogger{})
	const addr = "127.0.0.1:9008"
	for iter := 0; iter < 200; iter++ {
		c, _ := newMasterCluster(t)
		c.notifyDelMember = (&delSpy{failPeers: map[string]bool{}}).notify
		// Seed so Unregister has a target and the concurrent Register is a rejoin.
		if _, err := c.Register(context.Background(), &clusterpb.RegisterRequest{MemberInfo: memberInfo(addr, "Svc")}); err != nil {
			t.Fatalf("seed register: %v", err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, _ = c.Register(context.Background(), &clusterpb.RegisterRequest{MemberInfo: memberInfo(addr, "Svc")})
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _ = c.Unregister(context.Background(), &clusterpb.UnregisterRequest{ServiceAddr: addr})
		}()
		close(start)
		wg.Wait()

		if member, routed := c.isKnownAddr(addr), routable(c, "Svc", addr); member != routed {
			t.Fatalf("iter %d: membership/route diverged: isKnownAddr=%v routable=%v", iter, member, routed)
		}
	}
}

// A3. The peer DelMember receive path must purge the route to the departed
// address (the production hop the spy-based retry test stubs out): retry ->
// peer Node.DelMember -> handler.delMember (issue #7 review round 4).
func TestPeerDelMemberDropsRoute(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := newTestNode()
	n.handler.addRemoteService(memberInfo("backend:7", "Map"))
	if !routable(n.cluster, "Map", "backend:7") {
		t.Fatal("setup: backend:7 should be routable for Map")
	}
	if _, err := n.DelMember(context.Background(), &clusterpb.DelMemberRequest{ServiceAddr: "backend:7"}); err != nil {
		t.Fatalf("DelMember: %v", err)
	}
	if routable(n.cluster, "Map", "backend:7") {
		t.Fatal("Node.DelMember did not purge the peer's route to the departed address")
	}
}

// 5d. Stress the peer NewMember vs DelMember interleaving for the same address
// (a rollover broadcast race on a non-master node). Invariant: peer membership
// and the local route table agree — member iff routable (issue #7 round 4).
func TestPeerNewMemberDelMemberRaceConsistent(t *testing.T) {
	log.SetLogger(&noopLogger{})
	const addr = "backend:9"
	for iter := 0; iter < 200; iter++ {
		n := newTestNode()
		if _, err := n.NewMember(context.Background(), &clusterpb.NewMemberRequest{MemberInfo: memberInfo(addr, "Svc")}); err != nil {
			t.Fatalf("seed NewMember: %v", err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, _ = n.NewMember(context.Background(), &clusterpb.NewMemberRequest{MemberInfo: memberInfo(addr, "Svc")})
		}()
		go func() {
			defer wg.Done()
			<-start
			_, _ = n.DelMember(context.Background(), &clusterpb.DelMemberRequest{ServiceAddr: addr})
		}()
		close(start)
		wg.Wait()

		if member, routed := n.cluster.isKnownAddr(addr), routable(n.cluster, "Svc", addr); member != routed {
			t.Fatalf("iter %d: peer membership/route diverged: isKnownAddr=%v routable=%v", iter, member, routed)
		}
	}
}

// 4d. The master heartbeat tick prunes ledger entries for peers that have left
// (they never heartbeat to flush), so a snapshot-fan-out race cannot accumulate
// dead keys and fill the bound (issue #7, review round 5).
func TestPruneStalePendingDeletesDropsDepartedPeers(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)

	c.addMember(memberInfo("live-peer:1", "Svc")) // a live member peer
	c.recordPendingDelete("live-peer:1", "dead-target:1")
	c.recordPendingDelete("gone-peer:1", "dead-target:1") // never registered as a member

	c.pruneStalePendingDeletes()

	if pendingCount(c, "live-peer:1") != 1 {
		t.Fatal("prune dropped a queued delete for a still-live member peer")
	}
	if pendingCount(c, "gone-peer:1") != 0 {
		t.Fatal("prune did not drop the queued delete for a departed (non-member) peer")
	}
}
