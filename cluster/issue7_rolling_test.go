// Copyright (c) nano Authors. All Rights Reserved.
//
// Regression tests for issue #7: "Fix Nano stale member routing during rolling
// rollouts". They exercise the two root causes and their fixes end-to-end:
//   1. a failed DelMember notification must be queued and retried (not just
//      logged) so a peer that briefly missed it stops routing to a dead pod;
//   2. a same-address rejoin with a changed/shrunk service list must purge the
//      stale services so they are no longer routable.
//
// White-box (package cluster) so the tests can read unexported fields/methods
// and override the notify seam. They are written against the cluster.go
// CONTRACT (pendingDeletes / notifyDelMember and the pending-delete helpers);
// no production code is implemented here.

package cluster

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/internal/log"
)

// delCall records a single notifyDelMember invocation.
type delCall struct{ peer, deleted string }

// delSpy is a recording stub for the cluster.notifyDelMember seam. It captures
// every (peer, deleted) pair and can be configured to fail for specific peers
// so the pending-delete queue/retry path can be driven deterministically.
type delSpy struct {
	mu        sync.Mutex
	calls     []delCall
	failPeers map[string]bool // peer -> notify returns error
}

func (s *delSpy) notify(peer, deleted string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, delCall{peer, deleted})
	if s.failPeers[peer] {
		return errors.New("unreachable")
	}
	return nil
}

func (s *delSpy) sent(peer, deleted string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		if c.peer == peer && c.deleted == deleted {
			return true
		}
	}
	return false
}

func (s *delSpy) sentToPeer(peer string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.calls {
		if c.peer == peer {
			return true
		}
	}
	return false
}

func (s *delSpy) count() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.calls) }

// countPair returns how many times the exact (peer, deleted) pair was notified.
// Unlike sent, it lets a test distinguish a retry from the original call.
func (s *delSpy) countPair(peer, deleted string) int {
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

// pendingCount reads the size of the pending-delete ledger for peer under the
// cluster lock.
func pendingCount(c *cluster, peer string) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.pendingDeletes[peer])
}

// memberCount returns the number of registered members under the cluster lock.
func memberCount(c *cluster) int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.members)
}

// lastHeartbeat returns the recorded lastHeartbeatAt for addr (and whether the
// member exists) under the cluster lock.
func lastHeartbeat(c *cluster, addr string) (time.Time, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, m := range c.members {
		if m.memberInfo.ServiceAddr == addr {
			return m.lastHeartbeatAt, true
		}
	}
	return time.Time{}, false
}

// routableCount reports how many times addr is selectable for service in the
// handler's routing table (a copy is returned by findMembers).
func routableCount(c *cluster, service, addr string) int {
	n := 0
	for _, m := range c.currentNode.handler.findMembers(service) {
		if m.ServiceAddr == addr {
			n++
		}
	}
	return n
}

// routable reports whether addr is selectable for service at all.
func routable(c *cluster, service, addr string) bool {
	return routableCount(c, service, addr) > 0
}

//  1. A new pod must register successfully even when an existing peer is
//     unreachable: the join is local-first and the new member becomes routable
//     regardless of best-effort peer fan-out failures (fixes 1 & 2).
func TestNewPodRegisterSucceedsWhenPeerUnreachableIssue7(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, rc := newMasterCluster(t)
	c.addMember(memberInfo("127.0.0.1:9001", "peer"))
	rc.closePool() // every getConnPool now errors -> peer fan-out must be best-effort

	resp, err := c.Register(context.Background(), &clusterpb.RegisterRequest{
		MemberInfo: memberInfo("127.0.0.1:9002", "Map"),
	})
	if err != nil {
		t.Fatalf("Register aborted on an unreachable peer (issue #7 fix 1): %v", err)
	}
	if resp == nil {
		t.Fatal("nil register response")
	}
	if !c.isKnownAddr("127.0.0.1:9002") {
		t.Fatal("new member not added to the local registry despite peer fan-out failure")
	}
	if !routable(c, "Map", "127.0.0.1:9002") {
		t.Fatal("new member 127.0.0.1:9002 is not routable for service Map after register")
	}
}

//  2. A same-address rejoin that drops a service must leave no stale route for
//     that service under the old address (fixes 2 & 7).
func TestSameAddrRejoinRemovesOldServiceRoutes(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)

	if _, err := c.Register(context.Background(), &clusterpb.RegisterRequest{
		MemberInfo: memberInfo("127.0.0.1:9001", "Map", "Chat"),
	}); err != nil {
		t.Fatalf("initial Register failed: %v", err)
	}
	if !routable(c, "Map", "127.0.0.1:9001") {
		t.Fatal("127.0.0.1:9001 not routable for Map after initial register")
	}
	if !routable(c, "Chat", "127.0.0.1:9001") {
		t.Fatal("127.0.0.1:9001 not routable for Chat after initial register")
	}

	// Rejoin with the SAME address but only the Map service.
	if _, err := c.Register(context.Background(), &clusterpb.RegisterRequest{
		MemberInfo: memberInfo("127.0.0.1:9001", "Map"),
	}); err != nil {
		t.Fatalf("rejoin Register failed: %v", err)
	}
	if got := routableCount(c, "Map", "127.0.0.1:9001"); got != 1 {
		t.Fatalf("expected 127.0.0.1:9001 routable for Map exactly once after rejoin, got %d", got)
	}
	if routable(c, "Chat", "127.0.0.1:9001") {
		t.Fatal("stale Chat route for 127.0.0.1:9001 survived a rejoin that dropped the service")
	}
}

//  3. Unregister must tell remaining peers to drop the departing member (via the
//     notify seam) without notifying the departing member itself, and clear it
//     from the local registry (fix 3).
func TestUnregisterBroadcastsDeleteAndClearsRoutes(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	spy := &delSpy{failPeers: map[string]bool{}}
	c.notifyDelMember = spy.notify

	c.addMember(memberInfo("127.0.0.1:9002", "Map"))  // the departing member
	c.addMember(memberInfo("127.0.0.1:9001", "Chat")) // a live peer to notify

	if _, err := c.Unregister(context.Background(), &clusterpb.UnregisterRequest{
		ServiceAddr: "127.0.0.1:9002",
	}); err != nil {
		t.Fatalf("Unregister returned error: %v", err)
	}
	if !spy.sent("127.0.0.1:9001", "127.0.0.1:9002") {
		t.Fatal("peer 127.0.0.1:9001 was not told to drop departing member 127.0.0.1:9002")
	}
	if spy.sentToPeer("127.0.0.1:9002") {
		t.Fatal("departing member 127.0.0.1:9002 must not be notified about its own removal")
	}
	if c.isKnownAddr("127.0.0.1:9002") {
		t.Fatal("departing member 127.0.0.1:9002 still present in the registry after Unregister")
	}
}

//  4. The headline fix: a DelMember notification that fails is queued and then
//     retried (and cleared) when the recipient peer next heartbeats (fix 4).
func TestMissedDeleteQueuedAndRetriedOnHeartbeat(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	spy := &delSpy{failPeers: map[string]bool{"127.0.0.1:9001": true}}
	c.notifyDelMember = spy.notify

	c.addMember(memberInfo("127.0.0.1:9001", "Chat")) // peer that will miss the delete
	c.addMember(memberInfo("127.0.0.1:9002", "Map"))  // the departing member

	if _, err := c.Unregister(context.Background(), &clusterpb.UnregisterRequest{
		ServiceAddr: "127.0.0.1:9002",
	}); err != nil {
		t.Fatalf("Unregister returned error: %v", err)
	}
	if got := pendingCount(c, "127.0.0.1:9001"); got != 1 {
		t.Fatalf("expected 1 pending delete queued for unreachable peer 127.0.0.1:9001, got %d", got)
	}

	// Peer becomes reachable again and heartbeats -> queued delete is retried.
	// notify records the call before returning the initial failure, so a bare
	// "was it ever sent" check is already true here. Capture the per-pair count
	// and require it to strictly increase so the assertion only passes when the
	// heartbeat actually re-issues the notification (not merely clears the
	// ledger).
	before := spy.countPair("127.0.0.1:9001", "127.0.0.1:9002")
	spy.failPeers["127.0.0.1:9001"] = false
	if _, err := c.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{
		MemberInfo: memberInfo("127.0.0.1:9001", "Chat"),
	}); err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	if after := spy.countPair("127.0.0.1:9001", "127.0.0.1:9002"); after <= before {
		t.Fatalf("queued delete for 127.0.0.1:9002 was not retried to 127.0.0.1:9001 on heartbeat: notify count stayed at %d", after)
	}
	if got := pendingCount(c, "127.0.0.1:9001"); got != 0 {
		t.Fatalf("expected pending deletes cleared after successful retry, got %d", got)
	}
}

//  5. A synchronous UnregisterCallback that re-registers the same address must
//     not deadlock Unregister, and the member must end up re-registered (fix 6).
func TestSyncUnregisterCallbackReregistersWithoutDeadlock(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	c.currentNode.UnregisterCallback = func(_ Member, reReg func()) { reReg() }

	c.addMember(memberInfo("127.0.0.1:9002", "Map"))

	done := make(chan struct{})
	go func() {
		_, _ = c.Unregister(context.Background(), &clusterpb.UnregisterRequest{
			ServiceAddr: "127.0.0.1:9002",
		})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Unregister deadlocked on synchronous re-register callback")
	}
	if !c.isKnownAddr("127.0.0.1:9002") {
		t.Fatal("synchronous callback did not re-register 127.0.0.1:9002")
	}
}

//  6. A heartbeat from a known, stable member only refreshes its timestamp: no
//     broadcast and no route churn (fix 5).
func TestStableHeartbeatNoBroadcastOrRouteChurn(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	spy := &delSpy{failPeers: map[string]bool{}}
	c.notifyDelMember = spy.notify

	if _, err := c.Register(context.Background(), &clusterpb.RegisterRequest{
		MemberInfo: memberInfo("127.0.0.1:9001", "Map"),
	}); err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	routesBefore := len(c.currentNode.handler.findMembers("Map"))
	membersBefore := memberCount(c)
	hbBefore, ok := lastHeartbeat(c, "127.0.0.1:9001")
	if !ok {
		t.Fatal("member 127.0.0.1:9001 missing after register")
	}

	time.Sleep(5 * time.Millisecond)

	if _, err := c.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{
		MemberInfo: memberInfo("127.0.0.1:9001", "Map"),
	}); err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}

	if got := spy.count(); got != 0 {
		t.Fatalf("stable heartbeat must not broadcast any delete, got %d notify calls", got)
	}
	if got := len(c.currentNode.handler.findMembers("Map")); got != routesBefore {
		t.Fatalf("route count for Map churned on stable heartbeat: before=%d after=%d", routesBefore, got)
	}
	if routableCount(c, "Map", "127.0.0.1:9001") != 1 {
		t.Fatal("127.0.0.1:9001 should remain routable for Map exactly once after stable heartbeat")
	}
	if got := memberCount(c); got != membersBefore {
		t.Fatalf("member count changed on stable heartbeat: before=%d after=%d", membersBefore, got)
	}
	hbAfter, ok := lastHeartbeat(c, "127.0.0.1:9001")
	if !ok {
		t.Fatal("member 127.0.0.1:9001 missing after heartbeat")
	}
	if !hbAfter.After(hbBefore) {
		t.Fatalf("lastHeartbeatAt did not advance on heartbeat: before=%s after=%s", hbBefore, hbAfter)
	}
}

//  7. After a master restart it no longer knows a live member; a heartbeat from
//     that unknown member must re-learn the member and its services (fix 7).
func TestMasterRestartHeartbeatRelearnsMemberServices(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)

	// 127.0.0.1:9003 was never registered with this (restarted) master.
	if _, err := c.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{
		MemberInfo: memberInfo("127.0.0.1:9003", "World"),
	}); err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	if !c.isKnownAddr("127.0.0.1:9003") {
		t.Fatal("unknown-member heartbeat did not re-learn 127.0.0.1:9003")
	}
	if !routable(c, "World", "127.0.0.1:9003") {
		t.Fatal("unknown-member heartbeat did not re-learn service World for 127.0.0.1:9003")
	}
}

//  8. When a peer that still has queued retries itself unregisters, those
//     retries must be dropped so they are never sent to a gone peer (fixes 4 & 8).
func TestUnregisterDropsPendingDeletesForDepartingPeer(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	spy := &delSpy{failPeers: map[string]bool{"127.0.0.1:9001": true}}
	c.notifyDelMember = spy.notify

	c.addMember(memberInfo("127.0.0.1:9001", "Chat")) // peer whose delete will be queued
	c.addMember(memberInfo("127.0.0.1:9002", "Map"))  // the first departing member

	if _, err := c.Unregister(context.Background(), &clusterpb.UnregisterRequest{
		ServiceAddr: "127.0.0.1:9002",
	}); err != nil {
		t.Fatalf("Unregister 9002 returned error: %v", err)
	}
	if got := pendingCount(c, "127.0.0.1:9001"); got != 1 {
		t.Fatalf("expected 1 queued delete for unreachable peer 127.0.0.1:9001, got %d", got)
	}

	// The peer itself now leaves -> its queued retries must be discarded.
	if _, err := c.Unregister(context.Background(), &clusterpb.UnregisterRequest{
		ServiceAddr: "127.0.0.1:9001",
	}); err != nil {
		t.Fatalf("Unregister 9001 returned error: %v", err)
	}
	if got := pendingCount(c, "127.0.0.1:9001"); got != 0 {
		t.Fatalf("queued retries for departed peer 127.0.0.1:9001 were not dropped, got %d", got)
	}
	if c.isKnownAddr("127.0.0.1:9001") {
		t.Fatal("departed peer 127.0.0.1:9001 still present in the registry")
	}
}
