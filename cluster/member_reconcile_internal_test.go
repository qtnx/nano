package cluster

import (
	"context"
	"testing"

	"github.com/lonng/nano/cluster/clusterpb"
)

func mkMemberInfo(label, addr string, services ...string) *clusterpb.MemberInfo {
	return &clusterpb.MemberInfo{Label: label, ServiceAddr: addr, Services: services}
}

func countMemberAddr(c *cluster, addr string) int {
	n := 0
	for _, m := range c.members {
		if m.memberInfo != nil && m.memberInfo.ServiceAddr == addr {
			n++
		}
	}
	return n
}

// A member that registered after this node's initial sync, whose NewMember push
// was lost during cluster churn, must be re-learned from the authoritative list.
// This is the gateway-readiness-wedge fix: pingNodes only sees c.members, so a
// missed push left the gateway permanently NotReady.
func TestReconcileMembersAddsMembersMissedByPush(t *testing.T) {
	self := mkMemberInfo("gateway", "gateway:8080")
	known := mkMemberInfo("master-service", "master:8085")
	c := &cluster{currentNode: &Node{ServiceAddr: self.ServiceAddr}}
	c.members = []*Member{{memberInfo: known}}

	missed := mkMemberInfo("world-map-service", "worldmap:8088", "MapService")
	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{self, known, missed})

	if len(added) != 1 || added[0].ServiceAddr != missed.ServiceAddr {
		t.Fatalf("added = %v, want exactly [%s]", added, missed.ServiceAddr)
	}
	if len(updated) != 0 {
		t.Fatalf("updated = %v, want none", updated)
	}
	if countMemberAddr(c, missed.ServiceAddr) != 1 {
		t.Fatalf("missed member %s not added exactly once: count=%d", missed.ServiceAddr, countMemberAddr(c, missed.ServiceAddr))
	}
	if countMemberAddr(c, self.ServiceAddr) != 0 {
		t.Fatalf("self %s must never be added as a remote member", self.ServiceAddr)
	}
	if len(c.members) != 2 {
		t.Fatalf("member count = %d, want 2 (known + missed)", len(c.members))
	}
}

// Reconcile is ADD-ONLY: a member absent from the authoritative list must NOT be
// removed. Pruning here would let a transient/incomplete master view (e.g. right
// after a master restart, before all heartbeats arrive) drop a live peer and
// break routing. Removal stays with the heartbeat-timeout/DelMember path.
func TestReconcileMembersNeverRemovesLivePeers(t *testing.T) {
	a := mkMemberInfo("user-service", "user:8085")
	b := mkMemberInfo("world-map-service", "worldmap:8088")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: a}, {memberInfo: b}}

	// Authoritative list omits b (incomplete master view).
	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{a})
	if len(added) != 0 || len(updated) != 0 {
		t.Fatalf("reconcile reported added=%v updated=%v, want no route changes", added, updated)
	}

	if countMemberAddr(c, b.ServiceAddr) != 1 {
		t.Fatalf("add-only reconcile removed live peer %s", b.ServiceAddr)
	}
	if len(c.members) != 2 {
		t.Fatalf("member count = %d, want 2 (no removals)", len(c.members))
	}
}

// Reconcile must be idempotent: a member already known is not duplicated, and a
// repeat reconcile reports nothing newly added (so callers never re-register an
// already-known member's routes — addRemoteService appends without dedup).
func TestReconcileMembersIdempotentNoDuplicates(t *testing.T) {
	known := mkMemberInfo("user-service", "user:8085")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: known}}

	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{known})
	if len(added) != 0 || len(updated) != 0 {
		t.Fatalf("re-reconciling a known member reported changes: added=%v updated=%v", added, updated)
	}
	if countMemberAddr(c, known.ServiceAddr) != 1 {
		t.Fatalf("known member duplicated: count=%d", countMemberAddr(c, known.ServiceAddr))
	}
}

// Reconcile must refresh same-address metadata. This handles a node restarting
// or redeploying at the same service address with a changed service list after a
// peer missed the NewMember push.
func TestReconcileMembersUpdatesSameAddressMetadata(t *testing.T) {
	oldInfo := mkMemberInfo("user-service", "user:8085", "UserService")
	newInfo := mkMemberInfo("user-service-v2", "user:8085", "UserServiceV2")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: oldInfo}}

	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{newInfo})
	if len(added) != 0 {
		t.Fatalf("added = %v, want none for same address", added)
	}
	if len(updated) != 1 || updated[0].ServiceAddr != newInfo.ServiceAddr {
		t.Fatalf("updated = %v, want exactly [%s]", updated, newInfo.ServiceAddr)
	}
	if countMemberAddr(c, newInfo.ServiceAddr) != 1 {
		t.Fatalf("member %s duplicated during update: count=%d", newInfo.ServiceAddr, countMemberAddr(c, newInfo.ServiceAddr))
	}
	got := c.members[0].memberInfo
	if got.Label != newInfo.Label || len(got.Services) != 1 || got.Services[0] != newInfo.Services[0] {
		t.Fatalf("member metadata = %+v, want %+v", got, newInfo)
	}
}

func TestUpdatedMemberRefreshesRemoteServiceRoutesWithoutDuplicates(t *testing.T) {
	oldInfo := mkMemberInfo("user-service", "user:8085", "UserService")
	newInfo := mkMemberInfo("user-service", "user:8085", "UserServiceV2")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: oldInfo}}
	h := NewHandler(nil, nil)
	h.addRemoteService(oldInfo)

	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{newInfo})
	for _, info := range updated {
		h.delMember(info.ServiceAddr)
		h.addRemoteService(info)
	}
	for _, info := range added {
		h.addRemoteService(info)
	}

	if members := h.findMembers("UserService"); len(members) != 0 {
		t.Fatalf("old service route still exists: %v", members)
	}
	members := h.findMembers("UserServiceV2")
	if len(members) != 1 || members[0].ServiceAddr != newInfo.ServiceAddr {
		t.Fatalf("new service route members = %v, want one %s", members, newInfo.ServiceAddr)
	}
}

// The master's Heartbeat response must carry its authoritative member list so
// members can reconcile missed NewMember pushes each heartbeat.
func TestHeartbeatResponseIncludesCurrentMembers(t *testing.T) {
	master := &Node{ServiceAddr: "master:8085"}
	master.IsMaster = true
	c := &cluster{currentNode: master}
	memberA := mkMemberInfo("user-service", "user:8085")
	memberB := mkMemberInfo("world-map-service", "worldmap:8088")
	c.members = []*Member{
		{memberInfo: memberA, isMaster: false},
		{memberInfo: memberB, isMaster: false},
	}

	resp, err := c.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{MemberInfo: memberA})
	if err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	got := map[string]bool{}
	for _, m := range resp.GetMembers() {
		got[m.GetServiceAddr()] = true
	}
	if !got[memberA.ServiceAddr] || !got[memberB.ServiceAddr] {
		t.Fatalf("Heartbeat response members = %v, want both %s and %s", got, memberA.ServiceAddr, memberB.ServiceAddr)
	}
}
