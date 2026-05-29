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
	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{self, known, missed}, 0)

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
	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{a}, 0)
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

// A heartbeat response built before DelMember can arrive after DelMember was
// already applied locally. That older snapshot must not re-add the deleted peer.
func TestReconcileMembersSkipsStaleHeartbeatAfterVersionedDelMember(t *testing.T) {
	deleted := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: deleted}}
	c.membershipVersion = 1

	if !c.delMember(deleted.ServiceAddr, 2) {
		t.Fatal("versioned DelMember was not applied")
	}
	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{deleted}, 1)

	if len(added) != 0 || len(updated) != 0 {
		t.Fatalf("stale heartbeat resurrected deleted member: added=%v updated=%v", added, updated)
	}
	if countMemberAddr(c, deleted.ServiceAddr) != 0 {
		t.Fatalf("deleted member %s was re-added", deleted.ServiceAddr)
	}
}

func TestReconcileMembersAllowsNewerHeartbeatAfterMissedReRegister(t *testing.T) {
	restarted := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: restarted}}
	c.membershipVersion = 1

	if !c.delMember(restarted.ServiceAddr, 2) {
		t.Fatal("versioned DelMember was not applied")
	}
	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{restarted}, 3)

	if len(added) != 1 || added[0].ServiceAddr != restarted.ServiceAddr {
		t.Fatalf("newer heartbeat did not re-add restarted member: added=%v", added)
	}
	if len(updated) != 0 {
		t.Fatalf("updated = %v, want none", updated)
	}
	if countMemberAddr(c, restarted.ServiceAddr) != 1 {
		t.Fatalf("restarted member count = %d, want 1", countMemberAddr(c, restarted.ServiceAddr))
	}
}

func TestAddMemberAppliesNewerReRegisterAfterDelete(t *testing.T) {
	restarted := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: restarted}}
	c.membershipVersion = 1

	if !c.delMember(restarted.ServiceAddr, 2) {
		t.Fatal("versioned DelMember was not applied")
	}
	if !c.addMember(restarted, 3) {
		t.Fatal("newer NewMember was not applied")
	}

	if countMemberAddr(c, restarted.ServiceAddr) != 1 {
		t.Fatalf("restarted member count = %d, want 1", countMemberAddr(c, restarted.ServiceAddr))
	}
}

func TestDelMemberSkipsStaleDeleteAfterReRegister(t *testing.T) {
	restarted := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: restarted}}
	c.membershipVersion = 3

	if c.delMember(restarted.ServiceAddr, 2) {
		t.Fatal("stale DelMember was applied")
	}
	if countMemberAddr(c, restarted.ServiceAddr) != 1 {
		t.Fatalf("member count after stale delete = %d, want 1", countMemberAddr(c, restarted.ServiceAddr))
	}
}

// Reconcile must be idempotent: a member already known is not duplicated, and a
// repeat reconcile reports nothing newly added.
func TestReconcileMembersIdempotentNoDuplicates(t *testing.T) {
	known := mkMemberInfo("user-service", "user:8085")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: known}}

	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{known}, 0)
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

	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{newInfo}, 0)
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

	added, updated := c.reconcileMembers([]*clusterpb.MemberInfo{newInfo}, 0)
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

func TestNewMemberRefreshesExistingRemoteServiceRoutes(t *testing.T) {
	oldInfo := mkMemberInfo("user-service", "user:8085", "UserService")
	newInfo := mkMemberInfo("user-service", "user:8085", "UserServiceV2")
	n := &Node{
		cluster: &cluster{},
		handler: NewHandler(nil, nil),
	}
	n.cluster.addMember(oldInfo, 1)
	n.handler.addRemoteService(oldInfo)

	if _, err := n.NewMember(context.Background(), &clusterpb.NewMemberRequest{MemberInfo: newInfo, MembershipVersion: 2}); err != nil {
		t.Fatalf("NewMember returned error: %v", err)
	}

	if members := n.handler.findMembers("UserService"); len(members) != 0 {
		t.Fatalf("old service route still exists after NewMember refresh: %v", members)
	}
	members := n.handler.findMembers("UserServiceV2")
	if len(members) != 1 || members[0].ServiceAddr != newInfo.ServiceAddr {
		t.Fatalf("new service route members = %v, want one %s", members, newInfo.ServiceAddr)
	}
	if countMemberAddr(n.cluster, newInfo.ServiceAddr) != 1 {
		t.Fatalf("member %s duplicated during NewMember refresh", newInfo.ServiceAddr)
	}
}

func TestAddRemoteServiceIsIdempotentByServiceAddr(t *testing.T) {
	h := NewHandler(nil, nil)
	info := mkMemberInfo("user-service", "user:8085", "UserService")

	h.addRemoteService(info)
	h.addRemoteService(info)

	members := h.findMembers("UserService")
	if len(members) != 1 || members[0].ServiceAddr != info.ServiceAddr {
		t.Fatalf("remote service members = %v, want one %s", members, info.ServiceAddr)
	}
}

func TestDelMemberRemovesDuplicateRemoteServiceRoutes(t *testing.T) {
	h := NewHandler(nil, nil)
	info := mkMemberInfo("user-service", "user:8085", "UserService")
	h.remoteServices["UserService"] = []*clusterpb.MemberInfo{info, info}

	h.delMember(info.ServiceAddr)

	if members := h.findMembers("UserService"); len(members) != 0 {
		t.Fatalf("duplicate remote service routes were not removed: %v", members)
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
	c.membershipVersion = 7

	resp, err := c.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{MemberInfo: memberA})
	if err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	if resp.GetMembershipVersion() != 7 {
		t.Fatalf("Heartbeat response membershipVersion = %d, want 7", resp.GetMembershipVersion())
	}
	got := map[string]bool{}
	for _, m := range resp.GetMembers() {
		got[m.GetServiceAddr()] = true
	}
	if !got[memberA.ServiceAddr] || !got[memberB.ServiceAddr] {
		t.Fatalf("Heartbeat response members = %v, want both %s and %s", got, memberA.ServiceAddr, memberB.ServiceAddr)
	}
}
