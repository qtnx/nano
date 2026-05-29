package cluster

import (
	"context"
	"testing"
	"time"

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
	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{self, known, missed}, nil, 0, 0)

	if len(added) != 1 || added[0].ServiceAddr != missed.ServiceAddr {
		t.Fatalf("added = %v, want exactly [%s]", added, missed.ServiceAddr)
	}
	if len(updated) != 0 {
		t.Fatalf("updated = %v, want none", updated)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none", removed)
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
	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{a}, nil, 0, 0)
	if len(added) != 0 || len(updated) != 0 {
		t.Fatalf("reconcile reported added=%v updated=%v, want no route changes", added, updated)
	}
	if len(removed) != 0 {
		t.Fatalf("reconcile reported removed=%v, want no removals", removed)
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
	c.membershipEpoch = 10

	if !c.delMember(deleted.ServiceAddr, 2, 10) {
		t.Fatal("versioned DelMember was not applied")
	}
	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{deleted}, nil, 1, 10)

	if len(added) != 0 || len(updated) != 0 {
		t.Fatalf("stale heartbeat resurrected deleted member: added=%v updated=%v", added, updated)
	}
	if len(removed) != 0 {
		t.Fatalf("stale heartbeat reported removed=%v, want none", removed)
	}
	if countMemberAddr(c, deleted.ServiceAddr) != 0 {
		t.Fatalf("deleted member %s was re-added", deleted.ServiceAddr)
	}
}

func TestReconcileMembersAcceptsLowerVersionFromNewMasterEpoch(t *testing.T) {
	known := mkMemberInfo("user-service", "user:8085", "UserService")
	missed := mkMemberInfo("world-map-service", "worldmap:8088", "MapService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: known}}
	c.membershipVersion = 10
	c.membershipEpoch = 100

	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{known, missed}, nil, 1, 200)

	if len(added) != 1 || added[0].ServiceAddr != missed.ServiceAddr {
		t.Fatalf("new master epoch did not add missed member: added=%v", added)
	}
	if len(updated) != 0 || len(removed) != 0 {
		t.Fatalf("updated=%v removed=%v, want none", updated, removed)
	}
	if c.membershipEpoch != 200 || c.membershipVersion != 1 {
		t.Fatalf("membership state = epoch %d version %d, want epoch 200 version 1", c.membershipEpoch, c.membershipVersion)
	}
}

func TestReconcileMembersAcceptsLowerEpochFromRestartedMaster(t *testing.T) {
	known := mkMemberInfo("user-service", "user:8085", "UserService")
	missed := mkMemberInfo("world-map-service", "worldmap:8088", "MapService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: known}}
	c.membershipVersion = 10
	c.membershipEpoch = 200

	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{known, missed}, nil, 1, 100)

	if len(added) != 1 || added[0].ServiceAddr != missed.ServiceAddr {
		t.Fatalf("lower restarted master epoch did not add missed member: added=%v", added)
	}
	if len(updated) != 0 || len(removed) != 0 {
		t.Fatalf("updated=%v removed=%v, want none", updated, removed)
	}
	if c.membershipEpoch != 100 || c.membershipVersion != 1 {
		t.Fatalf("membership state = epoch %d version %d, want epoch 100 version 1", c.membershipEpoch, c.membershipVersion)
	}
}

func TestReconcileMembersSkipsOlderHeartbeatSnapshotAfterNewEpochAccepted(t *testing.T) {
	newMember := mkMemberInfo("world-map-service", "worldmap:8088", "MapService")
	oldMember := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: newMember, membershipVersion: 1, membershipEpoch: 200}}
	c.membershipVersion = 1
	c.membershipEpoch = 200
	c.membershipSnapshotSeq = 2

	added, updated, removed := c.reconcileMembersSnapshot([]*clusterpb.MemberInfo{oldMember}, nil, 10, 100, 1)

	if len(added) != 0 || len(updated) != 0 || len(removed) != 0 {
		t.Fatalf("stale heartbeat snapshot changed membership: added=%v updated=%v removed=%v", added, updated, removed)
	}
	if c.membershipEpoch != 200 || c.membershipVersion != 1 {
		t.Fatalf("membership state changed to epoch %d version %d", c.membershipEpoch, c.membershipVersion)
	}
	if countMemberAddr(c, oldMember.ServiceAddr) != 0 {
		t.Fatalf("stale heartbeat snapshot added old member %s", oldMember.ServiceAddr)
	}
	if countMemberAddr(c, newMember.ServiceAddr) != 1 {
		t.Fatalf("current member %s count = %d, want 1", newMember.ServiceAddr, countMemberAddr(c, newMember.ServiceAddr))
	}
}

func TestReconcileMembersAcceptsFirstEpochWhenLocalVersionIsHigher(t *testing.T) {
	missed := mkMemberInfo("world-map-service", "worldmap:8088", "MapService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.membershipVersion = 10

	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{missed}, nil, 1, 200)

	if len(added) != 1 || added[0].ServiceAddr != missed.ServiceAddr {
		t.Fatalf("first master epoch did not add missed member: added=%v", added)
	}
	if len(updated) != 0 || len(removed) != 0 {
		t.Fatalf("updated=%v removed=%v, want none", updated, removed)
	}
	if c.membershipEpoch != 200 || c.membershipVersion != 1 {
		t.Fatalf("membership state = epoch %d version %d, want epoch 200 version 1", c.membershipEpoch, c.membershipVersion)
	}
}

func TestNewMemberSkipsDifferentEpochEventUntilHeartbeatReconcile(t *testing.T) {
	newInfo := mkMemberInfo("world-map-service", "worldmap:8088", "MapService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.membershipVersion = 1
	c.membershipEpoch = 200

	if c.addMember(newInfo, 1, 100) {
		t.Fatal("different-epoch NewMember event was applied before heartbeat reconciliation")
	}
	if countMemberAddr(c, newInfo.ServiceAddr) != 0 {
		t.Fatalf("different-epoch NewMember added %s before heartbeat reconciliation", newInfo.ServiceAddr)
	}
}

func TestDelMemberSkipsDifferentEpochEventUntilHeartbeatReconcile(t *testing.T) {
	known := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: known, membershipVersion: 1, membershipEpoch: 200}}
	c.membershipVersion = 1
	c.membershipEpoch = 200

	if c.delMember(known.ServiceAddr, 1, 100) {
		t.Fatal("different-epoch DelMember event was applied before heartbeat reconciliation")
	}
	if countMemberAddr(c, known.ServiceAddr) != 1 {
		t.Fatalf("different-epoch DelMember removed %s before heartbeat reconciliation", known.ServiceAddr)
	}
}

func TestReconcileMembersAllowsNewerHeartbeatAfterMissedReRegister(t *testing.T) {
	restarted := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: restarted}}
	c.membershipVersion = 1
	c.membershipEpoch = 10

	if !c.delMember(restarted.ServiceAddr, 2, 10) {
		t.Fatal("versioned DelMember was not applied")
	}
	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{restarted}, nil, 3, 10)

	if len(added) != 1 || added[0].ServiceAddr != restarted.ServiceAddr {
		t.Fatalf("newer heartbeat did not re-add restarted member: added=%v", added)
	}
	if len(updated) != 0 {
		t.Fatalf("updated = %v, want none", updated)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none", removed)
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
	c.membershipEpoch = 10

	if !c.delMember(restarted.ServiceAddr, 2, 10) {
		t.Fatal("versioned DelMember was not applied")
	}
	if !c.addMember(restarted, 3, 10) {
		t.Fatal("newer NewMember was not applied")
	}

	if countMemberAddr(c, restarted.ServiceAddr) != 1 {
		t.Fatalf("restarted member count = %d, want 1", countMemberAddr(c, restarted.ServiceAddr))
	}
}

func TestDelMemberSkipsStaleDeleteAfterReRegister(t *testing.T) {
	restarted := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: restarted, membershipVersion: 3, membershipEpoch: 10}}
	c.membershipVersion = 3
	c.membershipEpoch = 10

	if c.delMember(restarted.ServiceAddr, 2, 10) {
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

	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{known}, nil, 0, 0)
	if len(added) != 0 || len(updated) != 0 {
		t.Fatalf("re-reconciling a known member reported changes: added=%v updated=%v", added, updated)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none", removed)
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

	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{newInfo}, nil, 0, 0)
	if len(added) != 0 {
		t.Fatalf("added = %v, want none for same address", added)
	}
	if len(updated) != 1 || updated[0].ServiceAddr != newInfo.ServiceAddr {
		t.Fatalf("updated = %v, want exactly [%s]", updated, newInfo.ServiceAddr)
	}
	if len(removed) != 0 {
		t.Fatalf("removed = %v, want none", removed)
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

	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{newInfo}, nil, 0, 0)
	for _, info := range updated {
		h.delMember(info.ServiceAddr)
		h.addRemoteService(info)
	}
	for _, info := range added {
		h.addRemoteService(info)
	}
	for _, addr := range removed {
		h.delMember(addr)
	}

	if members := h.findMembers("UserService"); len(members) != 0 {
		t.Fatalf("old service route still exists: %v", members)
	}
	members := h.findMembers("UserServiceV2")
	if len(members) != 1 || members[0].ServiceAddr != newInfo.ServiceAddr {
		t.Fatalf("new service route members = %v, want one %s", members, newInfo.ServiceAddr)
	}
}

func TestReconcileMembersRemovesHeartbeatTombstones(t *testing.T) {
	stale := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: stale}}
	c.membershipVersion = 1
	c.membershipEpoch = 10
	h := NewHandler(nil, nil)
	h.addRemoteService(stale)

	added, updated, removed := c.reconcileMembers(nil, []*clusterpb.RemovedMember{{
		ServiceAddr:       stale.ServiceAddr,
		MembershipVersion: 2,
	}}, 2, 10)
	for _, addr := range removed {
		h.delMember(addr)
	}

	if len(added) != 0 || len(updated) != 0 {
		t.Fatalf("added=%v updated=%v, want none", added, updated)
	}
	if len(removed) != 1 || removed[0] != stale.ServiceAddr {
		t.Fatalf("removed=%v, want exactly [%s]", removed, stale.ServiceAddr)
	}
	if countMemberAddr(c, stale.ServiceAddr) != 0 {
		t.Fatalf("tombstoned member %s remains in cluster", stale.ServiceAddr)
	}
	if members := h.findMembers("UserService"); len(members) != 0 {
		t.Fatalf("tombstoned member route still exists: %v", members)
	}
}

func TestReconcileMembersAppliesTombstoneOlderThanGlobalVersionWhenMemberIsOlder(t *testing.T) {
	stale := mkMemberInfo("user-service", "user:8085", "UserService")
	other := mkMemberInfo("world-map-service", "worldmap:8088", "MapService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{
		{memberInfo: stale, membershipVersion: 1, membershipEpoch: 10},
		{memberInfo: other, membershipVersion: 7, membershipEpoch: 10},
	}
	c.membershipVersion = 7
	c.membershipEpoch = 10

	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{other}, []*clusterpb.RemovedMember{{
		ServiceAddr:       stale.ServiceAddr,
		MembershipVersion: 6,
	}}, 7, 10)

	if len(added) != 0 || len(updated) != 0 {
		t.Fatalf("added=%v updated=%v, want none", added, updated)
	}
	if len(removed) != 1 || removed[0] != stale.ServiceAddr {
		t.Fatalf("removed=%v, want exactly [%s]", removed, stale.ServiceAddr)
	}
	if countMemberAddr(c, stale.ServiceAddr) != 0 {
		t.Fatalf("stale member %s remains in cluster", stale.ServiceAddr)
	}
	if countMemberAddr(c, other.ServiceAddr) != 1 {
		t.Fatalf("other member %s count = %d, want 1", other.ServiceAddr, countMemberAddr(c, other.ServiceAddr))
	}
}

func TestReconcileMembersSkipsStaleTombstoneAfterReRegister(t *testing.T) {
	restarted := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{currentNode: &Node{ServiceAddr: "gateway:8080"}}
	c.members = []*Member{{memberInfo: restarted, membershipVersion: 3, membershipEpoch: 10}}
	c.membershipVersion = 3
	c.membershipEpoch = 10

	added, updated, removed := c.reconcileMembers([]*clusterpb.MemberInfo{restarted}, []*clusterpb.RemovedMember{{
		ServiceAddr:       restarted.ServiceAddr,
		MembershipVersion: 2,
	}}, 3, 10)

	if len(added) != 0 || len(updated) != 0 || len(removed) != 0 {
		t.Fatalf("stale tombstone changed membership: added=%v updated=%v removed=%v", added, updated, removed)
	}
	if countMemberAddr(c, restarted.ServiceAddr) != 1 {
		t.Fatalf("restarted member count = %d, want 1", countMemberAddr(c, restarted.ServiceAddr))
	}
}

func TestRegisterClearsRemovedMemberTombstoneForSameAddress(t *testing.T) {
	restarted := mkMemberInfo("user-service", "user:8085", "UserService")
	c := &cluster{
		currentNode: &Node{ServiceAddr: "master:8085", handler: NewHandler(nil, nil)},
		removedMembers: map[string]removedMemberTombstone{
			restarted.ServiceAddr: {membershipVersion: 2, removedAt: time.Now()},
		},
		membershipVersion: 2,
		membershipEpoch:   10,
	}

	resp, err := c.Register(context.Background(), &clusterpb.RegisterRequest{MemberInfo: restarted})
	if err != nil {
		t.Fatalf("Register returned error: %v", err)
	}
	if resp.GetMembershipVersion() != 3 || resp.GetMembershipEpoch() != 10 {
		t.Fatalf("Register membership state = version %d epoch %d, want version 3 epoch 10", resp.GetMembershipVersion(), resp.GetMembershipEpoch())
	}
	if _, ok := c.removedMembers[restarted.ServiceAddr]; ok {
		t.Fatalf("removed member tombstone for %s was not cleared", restarted.ServiceAddr)
	}
}

func TestUnregisterKeepsStateChangeWhenPeerNotificationsFail(t *testing.T) {
	removed := mkMemberInfo("user-service", "user:8085", "UserService")
	peerA := mkMemberInfo("world-map-service", "worldmap:8088", "MapService")
	peerB := mkMemberInfo("chat-service", "chat:8090", "ChatService")
	h := NewHandler(nil, nil)
	h.addRemoteService(removed)
	c := &cluster{
		currentNode:       &Node{ServiceAddr: "master:8085", handler: h},
		rpcClient:         &rpcClient{isClosed: true, pools: map[string]*connPool{}},
		membershipVersion: 4,
		membershipEpoch:   10,
		members: []*Member{
			{memberInfo: removed, membershipVersion: 4, membershipEpoch: 10},
			{memberInfo: peerA, membershipVersion: 4, membershipEpoch: 10},
			{memberInfo: peerB, membershipVersion: 4, membershipEpoch: 10},
		},
	}

	resp, err := c.Unregister(context.Background(), &clusterpb.UnregisterRequest{ServiceAddr: removed.ServiceAddr})
	if err != nil {
		t.Fatalf("Unregister returned notification error after mutating master state: %v", err)
	}
	if resp == nil {
		t.Fatal("Unregister response is nil")
	}
	if countMemberAddr(c, removed.ServiceAddr) != 0 {
		t.Fatalf("removed member %s still exists", removed.ServiceAddr)
	}
	if c.removedMembers[removed.ServiceAddr].membershipVersion != 5 {
		t.Fatalf("removed member tombstone version = %d, want 5", c.removedMembers[removed.ServiceAddr].membershipVersion)
	}
	if members := h.findMembers("UserService"); len(members) != 0 {
		t.Fatalf("removed member route still exists locally: %v", members)
	}
	if countMemberAddr(c, peerA.ServiceAddr) != 1 || countMemberAddr(c, peerB.ServiceAddr) != 1 {
		t.Fatalf("unrelated peers were changed: peerA=%d peerB=%d", countMemberAddr(c, peerA.ServiceAddr), countMemberAddr(c, peerB.ServiceAddr))
	}
}

func TestNewMemberRefreshesExistingRemoteServiceRoutes(t *testing.T) {
	oldInfo := mkMemberInfo("user-service", "user:8085", "UserService")
	newInfo := mkMemberInfo("user-service", "user:8085", "UserServiceV2")
	n := &Node{
		cluster: &cluster{},
		handler: NewHandler(nil, nil),
	}
	n.cluster.addMember(oldInfo, 1, 10)
	n.handler.addRemoteService(oldInfo)

	if _, err := n.NewMember(context.Background(), &clusterpb.NewMemberRequest{MemberInfo: newInfo, MembershipVersion: 2, MembershipEpoch: 10}); err != nil {
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
	c.membershipEpoch = 10
	c.removedMembers = map[string]removedMemberTombstone{
		"old:8085": {membershipVersion: 6, removedAt: time.Now()},
	}

	resp, err := c.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{MemberInfo: memberA})
	if err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}
	if resp.GetMembershipVersion() != 7 {
		t.Fatalf("Heartbeat response membershipVersion = %d, want 7", resp.GetMembershipVersion())
	}
	if resp.GetMembershipEpoch() != 10 {
		t.Fatalf("Heartbeat response membershipEpoch = %d, want 10", resp.GetMembershipEpoch())
	}
	got := map[string]bool{}
	for _, m := range resp.GetMembers() {
		got[m.GetServiceAddr()] = true
	}
	if !got[memberA.ServiceAddr] || !got[memberB.ServiceAddr] {
		t.Fatalf("Heartbeat response members = %v, want both %s and %s", got, memberA.ServiceAddr, memberB.ServiceAddr)
	}
	if len(resp.GetRemovedMembers()) != 1 || resp.GetRemovedMembers()[0].GetServiceAddr() != "old:8085" || resp.GetRemovedMembers()[0].GetMembershipVersion() != 6 {
		t.Fatalf("Heartbeat response removed members = %v, want old:8085 at version 6", resp.GetRemovedMembers())
	}
}

func TestHeartbeatPrunesExpiredRemovedMemberTombstones(t *testing.T) {
	master := &Node{ServiceAddr: "master:8085"}
	master.IsMaster = true
	c := &cluster{currentNode: master}
	member := mkMemberInfo("user-service", "user:8085")
	c.members = []*Member{{memberInfo: member, isMaster: false}}
	c.membershipVersion = 7
	c.membershipEpoch = 10
	c.removedMembers = map[string]removedMemberTombstone{
		"expired:8085": {membershipVersion: 5, removedAt: time.Now().Add(-removedMemberRetention() - time.Second)},
		"recent:8085":  {membershipVersion: 6, removedAt: time.Now()},
	}

	resp, err := c.Heartbeat(context.Background(), &clusterpb.HeartbeatRequest{MemberInfo: member})
	if err != nil {
		t.Fatalf("Heartbeat returned error: %v", err)
	}

	if _, ok := c.removedMembers["expired:8085"]; ok {
		t.Fatal("expired tombstone was not pruned")
	}
	if _, ok := c.removedMembers["recent:8085"]; !ok {
		t.Fatal("recent tombstone was pruned")
	}
	if len(resp.GetRemovedMembers()) != 1 || resp.GetRemovedMembers()[0].GetServiceAddr() != "recent:8085" {
		t.Fatalf("Heartbeat response removed members = %v, want only recent:8085", resp.GetRemovedMembers())
	}
}

func TestRememberRemovedMemberPrunesExpiredTombstones(t *testing.T) {
	c := &cluster{
		removedMembers: map[string]removedMemberTombstone{
			"expired:8085": {membershipVersion: 5, removedAt: time.Now().Add(-removedMemberRetention() - time.Second)},
			"recent:8085":  {membershipVersion: 6, removedAt: time.Now()},
		},
	}

	c.rememberRemovedMemberLocked("new:8085", 7)

	if _, ok := c.removedMembers["expired:8085"]; ok {
		t.Fatal("expired tombstone was not pruned while remembering a new removal")
	}
	if _, ok := c.removedMembers["recent:8085"]; !ok {
		t.Fatal("recent tombstone was pruned while remembering a new removal")
	}
	if got := c.removedMembers["new:8085"].membershipVersion; got != 7 {
		t.Fatalf("new tombstone version = %d, want 7", got)
	}
}
