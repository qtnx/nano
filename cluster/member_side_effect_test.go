package cluster

import "testing"

func TestDelayedRemoveSideEffectDoesNotDeleteNewerReAdd(t *testing.T) {
	n := newTestNode()
	info := memberInfo("svc:8088", "Svc")
	if !n.cluster.addMember(info, 5, 10) {
		t.Fatal("initial addMember was not applied")
	}
	if !n.cluster.delMember(info.ServiceAddr, 6, 10) {
		t.Fatal("delMember was not applied")
	}
	if !n.cluster.addMember(info, 7, 10) {
		t.Fatal("newer re-add was not applied")
	}
	n.addRemoteMemberIfCurrent(info)

	// Simulate the old DelMember goroutine resuming after the newer re-add route
	// was already published. It must not tear down the live member's route/pool.
	n.removeRemoteMemberIfAbsent(info.ServiceAddr)

	members := n.handler.findMembers("Svc")
	if len(members) != 1 || members[0].ServiceAddr != info.ServiceAddr {
		t.Fatalf("members after delayed remove side effect = %v, want live re-add", members)
	}
}

func TestDelayedAddSideEffectDoesNotPublishRemovedMember(t *testing.T) {
	n := newTestNode()
	info := memberInfo("svc:8088", "Svc")
	if !n.cluster.addMember(info, 5, 10) {
		t.Fatal("addMember was not applied")
	}
	if !n.cluster.delMember(info.ServiceAddr, 6, 10) {
		t.Fatal("delMember was not applied")
	}

	// Simulate an older NewMember/heartbeat side effect resuming after a newer
	// remove already won the membership state.
	n.addRemoteMemberIfCurrent(info)

	if members := n.handler.findMembers("Svc"); len(members) != 0 {
		t.Fatalf("members after delayed add side effect = %v, want none", members)
	}
}
