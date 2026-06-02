package cluster

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/component"
	"github.com/lonng/nano/internal/log"
)

func TestLocalNodeStatusReflectsMasterStartup(t *testing.T) {
	log.SetLogger(&noopLogger{})
	n := &Node{
		Options:     Options{IsMaster: true, Components: &component.Components{}, ClientAddr: freeAddr(t)},
		ServiceAddr: freeAddr(t),
	}
	if err := n.Startup(); err != nil {
		t.Fatalf("Startup: %v", err)
	}
	t.Cleanup(n.Shutdown)

	got := n.LocalNodeStatus()
	want := LocalNodeStatus{
		Initialized:          true,
		ClusterListening:     true,
		ClientRequired:       true,
		ClientListening:      true,
		RegistrationRequired: false,
		Registered:           true,
	}
	if got != want {
		t.Fatalf("LocalNodeStatus() = %+v, want %+v", got, want)
	}
}

func TestLocalNodeStatusReflectsMemberRegistrationWithoutPeerHealth(t *testing.T) {
	log.SetLogger(&noopLogger{})
	master := &Node{
		Options:     Options{IsMaster: true, Components: &component.Components{}},
		ServiceAddr: freeAddr(t),
	}
	if err := master.Startup(); err != nil {
		t.Fatalf("master Startup: %v", err)
	}
	t.Cleanup(master.Shutdown)

	member := &Node{
		Options: Options{
			AdvertiseAddr: master.ServiceAddr,
			RetryInterval: 10 * time.Millisecond,
			Components:    &component.Components{},
		},
		ServiceAddr: freeAddr(t),
	}
	if err := member.Startup(); err != nil {
		t.Fatalf("member Startup: %v", err)
	}
	t.Cleanup(member.Shutdown)

	member.cluster.pingMemberFn = func(context.Context, string) error {
		t.Fatal("LocalNodeStatus must not ping peers")
		return nil
	}

	got := member.LocalNodeStatus()
	if !got.Initialized || !got.ClusterListening || !got.RegistrationRequired || !got.Registered {
		t.Fatalf("member LocalNodeStatus() = %+v, want initialized cluster listener and master registration", got)
	}
	if got.ClientRequired || got.ClientListening {
		t.Fatalf("member LocalNodeStatus() = %+v, want no client listener requirement", got)
	}
}

func TestStartupFailureAfterRegistrationCleansLocalStatus(t *testing.T) {
	log.SetLogger(&noopLogger{})
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer busy.Close()

	n := &Node{
		Options:     Options{IsMaster: true, Components: &component.Components{}, ClientAddr: busy.Addr().String()},
		ServiceAddr: freeAddr(t),
	}
	if err := n.Startup(); err == nil {
		n.Shutdown()
		t.Fatal("expected Startup to fail when client listener is already bound")
	}

	got := n.LocalNodeStatus()
	if got.Initialized || got.ClusterListening || got.ClientListening || got.Registered || got.Draining || got.Drained || got.Shutdown {
		t.Fatalf("LocalNodeStatus after failed Startup = %+v, want no ready/drain/shutdown proof", got)
	}
	if n.cluster == nil || n.cluster.heartbeatDone == nil {
		t.Fatal("failed master startup did not initialize heartbeat checker state")
	}
	select {
	case <-n.cluster.heartbeatDone:
	case <-time.After(time.Second):
		t.Fatal("heartbeat checker still running after failed master Startup cleanup")
	}
}

func TestDrainReturnsCanceledContextWithoutDialing(t *testing.T) {
	n := newTestNode()
	n.ServiceAddr = "member:8088"
	n.AdvertiseAddr = "master:8088"
	n.rpcClient = newRPCClient()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := n.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Drain canceled context error = %v, want context.Canceled", err)
	}
}

func TestDrainFailureKeepsNodeRetryable(t *testing.T) {
	n := newTestNode()
	n.ServiceAddr = "member:8088"
	n.AdvertiseAddr = "master:8088"
	n.rpcClient = newRPCClient()
	n.rpcClient.closePool()
	n.keepaliveExit = make(chan struct{})

	if err := n.Drain(context.Background()); err == nil {
		t.Fatal("Drain returned nil with a closed rpc client, want error")
	}
	got := n.LocalNodeStatus()
	if got.Draining {
		t.Fatalf("LocalNodeStatus after failed Drain = %+v, want draining cleared for retry", got)
	}
	if n.drained.Load() {
		t.Fatal("failed Drain marked node drained")
	}
	select {
	case <-n.keepaliveExit:
		t.Fatal("failed Drain stopped keepalive; node cannot recover without restart")
	default:
	}
}

func TestDrainWaitsForInFlightHeartbeatBeforeUnregister(t *testing.T) {
	log.SetLogger(&noopLogger{})
	master := &Node{
		Options:     Options{IsMaster: true, Components: &component.Components{}},
		ServiceAddr: freeAddr(t),
	}
	if err := master.Startup(); err != nil {
		t.Fatalf("master Startup: %v", err)
	}
	t.Cleanup(master.Shutdown)

	member := &Node{
		Options: Options{
			AdvertiseAddr: master.ServiceAddr,
			RetryInterval: 10 * time.Millisecond,
			Components:    &component.Components{},
		},
		ServiceAddr: freeAddr(t),
	}
	if err := member.Startup(); err != nil {
		t.Fatalf("member Startup: %v", err)
	}
	t.Cleanup(member.Shutdown)

	releaseHeartbeat, err := member.acquireHeartbeatGuard(context.Background())
	if err != nil {
		t.Fatalf("acquireHeartbeatGuard: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- member.Drain(context.Background()) }()
	select {
	case err := <-done:
		releaseHeartbeat()
		t.Fatalf("Drain returned while heartbeat was in-flight: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	releaseHeartbeat()
	if err := <-done; err != nil {
		t.Fatalf("Drain after heartbeat release returned error: %v", err)
	}
	if master.cluster.isKnownAddr(member.ServiceAddr) {
		t.Fatalf("master still knows drained member %s", member.ServiceAddr)
	}
	if got := member.LocalNodeStatus(); !got.Draining || !got.Drained {
		t.Fatalf("LocalNodeStatus after successful Drain = %+v, want draining and drained", got)
	}
}

func TestUnregisterAlreadyRemovedMemberIsIdempotent(t *testing.T) {
	log.SetLogger(&noopLogger{})
	c, _ := newMasterCluster(t)
	c.addMember(memberInfo("127.0.0.1:9002", "svc"), 0, 0)

	req := &clusterpb.UnregisterRequest{ServiceAddr: "127.0.0.1:9002"}
	if _, err := c.Unregister(context.Background(), req); err != nil {
		t.Fatalf("first Unregister returned error: %v", err)
	}
	if _, err := c.Unregister(context.Background(), req); err != nil {
		t.Fatalf("second Unregister returned error: %v, want idempotent success", err)
	}
	if c.isKnownAddr(req.ServiceAddr) {
		t.Fatalf("member %s still exists after idempotent unregister", req.ServiceAddr)
	}
}
