package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lonng/nano/cluster/clusterpb"
)

// LocalNodeStatus is a local startup and drain signal. It intentionally does not
// ping peers; callers can combine it with CheckClusterHealth for readiness.
//
// A typical readiness predicate is:
// Initialized && ClusterListening && (!ClientRequired || ClientListening) &&
// (!RegistrationRequired || Registered) && !Draining && !Shutdown.
type LocalNodeStatus struct {
	// Initialized is true only after Startup has completed successfully.
	Initialized bool
	// ClusterListening means the local cluster gRPC listener was bound.
	ClusterListening bool
	// ClientRequired means ClientAddr is configured for public client traffic.
	ClientRequired bool
	// ClientListening means the public client listener was bound.
	ClientListening bool
	// RegistrationRequired means this member must register with a remote master.
	RegistrationRequired bool
	// Registered means this node is represented in cluster membership: locally
	// for masters, or by a successful master registration for members.
	Registered bool
	// Draining is true after a Drain attempt has started. A drained node should
	// normally remain not-ready even after Drained becomes true.
	Draining bool
	// Drained is true after Drain completed successfully.
	Drained bool
	// Shutdown is true after Shutdown started.
	Shutdown bool
}

// LocalNodeStatus returns local listener and registration state without probing
// other cluster members.
func (n *Node) LocalNodeStatus() LocalNodeStatus {
	if n == nil {
		return LocalNodeStatus{}
	}
	return LocalNodeStatus{
		Initialized:          n.initialized.Load(),
		ClusterListening:     n.clusterListening.Load(),
		ClientRequired:       n.ClientAddr != "",
		ClientListening:      n.clientListening.Load(),
		RegistrationRequired: !n.IsMaster && n.AdvertiseAddr != "",
		Registered:           n.registeredWithMaster.Load(),
		Draining:             n.draining.Load(),
		Drained:              n.drained.Load(),
		Shutdown:             n.shutdown.Load(),
	}
}

// Drain stops this member from refreshing its master registration and unregisters
// it from the cluster using ctx as the caller-controlled bound. Drain is safe to
// call before Shutdown and is idempotent after a successful unregister.
func (n *Node) Drain(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if n == nil {
		return errors.New("node is nil")
	}

	n.drainMu.Lock()
	defer n.drainMu.Unlock()
	if n.drained.Load() {
		return nil
	}
	n.draining.Store(true)
	releaseHeartbeat, err := n.acquireHeartbeatGuard(ctx)
	if err != nil {
		n.draining.Store(false)
		return err
	}
	defer releaseHeartbeat()

	if n.IsMaster || n.AdvertiseAddr == "" {
		n.drained.Store(true)
		return nil
	}
	fail := func(err error) error {
		n.draining.Store(false)
		return err
	}
	if n.rpcClient == nil {
		return fail(errors.New("rpc client is nil"))
	}

	pool, err := n.rpcClient.getConnPoolWithContext(ctx, n.AdvertiseAddr)
	if err != nil {
		return fail(err)
	}
	_, err = clusterpb.NewMasterClient(pool.Get()).Unregister(ctx, &clusterpb.UnregisterRequest{ServiceAddr: n.ServiceAddr})
	if err != nil {
		return fail(fmt.Errorf("unregister current node: %w", err))
	}
	n.stopKeepalive()
	n.registeredWithMaster.Store(false)
	n.drained.Store(true)
	return nil
}

func (n *Node) resetRolloutState() {
	n.once = sync.Once{}
	n.keepaliveStopOnce = sync.Once{}
	n.keepaliveExit = nil
	n.initialized.Store(false)
	n.clusterListening.Store(false)
	n.clientListening.Store(false)
	n.heartbeatGuard = make(chan struct{}, 1)
	n.registeredWithMaster.Store(false)
	n.draining.Store(false)
	n.drained.Store(false)
	n.shutdown.Store(false)
}

func (n *Node) stopKeepalive() {
	if n == nil || n.keepaliveExit == nil {
		return
	}
	n.keepaliveStopOnce.Do(func() { close(n.keepaliveExit) })
}

func (n *Node) acquireHeartbeatGuard(ctx context.Context) (func(), error) {
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	ch := n.heartbeatGuardChan()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (n *Node) tryAcquireHeartbeatGuard() (func(), bool) {
	ch := n.heartbeatGuardChan()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, true
	default:
		return nil, false
	}
}

func (n *Node) heartbeatGuardChan() chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.heartbeatGuard == nil {
		n.heartbeatGuard = make(chan struct{}, 1)
	}
	return n.heartbeatGuard
}

func (n *Node) cleanupFailedStartup() {
	ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
	_ = n.Drain(ctx)
	cancel()
	if n.cluster != nil {
		n.cluster.stopHeartbeatChecker()
	}

	if n.server != nil {
		n.server.Stop()
	}
	if n.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = n.httpServer.Shutdown(ctx)
		cancel()
	}
	if n.clientServer != nil {
		_ = n.clientServer.Shutdown()
	}
	if n.metricsServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = n.metricsServer.Shutdown(ctx)
		cancel()
	}
	if n.rpcClient != nil {
		n.rpcClient.closePool()
	}
	n.initialized.Store(false)
	n.clusterListening.Store(false)
	n.clientListening.Store(false)
	n.registeredWithMaster.Store(false)
	n.draining.Store(false)
	n.drained.Store(false)
	n.shutdown.Store(false)
}
