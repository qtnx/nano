// Copyright (c) nano Authors. All Rights Reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cluster

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lonng/nano/cluster/clusterpb"
	"github.com/lonng/nano/internal/env"
	"github.com/lonng/nano/internal/log"
)

// cluster represents a nano cluster, which contains a bunch of nano nodes
// and each of them provide a group of different services. All services requests
// from client will send to gate firstly and be forwarded to appropriate node.
type cluster struct {
	// If cluster is not large enough, use slice is OK
	currentNode *Node
	rpcClient   *rpcClient

	mu      sync.RWMutex
	members []*Member

	// heartbeat-checker lifecycle (master only). heartbeatStop is closed by
	// stopHeartbeatChecker to terminate the goroutine on Shutdown; heartbeatDone
	// is closed by the goroutine when it exits so shutdown/tests can join it.
	heartbeatStop chan struct{}
	heartbeatDone chan struct{}
	heartbeatOnce sync.Once

	// pendingDeletes queues DelMember notifications that failed to reach a live
	// peer during Unregister, keyed by the recipient peer's ServiceAddr -> set of
	// member addresses it must still drop. Retried on that peer's next heartbeat
	// so a peer that briefly missed a delete cannot route to an unregistered pod
	// indefinitely (issue #7, fix 4). Guarded by mu.
	pendingDeletes map[string]map[string]struct{}

	// notifyDelMember sends one DelMember to a peer. Indirected so tests can
	// observe/stub the fan-out without a live gRPC peer; wired to sendDelMember in
	// newCluster.
	notifyDelMember func(peerAddr, deletedAddr string) error
}

func newCluster(currentNode *Node) *cluster {
	c := &cluster{
		currentNode:    currentNode,
		pendingDeletes: map[string]map[string]struct{}{},
	}
	c.notifyDelMember = c.sendDelMember
	if currentNode.IsMaster {
		c.checkMemberHeartbeat()
	}
	return c
}

// Register implements the MasterServer gRPC service
func (c *cluster) Register(_ context.Context, req *clusterpb.RegisterRequest) (*clusterpb.RegisterResponse, error) {
	if req == nil || req.MemberInfo == nil || req.MemberInfo.ServiceAddr == "" {
		return nil, ErrInvalidRegisterReq
	}
	resp := &clusterpb.RegisterResponse{}
	c.mu.Lock()
	for k, m := range c.members {
		if m.memberInfo.ServiceAddr == req.MemberInfo.ServiceAddr {
			// 节点异常崩溃，不会执行unregister，此时再次启动该节点，由于已存在注册信息，将再也无法成功注册，这里做个修改，先移除后重新注册
			if k >= len(c.members)-1 {
				c.members = c.members[:k]
			} else {
				c.members = append(c.members[:k], c.members[k+1:]...)
			}
			break
			//return nil, fmt.Errorf("address %s has registered", req.MemberInfo.ServiceAddr)
		}
	}
	// Snapshot the surviving members under the lock so the fan-out below never
	// races concurrent membership mutations on the backing array (H4).
	snapshot := make([]*Member, len(c.members))
	copy(snapshot, c.members)
	c.mu.Unlock()

	// Register the new member locally FIRST so a single unreachable or stale
	// peer cannot block the join (and leave the registering node retrying
	// forever). Peer notification below is best-effort (H25).
	for _, m := range snapshot {
		resp.Members = append(resp.Members, m.memberInfo)
	}

	// Clean-replace the (re)joining member's routes so a changed/shrunk service
	// set cannot leave stale services routable for this address (issue #7).
	c.currentNode.handler.replaceRemoteService(req.MemberInfo)
	c.mu.Lock()
	c.members = append(c.members, &Member{isMaster: false, memberInfo: req.MemberInfo, lastHeartbeatAt: time.Now()})
	// Cancel any queued delete-propagation for this address WHILE holding c.mu,
	// atomically with making the member live again. Cancelling after unlocking
	// raced a concurrent Unregister that could re-queue a legitimate delete in
	// the gap, which the late cancel would then erase (issue #7, review round 2).
	c.cancelPendingDeleteLocked(req.MemberInfo.ServiceAddr)
	c.mu.Unlock()

	log.Println("New peer register to cluster", req.MemberInfo.ServiceAddr)

	// Notify already-registered nodes about the new member. Failures are logged
	// and skipped instead of aborting the join; each call is bounded so a slow
	// peer cannot stall registration (H25).
	newMember := &clusterpb.NewMemberRequest{MemberInfo: req.MemberInfo}
	for _, m := range snapshot {
		if m.isMaster {
			continue
		}
		pool, err := c.rpcClient.getConnPool(m.memberInfo.ServiceAddr)
		if err != nil {
			log.Errorf("cluster: notify new member %s -> %s failed: %v", req.MemberInfo.ServiceAddr, m.memberInfo.ServiceAddr, err)
			continue
		}
		client := clusterpb.NewMemberClient(pool.Get())
		ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
		_, err = client.NewMember(ctx, newMember)
		cancel()
		if err != nil {
			log.Errorf("cluster: notify new member %s -> %s failed: %v", req.MemberInfo.ServiceAddr, m.memberInfo.ServiceAddr, err)
		}
	}
	return resp, nil
}

// Unregister implements the MasterServer gRPC service
func (c *cluster) Unregister(_ context.Context, req *clusterpb.UnregisterRequest) (*clusterpb.UnregisterResponse, error) {
	if req == nil || req.ServiceAddr == "" {
		return nil, ErrInvalidRegisterReq
	}

	resp := &clusterpb.UnregisterResponse{}

	// Snapshot members under the lock; the fan-out below issues RPCs and must
	// not hold the lock or read the live backing array (H4).
	c.mu.RLock()
	snapshot := make([]*Member, len(c.members))
	copy(snapshot, c.members)
	c.mu.RUnlock()

	var target *Member
	for _, m := range snapshot {
		if m.memberInfo.ServiceAddr == req.ServiceAddr {
			target = m
			break
		}
	}
	if target == nil {
		return nil, fmt.Errorf("address %s has not registered", req.ServiceAddr)
	}

	// Remove the departed member from the master/local registry FIRST so a
	// single unreachable peer cannot block removal and leave a stale routable
	// address (plus a heartbeat retry loop) (H25).
	c.currentNode.handler.delMember(req.ServiceAddr)
	c.mu.Lock()
	for i, m := range c.members {
		if m.memberInfo.ServiceAddr == req.ServiceAddr {
			if i >= len(c.members)-1 {
				c.members = c.members[:i]
			} else {
				c.members = append(c.members[:i], c.members[i+1:]...)
			}
			break
		}
	}
	c.mu.Unlock()

	// Close the outbound connection pool to the departed member so its gRPC
	// ClientConns/goroutines are reclaimed (M3).
	if c.rpcClient != nil {
		c.rpcClient.removePool(req.ServiceAddr)
	}
	c.dropPendingDeletesForPeer(req.ServiceAddr)

	log.Println("Exists peer unregister to cluster", req.ServiceAddr)

	// Notify the remaining peers best-effort; a failed peer is logged and
	// skipped rather than aborting removal, and each call is bounded (H25).
	for _, m := range snapshot {
		if m.memberInfo.ServiceAddr == req.ServiceAddr {
			// this node is down.
			continue
		}
		if m.MemberInfo().ServiceAddr == c.currentNode.ServiceAddr {
			continue
		}
		if err := c.notifyDelMember(m.memberInfo.ServiceAddr, req.ServiceAddr); err != nil {
			// The peer was unreachable: queue the delete so it is retried on the
			// peer's next heartbeat. Without this, a peer that briefly missed the
			// notification routes to the unregistered pod indefinitely (issue #7).
			log.Errorf("cluster: notify del member %s -> %s failed (queued for retry): %v", req.ServiceAddr, m.memberInfo.ServiceAddr, err)
			c.recordPendingDelete(m.memberInfo.ServiceAddr, req.ServiceAddr)
		}
	}

	if c.currentNode.UnregisterCallback != nil {
		c.currentNode.UnregisterCallback(*target, func() {
			log.Println("UnregisterCallback")
			res, err := c.Register(context.Background(), &clusterpb.RegisterRequest{
				MemberInfo: target.MemberInfo(),
			})
			if err != nil {
				log.Error("UnregisterCallback register error", err)
			} else {
				log.Infof("UnregisterCallback register success with response: %v", res)
			}
		})
	}

	return resp, nil
}

func (c *cluster) Heartbeat(_ context.Context, req *clusterpb.HeartbeatRequest) (*clusterpb.HeartbeatResponse, error) {
	// MemberInfo is an optional proto message: a malformed RPC can omit it.
	// Validate before touching shared state so a nil deref cannot crash the
	// master node.
	if req == nil || req.MemberInfo == nil || req.MemberInfo.ServiceAddr == "" {
		return nil, ErrInvalidRegisterReq
	}
	addr := req.MemberInfo.ServiceAddr
	c.mu.Lock()
	log.Println("Receive Heartbeat from: ", req.MemberInfo.Label)
	isHit := false
	for i, m := range c.members {
		if m.MemberInfo().GetServiceAddr() == addr {
			c.members[i].lastHeartbeatAt = time.Now()
			isHit = true
		}
	}
	if !isHit {
		c.members = append(c.members, &Member{
			isMaster:        false,
			memberInfo:      req.MemberInfo,
			lastHeartbeatAt: time.Now(),
		})
		// Install the recovered member's routes AND cancel any stale delete for
		// it while still holding c.mu, atomically with making it live again.
		// Doing either after unlocking raced a concurrent Unregister: the route
		// could be reinstalled after removal, or a freshly re-queued delete could
		// be erased by a late cancel (issue #7, review rounds 1 & 2). c.mu -> h.mu
		// order is safe: no handler method acquires c.mu.
		c.currentNode.handler.replaceRemoteService(req.MemberInfo)
		c.cancelPendingDeleteLocked(addr)
	}
	c.mu.Unlock()

	if !isHit {
		// A *stable* heartbeat skips all of the above and only refreshes the
		// timestamp, so steady-state heartbeats stay O(1) with no route sync or
		// broadcast (issue #7, fixes 5 & 7).
		log.Println("Heartbeat peer register to cluster", addr)
	}

	// Retry any delete notifications this peer missed while it was briefly
	// unreachable, now that it has proven liveness by heartbeating. Gated on
	// pending work, so a steady-state cluster does no extra work (issue #7,
	// fixes 4 & 5).
	c.flushPendingDeletes(addr)

	return &clusterpb.HeartbeatResponse{}, nil
}

func (c *cluster) checkMemberHeartbeat() {
	c.heartbeatStop = make(chan struct{})
	c.heartbeatDone = make(chan struct{})
	check := func() {
		// Snapshot members under the lock so the check never reads the live
		// backing array concurrently with membership mutations (H4).
		c.mu.RLock()
		snapshot := make([]*Member, len(c.members))
		copy(snapshot, c.members)
		c.mu.RUnlock()

		unregisterMembers := make([]*Member, 0)
		// check heartbeat time
		for _, m := range snapshot {
			log.Infof("Check heartbeat for %s, last heartbeat: %v, diff %v, deadline: %v", m.MemberInfo().ServiceAddr, m.lastHeartbeatAt, time.Now().Sub(m.lastHeartbeatAt), 4*env.Heartbeat)
			if time.Now().Sub(m.lastHeartbeatAt) > 4*env.Heartbeat && !m.isMaster {
				unregisterMembers = append(unregisterMembers, m)
			}
		}

		for _, m := range unregisterMembers {
			log.Println("Heartbeat timeout, unregister: ", m.MemberInfo().Label, m.MemberInfo().ServiceAddr)
			if _, err := c.Unregister(context.Background(), &clusterpb.UnregisterRequest{
				ServiceAddr: m.MemberInfo().ServiceAddr,
			}); err != nil {
				log.Println("Heartbeat unregister error", err)
			}
		}
	}
	go func() {
		defer close(c.heartbeatDone)
		ticker := time.NewTicker(env.Heartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !c.currentNode.IsMaster {
					return
				}
				check()
			case <-c.heartbeatStop:
				// Stopped by Shutdown so the goroutine no longer mutates stale
				// cluster state after the node is torn down (M13).
				return
			}
		}
	}()
}

// stopHeartbeatChecker terminates the master heartbeat-checker goroutine. It is
// idempotent and a no-op when the checker was never started.
func (c *cluster) stopHeartbeatChecker() {
	c.heartbeatOnce.Do(func() {
		if c.heartbeatStop != nil {
			close(c.heartbeatStop)
		}
	})
}

// pingNodes sends ping request to all nodes in the cluster
// withLabels is the labels of nodes that should be pinged
// returns the labels of nodes that are alive and dead
func (c *cluster) pingNodes(withLabels []string) (lives []string, dies []string, err error) {
	// Snapshot the membership so iteration never races concurrent
	// register/unregister mutations of the backing array (H4).
	c.mu.RLock()
	snapshot := make([]*Member, len(c.members))
	copy(snapshot, c.members)
	c.mu.RUnlock()
	for _, m := range snapshot {
		if m.isMaster {
			continue
		}
		if c.rpcClient == nil {
			return nil, nil, fmt.Errorf("rpc client is nil")
		}
		log.Debug("Ping node: ", m.memberInfo.Label)
		pool, err := c.rpcClient.getConnPool(m.memberInfo.ServiceAddr)
		if pool == nil {
			log.Error("Get connection pool error", err)
			continue
		}
		if err != nil {
			log.Error("Get connection pool error", err)
			continue
		}
		client := clusterpb.NewMemberClient(pool.Get())
		// log.Println("Get client: ", client)
		if resp, err := client.Ping(context.Background(), &clusterpb.PingRequest{}); err != nil {
			log.Error("Ping node error", m.memberInfo.Label, err)
			dies = append(dies, m.memberInfo.Label)
		} else {
			log.Debugf("Ping node %s, label %s success, response: %s", m.memberInfo.ServiceAddr, m.memberInfo.Label, string(resp.String()))
			if resp.Msg == "pong" {
				lives = append(lives, m.memberInfo.Label)
			} else {
				dies = append(dies, m.memberInfo.Label)
			}
		}
	}

	for _, label := range withLabels {
		var found bool
		for _, m := range snapshot {
			if m.memberInfo.Label == label {
				found = true
			}
		}
		if !found {
			dies = append(dies, label)
		}
	}

	return lives, dies, nil
}

func (c *cluster) setRpcClient(client *rpcClient) {
	c.rpcClient = client
}

func (c *cluster) remoteAddrs() []string {
	var addrs []string
	c.mu.RLock()
	for _, m := range c.members {
		addrs = append(addrs, m.memberInfo.ServiceAddr)
	}
	c.mu.RUnlock()
	return addrs
}

func (c *cluster) initMembers(members []*clusterpb.MemberInfo) {
	c.mu.Lock()
	for _, info := range members {
		c.members = append(c.members, &Member{
			memberInfo: info,
		})
	}
	c.mu.Unlock()
}

func (c *cluster) addMember(info *clusterpb.MemberInfo) {
	c.mu.Lock()
	var found bool
	for _, member := range c.members {
		if member.memberInfo.ServiceAddr == info.ServiceAddr {
			member.memberInfo = info
			found = true
			break
		}
	}
	if !found {
		c.members = append(c.members, &Member{
			memberInfo: info,
		})
	}
	c.mu.Unlock()
}

func (c *cluster) delMember(addr string) {
	c.mu.Lock()
	var index = -1
	for i, member := range c.members {
		if member.memberInfo.ServiceAddr == addr {
			index = i
			break
		}
	}
	if index != -1 {
		if index >= len(c.members)-1 {
			c.members = c.members[:index]
		} else {
			c.members = append(c.members[:index], c.members[index+1:]...)
		}
	}
	c.mu.Unlock()

	// Close the outbound connection pool to the departed member so its gRPC
	// ClientConns/goroutines are reclaimed on member removal (M3).
	if c.rpcClient != nil {
		c.rpcClient.removePool(addr)
	}
	c.dropPendingDeletesForPeer(addr)
}

// addLocalMember appends a member under the lock. Used by node startup to
// register the master's own member entry without racing routing/heartbeat
// readers (H4).
func (c *cluster) addLocalMember(m *Member) {
	c.mu.Lock()
	c.members = append(c.members, m)
	c.mu.Unlock()
}

// isKnownAddr reports whether addr is a currently-registered member's service
// address. Used to validate RPC-supplied gate addresses before dialing (M9).
func (c *cluster) isKnownAddr(addr string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, m := range c.members {
		if m.memberInfo.ServiceAddr == addr {
			return true
		}
	}
	return false
}

// maxPendingDeletePeers bounds how many distinct peers may hold queued
// delete-retries, so an endlessly unreachable peer cannot grow the ledger
// without limit; beyond it the stale target is reclaimed by heartbeat timeout.
const maxPendingDeletePeers = 4096

// maxDeleteRetriesPerHeartbeat bounds how many queued deletes a single heartbeat
// flushes, so a peer with a large backlog cannot pin the heartbeat handler on
// synchronous DelMember RPCs; the remainder is retried on the next heartbeat
// (issue #7, review round 1).
const maxDeleteRetriesPerHeartbeat = 64

// recordPendingDelete queues deletedAddr to be re-sent to peerAddr on its next
// heartbeat after a live DelMember broadcast to peerAddr failed (issue #7).
func (c *cluster) recordPendingDelete(peerAddr, deletedAddr string) {
	if peerAddr == "" || deletedAddr == "" || peerAddr == deletedAddr {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.pendingDeletes == nil {
		c.pendingDeletes = map[string]map[string]struct{}{}
	}
	set := c.pendingDeletes[peerAddr]
	if set == nil {
		if len(c.pendingDeletes) >= maxPendingDeletePeers {
			log.Warn(fmt.Sprintf("cluster: pending-delete ledger full (%d peers); dropping retry to %s for departed %s; that peer may keep a stale route until it re-registers (issue #7)", maxPendingDeletePeers, peerAddr, deletedAddr))
			return
		}
		set = map[string]struct{}{}
		c.pendingDeletes[peerAddr] = set
	}
	set[deletedAddr] = struct{}{}
}

// flushPendingDeletes re-sends queued deletes to peerAddr, which has just proven
// liveness by heartbeating. It drops any target that is a live member again
// (re-registered) so a peer is never told to drop a rejoined pod, caps the batch
// so a large backlog cannot pin the heartbeat handler on synchronous RPCs (the
// remainder is retried next beat), and re-checks liveness right before each
// send. Successful sends leave the ledger; failures are re-queued. RPCs run
// without the lock held (issue #7, fixes 4 & 5; review round 1).
func (c *cluster) flushPendingDeletes(peerAddr string) {
	c.mu.Lock()
	set := c.pendingDeletes[peerAddr]
	if len(set) == 0 {
		c.mu.Unlock()
		return
	}
	alive := make(map[string]struct{}, len(c.members))
	for _, m := range c.members {
		alive[m.memberInfo.ServiceAddr] = struct{}{}
	}
	pending := make([]string, 0, len(set))
	for addr := range set {
		if _, ok := alive[addr]; ok {
			delete(set, addr) // re-registered -> the queued delete is stale, drop it
			continue
		}
		if len(pending) >= maxDeleteRetriesPerHeartbeat {
			break
		}
		pending = append(pending, addr)
		delete(set, addr)
	}
	if len(set) == 0 {
		delete(c.pendingDeletes, peerAddr)
	}
	c.mu.Unlock()

	for _, deletedAddr := range pending {
		// The target may have re-registered between the snapshot above and now;
		// never tell a peer to drop a currently-registered member (issue #7).
		if c.isKnownAddr(deletedAddr) {
			continue
		}
		if err := c.notifyDelMember(peerAddr, deletedAddr); err != nil {
			log.Errorf("cluster: retry del member %s -> %s failed (re-queued): %v", deletedAddr, peerAddr, err)
			c.recordPendingDelete(peerAddr, deletedAddr)
		} else {
			log.Infof("cluster: repaired missed del member %s -> %s on heartbeat", deletedAddr, peerAddr)
		}
	}
}

// cancelPendingDeleteLocked removes deletedAddr from every peer's retry set, used
// when a node (re)registers so peers are not later taught to drop a member that
// has returned under the same address (issue #7). The caller must hold c.mu so
// the cancel runs atomically with the membership change that justifies it,
// instead of in a post-unlock window where a concurrent Unregister could have
// queued a fresh, legitimate delete (issue #7, review round 2).
func (c *cluster) cancelPendingDeleteLocked(deletedAddr string) {
	for peer, set := range c.pendingDeletes {
		delete(set, deletedAddr)
		if len(set) == 0 {
			delete(c.pendingDeletes, peer)
		}
	}
}

// dropPendingDeletesForPeer discards every queued retry destined for peerAddr,
// used when that peer itself leaves the cluster (issue #7).
func (c *cluster) dropPendingDeletesForPeer(peerAddr string) {
	c.mu.Lock()
	delete(c.pendingDeletes, peerAddr)
	c.mu.Unlock()
}

// sendDelMember delivers one DelMember RPC; it is the production implementation
// behind the notifyDelMember test seam.
func (c *cluster) sendDelMember(peerAddr, deletedAddr string) error {
	if c.rpcClient == nil {
		return fmt.Errorf("cluster: rpc client is nil")
	}
	pool, err := c.rpcClient.getConnPool(peerAddr)
	if err != nil {
		return err
	}
	client := clusterpb.NewMemberClient(pool.Get())
	ctx, cancel := context.WithTimeout(context.Background(), remoteRPCTimeout)
	defer cancel()
	_, err = client.DelMember(ctx, &clusterpb.DelMemberRequest{ServiceAddr: deletedAddr})
	return err
}
