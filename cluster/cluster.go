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
	"crypto/rand"
	"encoding/binary"
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
	currentNode  *Node
	rpcClient    *rpcClient
	pingMemberFn func(context.Context, string) error

	mu                sync.RWMutex
	members           []*Member
	removedMembers    map[string]removedMemberTombstone
	membershipEpoch   uint64
	membershipVersion uint64
	// Highest removal version observed through an ordered heartbeat snapshot.
	// DelMember pushes do not advance this because independent pushes can be
	// missed or delivered without proving older removals were seen.
	removedMembershipVersion uint64
	membershipCompactVersion uint64
	membershipSnapshotSeq    uint64

	// heartbeat-checker lifecycle (master only). heartbeatStop is closed by
	// stopHeartbeatChecker to terminate the goroutine on Shutdown; heartbeatDone
	// is closed by the goroutine when it exits so shutdown/tests can join it.
	heartbeatStop chan struct{}
	heartbeatDone chan struct{}
	heartbeatOnce sync.Once
}

type removedMemberTombstone struct {
	membershipVersion uint64
	removedAt         time.Time
}

func removedMemberRetention() time.Duration {
	return 4 * env.Heartbeat
}

func newCluster(currentNode *Node) *cluster {
	c := &cluster{currentNode: currentNode}
	if currentNode.IsMaster {
		c.membershipEpoch = newMembershipEpoch()
		c.checkMemberHeartbeat()
	}
	return c
}

func newMembershipEpoch() uint64 {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err == nil {
		epoch := binary.LittleEndian.Uint64(buf[:])
		if epoch != 0 {
			return epoch
		}
	}
	epoch := uint64(time.Now().UnixNano())
	if epoch == 0 {
		return 1
	}
	return epoch
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
	snapshot := make([]*Member, len(c.members))
	copy(snapshot, c.members)
	for _, m := range snapshot {
		resp.Members = append(resp.Members, m.memberInfo)
	}
	c.membershipVersion++
	c.clearRemovedMemberLocked(req.MemberInfo.ServiceAddr)
	membershipEpoch := c.ensureMembershipEpochLocked()
	resp.MembershipVersion = c.membershipVersion
	resp.MembershipEpoch = membershipEpoch
	c.members = append(c.members, &Member{
		isMaster:          false,
		memberInfo:        req.MemberInfo,
		lastHeartbeatAt:   time.Now(),
		membershipEpoch:   membershipEpoch,
		membershipVersion: c.membershipVersion,
	})
	// Update the master's own route table while still holding c.mu, atomically
	// with the membership append. The master does not self-reconcile its route
	// cache (only peers do, via heartbeat responses), so doing this after unlock
	// let a concurrent same-address Unregister interleave and leave the master
	// routing to a removed member (issue #7 follow-up). c.mu -> h.mu is the global
	// order; handler route methods take only h.mu and do no network I/O.
	c.currentNode.handler.delMember(req.MemberInfo.ServiceAddr)
	c.currentNode.handler.addRemoteService(req.MemberInfo)
	c.mu.Unlock()
	if c.rpcClient != nil {
		c.rpcClient.allowPool(req.MemberInfo.ServiceAddr)
	}

	log.Println("New peer register to cluster", req.MemberInfo.ServiceAddr)

	newMember := &clusterpb.NewMemberRequest{MemberInfo: req.MemberInfo, MembershipVersion: resp.MembershipVersion, MembershipEpoch: resp.MembershipEpoch}
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
func (c *cluster) Unregister(ctx context.Context, req *clusterpb.UnregisterRequest) (*clusterpb.UnregisterResponse, error) {
	if req == nil || req.ServiceAddr == "" {
		return nil, ErrInvalidRegisterReq
	}

	resp := &clusterpb.UnregisterResponse{}
	snapshot := c.memberSnapshots()

	var target memberSnapshot
	var foundTarget bool
	for _, m := range snapshot {
		if m.serviceAddr == req.ServiceAddr {
			target = m
			foundTarget = true
			break
		}
	}
	if !foundTarget {
		return resp, nil
	}

	var (
		membershipEpoch uint64
		version         uint64
	)
	c.mu.Lock()
	index := -1
	for i, m := range c.members {
		if m.memberInfo != nil && m.memberInfo.ServiceAddr == req.ServiceAddr {
			index = i
			break
		}
	}
	if index < 0 {
		c.mu.Unlock()
		return resp, nil
	}
	c.membershipVersion++
	membershipEpoch = c.ensureMembershipEpochLocked()
	version = c.membershipVersion
	c.rememberRemovedMemberLocked(req.ServiceAddr, version)
	if index >= len(c.members)-1 {
		c.members = c.members[:index]
	} else {
		c.members = append(c.members[:index], c.members[index+1:]...)
	}
	// Remove from the master's own route table while still holding c.mu,
	// atomically with the membership removal (the master does not self-reconcile
	// its route cache), so a concurrent same-address Register cannot leave the
	// master routing to a removed member (issue #7 follow-up). removeRemoteMember
	// below repeats handler.delMember (a harmless no-op) plus the session/pool
	// teardown that must stay outside the lock.
	if c.currentNode != nil {
		c.currentNode.handler.delMember(req.ServiceAddr)
	}
	c.mu.Unlock()

	if c.currentNode != nil {
		c.currentNode.removeRemoteMemberIfAbsent(req.ServiceAddr)
	}

	log.Println("Exists peer unregister to cluster", req.ServiceAddr)

	delMember := &clusterpb.DelMemberRequest{ServiceAddr: req.ServiceAddr, MembershipVersion: version, MembershipEpoch: membershipEpoch}
	for _, m := range snapshot {
		if m.serviceAddr == "" || m.serviceAddr == req.ServiceAddr {
			continue
		}
		if c.currentNode != nil && m.serviceAddr == c.currentNode.ServiceAddr {
			continue
		}
		notifyCtx := ctx
		if notifyCtx.Err() != nil {
			notifyCtx = context.Background()
		}
		pool, err := c.rpcClient.getConnPoolWithContext(notifyCtx, m.serviceAddr)
		if err != nil {
			log.Errorf("cluster: notify del member %s -> %s failed: %v", req.ServiceAddr, m.serviceAddr, err)
			continue
		}
		client := clusterpb.NewMemberClient(pool.Get())
		peerCtx, cancel := context.WithTimeout(notifyCtx, remoteRPCTimeout)
		_, err = client.DelMember(peerCtx, delMember)
		cancel()
		if err != nil {
			log.Errorf("cluster: notify del member %s -> %s failed: %v", req.ServiceAddr, m.serviceAddr, err)
		}
	}

	if c.currentNode != nil && c.currentNode.UnregisterCallback != nil {
		removedInfo := cloneMemberInfo(target.memberInfo)
		removedMember := target.member()
		c.currentNode.UnregisterCallback(removedMember, func() {
			log.Println("UnregisterCallback")
			res, err := c.Register(context.Background(), &clusterpb.RegisterRequest{
				MemberInfo: removedInfo,
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
	if req == nil || req.MemberInfo == nil || req.MemberInfo.ServiceAddr == "" {
		return nil, ErrInvalidRegisterReq
	}
	log.Println("Receive Heartbeat from: ", req.MemberInfo.Label)

	c.mu.Lock()
	membershipEpoch := c.ensureMembershipEpochLocked()

	isHit := false
	for i, m := range c.members {
		if m.MemberInfo().GetServiceAddr() == req.GetMemberInfo().GetServiceAddr() {
			c.members[i].lastHeartbeatAt = time.Now()
			isHit = true
		}
	}
	if !isHit {
		// master local not binding this node, other members do not need to be notified, because this node registered.
		// maybe the master process reload
		m := &Member{
			isMaster:        false,
			memberInfo:      req.GetMemberInfo(),
			lastHeartbeatAt: time.Now(),
			membershipEpoch: membershipEpoch,
		}
		c.members = append(c.members, m)
		c.membershipVersion++
		m.membershipVersion = c.membershipVersion
		c.clearRemovedMemberLocked(req.GetMemberInfo().GetServiceAddr())
		c.currentNode.handler.addRemoteService(req.MemberInfo)
		log.Println("Heartbeat peer register to cluster", req.MemberInfo.ServiceAddr)
	}

	c.pruneRemovedMembersLocked(time.Now())
	resetMembership := c.heartbeatRequiresResetLocked(req, membershipEpoch)
	resp := &clusterpb.HeartbeatResponse{
		MembershipVersion: c.membershipVersion,
		MembershipEpoch:   membershipEpoch,
		HeartbeatSeq:      req.GetHeartbeatSeq(),
		ResetMembership:   resetMembership,
	}
	if resetMembership || !c.heartbeatMemberListSyncedLocked(req, membershipEpoch) {
		for _, m := range c.members {
			resp.Members = append(resp.Members, m.MemberInfo())
		}
	}
	if !resetMembership {
		removedMembershipVersion := req.GetRemovedMembershipVersion()
		if req.GetMembershipEpoch() != membershipEpoch {
			removedMembershipVersion = 0
		}
		for addr, tombstone := range c.removedMembers {
			if tombstone.membershipVersion <= removedMembershipVersion {
				continue
			}
			resp.RemovedMembers = append(resp.RemovedMembers, &clusterpb.RemovedMember{
				ServiceAddr:       addr,
				MembershipVersion: tombstone.membershipVersion,
			})
		}
	}
	c.mu.Unlock()

	return resp, nil
}

func (c *cluster) checkMemberHeartbeat() {
	c.heartbeatStop = make(chan struct{})
	c.heartbeatDone = make(chan struct{})
	check := func() {
		// Snapshot members under the lock so the check never reads the live
		// backing array concurrently with membership mutations (H4).
		snapshot := c.memberSnapshots()

		unregisterMembers := make([]memberSnapshot, 0)
		// check heartbeat time
		now := time.Now()
		for _, m := range snapshot {
			log.Infof("Check heartbeat for %s, last heartbeat: %v, diff %v, deadline: %v", m.serviceAddr, m.lastHeartbeatAt, now.Sub(m.lastHeartbeatAt), 4*env.Heartbeat)
			if now.Sub(m.lastHeartbeatAt) > 4*env.Heartbeat && !m.isMaster {
				unregisterMembers = append(unregisterMembers, m)
			}
		}

		for _, m := range unregisterMembers {
			log.Println("Heartbeat timeout, unregister: ", m.label, m.serviceAddr)
			if _, err := c.Unregister(context.Background(), &clusterpb.UnregisterRequest{
				ServiceAddr: m.serviceAddr,
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
	snapshot := c.memberSnapshots()
	for _, m := range snapshot {
		if m.isMaster {
			continue
		}
		if c.rpcClient == nil {
			return nil, nil, fmt.Errorf("rpc client is nil")
		}
		log.Debug("Ping node: ", m.label)
		pool, err := c.rpcClient.getConnPool(m.serviceAddr)
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
			log.Error("Ping node error", m.label, err)
			dies = append(dies, m.label)
		} else {
			log.Debugf("Ping node %s, label %s success, response: %s", m.serviceAddr, m.label, string(resp.String()))
			if resp.Msg == "pong" {
				lives = append(lives, m.label)
			} else {
				dies = append(dies, m.label)
			}
		}
	}

	for _, label := range withLabels {
		var found bool
		for _, m := range snapshot {
			if m.label == label {
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

func (c *cluster) initMembers(members []*clusterpb.MemberInfo, membershipVersion, membershipEpoch uint64) {
	c.mu.Lock()
	c.acceptMembershipSnapshotLocked(membershipVersion, membershipEpoch, 0)
	for _, info := range members {
		if c.rpcClient != nil {
			c.rpcClient.allowPool(info.ServiceAddr)
		}
		c.members = append(c.members, &Member{
			memberInfo:        info,
			membershipEpoch:   c.membershipEpoch,
			membershipVersion: c.membershipVersion,
		})
	}
	c.mu.Unlock()
}

func (c *cluster) addMember(info *clusterpb.MemberInfo, membershipVersion, membershipEpoch uint64) bool {
	if info == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.acceptMembershipEventLocked(membershipVersion, membershipEpoch) {
		return false
	}
	c.clearRemovedMemberLocked(info.GetServiceAddr())
	if c.rpcClient != nil {
		c.rpcClient.allowPool(info.GetServiceAddr())
	}
	now := time.Now()
	var found bool
	for _, member := range c.members {
		if member.memberInfo.ServiceAddr == info.ServiceAddr {
			member.memberInfo = info
			member.lastHeartbeatAt = now
			member.epochStaleSince = time.Time{}
			member.membershipEpoch = c.membershipEpoch
			member.membershipVersion = c.membershipVersion
			found = true
			break
		}
	}
	if !found {
		c.members = append(c.members, &Member{
			memberInfo:        info,
			lastHeartbeatAt:   now,
			membershipEpoch:   c.membershipEpoch,
			membershipVersion: c.membershipVersion,
		})
	}
	return true
}

// reconcileMembers adds or updates any live members reported by the master
// (e.g. in a heartbeat response), applies explicit removal tombstones, and
// returns the remote-service route changes needed by the caller. Newly added
// members can be registered directly; updated members must replace existing
// remote-service routes because addRemoteService cannot remove routes for
// services the member no longer advertises.
//
// Same-epoch members are intentionally never removed only because they are
// absent from the live list: pruning them would let a transient or incomplete
// master view (for example right after a master restart, before every member has
// re-heartbeated) drop a live peer and break routing. Old-epoch members missing
// from repeated snapshots are pruned only after a heartbeat grace period; before
// then they can reappear in the new epoch without losing routes or sessions.
// Explicit RemovedMember tombstones and full reset snapshots still remove peers
// immediately.
//
// This is what lets a node self-heal a missed NewMember push: pingNodes and route
// resolution only see c.members, so a member that registered after this node's
// initial sync would otherwise stay invisible until a process restart.
func (c *cluster) reconcileMembers(infos []*clusterpb.MemberInfo, removedInfos []*clusterpb.RemovedMember, membershipVersion, membershipEpoch uint64) (added, updated []*clusterpb.MemberInfo, removed []string) {
	return c.reconcileMembersSnapshot(infos, removedInfos, membershipVersion, membershipEpoch, 0, false)
}

func (c *cluster) reconcileMembersSnapshot(infos []*clusterpb.MemberInfo, removedInfos []*clusterpb.RemovedMember, membershipVersion, membershipEpoch, snapshotSeq uint64, resetMembership bool) (added, updated []*clusterpb.MemberInfo, removed []string) {
	if len(infos) == 0 && len(removedInfos) == 0 && membershipVersion == 0 && membershipEpoch == 0 {
		return nil, nil, nil
	}
	var selfAddr string
	if c.currentNode != nil {
		selfAddr = c.currentNode.ServiceAddr
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if !c.acceptMembershipSnapshotLocked(membershipVersion, membershipEpoch, snapshotSeq) {
		return nil, nil, nil
	}

	known := make(map[string]*Member, len(c.members))
	for _, m := range c.members {
		if m.memberInfo != nil {
			known[m.memberInfo.ServiceAddr] = m
		}
	}

	snapshotMembers := make(map[string]struct{}, len(infos))
	for _, info := range infos {
		if info == nil || info.ServiceAddr == "" || info.ServiceAddr == selfAddr {
			continue
		}
		snapshotMembers[info.ServiceAddr] = struct{}{}
		if c.rpcClient != nil {
			c.rpcClient.allowPool(info.ServiceAddr)
		}
		if member, ok := known[info.ServiceAddr]; ok {
			if !memberInfoEqual(member.memberInfo, info) {
				member.memberInfo = info
				updated = append(updated, info)
			}
			member.lastHeartbeatAt = now
			member.epochStaleSince = time.Time{}
			member.membershipEpoch = c.membershipEpoch
			member.membershipVersion = c.membershipVersion
			continue
		}
		member := &Member{memberInfo: info, lastHeartbeatAt: now, membershipEpoch: c.membershipEpoch, membershipVersion: c.membershipVersion}
		c.members = append(c.members, member)
		known[info.ServiceAddr] = member
		added = append(added, info)
	}

	for _, info := range removedInfos {
		if info == nil || info.GetServiceAddr() == "" || info.GetServiceAddr() == selfAddr {
			continue
		}
		if info.GetMembershipVersion() > c.removedMembershipVersion {
			c.removedMembershipVersion = info.GetMembershipVersion()
		}
		member, ok := known[info.GetServiceAddr()]
		if ok && member.membershipEpoch == c.membershipEpoch && member.membershipVersion > info.GetMembershipVersion() {
			continue
		}
		if c.delMemberLocked(info.GetServiceAddr()) {
			delete(known, info.GetServiceAddr())
			removed = append(removed, info.GetServiceAddr())
		}
	}
	if resetMembership {
		if c.removedMembershipVersion < membershipVersion {
			c.removedMembershipVersion = membershipVersion
		}
		removed = append(removed, c.pruneAbsentMembersLocked(known, snapshotMembers, selfAddr, now, false)...)
	} else {
		removed = append(removed, c.pruneAbsentMembersLocked(known, snapshotMembers, selfAddr, now, true)...)
	}
	return added, updated, removed
}

func (c *cluster) pruneAbsentMembersLocked(known map[string]*Member, snapshotMembers map[string]struct{}, selfAddr string, now time.Time, staleEpochOnly bool) (removed []string) {
	staleDeadline := 4 * env.Heartbeat
	for i := 0; i < len(c.members); {
		member := c.members[i]
		addr := ""
		if member.memberInfo != nil {
			addr = member.memberInfo.ServiceAddr
		}
		if addr == "" || addr == selfAddr {
			i++
			continue
		}
		if _, ok := snapshotMembers[addr]; ok {
			i++
			continue
		}
		if staleEpochOnly {
			if member.membershipEpoch == c.membershipEpoch {
				i++
				continue
			}
			if member.epochStaleSince.IsZero() {
				member.epochStaleSince = now
				i++
				continue
			}
			if now.Sub(member.epochStaleSince) <= staleDeadline {
				i++
				continue
			}
		}
		c.members = append(c.members[:i], c.members[i+1:]...)
		delete(known, addr)
		removed = append(removed, addr)
	}
	return removed
}

func (c *cluster) pruneExpiredStaleEpochMembers(now time.Time) []string {
	var selfAddr string
	if c.currentNode != nil {
		selfAddr = c.currentNode.ServiceAddr
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pruneAbsentMembersLocked(nil, nil, selfAddr, now, true)
}
func memberInfoEqual(a, b *clusterpb.MemberInfo) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.GetLabel() != b.GetLabel() || a.GetServiceAddr() != b.GetServiceAddr() {
		return false
	}
	aServices := a.GetServices()
	bServices := b.GetServices()
	if len(aServices) != len(bServices) {
		return false
	}
	for i := range aServices {
		if aServices[i] != bServices[i] {
			return false
		}
	}
	return true
}

func (c *cluster) delMember(addr string, membershipVersion, membershipEpoch uint64) bool {
	c.mu.Lock()
	if !c.acceptMembershipEventLocked(membershipVersion, membershipEpoch) {
		c.mu.Unlock()
		return false
	}
	c.delMemberLocked(addr)
	c.mu.Unlock()
	if c.rpcClient != nil {
		c.rpcClient.removePool(addr)
	}
	return true
}

func (c *cluster) delMemberLocked(addr string) bool {
	var index = -1
	for i, member := range c.members {
		if member.memberInfo.ServiceAddr == addr {
			index = i
			break
		}
	}
	if index == -1 {
		return false
	}
	if index >= len(c.members)-1 {
		c.members = c.members[:index]
	} else {
		c.members = append(c.members[:index], c.members[index+1:]...)
	}
	return true
}

func (c *cluster) ensureMembershipEpochLocked() uint64 {
	if c.membershipEpoch == 0 {
		c.membershipEpoch = newMembershipEpoch()
	}
	return c.membershipEpoch
}

func (c *cluster) nextMembershipSnapshotSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.membershipSnapshotSeq++
	if c.membershipSnapshotSeq == 0 {
		c.membershipSnapshotSeq = 1
	}
	return c.membershipSnapshotSeq
}

func (c *cluster) membershipState() (membershipVersion, membershipEpoch, removedMembershipVersion uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.membershipVersion, c.membershipEpoch, c.removedMembershipVersion
}

func (c *cluster) heartbeatRequiresResetLocked(req *clusterpb.HeartbeatRequest, membershipEpoch uint64) bool {
	if c.membershipCompactVersion == 0 {
		return false
	}
	if req.GetMembershipEpoch() != membershipEpoch {
		return false
	}
	return req.GetRemovedMembershipVersion() < c.membershipCompactVersion
}

func (c *cluster) heartbeatMemberListSyncedLocked(req *clusterpb.HeartbeatRequest, membershipEpoch uint64) bool {
	return req.GetMembershipEpoch() == membershipEpoch && req.GetMembershipVersion() == c.membershipVersion
}

func (c *cluster) acceptMembershipSnapshotLocked(membershipVersion, membershipEpoch, snapshotSeq uint64) bool {
	if snapshotSeq > 0 {
		if snapshotSeq < c.membershipSnapshotSeq {
			return false
		}
		if snapshotSeq > c.membershipSnapshotSeq {
			c.membershipSnapshotSeq = snapshotSeq
		}
	}
	if membershipEpoch > 0 {
		if membershipEpoch != c.membershipEpoch {
			c.membershipEpoch = membershipEpoch
			c.membershipVersion = 0
			c.removedMembershipVersion = 0
			c.membershipCompactVersion = 0
		}
	}
	return c.acceptMembershipVersionLocked(membershipVersion)
}

func (c *cluster) acceptMembershipEventLocked(membershipVersion, membershipEpoch uint64) bool {
	if membershipEpoch > 0 {
		if c.membershipEpoch == 0 {
			c.membershipEpoch = membershipEpoch
			c.membershipVersion = 0
			c.removedMembershipVersion = 0
			c.membershipCompactVersion = 0
		} else if membershipEpoch != c.membershipEpoch {
			return false
		}
	}
	return c.acceptMembershipVersionLocked(membershipVersion)
}

func (c *cluster) acceptMembershipVersionLocked(membershipVersion uint64) bool {
	if membershipVersion > 0 && membershipVersion < c.membershipVersion {
		return false
	}
	if membershipVersion > c.membershipVersion {
		c.membershipVersion = membershipVersion
	}
	return true
}

func (c *cluster) rememberRemovedMemberLocked(addr string, membershipVersion uint64) {
	if addr == "" {
		return
	}
	now := time.Now()
	c.pruneRemovedMembersLocked(now)
	if c.removedMembers == nil {
		c.removedMembers = make(map[string]removedMemberTombstone)
	}
	c.removedMembers[addr] = removedMemberTombstone{
		membershipVersion: membershipVersion,
		removedAt:         now,
	}
}

func (c *cluster) clearRemovedMemberLocked(addr string) {
	if c.removedMembers == nil || addr == "" {
		return
	}
	delete(c.removedMembers, addr)
}

func (c *cluster) pruneRemovedMembersLocked(now time.Time) {
	retention := removedMemberRetention()
	if c.removedMembers == nil || retention <= 0 {
		return
	}
	for addr, tombstone := range c.removedMembers {
		if tombstone.removedAt.IsZero() {
			continue
		}
		if now.Sub(tombstone.removedAt) > retention {
			if tombstone.membershipVersion > c.membershipCompactVersion {
				c.membershipCompactVersion = tombstone.membershipVersion
			}
			delete(c.removedMembers, addr)
		}
	}
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
