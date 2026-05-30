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
	currentNode *Node
	rpcClient   *rpcClient

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
	if req.MemberInfo == nil {
		return nil, ErrInvalidRegisterReq
	}
	resp := &clusterpb.RegisterResponse{}
	notifyMembers := make([]*Member, 0)

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
	for _, m := range c.members {
		resp.Members = append(resp.Members, m.memberInfo)
		if !m.isMaster {
			notifyMembers = append(notifyMembers, m)
		}
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
	c.mu.Unlock()

	// Notify registered node to update remote services
	newMember := &clusterpb.NewMemberRequest{MemberInfo: req.MemberInfo, MembershipVersion: resp.MembershipVersion, MembershipEpoch: resp.MembershipEpoch}
	for _, m := range notifyMembers {
		// Best-effort notification: a peer that is briefly unreachable during
		// churn must NOT abort the registration (which would also skip notifying
		// every remaining peer and fail the registering node). The member→master
		// heartbeat carries the authoritative member list, so any missed
		// NewMember push self-heals on the next heartbeat via reconcileMembers.
		pool, err := c.rpcClient.getConnPool(m.memberInfo.ServiceAddr)
		if err != nil {
			log.Println("Notify peer of new member failed (best-effort), get conn pool", m.memberInfo.ServiceAddr, err)
			continue
		}
		client := clusterpb.NewMemberClient(pool.Get())
		if _, err := client.NewMember(context.Background(), newMember); err != nil {
			log.Println("Notify peer of new member failed (best-effort)", m.memberInfo.ServiceAddr, err)
			continue
		}
	}

	log.Println("New peer register to cluster", req.MemberInfo.ServiceAddr)

	// Register services to current node
	c.currentNode.handler.delMember(req.MemberInfo.ServiceAddr)
	c.currentNode.handler.addRemoteService(req.MemberInfo)
	return resp, nil
}

// Unregister implements the MasterServer gRPC service
func (c *cluster) Unregister(_ context.Context, req *clusterpb.UnregisterRequest) (*clusterpb.UnregisterResponse, error) {
	if req.ServiceAddr == "" {
		return nil, ErrInvalidRegisterReq
	}

	resp := &clusterpb.UnregisterResponse{}
	var (
		index         = -1
		removedMember *Member
		notifyMembers = make([]*Member, 0)
		version       uint64
	)
	c.mu.Lock()
	for i, m := range c.members {
		if m.memberInfo.ServiceAddr == req.ServiceAddr {
			index = i
			break
		}
	}
	if index < 0 {
		c.mu.Unlock()
		return nil, fmt.Errorf("address %s has not registered", req.ServiceAddr)
	}
	removedMember = c.members[index]
	for i, m := range c.members {
		if i == index {
			// this node is down.
			continue
		}

		if m.MemberInfo().ServiceAddr == c.currentNode.ServiceAddr {
			continue
		}
		notifyMembers = append(notifyMembers, m)
	}
	c.membershipVersion++
	membershipEpoch := c.ensureMembershipEpochLocked()
	version = c.membershipVersion
	c.rememberRemovedMemberLocked(req.ServiceAddr, version)
	if index >= len(c.members)-1 {
		c.members = c.members[:index]
	} else {
		c.members = append(c.members[:index], c.members[index+1:]...)
	}
	c.mu.Unlock()
	c.currentNode.handler.delMember(req.ServiceAddr)

	// Notify registered node to update remote services
	delMember := &clusterpb.DelMemberRequest{ServiceAddr: req.ServiceAddr, MembershipVersion: version, MembershipEpoch: membershipEpoch}
	for _, m := range notifyMembers {
		pool, err := c.rpcClient.getConnPool(m.memberInfo.ServiceAddr)
		if err != nil {
			log.Println("Notify peer of deleted member failed (best-effort), get conn pool", m.memberInfo.ServiceAddr, err)
			continue
		}
		client := clusterpb.NewMemberClient(pool.Get())
		_, err = client.DelMember(context.Background(), delMember)
		if err != nil {
			log.Println("Notify peer of deleted member failed (best-effort)", m.memberInfo.ServiceAddr, err)
			continue
		}
	}

	log.Println("Exists peer unregister to cluster", req.ServiceAddr)

	if c.currentNode.UnregisterCallback != nil {
		removedInfo := removedMember.MemberInfo()
		c.currentNode.UnregisterCallback(*removedMember, func() {
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
	log.Println("Receive Heartbeat from: ", req.MemberInfo.Label)

	var addedMember *clusterpb.MemberInfo
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
		addedMember = req.MemberInfo
		log.Println("Heartbeat peer register to cluster", req.MemberInfo.ServiceAddr)
	}

	// Return the authoritative member list so the requesting node can reconcile
	// any NewMember push it missed during churn (see reconcileMembers).
	// Mirrors RegisterResponse: include every member, master included.
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

	if addedMember != nil {
		c.currentNode.handler.addRemoteService(addedMember)
	}
	return resp, nil
}

func (c *cluster) checkMemberHeartbeat() {
	check := func() {
		unregisterMembers := make([]*Member, 0)
		// check heartbeat time
		for _, m := range c.members {
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
		ticker := time.NewTicker(env.Heartbeat)
		for {
			select {
			case <-ticker.C:
				if !c.currentNode.IsMaster {
					ticker.Stop()
					return
				}
				check()
			}
		}
	}()
}

// pingNodes sends ping request to all nodes in the cluster
// withLabels is the labels of nodes that should be pinged
// returns the labels of nodes that are alive and dead
func (c *cluster) pingNodes(withLabels []string) (lives []string, dies []string, err error) {
	for _, m := range c.members {
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
			log.Debug("Ping node %s, label %s success, response: %s", m.memberInfo.ServiceAddr, m.memberInfo.Label, string(resp.String()))
			if resp.Msg == "pong" {
				lives = append(lives, m.memberInfo.Label)
			} else {
				dies = append(dies, m.memberInfo.Label)
			}
		}
	}

	for _, label := range withLabels {
		var found bool
		for _, m := range c.members {
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

func (c *cluster) initMembers(members []*clusterpb.MemberInfo, membershipVersion, membershipEpoch uint64) {
	c.mu.Lock()
	c.acceptMembershipSnapshotLocked(membershipVersion, membershipEpoch, 0)
	for _, info := range members {
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
	var found bool
	for _, member := range c.members {
		if member.memberInfo.ServiceAddr == info.ServiceAddr {
			member.memberInfo = info
			member.membershipEpoch = c.membershipEpoch
			member.membershipVersion = c.membershipVersion
			found = true
			break
		}
	}
	if !found {
		c.members = append(c.members, &Member{
			memberInfo:        info,
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
// It intentionally never removes members only because they are absent from the
// live list: pruning here would let a transient or incomplete master view (for
// example right after a master restart, before every member has re-heartbeated)
// drop a live peer and break routing. Removals self-heal via explicit
// RemovedMember tombstones; if old tombstones have already been compacted,
// the master marks stale peers for a full membership reset.
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
		if member, ok := known[info.ServiceAddr]; ok {
			if !memberInfoEqual(member.memberInfo, info) {
				member.memberInfo = info
				member.membershipEpoch = c.membershipEpoch
				member.membershipVersion = c.membershipVersion
				updated = append(updated, info)
			} else {
				member.membershipEpoch = c.membershipEpoch
				member.membershipVersion = c.membershipVersion
			}
			continue
		}
		member := &Member{memberInfo: info, membershipEpoch: c.membershipEpoch, membershipVersion: c.membershipVersion}
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
			c.members = append(c.members[:i], c.members[i+1:]...)
			delete(known, addr)
			removed = append(removed, addr)
		}
	}
	if resetMembership && membershipVersion > c.removedMembershipVersion {
		c.removedMembershipVersion = membershipVersion
	}
	return added, updated, removed
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
	defer c.mu.Unlock()
	if !c.acceptMembershipEventLocked(membershipVersion, membershipEpoch) {
		return false
	}
	return c.delMemberLocked(addr)
}

func (c *cluster) delMemberLocked(addr string) bool {
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
