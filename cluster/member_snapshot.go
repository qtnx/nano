package cluster

import (
	"time"

	"github.com/lonng/nano/cluster/clusterpb"
)

type memberSnapshot struct {
	isMaster          bool
	label             string
	serviceAddr       string
	lastHeartbeatAt   time.Time
	memberInfo        *clusterpb.MemberInfo
	membershipEpoch   uint64
	membershipVersion uint64
}

func (c *cluster) memberSnapshots() []memberSnapshot {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.memberSnapshotsLocked()
}

func (c *cluster) memberSnapshotsLocked() []memberSnapshot {
	snapshot := make([]memberSnapshot, 0, len(c.members))
	for _, m := range c.members {
		if m == nil {
			continue
		}
		info := cloneMemberInfo(m.memberInfo)
		s := memberSnapshot{
			isMaster:          m.isMaster,
			lastHeartbeatAt:   m.lastHeartbeatAt,
			memberInfo:        info,
			membershipEpoch:   m.membershipEpoch,
			membershipVersion: m.membershipVersion,
		}
		if info != nil {
			s.label = info.Label
			s.serviceAddr = info.ServiceAddr
		}
		snapshot = append(snapshot, s)
	}
	return snapshot
}

func cloneMemberInfo(info *clusterpb.MemberInfo) *clusterpb.MemberInfo {
	if info == nil {
		return nil
	}
	clone := *info
	if len(info.Services) != 0 {
		clone.Services = append([]string(nil), info.Services...)
	}
	return &clone
}

func (s memberSnapshot) member() Member {
	return Member{
		isMaster:          s.isMaster,
		memberInfo:        cloneMemberInfo(s.memberInfo),
		lastHeartbeatAt:   s.lastHeartbeatAt,
		membershipEpoch:   s.membershipEpoch,
		membershipVersion: s.membershipVersion,
	}
}
