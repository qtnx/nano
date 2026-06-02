package cluster

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lonng/nano/cluster/clusterpb"
)

const maxHealthProbeConcurrency = 32

// HealthReport describes cluster member health at both label and address level.
// Labels are unique by Label. Members contains one row for each probed member.
type HealthReport struct {
	Labels  []LabelHealth
	Members []MemberHealth
}

// LabelHealth summarizes live/dead member counts for one service label. For
// non-missing labels, Total equals Live+Dead. Missing means a requested label had
// no matching member candidates, so no member was dialed for that label.
type LabelHealth struct {
	Label   string
	Total   int
	Live    int
	Dead    int
	Missing bool
}

// MemberHealth describes the health result for one advertised member address.
// Error is diagnostic text for a non-live probe and is empty for live members.
type MemberHealth struct {
	Label       string
	ServiceAddr string
	Alive       bool
	Error       string
}

// CheckClusterHealth probes cluster members matching labels and reports per-label
// and per-address health. When labels is non-empty, members with other labels are
// filtered before any dial or ping is attempted.
func (n *Node) CheckClusterHealth(ctx context.Context, labels []string) (*HealthReport, error) {
	if ctx == nil {
		return nil, errors.New("nil context")
	}
	if n == nil || n.cluster == nil {
		return nil, fmt.Errorf("Node is not initialized")
	}
	return n.cluster.checkClusterHealth(ctx, labels)
}

func (c *cluster) checkClusterHealth(ctx context.Context, labels []string) (*HealthReport, error) {
	if c == nil {
		return nil, fmt.Errorf("Node is not initialized")
	}

	snapshot := c.memberSnapshots()

	report := &HealthReport{}
	requested := makeLabelSet(labels)
	labelIndex := make(map[string]int, len(labels))

	// Pre-populate every requested label as Missing; the candidate scan below
	// clears Missing and accumulates live/dead counts for labels that match a member.
	for _, label := range labels {
		if _, exists := labelIndex[label]; exists {
			continue
		}
		labelIndex[label] = len(report.Labels)
		report.Labels = append(report.Labels, LabelHealth{Label: label, Missing: true})
	}

	candidates := make([]memberSnapshot, 0, len(snapshot))
	for _, m := range snapshot {
		if m.isMaster || m.serviceAddr == "" {
			continue
		}
		label := m.label
		if len(requested) > 0 {
			if _, ok := requested[label]; !ok {
				continue
			}
		}
		if _, exists := labelIndex[label]; !exists {
			labelIndex[label] = len(report.Labels)
			report.Labels = append(report.Labels, LabelHealth{Label: label})
		}
		idx := labelIndex[label]
		report.Labels[idx].Total++
		report.Labels[idx].Missing = false
		candidates = append(candidates, m)
	}

	if len(candidates) == 0 {
		return report, nil
	}

	report.Members = make([]MemberHealth, len(candidates))
	semSize := len(candidates)
	if semSize > maxHealthProbeConcurrency {
		semSize = maxHealthProbeConcurrency
	}
	sem := make(chan struct{}, semSize)
	var wg sync.WaitGroup
	for i, m := range candidates {
		i, m := i, m
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				report.Members[i] = memberHealthFromError(m, ctx.Err())
				return
			}
			if err := c.pingMember(ctx, m.serviceAddr); err != nil {
				report.Members[i] = memberHealthFromError(m, err)
				return
			}
			report.Members[i] = MemberHealth{Label: m.label, ServiceAddr: m.serviceAddr, Alive: true}
		}()
	}
	wg.Wait()

	for _, member := range report.Members {
		idx := labelIndex[member.Label]
		if member.Alive {
			report.Labels[idx].Live++
		} else {
			report.Labels[idx].Dead++
		}
	}
	return report, nil
}

func makeLabelSet(labels []string) map[string]struct{} {
	if len(labels) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		set[label] = struct{}{}
	}
	return set
}

func memberHealthFromError(m memberSnapshot, err error) MemberHealth {
	mh := MemberHealth{Label: m.label, ServiceAddr: m.serviceAddr}
	if err != nil {
		mh.Error = err.Error()
	}
	return mh
}

func (c *cluster) pingMember(ctx context.Context, addr string) error {
	if c.pingMemberFn != nil {
		return c.pingMemberFn(ctx, addr)
	}
	if c.rpcClient == nil {
		return fmt.Errorf("rpc client is nil")
	}
	pool, err := c.rpcClient.getConnPoolWithContext(ctx, addr)
	if err != nil {
		return err
	}
	resp, err := clusterpb.NewMemberClient(pool.Get()).Ping(ctx, &clusterpb.PingRequest{})
	if err != nil {
		return err
	}
	if resp.GetMsg() != "pong" {
		return fmt.Errorf("unexpected ping response %q", resp.GetMsg())
	}
	return nil
}
