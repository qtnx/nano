package cluster

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/lonng/nano/cluster/clusterpb"
)

func TestCheckClusterHealthFiltersUnrequestedLabelsBeforePing(t *testing.T) {
	n := newTestNode()
	c := n.cluster
	c.members = []*Member{
		{memberInfo: mkMemberInfo("user-service", "user-live:8088")},
		{memberInfo: mkMemberInfo("chat-service", "chat-stale:8088")},
	}

	var called []string
	c.pingMemberFn = func(_ context.Context, addr string) error {
		called = append(called, addr)
		if addr == "chat-stale:8088" {
			return errors.New("unrequested stale member was dialed")
		}
		return nil
	}

	report, err := n.CheckClusterHealth(context.Background(), []string{"user-service"})
	if err != nil {
		t.Fatalf("CheckClusterHealth returned error: %v", err)
	}
	if !reflect.DeepEqual(called, []string{"user-live:8088"}) {
		t.Fatalf("pinged addresses = %v, want only requested label address", called)
	}
	assertLabelHealth(t, report, "user-service", LabelHealth{Label: "user-service", Total: 1, Live: 1, Dead: 0})
	if len(report.Members) != 1 || report.Members[0].ServiceAddr != "user-live:8088" || !report.Members[0].Alive {
		t.Fatalf("members = %+v, want one live user-service member", report.Members)
	}
}

func TestCheckClusterHealthReportsMissingRequestedLabelWithoutDialingUnrequestedMembers(t *testing.T) {
	n := newTestNode()
	c := n.cluster
	c.members = []*Member{{memberInfo: mkMemberInfo("chat-service", "chat-stale:8088")}}

	var called []string
	c.pingMemberFn = func(_ context.Context, addr string) error {
		called = append(called, addr)
		return errors.New("unexpected dial")
	}

	report, err := n.CheckClusterHealth(context.Background(), []string{"user-service"})
	if err != nil {
		t.Fatalf("CheckClusterHealth returned error: %v", err)
	}
	if len(called) != 0 {
		t.Fatalf("pinged addresses = %v, want none for missing requested label", called)
	}
	assertLabelHealth(t, report, "user-service", LabelHealth{Label: "user-service", Missing: true})
	if len(report.Members) != 0 {
		t.Fatalf("members = %+v, want none", report.Members)
	}
}

func TestCheckClusterHealthReportsSameLabelReplicaDetails(t *testing.T) {
	n := newTestNode()
	c := n.cluster
	c.members = []*Member{
		{memberInfo: mkMemberInfo("user-service", "user-live:8088")},
		{memberInfo: mkMemberInfo("user-service", "user-dead:8088")},
		{memberInfo: mkMemberInfo("chat-service", "chat-stale:8088")},
	}
	c.pingMemberFn = func(_ context.Context, addr string) error {
		if addr == "user-dead:8088" {
			return errors.New("connection refused")
		}
		if addr == "chat-stale:8088" {
			return errors.New("unrequested stale member was dialed")
		}
		return nil
	}

	report, err := n.CheckClusterHealth(context.Background(), []string{"user-service"})
	if err != nil {
		t.Fatalf("CheckClusterHealth returned error: %v", err)
	}
	assertLabelHealth(t, report, "user-service", LabelHealth{Label: "user-service", Total: 2, Live: 1, Dead: 1})
	if len(report.Members) != 2 {
		t.Fatalf("members = %+v, want two user-service entries", report.Members)
	}
	assertMemberHealth(t, report, "user-live:8088", true)
	assertMemberHealth(t, report, "user-dead:8088", false)
}

func TestCheckClusterHealthReportsAllDeadSameLabel(t *testing.T) {
	n := newTestNode()
	c := n.cluster
	c.members = []*Member{
		{memberInfo: mkMemberInfo("user-service", "user-a:8088")},
		{memberInfo: mkMemberInfo("user-service", "user-b:8088")},
	}
	c.pingMemberFn = func(context.Context, string) error { return errors.New("no route to host") }

	report, err := n.CheckClusterHealth(context.Background(), []string{"user-service"})
	if err != nil {
		t.Fatalf("CheckClusterHealth returned error: %v", err)
	}
	assertLabelHealth(t, report, "user-service", LabelHealth{Label: "user-service", Total: 2, Live: 0, Dead: 2})
}

func TestPingNodesKeepsLegacyAllMemberScan(t *testing.T) {
	c := &cluster{rpcClient: &rpcClient{isClosed: true, pools: map[string]*connPool{}}}
	c.members = []*Member{
		{isMaster: true, memberInfo: &clusterpb.MemberInfo{Label: "master", ServiceAddr: "master:8088"}},
		{memberInfo: mkMemberInfo("user-service", "user-dead:8088")},
		{memberInfo: mkMemberInfo("chat-service", "chat-dead:8088")},
	}

	lives, dies, err := c.pingNodes([]string{"missing-service"})
	if err != nil {
		t.Fatalf("pingNodes returned error: %v", err)
	}
	if len(lives) != 0 {
		t.Fatalf("lives = %v, want none", lives)
	}
	if !reflect.DeepEqual(dies, []string{"missing-service"}) {
		t.Fatalf("dies = %v, want only missing requested label because legacy pool failures are skipped", dies)
	}
}

func assertLabelHealth(t *testing.T, report *HealthReport, label string, want LabelHealth) {
	t.Helper()
	for _, got := range report.Labels {
		if got.Label == label {
			if got != want {
				t.Fatalf("label %q health = %+v, want %+v", label, got, want)
			}
			return
		}
	}
	t.Fatalf("label %q not found in report %+v", label, report.Labels)
}

func assertMemberHealth(t *testing.T, report *HealthReport, addr string, alive bool) {
	t.Helper()
	for _, got := range report.Members {
		if got.ServiceAddr == addr {
			if got.Alive != alive {
				t.Fatalf("member %q alive = %v, want %v", addr, got.Alive, alive)
			}
			if !alive && got.Error == "" {
				t.Fatalf("dead member %q has empty error detail", addr)
			}
			return
		}
	}
	t.Fatalf("member %q not found in report %+v", addr, report.Members)
}
