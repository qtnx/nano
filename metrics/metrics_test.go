package metrics

import (
	"fmt"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// resetKnownRoutes clears the registered-route set between tests.
func resetKnownRoutes() {
	knownRoutesMu.Lock()
	knownRoutes = map[string]struct{}{}
	knownRoutesMu.Unlock()
}

// resetIPConnCounts clears the per-IP active-connection tracking between tests.
func resetIPConnCounts() {
	ipConnMu.Lock()
	ipConnCounts = map[string]int{}
	ipConnMu.Unlock()
}

// TestObserveRouteRequestDurationBoundsCardinality reproduces H28: a client can
// vary the method suffix of a route to create an unbounded number of retained
// histogram series (memory DoS). ObserveRouteRequestDuration must collapse
// unregistered routes into a single bounded sentinel label.
func TestObserveRouteRequestDurationBoundsCardinality(t *testing.T) {
	RouteRequestDuration.Reset()
	resetKnownRoutes()

	RegisterRoute("room.known")
	ObserveRouteRequestDuration("room.known", "Request", "local", 0.01)

	// Attacker varies the method suffix under a known service.
	for i := 0; i < 1000; i++ {
		ObserveRouteRequestDuration(fmt.Sprintf("room.attack%d", i), "Request", "remote", 0.01)
	}

	// Expected bounded series: (room.known, Request, local) and
	// (unknown, Request, remote) => 2 regardless of attacker route count.
	if got := testutil.CollectAndCount(RouteRequestDuration); got != 2 {
		t.Fatalf("route label cardinality unbounded: got %d series, want 2", got)
	}
}

// TestObserveRouteRequestDurationRecordsKnownRoutes ensures registered routes
// are still recorded under their real name (no loss of useful observability)
// while unregistered routes collapse to the sentinel.
func TestObserveRouteRequestDurationRecordsKnownRoutes(t *testing.T) {
	RouteRequestDuration.Reset()
	resetKnownRoutes()

	RegisterRoute("room.join")
	RegisterRoute("room.leave")
	ObserveRouteRequestDuration("room.join", "Request", "local", 0.01)
	ObserveRouteRequestDuration("room.leave", "Notify", "remote", 0.02)
	ObserveRouteRequestDuration("room.unregistered", "Request", "remote", 0.03)

	// The real route must keep a non-zero sample count.
	if c := testutil.CollectAndCount(RouteRequestDuration); c != 3 {
		t.Fatalf("expected 3 bounded series, got %d", c)
	}
}

// TestConnectionsPerIPReclaimedAtZero reproduces H29: per-IP gauge children are
// never deleted, so IP churn (reconnect/botnet) grows the registry without
// bound. Inc/Dec helpers must delete the series once the active count is zero.
func TestConnectionsPerIPReclaimedAtZero(t *testing.T) {
	ConnectionsPerIP.Reset()
	resetIPConnCounts()

	for i := 0; i < 1000; i++ {
		ip := fmt.Sprintf("10.0.0.%d", i)
		IncConnectionsPerIP(ip)
		DecConnectionsPerIP(ip)
	}

	if got := testutil.CollectAndCount(ConnectionsPerIP); got != 0 {
		t.Fatalf("per-ip series not reclaimed after disconnect: got %d, want 0", got)
	}
}

// TestConnectionsPerIPTracksActive verifies that active connections are still
// reflected accurately and only fully-drained IPs are reclaimed.
func TestConnectionsPerIPTracksActive(t *testing.T) {
	ConnectionsPerIP.Reset()
	resetIPConnCounts()

	IncConnectionsPerIP("10.1.1.1")
	IncConnectionsPerIP("10.1.1.1")
	IncConnectionsPerIP("10.2.2.2")
	DecConnectionsPerIP("10.1.1.1") // 10.1.1.1 -> 1, still active
	DecConnectionsPerIP("10.2.2.2") // 10.2.2.2 -> 0, reclaimed

	if got := testutil.CollectAndCount(ConnectionsPerIP); got != 1 {
		t.Fatalf("active ip series wrong: got %d, want 1", got)
	}
	if v := testutil.ToFloat64(ConnectionsPerIP.WithLabelValues("10.1.1.1")); v != 1 {
		t.Fatalf("active gauge value wrong: got %v, want 1", v)
	}
}

// TestConnectionsPerIPDecWithoutInc ensures a stray Dec (no tracked Inc) does
// not create or corrupt a series.
func TestConnectionsPerIPDecWithoutInc(t *testing.T) {
	ConnectionsPerIP.Reset()
	resetIPConnCounts()

	DecConnectionsPerIP("10.9.9.9")

	if got := testutil.CollectAndCount(ConnectionsPerIP); got != 0 {
		t.Fatalf("stray Dec created series: got %d, want 0", got)
	}
}

// TestMetricsHelpersRaceSafe exercises the helpers concurrently; run under
// -race to confirm the internal tracking maps are properly synchronized.
func TestMetricsHelpersRaceSafe(t *testing.T) {
	RouteRequestDuration.Reset()
	ConnectionsPerIP.Reset()
	resetKnownRoutes()
	resetIPConnCounts()

	RegisterRoute("room.join")

	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				RegisterRoute(fmt.Sprintf("svc.route%d", i%16))
				ObserveRouteRequestDuration(fmt.Sprintf("svc.route%d", i), "Request", "local", 0.01)
				ip := fmt.Sprintf("172.16.%d.%d", g, i%32)
				IncConnectionsPerIP(ip)
				DecConnectionsPerIP(ip)
			}
		}(g)
	}
	wg.Wait()
}
