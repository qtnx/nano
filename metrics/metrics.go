package metrics

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/shirou/gopsutil/cpu"
)

var (
	TotalConnections = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "total_connections",
			Help: "Total number of connections established",
		},
	)

	CurrentConnections = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "current_connections",
			Help: "Current number of active connections",
		},
	)

	ConnectionDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "connection_duration_seconds",
			Help:    "Duration of connections in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	RouteRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "route_request_duration_seconds",
			Help:    "Duration of processing requests per route in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"route", "type", "request_type"},
	)

	ConnectionsPerIP = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "connections_per_ip",
			Help: "Number of active connections per IP address",
		},
		[]string{"ip"},
	)

	AgentClose = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "agent_close_total",
			Help: "Total number of agent close operations",
		},
	)
	ClientClosedConnections = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "client_closed_connections_total",
			Help: "Total number of connections closed by the client",
		},
	)

	ServerClosedConnections = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "server_closed_connections_total",
			Help: "Total number of connections closed by the server",
		},
	)

	SchedulePendingTasks = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "schedule_pending_tasks",
			Help: "Number of pending tasks in the scheduler",
		},
	)

	CPUUsage = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cpu_usage_percent",
			Help: "Current CPU usage in percent",
		},
		[]string{"cpu"},
	)

	CPUUserTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cpu_user_time_seconds",
			Help: "CPU time spent in user mode in seconds",
		},
		[]string{"cpu"},
	)

	CPUSystemTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cpu_system_time_seconds",
			Help: "CPU time spent in system mode in seconds",
		},
		[]string{"cpu"},
	)
)

func init() {
	prometheus.MustRegister(TotalConnections)
	prometheus.MustRegister(CurrentConnections)
	prometheus.MustRegister(ConnectionDuration)
	prometheus.MustRegister(RouteRequestDuration)
	prometheus.MustRegister(ConnectionsPerIP)
	prometheus.MustRegister(AgentClose)
	prometheus.MustRegister(ClientClosedConnections)
	prometheus.MustRegister(ServerClosedConnections)
	prometheus.MustRegister(SchedulePendingTasks)
	prometheus.MustRegister(CPUUsage)
	prometheus.MustRegister(CPUUserTime)
	prometheus.MustRegister(CPUSystemTime)
	go updateCPUMetrics()
}

func updateCPUMetrics() {
	for {
		percentages, err := cpu.Percent(time.Second, true)
		if err != nil {
			log.Printf("Error getting CPU usage: %v", err)
			continue
		}

		for i, percentage := range percentages {
			CPUUsage.WithLabelValues(fmt.Sprintf("cpu%d", i)).Set(percentage)
		}

		times, err := cpu.Times(true)
		if err != nil {
			log.Printf("Error getting CPU times: %v", err)
			continue
		}

		for i, t := range times {
			CPUUserTime.WithLabelValues(fmt.Sprintf("cpu%d", i)).Set(t.User)
			CPUSystemTime.WithLabelValues(fmt.Sprintf("cpu%d", i)).Set(t.System)
		}

		time.Sleep(15 * time.Second)
	}
}

// --- H28: bounded route-label cardinality (memory DoS hardening) ---------
//
// RouteRequestDuration carries a client-influenced `route` label. A peer can
// vary the method suffix under a known service to mint an unbounded number of
// retained histogram series. To keep cardinality bounded, callers register the
// routes they actually serve via RegisterRoute (e.g. when binding handlers)
// and record observations through ObserveRouteRequestDuration, which collapses
// any unregistered route into the single `unknownRoute` sentinel.

var (
	knownRoutesMu sync.RWMutex
	knownRoutes   = map[string]struct{}{}
)

// unknownRoute is the bounded sentinel label used for any route that was never
// registered, preventing attacker-controlled label cardinality growth.
const unknownRoute = "unknown"

// RegisterRoute marks a route as known so that ObserveRouteRequestDuration
// records it under its real name instead of the unknownRoute sentinel. It is
// safe to call concurrently and is idempotent.
func RegisterRoute(route string) {
	if route == "" {
		return
	}
	knownRoutesMu.Lock()
	knownRoutes[route] = struct{}{}
	knownRoutesMu.Unlock()
}

// normalizeRoute returns route if it has been registered, otherwise the
// bounded unknownRoute sentinel.
func normalizeRoute(route string) string {
	knownRoutesMu.RLock()
	_, ok := knownRoutes[route]
	knownRoutesMu.RUnlock()
	if ok {
		return route
	}
	return unknownRoute
}

// ObserveRouteRequestDuration records the processing duration (in seconds) for
// a route, normalizing unregistered routes to a bounded sentinel label so the
// `route` dimension cannot be driven to unbounded cardinality by a client.
func ObserveRouteRequestDuration(route, msgType, requestType string, seconds float64) {
	RouteRequestDuration.WithLabelValues(normalizeRoute(route), msgType, requestType).Observe(seconds)
}

// --- H29: per-IP connection gauge with reclamation -----------------------
//
// ConnectionsPerIP carries a per-source-IP label. Inc/Dec alone never remove a
// child, so IP churn (reconnect storms / botnets) permanently grows the series
// count. The helpers below track the active count per IP and delete the gauge
// child once it drains to zero, bounding the registry to currently-connected
// IPs.

var (
	ipConnMu     sync.Mutex
	ipConnCounts = map[string]int{}
)

// IncConnectionsPerIP increments the active-connection gauge for ip and the
// internal active count. Safe for concurrent use.
func IncConnectionsPerIP(ip string) {
	ipConnMu.Lock()
	ipConnCounts[ip]++
	ipConnMu.Unlock()
	ConnectionsPerIP.WithLabelValues(ip).Inc()
}

// DecConnectionsPerIP decrements the active-connection gauge for ip. When the
// active count reaches zero the per-IP series is deleted from the registry to
// keep label cardinality bounded. A Dec with no tracked Inc is a no-op.
func DecConnectionsPerIP(ip string) {
	ipConnMu.Lock()
	n, ok := ipConnCounts[ip]
	if !ok {
		ipConnMu.Unlock()
		return
	}
	n--
	if n <= 0 {
		delete(ipConnCounts, ip)
	} else {
		ipConnCounts[ip] = n
	}
	ipConnMu.Unlock()

	if n <= 0 {
		ConnectionsPerIP.DeleteLabelValues(ip)
		return
	}
	ConnectionsPerIP.WithLabelValues(ip).Dec()
}