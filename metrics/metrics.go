package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
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
}
