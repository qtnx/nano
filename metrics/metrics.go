package metrics

import (
	"fmt"
	"log"
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
