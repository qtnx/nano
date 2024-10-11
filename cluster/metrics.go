package cluster

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
)

func init() {
	prometheus.MustRegister(TotalConnections)
	prometheus.MustRegister(CurrentConnections)
	prometheus.MustRegister(ConnectionDuration)
}
