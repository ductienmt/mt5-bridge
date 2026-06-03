package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// MastersTotal tracks the total number of registered masters
	MastersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "masters_total",
		Help: "Total number of registered master accounts",
	})

	// FollowersTotal tracks the total number of registered followers
	FollowersTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "followers_total",
		Help: "Total number of registered follower accounts",
	})

	// FollowersActive tracks the number of active followers
	FollowersActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "followers_active",
		Help: "Current number of active followers",
	})

	// ActiveMasters tracks the current number of non-deleted masters
	ActiveMasters = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "active_masters",
		Help: "Current number of non-deleted masters",
	})

	// SignalsReceived tracks signals received per master
	SignalsReceived = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "signals_received_total",
		Help: "Total signals received per master",
	}, []string{"master_id"})

	// SignalsDistributed tracks signals successfully distributed
	SignalsDistributed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "signals_distributed_total",
		Help: "Total signals successfully distributed per master",
	}, []string{"master_id"})

	// DistributionErrors tracks distribution failures
	DistributionErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "distribution_errors_total",
		Help: "Distribution failures by reason",
	}, []string{"reason"})

	// RedisErrors tracks Redis operation failures
	RedisErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "redis_errors_total",
		Help: "Redis operation failures by operation",
	}, []string{"operation"})

	// DatabaseErrors tracks database operation failures
	DatabaseErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "database_errors_total",
		Help: "Database operation failures by operation",
	}, []string{"operation"})

	// DistributionLatency tracks the time to distribute signals
	DistributionLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "distribution_latency_seconds",
		Help:    "Time to distribute signal to all followers",
		Buckets: []float64{0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 1.0},
	})

	// RedisOperationDuration tracks Redis operation latency
	RedisOperationDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "redis_operation_duration_seconds",
		Help:    "Redis operation latency",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25},
	})

	// DatabaseQueryDuration tracks database query latency
	DatabaseQueryDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "database_query_duration_seconds",
		Help:    "Database query latency",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5},
	})

	// FollowerCountPerMaster tracks distribution of followers across masters
	FollowerCountPerMaster = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "follower_count_per_master",
		Help:    "Distribution of followers across masters",
		Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500},
	})

	// RedisConnectionPoolActive tracks active Redis connections
	RedisConnectionPoolActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "redis_connection_pool_active",
		Help: "Active Redis connections",
	})

	// DatabaseConnectionPoolActive tracks active database connections
	DatabaseConnectionPoolActive = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "database_connection_pool_active",
		Help: "Active database connections",
	})

	// QueueSize tracks the current queue size
	QueueSize = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "queue_size",
		Help: "Current signal queue size",
	})

	// HTTPRequestsTotal tracks total HTTP requests
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests by method and path",
	}, []string{"method", "path", "status"})

	// HTTPRequestDuration tracks HTTP request duration
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request duration",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
	}, []string{"method", "path"})
)

// RecordDistributionSuccess records a successful signal distribution
func RecordDistributionSuccess(masterID string, followerCount int, latencyMs float64) {
	SignalsDistributed.WithLabelValues(masterID).Inc()
	DistributionLatency.Observe(latencyMs / 1000) // Convert ms to seconds
	FollowerCountPerMaster.Observe(float64(followerCount))
}

// RecordDistributionError records a distribution error
func RecordDistributionError(reason string) {
	DistributionErrors.WithLabelValues(reason).Inc()
}

// RecordRedisError records a Redis error
func RecordRedisError(operation string) {
	RedisErrors.WithLabelValues(operation).Inc()
}

// RecordDatabaseError records a database error
func RecordDatabaseError(operation string) {
	DatabaseErrors.WithLabelValues(operation).Inc()
}

// UpdateActiveFollowers updates the active followers gauge
func UpdateActiveFollowers(count int) {
	FollowersActive.Set(float64(count))
}

// UpdateActiveMasters updates the active masters gauge
func UpdateActiveMasters(count int) {
	ActiveMasters.Set(float64(count))
}
