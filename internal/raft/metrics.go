package raft

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	kvOpsTotal       *prometheus.CounterVec
	kvOpDuration     *prometheus.HistogramVec
	raftTerm         prometheus.Gauge
	raftIsLeader     prometheus.Gauge
	raftCommitIndex  prometheus.Gauge
	raftLogEntries   prometheus.Counter
	raftElection     prometheus.Counter
	storeKeysTotal   prometheus.Gauge
	grpcRequests     *prometheus.CounterVec
	replicationLag   prometheus.Gauge
}

var (
	metricsOnce sync.Once
	sharedMetrics *Metrics
)

func newMetrics(_ string) *Metrics {
	metricsOnce.Do(func() {
		sharedMetrics = &Metrics{
			kvOpsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
				Name: "kv_ops_total",
				Help: "Total GET/SET/DELETE operations served.",
			}, []string{"op", "status"}),
			kvOpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
				Name:    "kv_op_duration_seconds",
				Help:    "KV operation latency in seconds.",
				Buckets: prometheus.DefBuckets,
			}, []string{"op"}),
			raftTerm: prometheus.NewGauge(prometheus.GaugeOpts{Name: "raft_term", Help: "Current Raft term."}),
			raftIsLeader: prometheus.NewGauge(prometheus.GaugeOpts{Name: "raft_is_leader", Help: "1 when node is leader."}),
			raftCommitIndex: prometheus.NewGauge(prometheus.GaugeOpts{Name: "raft_commit_index", Help: "Committed log index."}),
			raftLogEntries: prometheus.NewCounter(prometheus.CounterOpts{Name: "raft_log_entries_total", Help: "Total entries appended to the Raft log."}),
			raftElection: prometheus.NewCounter(prometheus.CounterOpts{Name: "raft_election_total", Help: "Total elections triggered."}),
			storeKeysTotal: prometheus.NewGauge(prometheus.GaugeOpts{Name: "store_keys_total", Help: "Number of keys currently in LevelDB."}),
			grpcRequests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "grpc_requests_total", Help: "Total gRPC requests by method and status."}, []string{"method", "status"}),
			replicationLag: prometheus.NewGauge(prometheus.GaugeOpts{Name: "raft_replication_lag", Help: "Approximate replication lag."}),
		}
		prometheus.MustRegister(
			sharedMetrics.kvOpsTotal,
			sharedMetrics.kvOpDuration,
			sharedMetrics.raftTerm,
			sharedMetrics.raftIsLeader,
			sharedMetrics.raftCommitIndex,
			sharedMetrics.raftLogEntries,
			sharedMetrics.raftElection,
			sharedMetrics.storeKeysTotal,
			sharedMetrics.grpcRequests,
			sharedMetrics.replicationLag,
		)
	})
	return sharedMetrics
}

func (m *Metrics) Inc(op, status string) {
	if m == nil {
		return
	}
	m.kvOpsTotal.WithLabelValues(op, status).Inc()
}

func (m *Metrics) Observe(op string, seconds float64) {
	if m == nil {
		return
	}
	m.kvOpDuration.WithLabelValues(op).Observe(seconds)
}

func (m *Metrics) SetTerm(term uint64) {
	if m == nil {
		return
	}
	m.raftTerm.Set(float64(term))
}

func (m *Metrics) SetLeader(isLeader bool) {
	if m == nil {
		return
	}
	if isLeader {
		m.raftIsLeader.Set(1)
		return
	}
	m.raftIsLeader.Set(0)
}

func (m *Metrics) SetCommitIndex(index int64) {
	if m == nil {
		return
	}
	m.raftCommitIndex.Set(float64(index))
}

func (m *Metrics) IncLogEntry() {
	if m == nil {
		return
	}
	m.raftLogEntries.Inc()
}

func (m *Metrics) IncElection() {
	if m == nil {
		return
	}
	m.raftElection.Inc()
}

func (m *Metrics) SetStoreKeys(count int) {
	if m == nil {
		return
	}
	m.storeKeysTotal.Set(float64(count))
}

func (m *Metrics) IncGRPCRequest(method, status string) {
	if m == nil {
		return
	}
	m.grpcRequests.WithLabelValues(method, status).Inc()
}

func (m *Metrics) SetReplicationLag(lag float64) {
	if m == nil {
		return
	}
	m.replicationLag.Set(lag)
}
