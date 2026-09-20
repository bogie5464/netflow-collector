// Package obs owns observability. metrics.go creates every Prometheus
// instrument once, at package level, on client_golang's default registerer;
// no other package declares a metric name.
package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The six pipeline instruments. Names are a public interface: the runbook,
// the alerts and the step 10 /metrics gate all assert on them.
var (
	// RecordsIngested counts records accepted into the bounded buffer, by
	// source. With RecordsDropped it satisfies ingested + dropped == offered.
	RecordsIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "netflow_records_ingested_total",
		Help: "Flow records accepted into the ingest buffer, by source.",
	}, []string{"source"})

	// RecordsDropped counts records discarded instead of buffered. The
	// buffer_full reason is the entire backpressure strategy.
	RecordsDropped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "netflow_records_dropped_total",
		Help: "Flow records discarded by the pipeline, by reason.",
	}, []string{"reason"})

	// BatchWriteDuration times each WriteBatch call, by sink.
	BatchWriteDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "netflow_batch_write_duration_seconds",
		Help:    "Duration of one WriteBatch call, by sink.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
	}, []string{"sink"})

	// BatchWriteErrors counts WriteBatch calls that returned an error, by sink.
	// One error is a whole batch lost for that sink, not one record.
	BatchWriteErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "netflow_batch_write_errors_total",
		Help: "WriteBatch calls that failed, by sink.",
	}, []string{"sink"})

	// PipelineBufferLength is the current occupancy of the bounded buffer. It
	// is the leading indicator that fires before drops begin.
	PipelineBufferLength = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "netflow_pipeline_buffer_length",
		Help: "Records currently waiting in the ingest buffer.",
	})

	// RecordVisibilityLag is write-completion time minus each record's
	// ReceivedAt: how long a record took to become queryable.
	RecordVisibilityLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "netflow_record_visibility_lag_seconds",
		Help:    "Seconds between a record's received_at and the completion of the batch write that stored it.",
		Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
	})
)

// Drop reasons used with RecordsDropped.
const (
	DropBufferFull = "buffer_full"
	DropShutdown   = "shutdown"
)
