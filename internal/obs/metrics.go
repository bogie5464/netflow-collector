// Package obs owns observability. metrics.go creates every Prometheus
// instrument once, at package level, on client_golang's default registerer;
// no other package declares a metric name.
package obs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// The seven pipeline instruments. Names are a public interface: the runbook,
// the alerts and the step 10 /metrics gate all assert on them.
var (
	// DecodeErrors counts datagrams and messages that could not be decoded,
	// by source. A malformed input increments it, is skipped, and never
	// stops a loop.
	DecodeErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "netflow_packets_decode_errors_total",
		Help: "Datagrams or messages that failed to decode, by source.",
	}, []string{"source"})

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

// SourceRewinds counts the times a pull source went back to its last
// acknowledged position because a sink rejected a batch. Every rewind is
// records delivered again; the sinks that had them dedup.
var SourceRewinds = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "netflow_source_rewinds_total",
	Help: "Rewinds of a pull source to its last acknowledged position after a batch write failure, by source.",
}, []string{"source"})

// Drop reasons used with RecordsDropped.
const (
	DropBufferFull = "buffer_full"
	DropShutdown   = "shutdown"
)
