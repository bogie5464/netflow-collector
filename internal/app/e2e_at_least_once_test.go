package app

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/pipeline"
	"github.com/bogie5464/netflow-collector/internal/sink/postgres"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
	kafkasource "github.com/bogie5464/netflow-collector/internal/source/kafka"
	"github.com/bogie5464/netflow-collector/internal/wire"
)

// rejectingSink wraps a real backend and rejects the first n batches — a
// database that is briefly unavailable.
type rejectingSink struct {
	flow.Backend
	reject int32
	seen   atomic.Int32
}

func (s *rejectingSink) WriteBatch(ctx context.Context, records []flow.FlowRecord) error {
	if s.seen.Add(1) <= s.reject {
		return errors.New("database unavailable")
	}
	return s.Backend.WriteBatch(ctx, records)
}

// TestCentralTierIsAtLeastOnce is the claim the tiered layout makes: with
// the kafka source feeding a real pipeline and a real Postgres, a batch the
// database rejects is not lost. The source rewinds, the records come back
// through, and the database ends up with exactly one row per message —
// neither the rejected batch nor the redelivery leaves a mark.
func TestCentralTierIsAtLeastOnce(t *testing.T) {
	const n = 50
	topic := fmt.Sprintf("nfc-test-alo-%d", time.Now().UnixNano())

	// Producer side: n distinct flow records on the topic, as an edge tier
	// would write them.
	cl, err := kgo.NewClient(kgo.SeedBrokers(kafkaBrokers...), kgo.AllowAutoTopicCreation())
	require.NoError(t, err)
	defer cl.Close()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return cl.Ping(ctx) == nil
	}, 30*time.Second, time.Second, "redpanda at %v not reachable", kafkaBrokers)
	at := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	exporter := netip.MustParseAddr("192.0.2.40")
	var recs []*kgo.Record
	for i := range n {
		body, err := wire.Encode(sinktest.Fixture(i, exporter, at.Add(time.Duration(i)*time.Millisecond), false))
		require.NoError(t, err)
		recs = append(recs, &kgo.Record{Topic: topic, Key: []byte(exporter.String()), Value: body})
	}
	require.NoError(t, cl.ProduceSync(context.Background(), recs...).FirstErr())

	// Consumer side: the real source into the real pipeline into a real
	// Postgres that rejects the first batch it sees.
	ctx := context.Background()
	backend, err := postgres.New(ctx, sinktest.StartTimescale(t), 30)
	require.NoError(t, err)
	t.Cleanup(func() { _ = backend.Close() })
	require.NoError(t, backend.Migrate(ctx))
	sink := &rejectingSink{Backend: backend, reject: 1}

	pipe, err := pipeline.New(pipeline.Options{BufferSize: 1024, BatchSize: 20, BatchInterval: 200 * time.Millisecond, Workers: 2},
		pipeline.Sink{Name: "postgres", Sink: sink})
	require.NoError(t, err)
	src, err := kafkasource.New(testConfigKafka(topic))
	require.NoError(t, err)

	pipeCtx, stopPipe := context.WithCancel(ctx)
	pipeDone := make(chan error, 1)
	go func() { pipeDone <- pipe.Run(pipeCtx) }()
	srcCtx, stopSrc := context.WithCancel(ctx)
	srcDone := make(chan error, 1)
	go func() { srcDone <- pipe.RunSource(srcCtx, "kafka", src) }()

	q := flow.FlowQuery{Start: at.Add(-time.Hour), End: at.Add(time.Hour), Limit: 1000}
	require.Eventually(t, func() bool {
		page, err := backend.Query(ctx, q)
		return err == nil && len(page.Records) == n
	}, 60*time.Second, 200*time.Millisecond, "every message reaches the database despite the rejected batch")
	require.GreaterOrEqual(t, sink.seen.Load(), int32(2), "the rejected batch was retried")

	// Hold the row count for a moment: a redelivery must not add rows.
	time.Sleep(time.Second)
	page, err := backend.Query(ctx, q)
	require.NoError(t, err)
	require.Len(t, page.Records, n, "no duplicates: the dedup key collapses the redelivery")

	stopSrc()
	require.NoError(t, <-srcDone)
	stopPipe()
	require.NoError(t, <-pipeDone)
}
