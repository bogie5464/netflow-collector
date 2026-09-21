package kafka_test

import (
	"context"
	"fmt"
	"io"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/sink/kafka"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
	"github.com/bogie5464/netflow-collector/internal/wire"
)

// The compose redpanda service, like the kafka source test: a broker must
// advertise an address known before it starts, so it is not a container
// per test.
var brokers = []string{"127.0.0.1:19092"}

func uniqueTopic(t *testing.T) string {
	return fmt.Sprintf("nfc-test-sink-%d", time.Now().UnixNano())
}

// newSink builds the sink, retrying the first metadata fetch for up to 30 s
// because the broker may still be settling after --wait returns.
func newSink(t *testing.T, topic string) flow.Sink {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s, err := kafka.New(ctx, brokers, topic)
		cancel()
		if err == nil {
			t.Cleanup(func() { require.NoError(t, s.(io.Closer).Close()) })
			return s
		}
		require.Less(t, time.Now(), deadline, "redpanda at %v not reachable within 30s: %v", brokers, err)
		time.Sleep(time.Second)
	}
}

// consume reads n records from topic from the beginning with a plain client.
func consume(t *testing.T, topic string, n int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < n {
		fetches := cl.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "got %d of %d records", len(out), n)
		fetches.EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

func TestKafkaSinkProducesWireMessages(t *testing.T) {
	topic := uniqueTopic(t)
	s := newSink(t, topic)

	at := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	exporterA, exporterB := netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("2001:db8::9")
	batch := []flow.FlowRecord{
		sinktest.Fixture(0, exporterA, at, false),
		sinktest.Fixture(1, exporterB, at.Add(time.Microsecond), false),
		sinktest.Fixture(2, exporterA, at.Add(2*time.Microsecond), false),
	}
	require.NoError(t, s.WriteBatch(context.Background(), batch))

	got := consume(t, topic, len(batch))
	require.Len(t, got, len(batch))
	bySrcPort := map[uint16]*kgo.Record{}
	for _, r := range got {
		rec, err := wire.Decode(r.Value, r.Timestamp)
		require.NoError(t, err)
		bySrcPort[rec.SrcPort] = r
	}
	for _, want := range batch {
		r, ok := bySrcPort[want.SrcPort]
		require.True(t, ok, "record with src_port %d was produced", want.SrcPort)
		require.Equal(t, want.ExporterAddr.String(), string(r.Key), "the key is the exporter address")
		// Kafka record timestamps are milliseconds; the body carries received_at at full precision.
		require.Equal(t, want.ReceivedAt.Truncate(time.Millisecond), r.Timestamp.UTC(), "the record timestamp is received_at")
		rec, err := wire.Decode(r.Value, time.Time{})
		require.NoError(t, err)
		require.Equal(t, want, rec, "every field survives the wire")
	}
}

func TestKafkaSinkEmptyBatchIsANoOp(t *testing.T) {
	s := newSink(t, uniqueTopic(t))
	require.NoError(t, s.WriteBatch(context.Background(), nil))
}

func TestKafkaSinkPingIsTheReadinessProbe(t *testing.T) {
	s := newSink(t, uniqueTopic(t))
	p, ok := s.(flow.Pinger)
	require.True(t, ok, "the kafka sink cannot be queried, so it must implement flow.Pinger")
	require.NoError(t, p.Ping(context.Background()))
	_, isBackend := s.(flow.Backend)
	require.False(t, isBackend, "a transport is a Sink, not a storage Backend")
}

func TestKafkaSinkRejectsMissingConfig(t *testing.T) {
	_, err := kafka.New(context.Background(), nil, "flows")
	require.Error(t, err)
	_, err = kafka.New(context.Background(), brokers, "")
	require.Error(t, err)
}

func TestKafkaSinkUnreachableBrokerFailsFast(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := kafka.New(ctx, []string{"127.0.0.1:1"}, "flows")
	require.Error(t, err)
}
