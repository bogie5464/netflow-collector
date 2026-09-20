package kafka_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bogie5464/netflow-collector/internal/app"
	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
	"github.com/bogie5464/netflow-collector/internal/source/kafka"
)

// brokers is the compose redpanda service's advertised address; the compose
// file owns it. Start it with: docker compose up -d --wait redpanda
var brokers = []string{"127.0.0.1:19092"}

// producer connects to redpanda, retrying the first metadata fetch for up to
// 30 s because the broker may still be settling after --wait returns.
func producer(t *testing.T) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation())
	require.NoError(t, err)
	t.Cleanup(cl.Close)
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = cl.Ping(ctx)
		cancel()
		if err == nil {
			return cl
		}
		require.Less(t, time.Now(), deadline, "redpanda at %v not reachable within 30s: %v", brokers, err)
		time.Sleep(time.Second)
	}
}

// uniqueName gives each test its own topic and group so runs never share state.
func uniqueName(t *testing.T, kind string) string {
	return fmt.Sprintf("nfc-test-%s-%d", kind, time.Now().UnixNano())
}

func produce(t *testing.T, cl *kgo.Client, topic string, ts time.Time, body []byte) {
	t.Helper()
	rec := &kgo.Record{Topic: topic, Value: body}
	if !ts.IsZero() {
		rec.Timestamp = ts
	}
	require.NoError(t, cl.ProduceSync(context.Background(), rec).FirstErr())
}

var sample = kafka.Message{
	ExporterAddr:  netip.MustParseAddr("198.51.100.7"),
	FlowType:      "ipfix",
	FirstSwitched: time.Date(2026, 9, 19, 11, 59, 58, 0, time.UTC),
	LastSwitched:  time.Date(2026, 9, 19, 11, 59, 59, 500000000, time.UTC),
	SrcAddr:       netip.MustParseAddr("203.0.113.10"),
	DstAddr:       netip.MustParseAddr("198.51.100.200"),
	SrcPort:       51514, DstPort: 443, Protocol: 6, TCPFlags: 24,
	Packets: 12, Bytes: 9000, SamplingRate: 1000,
	InputIface: 3, OutputIface: 7, SrcAS: 64500, DstAS: 64501,
}

func withReceivedAt(m kafka.Message, at time.Time) []byte {
	m.ReceivedAt = &at
	nh := netip.MustParseAddr("198.51.100.1")
	m.NextHop = &nh
	b, _ := json.Marshal(m)
	return b
}

// startSource runs a consumer on topic/group and returns its output and a
// stop function that cancels and reports how long Start took to return.
func startSource(t *testing.T, topic, group string) (<-chan flow.FlowRecord, func() (time.Duration, error)) {
	t.Helper()
	cfg := config.Config{KafkaBrokers: brokers, KafkaTopic: topic, KafkaGroup: group}
	src, err := kafka.New(cfg)
	require.NoError(t, err)
	out := make(chan flow.FlowRecord, 64)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.Start(ctx, out) }()
	stopped := false
	stop := func() (time.Duration, error) {
		if stopped {
			return 0, nil
		}
		stopped = true
		started := time.Now()
		cancel()
		select {
		case err := <-done:
			return time.Since(started), err
		case <-time.After(10 * time.Second):
			return time.Since(started), fmt.Errorf("Start did not return after cancel")
		}
	}
	t.Cleanup(func() { _, _ = stop() })
	return out, stop
}

func recv(t *testing.T, out <-chan flow.FlowRecord, within time.Duration) flow.FlowRecord {
	t.Helper()
	select {
	case r := <-out:
		return r
	case <-time.After(within):
		t.Fatal("no record received")
		return flow.FlowRecord{}
	}
}

func TestKafkaDecodeIsPure(t *testing.T) {
	at := time.Date(2026, 9, 19, 12, 0, 0, 123456789, time.UTC)
	rec, err := kafka.Decode(withReceivedAt(sample, at), time.Now())
	require.NoError(t, err)
	require.Equal(t, at.Truncate(time.Microsecond), rec.ReceivedAt, "received_at from the message, truncated to µs")
	require.Len(t, rec.DedupKey, 16)
	require.Equal(t, kafka.DedupKey(rec), rec.DedupKey)

	// Fallback: no received_at in the message → the Kafka record timestamp.
	body, _ := json.Marshal(sample)
	kt := time.Date(2026, 9, 19, 12, 30, 0, 0, time.UTC)
	rec2, err := kafka.Decode(body, kt)
	require.NoError(t, err)
	require.Equal(t, kt, rec2.ReceivedAt)
	require.Equal(t, rec.DedupKey, rec2.DedupKey, "the digest ignores received_at: it is the natural key")

	// Unknown fields are ignored; malformed and incomplete bodies are errors.
	_, err = kafka.Decode([]byte(`{"exporter_addr":"1.2.3.4","flow_type":"ipfix","first_switched":"2026-09-19T12:00:00Z","last_switched":"2026-09-19T12:00:01Z","src_addr":"10.0.0.1","dst_addr":"10.0.0.2","future_field":42}`), kt)
	require.NoError(t, err)
	_, err = kafka.Decode([]byte(`not json`), kt)
	require.Error(t, err)
	_, err = kafka.Decode([]byte(`{"flow_type":"ipfix"}`), kt)
	require.Error(t, err)
	_, err = kafka.Decode([]byte(`{"exporter_addr":"1.2.3.4","flow_type":"sflow","src_addr":"10.0.0.1","dst_addr":"10.0.0.2","first_switched":"2026-09-19T12:00:00Z","last_switched":"2026-09-19T12:00:01Z"}`), kt)
	require.Error(t, err, "unknown flow_type")
}

func TestKafkaConsumesAndDecodes(t *testing.T) {
	cl := producer(t)
	topic, group := uniqueName(t, "topic"), uniqueName(t, "group")
	out, stop := startSource(t, topic, group)

	at := time.Date(2026, 9, 19, 12, 0, 0, 123456000, time.UTC)
	produce(t, cl, topic, time.Time{}, withReceivedAt(sample, at))
	got := recv(t, out, 30*time.Second)
	require.Equal(t, sample.ExporterAddr, got.ExporterAddr)
	require.Equal(t, flow.FlowTypeIPFIX, got.FlowType)
	require.Equal(t, at, got.ReceivedAt, "never the consumer's clock")
	require.Equal(t, sample.FirstSwitched, got.FirstSwitched)
	require.Equal(t, sample.LastSwitched, got.LastSwitched)
	require.Equal(t, sample.SrcAddr, got.SrcAddr)
	require.Equal(t, sample.DstAddr, got.DstAddr)
	require.Equal(t, sample.SrcPort, got.SrcPort)
	require.Equal(t, sample.DstPort, got.DstPort)
	require.Equal(t, sample.Protocol, got.Protocol)
	require.Equal(t, sample.TCPFlags, got.TCPFlags)
	require.Equal(t, sample.Packets, got.Packets)
	require.Equal(t, sample.Bytes, got.Bytes)
	require.Equal(t, sample.SamplingRate, got.SamplingRate)
	require.Equal(t, sample.InputIface, got.InputIface)
	require.Equal(t, sample.OutputIface, got.OutputIface)
	require.Equal(t, sample.SrcAS, got.SrcAS)
	require.Equal(t, sample.DstAS, got.DstAS)
	require.Equal(t, netip.MustParseAddr("198.51.100.1"), got.NextHop)
	require.Len(t, got.DedupKey, 16)

	// No received_at: the Kafka record timestamp is used.
	kt := time.Date(2026, 9, 19, 13, 0, 0, 0, time.UTC)
	body, _ := json.Marshal(sample)
	produce(t, cl, topic, kt, body)
	got = recv(t, out, 10*time.Second)
	require.Equal(t, kt, got.ReceivedAt)

	// Cancel closes the client and returns within 5 s.
	took, err := stop()
	require.NoError(t, err)
	require.Less(t, took, 5*time.Second)
}

func TestKafkaPoisonMessageIsCountedAndCommittedPast(t *testing.T) {
	cl := producer(t)
	topic, group := uniqueName(t, "topic"), uniqueName(t, "group")
	out, stop := startSource(t, topic, group)

	before := testutil.ToFloat64(obs.DecodeErrors.WithLabelValues("kafka"))
	produce(t, cl, topic, time.Time{}, []byte(`{"this is": "not a flow"}`))
	produce(t, cl, topic, time.Time{}, withReceivedAt(sample, time.Date(2026, 9, 19, 12, 0, 1, 0, time.UTC)))
	got := recv(t, out, 30*time.Second)
	require.Equal(t, uint16(51514), got.SrcPort, "the consumer keeps going after a poison message")
	require.Eventually(t, func() bool {
		return testutil.ToFloat64(obs.DecodeErrors.WithLabelValues("kafka")) == before+1
	}, 5*time.Second, 20*time.Millisecond)
	_, err := stop()
	require.NoError(t, err)

	// Committed past it: a new consumer in the same group sees only what is
	// produced after the restart, not the poison message or its neighbour.
	out2, _ := startSource(t, topic, group)
	after := testutil.ToFloat64(obs.DecodeErrors.WithLabelValues("kafka"))
	produce(t, cl, topic, time.Time{}, withReceivedAt(sample, time.Date(2026, 9, 19, 12, 0, 2, 0, time.UTC)))
	got = recv(t, out2, 30*time.Second)
	require.Equal(t, time.Date(2026, 9, 19, 12, 0, 2, 0, time.UTC), got.ReceivedAt, "only the new message is delivered")
	select {
	case extra := <-out2:
		t.Fatalf("earlier record redelivered after commit: %+v", extra)
	case <-time.After(500 * time.Millisecond):
	}
	require.Equal(t, after, testutil.ToFloat64(obs.DecodeErrors.WithLabelValues("kafka")), "poison message was not re-read")
}

// TestKafkaRedeliveryDedupsInBackend runs the real wiring: the same message
// produced twice reaches Postgres as exactly one row, with no code in the
// sink that knows Kafka exists.
func TestKafkaRedeliveryDedupsInBackend(t *testing.T) {
	cl := producer(t)
	dsn := sinktest.StartTimescale(t)
	topic, group := uniqueName(t, "topic"), uniqueName(t, "group")
	cfg := config.Config{
		LogLevel: "info",
		Sources:  []string{config.SourceKafka}, Sinks: []string{config.SinkPostgres},
		KafkaBrokers: brokers, KafkaTopic: topic, KafkaGroup: group,
		PostgresDSN: dsn, RetentionDays: 30,
		PipelineBuffer: 1024, BatchSize: 100, BatchInterval: 100 * time.Millisecond, Workers: 2,
		PostgresEnabled: true, KafkaEnabled: true,
	}
	ctx, cancel := context.WithCancel(context.Background())
	a, err := app.New(ctx, cfg)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-a.Ready():
	case err := <-done:
		t.Fatalf("app stopped early: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("app did not start")
	}

	at := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	body := withReceivedAt(sample, at)
	produce(t, cl, topic, time.Time{}, body)
	produce(t, cl, topic, time.Time{}, body) // the redelivery
	q := flow.FlowQuery{Start: at.Add(-time.Minute), End: at.Add(time.Minute), Limit: 10}
	require.Eventually(t, func() bool {
		page, err := a.Backend(config.SinkPostgres).Query(ctx, q)
		return err == nil && len(page.Records) >= 1
	}, 30*time.Second, 100*time.Millisecond)
	time.Sleep(500 * time.Millisecond) // give the second copy every chance to land
	page, err := a.Backend(config.SinkPostgres).Query(ctx, q)
	require.NoError(t, err)
	require.Len(t, page.Records, 1, "a redelivered message collides with itself on (received_at, dedup_key)")
	require.Equal(t, at, page.Records[0].ReceivedAt)
	require.Len(t, page.Records[0].DedupKey, 16)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("app did not stop")
	}
}
