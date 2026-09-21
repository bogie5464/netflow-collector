// Package kafka is the Kafka output adapter: a flow.Sink that produces one
// internal/wire message per record. It is not a storage engine — nothing
// can be queried back from it — so it implements flow.Sink and flow.Pinger,
// not flow.Backend. Its consumer is internal/source/kafka, which makes one
// binary both tiers of a tiered deployment (docs/deploy.md).
package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/wire"
)

type sink struct {
	cl    *kgo.Client
	topic string
}

var (
	_ flow.Sink   = (*sink)(nil)
	_ flow.Pinger = (*sink)(nil)
)

// New connects to brokers and returns the sink. Whether a missing topic is
// created is the cluster's policy (auto.create.topics.enable), not the
// sink's: the client permits it, and a cluster that forbids it answers the
// first batch with UNKNOWN_TOPIC_OR_PARTITION as a batch write error.
func New(ctx context.Context, brokers []string, topic string) (flow.Sink, error) {
	if len(brokers) == 0 || topic == "" {
		return nil, errors.New("kafka: NFC_KAFKA_BROKERS and NFC_KAFKA_TOPIC must be set")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.AllowAutoTopicCreation(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: client: %w", err)
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka: ping: %w", err)
	}
	return &sink{cl: cl, topic: topic}, nil
}

// WriteBatch produces every record and returns once the broker has
// acknowledged all of them. The key is the exporter address, so one
// exporter's records stay on one partition and arrive at the consumer in
// the order they were sent.
func (s *sink) WriteBatch(ctx context.Context, records []flow.FlowRecord) error {
	if len(records) == 0 {
		return nil
	}
	recs := make([]*kgo.Record, 0, len(records))
	for i := range records {
		r := &records[i]
		body, err := wire.Encode(*r)
		if err != nil {
			return fmt.Errorf("kafka: %w", err)
		}
		recs = append(recs, &kgo.Record{
			Key:       []byte(r.ExporterAddr.String()),
			Value:     body,
			Timestamp: r.ReceivedAt,
		})
	}
	if err := s.cl.ProduceSync(ctx, recs...).FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce %d records to %s: %w", len(records), s.topic, err)
	}
	return nil
}

// Ping is the readiness probe: a metadata round trip to the cluster.
func (s *sink) Ping(ctx context.Context) error {
	if err := s.cl.Ping(ctx); err != nil {
		return fmt.Errorf("kafka: ping: %w", err)
	}
	return nil
}

// Close flushes anything in flight and releases the client.
func (s *sink) Close() error {
	s.cl.Close()
	return nil
}
