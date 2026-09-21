// Package templatestore shares NetFlow v9/IPFIX templates between collector
// instances through a compacted Kafka topic. It satisfies
// netflow.TemplateStore without importing it: keys and values are bytes,
// the source owns their meaning.
//
// Every instance publishes each template it learns and consumes the whole
// topic — no consumer group, from the start — so each instance holds every
// template any instance has seen. Compaction keeps the latest value per key,
// which bounds the topic to the number of live templates in the fleet.
package templatestore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka is a TemplateStore over one compacted topic.
type Kafka struct {
	brokers []string
	topic   string
	cl      *kgo.Client
}

// NewKafka connects, and creates the topic as compacted if it does not
// exist. Creating it here is deliberate, unlike the flow topic: compaction
// is a correctness requirement of this store, not a deployment choice, and
// an auto-created topic would not have it.
func NewKafka(ctx context.Context, brokers []string, topic string) (*Kafka, error) {
	if len(brokers) == 0 || topic == "" {
		return nil, errors.New("templatestore: brokers and topic must be set")
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	)
	if err != nil {
		return nil, fmt.Errorf("templatestore: client: %w", err)
	}
	if err := cl.Ping(ctx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("templatestore: ping: %w", err)
	}
	if err := ensureCompacted(ctx, kadm.NewClient(cl), topic); err != nil {
		cl.Close()
		return nil, err
	}
	return &Kafka{brokers: brokers, topic: topic, cl: cl}, nil
}

func ensureCompacted(ctx context.Context, adm *kadm.Client, topic string) error {
	compact := "compact"
	resp, err := adm.CreateTopic(ctx, -1, -1, map[string]*string{"cleanup.policy": &compact}, topic)
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("templatestore: create topic %s: %w", topic, err)
	}
	if err == nil && resp.Err != nil && !errors.Is(resp.Err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("templatestore: create topic %s: %w", topic, resp.Err)
	}
	return nil
}

// Put publishes one template; the key is the compaction key.
func (k *Kafka) Put(ctx context.Context, key string, value []byte) error {
	rec := &kgo.Record{Key: []byte(key), Value: value}
	if err := k.cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("templatestore: publish %s: %w", key, err)
	}
	return nil
}

// Watch consumes the topic from the beginning on a dedicated client, calls
// synced once every partition has reached its end offset as of the call,
// and keeps applying new records until ctx is cancelled.
func (k *Kafka) Watch(ctx context.Context, apply func(key string, value []byte), synced func()) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(k.brokers...),
		kgo.ConsumeTopics(k.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("templatestore: consumer: %w", err)
	}
	defer cl.Close()

	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, k.topic)
	if err != nil {
		return fmt.Errorf("templatestore: end offsets: %w", err)
	}
	// remaining is how many records each partition must deliver before the
	// replay is complete; an empty partition is complete already.
	remaining := map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			remaining[o.Partition] = o.Offset
		}
	})
	if len(remaining) == 0 {
		synced()
	}

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			if !errors.Is(fe.Err, context.Canceled) {
				return fmt.Errorf("templatestore: fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			apply(string(r.Key), r.Value)
			if n, ok := remaining[r.Partition]; ok && r.Offset+1 >= n {
				delete(remaining, r.Partition)
				if len(remaining) == 0 {
					synced()
				}
			}
		})
	}
}

// Ping is the readiness probe.
func (k *Kafka) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return k.cl.Ping(ctx)
}

// Close releases the producer; Watch's consumer closes with its ctx.
func (k *Kafka) Close() error {
	k.cl.Close()
	return nil
}
