// Package kafka is the Kafka input adapter: a franz-go consumer group that
// decodes one internal/wire message per Kafka message and emits
// flow.FlowRecord values. The kafka sink produces the same format, so this
// source is the central tier of a tiered deployment (docs/deploy.md).
//
// Two rules make redelivery safe without any sink knowing Kafka exists:
// ReceivedAt comes from the message (or the Kafka record timestamp), never
// from the consumer's clock, and DedupKey is a digest of the record's natural
// key. A redelivered message therefore collides with itself on
// (received_at, dedup_key) and the sink's conflict-tolerant insert drops it.
package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
	"github.com/bogie5464/netflow-collector/internal/wire"
)

const sourceLabel = "kafka"

// Message is the wire schema, defined once in internal/wire.
type Message = wire.Message

// Source is a Kafka consumer. Create it with New.
type Source struct {
	brokers []string
	topic   string
	group   string
	log     *slog.Logger
}

var (
	_ flow.Source     = (*Source)(nil)
	_ flow.PullSource = (*Source)(nil)
)

// New builds a Source from the NFC_KAFKA_* configuration.
func New(cfg config.Config) (flow.Source, error) {
	if len(cfg.KafkaBrokers) == 0 || cfg.KafkaTopic == "" || cfg.KafkaGroup == "" {
		return nil, errors.New("kafka: NFC_KAFKA_BROKERS, NFC_KAFKA_TOPIC and NFC_KAFKA_GROUP must be set")
	}
	return newSource(cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup), nil
}

func newSource(brokers []string, topic, group string) *Source {
	return &Source{
		brokers: brokers,
		topic:   topic,
		group:   group,
		log:     slog.Default().With("source", sourceLabel),
	}
}

// Start is the push-shaped entry: consume, decode, send on out, and mark
// every polled record for autocommit whether or not it decoded (one poison
// message must never wedge the consumer). It is at-most-once — the pipeline
// may drop what it is sent — and exists so the source satisfies flow.Source;
// the pipeline runs StartPull instead.
func (s *Source) Start(ctx context.Context, out chan<- flow.FlowRecord) error {
	cl, err := s.client(kgo.AutoCommitMarks())
	if err != nil {
		return err
	}
	defer cl.Close()
	s.log.Info("kafka consumer started", "topic", s.topic, "group", s.group, "delivery", "at-most-once")

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			s.commitOnShutdown(cl)
			return nil
		}
		s.logFetchErrors(fetches)
		var stop bool
		fetches.EachRecord(func(r *kgo.Record) {
			if stop {
				return
			}
			cl.MarkCommitRecords(r)
			rec, ok := s.decode(r)
			if !ok {
				return
			}
			select {
			case out <- rec:
			case <-ctx.Done():
				stop = true
			}
		})
		if stop {
			s.commitOnShutdown(cl)
			return nil
		}
	}
}

// StartPull is the at-least-once entry the pipeline prefers. Offers block
// instead of dropping, and a poll's offsets are committed only once the
// pipeline reports every record in it written to every sink. If a sink
// rejects a batch, the source rewinds: it leaves the group and rejoins,
// which resumes from the last committed offset, so the rejected records
// are delivered again. Duplicates on the sinks that did write them are
// collapsed by the dedup key.
//
// Polling and committing are decoupled: the poll loop offers and hands
// each poll's (last sequence, offsets) to the committer, which waits on the
// pipeline and commits in order. The handoff channel is bounded, so a
// pipeline that stops making progress stops the polling too.
func (s *Source) StartPull(ctx context.Context, in flow.Intake) error {
	for {
		rewind, err := s.consumeUntilFailure(ctx, in)
		if err != nil || !rewind {
			return err
		}
		obs.SourceRewinds.WithLabelValues(sourceLabel).Inc()
		s.log.Warn("rewinding to the last committed offset: a sink rejected a batch")
	}
}

// pollAck is one poll's worth of records, awaiting acknowledgement.
type pollAck struct {
	seq     uint64
	records []*kgo.Record
}

// consumeUntilFailure runs one consumer session. It returns rewind=true
// when the pipeline reported a write failure and the caller should start a
// fresh session from the committed offsets.
func (s *Source) consumeUntilFailure(ctx context.Context, in flow.Intake) (rewind bool, err error) {
	cl, err := s.client(kgo.DisableAutoCommit())
	if err != nil {
		return false, err
	}
	defer cl.Close()
	s.log.Info("kafka consumer started", "topic", s.topic, "group", s.group, "delivery", "at-least-once")

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	acks := make(chan pollAck, ackBacklog)
	failed := make(chan struct{}, 1)
	var committer sync.WaitGroup
	committer.Add(1)
	go func() {
		defer committer.Done()
		for ack := range acks {
			if err := in.Written(sessionCtx, ack.seq); err != nil {
				if errors.Is(err, flow.ErrWriteFailed) {
					failed <- struct{}{}
				}
				cancel()
				return
			}
			if err := cl.CommitRecords(sessionCtx, ack.records...); err != nil && sessionCtx.Err() == nil {
				s.log.Warn("kafka commit failed", "err", err)
			}
		}
	}()

	for {
		fetches := cl.PollFetches(sessionCtx)
		if sessionCtx.Err() != nil {
			break
		}
		s.logFetchErrors(fetches)
		var (
			last    uint64
			records []*kgo.Record
			stop    bool
		)
		fetches.EachRecord(func(r *kgo.Record) {
			if stop {
				return
			}
			records = append(records, r)
			rec, ok := s.decode(r)
			if !ok {
				return // still committed: a poison message must not wedge the consumer
			}
			seq, err := in.Offer(sessionCtx, rec)
			if err != nil {
				stop = true
				return
			}
			last = seq
		})
		if stop {
			break
		}
		if len(records) == 0 {
			continue
		}
		select {
		case acks <- pollAck{seq: last, records: records}:
		case <-sessionCtx.Done():
			stop = true
		}
		if stop {
			break
		}
	}
	close(acks)
	committer.Wait()
	select {
	case <-failed:
		return ctx.Err() == nil, nil
	default:
		return false, nil
	}
}

// ackBacklog bounds polls awaiting acknowledgement, and with it how far the
// consumer runs ahead of what is committed.
const ackBacklog = 64

func (s *Source) client(opts ...kgo.Opt) (*kgo.Client, error) {
	cl, err := kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers(s.brokers...),
		kgo.ConsumerGroup(s.group),
		kgo.ConsumeTopics(s.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}, opts...)...)
	if err != nil {
		return nil, fmt.Errorf("kafka: client: %w", err)
	}
	return cl, nil
}

func (s *Source) logFetchErrors(fetches kgo.Fetches) {
	for _, fe := range fetches.Errors() {
		if errors.Is(fe.Err, context.Canceled) {
			continue
		}
		s.log.Warn("kafka fetch error", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
	}
}

// decode is Decode plus the error accounting every path shares.
func (s *Source) decode(r *kgo.Record) (flow.FlowRecord, bool) {
	rec, err := Decode(r.Value, r.Timestamp)
	if err != nil {
		obs.DecodeErrors.WithLabelValues(sourceLabel).Inc()
		s.log.Debug("kafka decode failed", "partition", r.Partition, "offset", r.Offset, "err", err)
		return flow.FlowRecord{}, false
	}
	return rec, true
}

// commitOnShutdown flushes marked offsets with a fresh, bounded context so a
// cancelled ctx does not lose the last poll's progress.
func (s *Source) commitOnShutdown(cl *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		s.log.Warn("kafka final commit failed", "err", err)
	}
}

// Decode turns one message body into a record and stamps it with the
// DedupKey that makes a redelivery collide with itself. recordTime is the
// Kafka record's own timestamp, used only when the message carries no
// received_at.
func Decode(body []byte, recordTime time.Time) (flow.FlowRecord, error) {
	r, err := wire.Decode(body, recordTime)
	if err != nil {
		return flow.FlowRecord{}, fmt.Errorf("kafka: %w", err)
	}
	r.DedupKey = DedupKey(r)
	return r, nil
}

// DedupKey is the 16-byte digest of the record's natural key:
// (exporter_addr, first_switched, src_addr, dst_addr, src_port, dst_port,
// protocol). Addresses are hashed in 16-byte form so IPv4 and IPv6 mix.
func DedupKey(r flow.FlowRecord) []byte {
	var buf [16 + 8 + 16 + 16 + 2 + 2 + 1]byte
	b := buf[:0]
	e, s, d := r.ExporterAddr.As16(), r.SrcAddr.As16(), r.DstAddr.As16()
	b = append(b, e[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(r.FirstSwitched.UTC().Truncate(time.Microsecond).UnixMicro()))
	b = append(b, s[:]...)
	b = append(b, d[:]...)
	b = binary.BigEndian.AppendUint16(b, r.SrcPort)
	b = binary.BigEndian.AppendUint16(b, r.DstPort)
	b = append(b, r.Protocol)
	sum := sha256.Sum256(b)
	return sum[:16]
}
