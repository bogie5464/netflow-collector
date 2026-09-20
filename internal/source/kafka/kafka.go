// Package kafka is the Kafka input adapter: a franz-go consumer group that
// decodes one JSON flow record per message (blueprint §5, "Kafka ingest
// message schema") and emits flow.FlowRecord values.
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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
)

const sourceLabel = "kafka"

// Message is the wire schema: one JSON object per Kafka message, no envelope.
// Unknown fields are ignored so producers can add fields. next_hop and
// received_at may be null or absent.
type Message struct {
	ExporterAddr  netip.Addr  `json:"exporter_addr"`
	FlowType      string      `json:"flow_type"`
	ReceivedAt    *time.Time  `json:"received_at"`
	FirstSwitched time.Time   `json:"first_switched"`
	LastSwitched  time.Time   `json:"last_switched"`
	SrcAddr       netip.Addr  `json:"src_addr"`
	DstAddr       netip.Addr  `json:"dst_addr"`
	SrcPort       uint16      `json:"src_port"`
	DstPort       uint16      `json:"dst_port"`
	Protocol      uint8       `json:"protocol"`
	TCPFlags      uint8       `json:"tcp_flags"`
	Packets       uint64      `json:"packets"`
	Bytes         uint64      `json:"bytes"`
	SamplingRate  uint32      `json:"sampling_rate"`
	InputIface    uint32      `json:"input_iface"`
	OutputIface   uint32      `json:"output_iface"`
	SrcAS         uint32      `json:"src_as"`
	DstAS         uint32      `json:"dst_as"`
	NextHop       *netip.Addr `json:"next_hop"`
}

// Source is a Kafka consumer. Create it with New.
type Source struct {
	brokers []string
	topic   string
	group   string
	log     *slog.Logger
}

var _ flow.Source = (*Source)(nil)

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

// Start consumes until ctx is cancelled, then commits what it has consumed,
// closes the client and returns nil. Every polled record is marked for
// commit whether or not it decoded: one poison message must never wedge the
// consumer.
func (s *Source) Start(ctx context.Context, out chan<- flow.FlowRecord) error {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(s.brokers...),
		kgo.ConsumerGroup(s.group),
		kgo.ConsumeTopics(s.topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		return fmt.Errorf("kafka: client: %w", err)
	}
	defer cl.Close()
	s.log.Info("kafka consumer started", "topic", s.topic, "group", s.group)

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			s.commitOnShutdown(cl)
			return nil
		}
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, context.Canceled) {
				continue
			}
			s.log.Warn("kafka fetch error", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
		}
		var stop bool
		fetches.EachRecord(func(r *kgo.Record) {
			if stop {
				return
			}
			cl.MarkCommitRecords(r)
			rec, err := Decode(r.Value, r.Timestamp)
			if err != nil {
				obs.DecodeErrors.WithLabelValues(sourceLabel).Inc()
				s.log.Debug("kafka decode failed", "partition", r.Partition, "offset", r.Offset, "err", err)
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

// commitOnShutdown flushes marked offsets with a fresh, bounded context so a
// cancelled ctx does not lose the last poll's progress.
func (s *Source) commitOnShutdown(cl *kgo.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		s.log.Warn("kafka final commit failed", "err", err)
	}
}

// Decode turns one message body into a record. recordTime is the Kafka
// record's own timestamp, used only when the message carries no received_at.
func Decode(body []byte, recordTime time.Time) (flow.FlowRecord, error) {
	var m Message
	if err := json.Unmarshal(body, &m); err != nil {
		return flow.FlowRecord{}, fmt.Errorf("kafka: decode: %w", err)
	}
	ft := flow.FlowType(m.FlowType)
	switch {
	case !m.ExporterAddr.IsValid():
		return flow.FlowRecord{}, errors.New("kafka: decode: exporter_addr is required")
	case !ft.Valid():
		return flow.FlowRecord{}, fmt.Errorf("kafka: decode: unknown flow_type %q", m.FlowType)
	case !m.SrcAddr.IsValid() || !m.DstAddr.IsValid():
		return flow.FlowRecord{}, errors.New("kafka: decode: src_addr and dst_addr are required")
	case m.FirstSwitched.IsZero() || m.LastSwitched.IsZero():
		return flow.FlowRecord{}, errors.New("kafka: decode: first_switched and last_switched are required")
	}
	receivedAt := recordTime
	if m.ReceivedAt != nil {
		receivedAt = *m.ReceivedAt
	}
	if receivedAt.IsZero() {
		return flow.FlowRecord{}, errors.New("kafka: decode: no received_at and no record timestamp")
	}
	r := flow.FlowRecord{
		ReceivedAt:    receivedAt.UTC().Truncate(time.Microsecond),
		ExporterAddr:  m.ExporterAddr.Unmap(),
		FlowType:      ft,
		FirstSwitched: m.FirstSwitched.UTC().Truncate(time.Microsecond),
		LastSwitched:  m.LastSwitched.UTC().Truncate(time.Microsecond),
		SrcAddr:       m.SrcAddr.Unmap(),
		DstAddr:       m.DstAddr.Unmap(),
		SrcPort:       m.SrcPort,
		DstPort:       m.DstPort,
		Protocol:      m.Protocol,
		TCPFlags:      m.TCPFlags,
		Packets:       m.Packets,
		Bytes:         m.Bytes,
		SamplingRate:  m.SamplingRate,
		InputIface:    m.InputIface,
		OutputIface:   m.OutputIface,
		SrcAS:         m.SrcAS,
		DstAS:         m.DstAS,
	}
	if r.SamplingRate == 0 {
		r.SamplingRate = 1
	}
	if m.NextHop != nil && m.NextHop.IsValid() {
		r.NextHop = m.NextHop.Unmap()
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
