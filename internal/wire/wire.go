// Package wire is the collector's own message format: one JSON object per
// flow record, no envelope. The kafka sink produces it and the kafka source
// consumes it, which is what lets one binary run as an edge tier
// (NFC_SOURCES=netflow NFC_SINKS=kafka) feeding a central tier
// (NFC_SOURCES=kafka NFC_SINKS=<database>). Unknown fields are ignored on
// decode so producers can add fields; next_hop and received_at may be null
// or absent.
//
// It imports only internal/flow, so both a source and a sink may import it
// without either importing the other.
package wire

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

// Message is the wire schema.
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

// Encode renders one record as a message body. DedupKey is not carried: it is
// a property of the transport that can redeliver, and the consumer derives
// it from the natural key.
func Encode(r flow.FlowRecord) ([]byte, error) {
	at := r.ReceivedAt.UTC().Truncate(time.Microsecond)
	m := Message{
		ExporterAddr:  r.ExporterAddr,
		FlowType:      string(r.FlowType),
		ReceivedAt:    &at,
		FirstSwitched: r.FirstSwitched.UTC().Truncate(time.Microsecond),
		LastSwitched:  r.LastSwitched.UTC().Truncate(time.Microsecond),
		SrcAddr:       r.SrcAddr,
		DstAddr:       r.DstAddr,
		SrcPort:       r.SrcPort,
		DstPort:       r.DstPort,
		Protocol:      r.Protocol,
		TCPFlags:      r.TCPFlags,
		Packets:       r.Packets,
		Bytes:         r.Bytes,
		SamplingRate:  r.SamplingRate,
		InputIface:    r.InputIface,
		OutputIface:   r.OutputIface,
		SrcAS:         r.SrcAS,
		DstAS:         r.DstAS,
	}
	if r.NextHop.IsValid() {
		nh := r.NextHop
		m.NextHop = &nh
	}
	body, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("wire: encode: %w", err)
	}
	return body, nil
}

// Decode turns one message body into a record. recordTime is the transport's
// own timestamp for the message, used only when the body carries no
// received_at. The returned record has no DedupKey; a source whose transport
// can redeliver sets one.
func Decode(body []byte, recordTime time.Time) (flow.FlowRecord, error) {
	var m Message
	if err := json.Unmarshal(body, &m); err != nil {
		return flow.FlowRecord{}, fmt.Errorf("wire: decode: %w", err)
	}
	ft := flow.FlowType(m.FlowType)
	switch {
	case !m.ExporterAddr.IsValid():
		return flow.FlowRecord{}, errors.New("wire: decode: exporter_addr is required")
	case !ft.Valid():
		return flow.FlowRecord{}, fmt.Errorf("wire: decode: unknown flow_type %q", m.FlowType)
	case !m.SrcAddr.IsValid() || !m.DstAddr.IsValid():
		return flow.FlowRecord{}, errors.New("wire: decode: src_addr and dst_addr are required")
	case m.FirstSwitched.IsZero() || m.LastSwitched.IsZero():
		return flow.FlowRecord{}, errors.New("wire: decode: first_switched and last_switched are required")
	}
	receivedAt := recordTime
	if m.ReceivedAt != nil {
		receivedAt = *m.ReceivedAt
	}
	if receivedAt.IsZero() {
		return flow.FlowRecord{}, errors.New("wire: decode: no received_at and no record timestamp")
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
	return r, nil
}
