package netflow

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net/netip"
	"sync"
	"time"

	nf "github.com/netsampler/goflow2/v2/decoders/netflow"
	"github.com/netsampler/goflow2/v2/decoders/netflowlegacy"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

const (
	v5HeaderLen = 24
	v5RecordLen = 48
)

var errShort = errors.New("datagram shorter than the header")

// decoder sniffs the wire version and dispatches to the right goflow2 package.
// v9 and IPFIX are template-driven, so a template system lives per exporter
// for the life of the process: a template arrives once and every later
// datagram from that exporter depends on it.
type decoder struct {
	mu        sync.Mutex
	templates map[netip.Addr]nf.NetFlowTemplateSystem
}

func newDecoder() *decoder {
	return &decoder{templates: map[netip.Addr]nf.NetFlowTemplateSystem{}}
}

func (d *decoder) templatesFor(exporter netip.Addr) nf.NetFlowTemplateSystem {
	d.mu.Lock()
	defer d.mu.Unlock()
	ts, ok := d.templates[exporter]
	if !ok {
		ts = nf.CreateTemplateSystem()
		d.templates[exporter] = ts
	}
	return ts
}

// decode turns one datagram into zero or more records. A template-only v9 or
// IPFIX datagram decodes to zero records and no error.
func (d *decoder) decode(b []byte, exporter netip.Addr, receivedAt time.Time) ([]flow.FlowRecord, error) {
	if len(b) < 2 {
		return nil, errShort
	}
	switch version := binary.BigEndian.Uint16(b); version {
	case 5:
		return decodeV5(b, exporter, receivedAt)
	case 9, 10:
		return d.decodeTemplated(b, exporter, receivedAt)
	default:
		return nil, fmt.Errorf("unsupported NetFlow version %d", version)
	}
}

func decodeV5(b []byte, exporter netip.Addr, receivedAt time.Time) ([]flow.FlowRecord, error) {
	if len(b) < v5HeaderLen {
		return nil, errShort
	}
	// goflow2 silently returns zeroed records when the payload runs short, so
	// the length is checked against the header's count here.
	count := int(binary.BigEndian.Uint16(b[2:4]))
	if len(b) < v5HeaderLen+count*v5RecordLen {
		return nil, fmt.Errorf("v5 datagram carries %d bytes for %d records", len(b), count)
	}
	var pkt netflowlegacy.PacketNetFlowV5
	if err := netflowlegacy.DecodeMessageVersion(bytes.NewBuffer(b), &pkt); err != nil {
		return nil, err
	}
	// v5 timestamps are milliseconds of system uptime; anchor them on the
	// exporter's wall clock from the same header.
	boot := time.Unix(int64(pkt.UnixSecs), int64(pkt.UnixNSecs)).Add(-time.Duration(pkt.SysUptime) * time.Millisecond)
	// The low 14 bits are the interval; the top 2 are the sampling mode.
	rate := uint32(pkt.SamplingInterval & 0x3fff)
	if rate == 0 {
		rate = 1
	}
	out := make([]flow.FlowRecord, 0, len(pkt.Records))
	for _, r := range pkt.Records {
		out = append(out, flow.FlowRecord{
			ReceivedAt:    receivedAt,
			ExporterAddr:  exporter,
			FlowType:      flow.FlowTypeNetFlow5,
			FirstSwitched: uptimeToTime(boot, r.First),
			LastSwitched:  uptimeToTime(boot, r.Last),
			SrcAddr:       ipv4(uint32(r.SrcAddr)),
			DstAddr:       ipv4(uint32(r.DstAddr)),
			SrcPort:       r.SrcPort,
			DstPort:       r.DstPort,
			Protocol:      r.Proto,
			TCPFlags:      r.TCPFlags,
			Packets:       uint64(r.DPkts),
			Bytes:         uint64(r.DOctets),
			SamplingRate:  rate,
			InputIface:    uint32(r.Input),
			OutputIface:   uint32(r.Output),
			SrcAS:         uint32(r.SrcAS),
			DstAS:         uint32(r.DstAS),
			NextHop:       nextHop4(uint32(r.NextHop)),
		})
	}
	return out, nil
}

func (d *decoder) decodeTemplated(b []byte, exporter netip.Addr, receivedAt time.Time) ([]flow.FlowRecord, error) {
	var (
		v9    nf.NFv9Packet
		ipfix nf.IPFIXPacket
	)
	if err := nf.DecodeMessageVersion(bytes.NewBuffer(b), d.templatesFor(exporter), &v9, &ipfix); err != nil {
		return nil, err
	}
	var (
		flowSets []interface{}
		base     flow.FlowRecord
		boot     time.Time // anchor for sysuptime-relative fields
	)
	switch v9.Version {
	case 9:
		flowSets = v9.FlowSets
		base.FlowType = flow.FlowTypeNetFlow9
		boot = time.Unix(int64(v9.UnixSeconds), 0).Add(-time.Duration(v9.SystemUptime) * time.Millisecond)
	default:
		flowSets = ipfix.FlowSets
		base.FlowType = flow.FlowTypeIPFIX
		boot = time.Unix(int64(ipfix.ExportTime), 0)
	}
	base.ReceivedAt = receivedAt
	base.ExporterAddr = exporter
	base.SamplingRate = 1

	var out []flow.FlowRecord
	for _, fs := range flowSets {
		data, ok := fs.(nf.DataFlowSet)
		if !ok {
			continue // templates and options are consumed by the template system
		}
		for _, rec := range data.Records {
			r := base
			for _, f := range rec.Values {
				applyField(&r, f, boot)
			}
			if r.LastSwitched.IsZero() {
				r.LastSwitched = r.FirstSwitched
			}
			out = append(out, r)
		}
	}
	return out, nil
}

// applyField maps one v9/IPFIX information element onto the record. The
// numeric ids are shared between the two protocols for everything used here.
func applyField(r *flow.FlowRecord, f nf.DataField, boot time.Time) {
	v, _ := f.Value.([]byte)
	if f.PenProvided || len(v) == 0 {
		return
	}
	switch f.Type {
	case nf.IPFIX_FIELD_octetDeltaCount:
		r.Bytes = beUint(v)
	case nf.IPFIX_FIELD_packetDeltaCount:
		r.Packets = beUint(v)
	case nf.IPFIX_FIELD_protocolIdentifier:
		r.Protocol = uint8(beUint(v))
	case nf.IPFIX_FIELD_tcpControlBits:
		r.TCPFlags = uint8(beUint(v))
	case nf.IPFIX_FIELD_sourceTransportPort:
		r.SrcPort = uint16(beUint(v))
	case nf.IPFIX_FIELD_destinationTransportPort:
		r.DstPort = uint16(beUint(v))
	case nf.IPFIX_FIELD_sourceIPv4Address, nf.IPFIX_FIELD_sourceIPv6Address:
		r.SrcAddr = addr(v)
	case nf.IPFIX_FIELD_destinationIPv4Address, nf.IPFIX_FIELD_destinationIPv6Address:
		r.DstAddr = addr(v)
	case nf.IPFIX_FIELD_ipNextHopIPv4Address, nf.IPFIX_FIELD_ipNextHopIPv6Address:
		if a := addr(v); a.IsValid() && !a.IsUnspecified() {
			r.NextHop = a
		}
	case nf.IPFIX_FIELD_ingressInterface:
		r.InputIface = uint32(beUint(v))
	case nf.IPFIX_FIELD_egressInterface:
		r.OutputIface = uint32(beUint(v))
	case nf.IPFIX_FIELD_bgpSourceAsNumber:
		r.SrcAS = uint32(beUint(v))
	case nf.IPFIX_FIELD_bgpDestinationAsNumber:
		r.DstAS = uint32(beUint(v))
	case nf.IPFIX_FIELD_samplingInterval, nf.IPFIX_FIELD_samplerRandomInterval, nf.IPFIX_FIELD_samplingPacketInterval:
		if n := uint32(beUint(v)); n > 0 {
			r.SamplingRate = n
		}
	case nf.IPFIX_FIELD_flowStartSysUpTime:
		r.FirstSwitched = uptimeToTime(boot, uint32(beUint(v)))
	case nf.IPFIX_FIELD_flowEndSysUpTime:
		r.LastSwitched = uptimeToTime(boot, uint32(beUint(v)))
	case nf.IPFIX_FIELD_flowStartSeconds:
		r.FirstSwitched = time.Unix(int64(beUint(v)), 0).UTC()
	case nf.IPFIX_FIELD_flowEndSeconds:
		r.LastSwitched = time.Unix(int64(beUint(v)), 0).UTC()
	case nf.IPFIX_FIELD_flowStartMilliseconds:
		r.FirstSwitched = time.UnixMilli(int64(beUint(v))).UTC()
	case nf.IPFIX_FIELD_flowEndMilliseconds:
		r.LastSwitched = time.UnixMilli(int64(beUint(v))).UTC()
	case nf.IPFIX_FIELD_flowStartMicroseconds:
		r.FirstSwitched = ntpToTime(beUint(v))
	case nf.IPFIX_FIELD_flowEndMicroseconds:
		r.LastSwitched = ntpToTime(beUint(v))
	}
}

// beUint reads a big-endian unsigned integer of 1 to 8 bytes.
func beUint(b []byte) uint64 {
	var n uint64
	for _, c := range b {
		n = n<<8 | uint64(c)
	}
	return n
}

func addr(b []byte) netip.Addr {
	a, _ := netip.AddrFromSlice(b)
	return a
}

func ipv4(n uint32) netip.Addr {
	return netip.AddrFrom4([4]byte{byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
}

// nextHop4 leaves the invalid zero Addr for 0.0.0.0, which is how v5 says
// "no next hop".
func nextHop4(n uint32) netip.Addr {
	if n == 0 {
		return netip.Addr{}
	}
	return ipv4(n)
}

func uptimeToTime(boot time.Time, ms uint32) time.Time {
	return boot.Add(time.Duration(ms) * time.Millisecond).UTC().Truncate(time.Microsecond)
}

// ntpToTime converts the 64-bit NTP format IPFIX uses for microsecond fields.
func ntpToTime(v uint64) time.Time {
	const ntpEpochOffset = 2208988800
	secs := int64(v>>32) - ntpEpochOffset
	frac := time.Duration(float64(v&0xffffffff) / (1 << 32) * float64(time.Second))
	return time.Unix(secs, int64(frac)).UTC().Truncate(time.Microsecond)
}
