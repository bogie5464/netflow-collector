package postgres

import (
	"net/netip"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

// insertSQL inserts a whole batch in one statement by unnesting parallel
// arrays, one per column. ON CONFLICT DO NOTHING targets the nullable unique
// index on (received_at, dedup_key).
const insertSQL = `
INSERT INTO flow_records (
    received_at, exporter_id, flow_type, first_switched, last_switched,
    src_addr, dst_addr, src_port, dst_port, protocol, tcp_flags,
    packets, bytes, sampling_rate, input_iface, output_iface, src_as, dst_as,
    next_hop, dedup_key)
SELECT * FROM unnest(
    $1::timestamptz[], $2::bigint[], $3::text[], $4::timestamptz[], $5::timestamptz[],
    $6::inet[], $7::inet[], $8::integer[], $9::integer[], $10::smallint[], $11::smallint[],
    $12::bigint[], $13::bigint[], $14::integer[], $15::integer[], $16::integer[], $17::integer[], $18::integer[],
    $19::inet[], $20::bytea[])
ON CONFLICT DO NOTHING`

const selectSQL = `
SELECT f.received_at, f.seq, f.exporter_id, e.ip_address, f.flow_type,
       f.first_switched, f.last_switched, f.src_addr, f.dst_addr,
       f.src_port, f.dst_port, f.protocol, f.tcp_flags,
       f.packets, f.bytes, f.sampling_rate, f.input_iface, f.output_iface,
       f.src_as, f.dst_as, f.next_hop, f.dedup_key
  FROM flow_records f
  JOIN exporters e ON e.id = f.exporter_id`

// columns accumulates one batch as parallel arrays for insertSQL. Timestamps
// are truncated to microseconds here, on the way in.
type columns struct {
	receivedAt, firstSwitched, lastSwitched []time.Time
	exporterID                              []int64
	flowType                                []string
	srcAddr, dstAddr                        []netip.Addr
	srcPort, dstPort                        []int32
	protocol, tcpFlags                      []int16
	packets, bytes                          []int64
	samplingRate, inIface, outIface         []int32
	srcAS, dstAS                            []int32
	nextHop                                 []*netip.Addr
	dedupKey                                [][]byte
}

func newColumns(n int) *columns {
	return &columns{
		receivedAt: make([]time.Time, 0, n), firstSwitched: make([]time.Time, 0, n), lastSwitched: make([]time.Time, 0, n),
		exporterID: make([]int64, 0, n), flowType: make([]string, 0, n),
		srcAddr: make([]netip.Addr, 0, n), dstAddr: make([]netip.Addr, 0, n),
		srcPort: make([]int32, 0, n), dstPort: make([]int32, 0, n),
		protocol: make([]int16, 0, n), tcpFlags: make([]int16, 0, n),
		packets: make([]int64, 0, n), bytes: make([]int64, 0, n),
		samplingRate: make([]int32, 0, n), inIface: make([]int32, 0, n), outIface: make([]int32, 0, n),
		srcAS: make([]int32, 0, n), dstAS: make([]int32, 0, n),
		nextHop: make([]*netip.Addr, 0, n), dedupKey: make([][]byte, 0, n),
	}
}

func micro(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

func (c *columns) add(r *flow.FlowRecord, exporterID int64) {
	c.receivedAt = append(c.receivedAt, micro(r.ReceivedAt))
	c.exporterID = append(c.exporterID, exporterID)
	c.flowType = append(c.flowType, string(r.FlowType))
	c.firstSwitched = append(c.firstSwitched, micro(r.FirstSwitched))
	c.lastSwitched = append(c.lastSwitched, micro(r.LastSwitched))
	c.srcAddr = append(c.srcAddr, r.SrcAddr)
	c.dstAddr = append(c.dstAddr, r.DstAddr)
	c.srcPort = append(c.srcPort, int32(r.SrcPort))
	c.dstPort = append(c.dstPort, int32(r.DstPort))
	c.protocol = append(c.protocol, int16(r.Protocol))
	c.tcpFlags = append(c.tcpFlags, int16(r.TCPFlags))
	c.packets = append(c.packets, int64(r.Packets))
	c.bytes = append(c.bytes, int64(r.Bytes))
	c.samplingRate = append(c.samplingRate, int32(r.SamplingRate))
	c.inIface = append(c.inIface, int32(r.InputIface))
	c.outIface = append(c.outIface, int32(r.OutputIface))
	c.srcAS = append(c.srcAS, int32(r.SrcAS))
	c.dstAS = append(c.dstAS, int32(r.DstAS))
	if r.NextHop.IsValid() {
		nh := r.NextHop
		c.nextHop = append(c.nextHop, &nh)
	} else {
		c.nextHop = append(c.nextHop, nil)
	}
	c.dedupKey = append(c.dedupKey, r.DedupKey) // nil stays NULL
}

func (c *columns) args() []any {
	return []any{
		c.receivedAt, c.exporterID, c.flowType, c.firstSwitched, c.lastSwitched,
		c.srcAddr, c.dstAddr, c.srcPort, c.dstPort, c.protocol, c.tcpFlags,
		c.packets, c.bytes, c.samplingRate, c.inIface, c.outIface, c.srcAS, c.dstAS,
		c.nextHop, c.dedupKey,
	}
}

// scanRecord reads one selectSQL row. It returns seq separately because seq
// is a cursor ingredient, not a public field.
func scanRecord(rows pgx.Rows) (flow.FlowRecord, int64, error) {
	var (
		r                           flow.FlowRecord
		seq                         int64
		exporter, src, dst          netip.Prefix
		nextHop                     *netip.Prefix
		flowType                    string
		srcPort, dstPort            int32
		protocol, tcpFlags          int16
		packets, bytes              int64
		sampling, inIface, outIface int32
		srcAS, dstAS                int32
	)
	err := rows.Scan(&r.ReceivedAt, &seq, &r.ExporterID, &exporter, &flowType,
		&r.FirstSwitched, &r.LastSwitched, &src, &dst,
		&srcPort, &dstPort, &protocol, &tcpFlags,
		&packets, &bytes, &sampling, &inIface, &outIface,
		&srcAS, &dstAS, &nextHop, &r.DedupKey)
	if err != nil {
		return r, 0, err
	}
	r.ReceivedAt = r.ReceivedAt.UTC()
	r.FirstSwitched = r.FirstSwitched.UTC()
	r.LastSwitched = r.LastSwitched.UTC()
	r.ExporterAddr = exporter.Addr()
	r.FlowType = flow.FlowType(flowType)
	r.SrcAddr, r.DstAddr = src.Addr(), dst.Addr()
	r.SrcPort, r.DstPort = uint16(srcPort), uint16(dstPort)
	r.Protocol, r.TCPFlags = uint8(protocol), uint8(tcpFlags)
	r.Packets, r.Bytes = uint64(packets), uint64(bytes)
	r.SamplingRate = uint32(sampling)
	r.InputIface, r.OutputIface = uint32(inIface), uint32(outIface)
	r.SrcAS, r.DstAS = uint32(srcAS), uint32(dstAS)
	if nextHop != nil {
		r.NextHop = nextHop.Addr()
	}
	return r, seq, nil
}
