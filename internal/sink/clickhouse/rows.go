package clickhouse

import (
	"net/netip"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

// insertSQL names every column the sink supplies; seq is MATERIALIZED and
// must not be listed.
const insertSQL = `INSERT INTO flow_records (
    received_at, dedup_key, exporter_id, exporter_addr, flow_type,
    first_switched, last_switched, src_addr, dst_addr,
    src_port, dst_port, protocol, tcp_flags,
    packets, bytes, sampling_rate, input_iface, output_iface, src_as, dst_as,
    next_hop)`

const selectSQL = `
SELECT received_at, seq, exporter_id, exporter_addr, flow_type,
       first_switched, last_switched, src_addr, dst_addr,
       src_port, dst_port, protocol, tcp_flags,
       packets, bytes, sampling_rate, input_iface, output_iface, src_as, dst_as,
       next_hop, dedup_key
  FROM flow_records FINAL`

func micro(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

// insertRow renders one record in insertSQL's column order. Timestamps are
// truncated to microseconds here, on the way in.
func insertRow(r *flow.FlowRecord) []any {
	var nextHop *netip.Addr
	if r.NextHop.IsValid() {
		nh := r.NextHop
		nextHop = &nh
	}
	return []any{
		micro(r.ReceivedAt), string(r.DedupKey), exporterID(r.ExporterAddr), r.ExporterAddr, string(r.FlowType),
		micro(r.FirstSwitched), micro(r.LastSwitched), r.SrcAddr, r.DstAddr,
		r.SrcPort, r.DstPort, r.Protocol, r.TCPFlags,
		r.Packets, r.Bytes, r.SamplingRate, r.InputIface, r.OutputIface, r.SrcAS, r.DstAS,
		nextHop,
	}
}

// scanRecord reads one selectSQL row. It returns seq separately because seq
// is a cursor ingredient, not a public field. IPv4 addresses come back from
// the IPv6 columns in mapped form and are unmapped so equality holds.
func scanRecord(rows driver.Rows) (flow.FlowRecord, uint64, error) {
	var (
		r          flow.FlowRecord
		seq, expID uint64
		exporter   netip.Addr
		src, dst   netip.Addr
		nextHop    *netip.Addr
		flowType   string
		dedupKey   string
	)
	err := rows.Scan(&r.ReceivedAt, &seq, &expID, &exporter, &flowType,
		&r.FirstSwitched, &r.LastSwitched, &src, &dst,
		&r.SrcPort, &r.DstPort, &r.Protocol, &r.TCPFlags,
		&r.Packets, &r.Bytes, &r.SamplingRate, &r.InputIface, &r.OutputIface,
		&r.SrcAS, &r.DstAS, &nextHop, &dedupKey)
	if err != nil {
		return r, 0, err
	}
	r.ReceivedAt, r.FirstSwitched, r.LastSwitched = r.ReceivedAt.UTC(), r.FirstSwitched.UTC(), r.LastSwitched.UTC()
	r.ExporterID = int64(expID)
	r.ExporterAddr = exporter.Unmap()
	r.FlowType = flow.FlowType(flowType)
	r.SrcAddr, r.DstAddr = src.Unmap(), dst.Unmap()
	if nextHop != nil {
		r.NextHop = nextHop.Unmap()
	}
	if dedupKey != "" {
		r.DedupKey = []byte(dedupKey)
	}
	return r, seq, nil
}
