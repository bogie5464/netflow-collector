package mariadb

import (
	"database/sql"
	"net/netip"
	"strings"
	"time"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

const insertPrefix = `INSERT IGNORE INTO flow_records (
    received_at, exporter_id, flow_type, first_switched, last_switched,
    src_addr, dst_addr, src_port, dst_port, protocol, tcp_flags,
    packets, bytes, sampling_rate, input_iface, output_iface, src_as, dst_as,
    next_hop, dedup_key) VALUES `

const insertRow = "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

func micro(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

func addrBytes(a netip.Addr) []byte {
	if !a.IsValid() {
		return nil
	}
	return a.AsSlice()
}

// insertStatement renders one multi-row INSERT IGNORE for records.
func insertStatement(records []flow.FlowRecord, ids map[netip.Addr]int64) (string, []any) {
	var sb strings.Builder
	sb.WriteString(insertPrefix)
	args := make([]any, 0, 20*len(records))
	for i := range records {
		r := &records[i]
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(insertRow)
		args = append(args,
			micro(r.ReceivedAt), ids[r.ExporterAddr], string(r.FlowType), micro(r.FirstSwitched), micro(r.LastSwitched),
			r.SrcAddr.AsSlice(), r.DstAddr.AsSlice(), r.SrcPort, r.DstPort, r.Protocol, r.TCPFlags,
			r.Packets, r.Bytes, r.SamplingRate, r.InputIface, r.OutputIface, r.SrcAS, r.DstAS,
			addrBytes(r.NextHop), r.DedupKey)
	}
	return sb.String(), args
}

const selectSQL = `
SELECT f.received_at, f.seq, f.exporter_id, e.ip_address, f.flow_type,
       f.first_switched, f.last_switched, f.src_addr, f.dst_addr,
       f.src_port, f.dst_port, f.protocol, f.tcp_flags,
       f.packets, f.bytes, f.sampling_rate, f.input_iface, f.output_iface,
       f.src_as, f.dst_as, f.next_hop, f.dedup_key
  FROM flow_records f
  JOIN exporters e ON e.id = f.exporter_id`

func scanRecord(rows *sql.Rows) (flow.FlowRecord, int64, error) {
	var (
		r                           flow.FlowRecord
		seq                         int64
		exporter, src, dst, nextHop []byte
		flowType                    string
	)
	err := rows.Scan(&r.ReceivedAt, &seq, &r.ExporterID, &exporter, &flowType,
		&r.FirstSwitched, &r.LastSwitched, &src, &dst,
		&r.SrcPort, &r.DstPort, &r.Protocol, &r.TCPFlags,
		&r.Packets, &r.Bytes, &r.SamplingRate, &r.InputIface, &r.OutputIface,
		&r.SrcAS, &r.DstAS, &nextHop, &r.DedupKey)
	if err != nil {
		return r, 0, err
	}
	r.ReceivedAt, r.FirstSwitched, r.LastSwitched = r.ReceivedAt.UTC(), r.FirstSwitched.UTC(), r.LastSwitched.UTC()
	r.FlowType = flow.FlowType(flowType)
	r.ExporterAddr, _ = netip.AddrFromSlice(exporter)
	r.SrcAddr, _ = netip.AddrFromSlice(src)
	r.DstAddr, _ = netip.AddrFromSlice(dst)
	if nextHop != nil {
		r.NextHop, _ = netip.AddrFromSlice(nextHop)
	}
	if len(r.DedupKey) == 0 {
		r.DedupKey = nil
	}
	return r, seq, nil
}
