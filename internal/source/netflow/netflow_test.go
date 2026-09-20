package netflow

import (
	"context"
	"encoding/binary"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
)

// v5Record is one NetFlow v5 record as the test wants it on the wire.
type v5Record struct {
	src, dst, nextHop netip.Addr
	input, output     uint16
	pkts, octets      uint32
	first, last       uint32
	srcPort, dstPort  uint16
	tcpFlags, proto   uint8
	srcAS, dstAS      uint16
}

// v5Datagram encodes a NetFlow v5 datagram from the documented layout:
// a 24-byte header followed by 48-byte records, big-endian throughout.
func v5Datagram(sysUptime, unixSecs uint32, sampling uint16, recs ...v5Record) []byte {
	b := make([]byte, 0, 24+48*len(recs))
	b = binary.BigEndian.AppendUint16(b, 5)
	b = binary.BigEndian.AppendUint16(b, uint16(len(recs)))
	b = binary.BigEndian.AppendUint32(b, sysUptime)
	b = binary.BigEndian.AppendUint32(b, unixSecs)
	b = binary.BigEndian.AppendUint32(b, 0) // unix_nsecs
	b = binary.BigEndian.AppendUint32(b, 1) // flow_sequence
	b = append(b, 0, 0)                     // engine_type, engine_id
	b = binary.BigEndian.AppendUint16(b, sampling)
	for _, r := range recs {
		b = append(b, r.src.AsSlice()...)
		b = append(b, r.dst.AsSlice()...)
		b = append(b, r.nextHop.AsSlice()...)
		b = binary.BigEndian.AppendUint16(b, r.input)
		b = binary.BigEndian.AppendUint16(b, r.output)
		b = binary.BigEndian.AppendUint32(b, r.pkts)
		b = binary.BigEndian.AppendUint32(b, r.octets)
		b = binary.BigEndian.AppendUint32(b, r.first)
		b = binary.BigEndian.AppendUint32(b, r.last)
		b = binary.BigEndian.AppendUint16(b, r.srcPort)
		b = binary.BigEndian.AppendUint16(b, r.dstPort)
		b = append(b, 0, r.tcpFlags, r.proto, 0) // pad, tcp_flags, prot, tos
		b = binary.BigEndian.AppendUint16(b, r.srcAS)
		b = binary.BigEndian.AppendUint16(b, r.dstAS)
		b = append(b, 24, 24, 0, 0) // src_mask, dst_mask, pad
	}
	return b
}

var (
	recA = v5Record{
		src: netip.MustParseAddr("203.0.113.10"), dst: netip.MustParseAddr("198.51.100.200"),
		nextHop: netip.MustParseAddr("198.51.100.1"), input: 3, output: 7,
		pkts: 12, octets: 9000, first: 1000, last: 2500, srcPort: 51514, dstPort: 443,
		tcpFlags: 24, proto: 6, srcAS: 64500, dstAS: 64501,
	}
	recB = v5Record{
		src: netip.MustParseAddr("10.0.0.1"), dst: netip.MustParseAddr("10.0.0.2"),
		nextHop: netip.MustParseAddr("0.0.0.0"), pkts: 1, octets: 64, first: 3000, last: 3000,
		srcPort: 53, dstPort: 53, proto: 17,
	}
)

// startSource runs a listener on an ephemeral port and returns the port and
// the output channel. The listener is stopped by t.Cleanup.
func startSource(t *testing.T, now func() time.Time) (*Source, netip.AddrPort, <-chan flow.FlowRecord, func() error) {
	t.Helper()
	src := newSource("127.0.0.1:0")
	if now != nil {
		src.now = now
	}
	out := make(chan flow.FlowRecord, 64)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- src.Start(ctx, out) }()
	select {
	case <-src.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not bind")
	}
	stop := func() error {
		cancel()
		select {
		case err := <-errc:
			return err
		case <-time.After(time.Second):
			return context.DeadlineExceeded
		}
	}
	t.Cleanup(func() { _ = stop() })
	return src, src.LocalAddr(), out, stop
}

func send(t *testing.T, to netip.AddrPort, b []byte) {
	t.Helper()
	conn, err := net.Dial("udp", to.String())
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close()) }()
	_, err = conn.Write(b)
	require.NoError(t, err)
}

func recv(t *testing.T, out <-chan flow.FlowRecord, n int) []flow.FlowRecord {
	t.Helper()
	got := make([]flow.FlowRecord, 0, n)
	for len(got) < n {
		select {
		case r := <-out:
			got = append(got, r)
		case <-time.After(2 * time.Second):
			t.Fatalf("received %d of %d records", len(got), n)
		}
	}
	return got
}

func TestV5(t *testing.T) {
	fixed := time.Date(2026, 9, 19, 12, 0, 0, 123456789, time.UTC)
	_, addr, out, _ := startSource(t, func() time.Time { return fixed })

	t.Run("emits one record per v5 record with flow_type netflow5", func(t *testing.T) {
		send(t, addr, v5Datagram(10_000, 1_758_288_000, 0, recA, recB))
		got := recv(t, out, 2)
		require.Equal(t, flow.FlowTypeNetFlow5, got[0].FlowType)
		require.Equal(t, flow.FlowTypeNetFlow5, got[1].FlowType)
		select {
		case extra := <-out:
			t.Fatalf("unexpected third record: %+v", extra)
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("reproduces the encoded addresses, ports, protocol and counters", func(t *testing.T) {
		send(t, addr, v5Datagram(10_000, 1_758_288_000, 0x0000|1000, recA))
		got := recv(t, out, 1)[0]
		require.Equal(t, recA.src, got.SrcAddr)
		require.Equal(t, recA.dst, got.DstAddr)
		require.Equal(t, recA.srcPort, got.SrcPort)
		require.Equal(t, recA.dstPort, got.DstPort)
		require.Equal(t, recA.proto, got.Protocol)
		require.Equal(t, uint64(recA.pkts), got.Packets)
		require.Equal(t, uint64(recA.octets), got.Bytes)
		require.Equal(t, recA.tcpFlags, got.TCPFlags)
		require.Equal(t, uint32(recA.input), got.InputIface)
		require.Equal(t, uint32(recA.output), got.OutputIface)
		require.Equal(t, uint32(recA.srcAS), got.SrcAS)
		require.Equal(t, uint32(recA.dstAS), got.DstAS)
		require.Equal(t, recA.nextHop, got.NextHop)
		require.Equal(t, uint32(1000), got.SamplingRate)
		require.Equal(t, netip.MustParseAddr("127.0.0.1"), got.ExporterAddr)
		require.Nil(t, got.DedupKey, "UDP is append-only")
		require.Equal(t, fixed.Truncate(time.Microsecond), got.ReceivedAt)
		// boot = unix_secs - sys_uptime; first/last are ms since boot.
		boot := time.Unix(1_758_288_000, 0).Add(-10_000 * time.Millisecond)
		require.Equal(t, boot.Add(1000*time.Millisecond).UTC(), got.FirstSwitched)
		require.Equal(t, boot.Add(2500*time.Millisecond).UTC(), got.LastSwitched)
	})

	t.Run("leaves NextHop invalid for 0.0.0.0 and sampling rate 1 when unsampled", func(t *testing.T) {
		send(t, addr, v5Datagram(10_000, 1_758_288_000, 0, recB))
		got := recv(t, out, 1)[0]
		require.False(t, got.NextHop.IsValid())
		require.Equal(t, uint32(1), got.SamplingRate)
	})

	t.Run("counts a datagram shorter than the v5 header, emits nothing, keeps listening", func(t *testing.T) {
		before := testutil.ToFloat64(obs.DecodeErrors.WithLabelValues(sourceLabel))
		send(t, addr, v5Datagram(0, 0, 0)[:16])
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(obs.DecodeErrors.WithLabelValues(sourceLabel)) == before+1
		}, 2*time.Second, 10*time.Millisecond)
		select {
		case r := <-out:
			t.Fatalf("short datagram produced a record: %+v", r)
		case <-time.After(50 * time.Millisecond):
		}
		send(t, addr, v5Datagram(10_000, 1_758_288_000, 0, recA))
		require.Len(t, recv(t, out, 1), 1, "listener must survive a bad datagram")
	})

	t.Run("counts a v5 datagram whose payload is shorter than its count claims", func(t *testing.T) {
		before := testutil.ToFloat64(obs.DecodeErrors.WithLabelValues(sourceLabel))
		send(t, addr, v5Datagram(0, 0, 0, recA, recB)[:24+48])
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(obs.DecodeErrors.WithLabelValues(sourceLabel)) == before+1
		}, 2*time.Second, 10*time.Millisecond)
	})
}

func TestCancelClosesSocketAndReturnsNil(t *testing.T) {
	_, addr, _, stop := startSource(t, nil)
	started := time.Now()
	require.NoError(t, stop())
	require.Less(t, time.Since(started), time.Second)
	// The port is released: a fresh listener can bind the same address.
	pc, err := net.ListenPacket("udp", addr.String())
	require.NoError(t, err)
	require.NoError(t, pc.Close())
}

// v9 fixtures: one template flowset, one data flowset, both built by hand.

type v9Field struct{ typ, length uint16 }

var v9Template = []v9Field{
	{8, 4}, {12, 4}, {7, 2}, {11, 2}, {4, 1}, {6, 1}, {1, 4}, {2, 4}, {22, 4}, {21, 4}, {10, 4}, {14, 4}, {16, 4}, {17, 4},
}

func v9Header(count uint16, sysUptime, unixSecs uint32) []byte {
	b := binary.BigEndian.AppendUint16(nil, 9)
	b = binary.BigEndian.AppendUint16(b, count)
	b = binary.BigEndian.AppendUint32(b, sysUptime)
	b = binary.BigEndian.AppendUint32(b, unixSecs)
	b = binary.BigEndian.AppendUint32(b, 7)   // sequence
	b = binary.BigEndian.AppendUint32(b, 100) // source id
	return b
}

func v9TemplateDatagram(sysUptime, unixSecs uint32) []byte {
	b := v9Header(1, sysUptime, unixSecs)
	b = binary.BigEndian.AppendUint16(b, 0)                             // flowset id 0 = template
	b = binary.BigEndian.AppendUint16(b, uint16(4+4+4*len(v9Template))) // length
	b = binary.BigEndian.AppendUint16(b, 256)                           // template id
	b = binary.BigEndian.AppendUint16(b, uint16(len(v9Template)))
	for _, f := range v9Template {
		b = binary.BigEndian.AppendUint16(b, f.typ)
		b = binary.BigEndian.AppendUint16(b, f.length)
	}
	return b
}

func v9DataDatagram(sysUptime, unixSecs uint32, r v5Record) []byte {
	b := v9Header(1, sysUptime, unixSecs)
	b = binary.BigEndian.AppendUint16(b, 256) // flowset id = template id
	var recLen uint16
	for _, f := range v9Template {
		recLen += f.length
	}
	b = binary.BigEndian.AppendUint16(b, 4+recLen) // length: header + one record
	b = append(b, r.src.AsSlice()...)
	b = append(b, r.dst.AsSlice()...)
	b = binary.BigEndian.AppendUint16(b, r.srcPort)
	b = binary.BigEndian.AppendUint16(b, r.dstPort)
	b = append(b, r.proto, r.tcpFlags)
	b = binary.BigEndian.AppendUint32(b, r.octets)
	b = binary.BigEndian.AppendUint32(b, r.pkts)
	b = binary.BigEndian.AppendUint32(b, r.first)
	b = binary.BigEndian.AppendUint32(b, r.last)
	b = binary.BigEndian.AppendUint32(b, uint32(r.input))
	b = binary.BigEndian.AppendUint32(b, uint32(r.output))
	b = binary.BigEndian.AppendUint32(b, uint32(r.srcAS))
	b = binary.BigEndian.AppendUint32(b, uint32(r.dstAS))
	return b
}

func TestV9(t *testing.T) {
	_, addr, out, _ := startSource(t, nil)
	exporter := netip.MustParseAddr("127.0.0.1")

	t.Run("a data datagram before any template decodes to nothing and is counted", func(t *testing.T) {
		before := testutil.ToFloat64(obs.DecodeErrors.WithLabelValues(sourceLabel))
		send(t, addr, v9DataDatagram(5000, 1_758_288_000, recA))
		require.Eventually(t, func() bool {
			return testutil.ToFloat64(obs.DecodeErrors.WithLabelValues(sourceLabel)) == before+1
		}, 2*time.Second, 10*time.Millisecond)
	})

	t.Run("the template persists across datagrams from the same exporter", func(t *testing.T) {
		send(t, addr, v9TemplateDatagram(5000, 1_758_288_000))
		send(t, addr, v9DataDatagram(6000, 1_758_288_001, recA))
		got := recv(t, out, 1)[0]
		require.Equal(t, flow.FlowTypeNetFlow9, got.FlowType)
		require.Equal(t, exporter, got.ExporterAddr)
		require.Equal(t, recA.src, got.SrcAddr)
		require.Equal(t, recA.dst, got.DstAddr)
		require.Equal(t, recA.srcPort, got.SrcPort)
		require.Equal(t, recA.dstPort, got.DstPort)
		require.Equal(t, recA.proto, got.Protocol)
		require.Equal(t, recA.tcpFlags, got.TCPFlags)
		require.Equal(t, uint64(recA.octets), got.Bytes)
		require.Equal(t, uint64(recA.pkts), got.Packets)
		require.Equal(t, uint32(recA.input), got.InputIface)
		require.Equal(t, uint32(recA.srcAS), got.SrcAS)
		require.Equal(t, uint32(recA.dstAS), got.DstAS)
		require.Equal(t, uint32(1), got.SamplingRate)
		require.Nil(t, got.DedupKey)
		boot := time.Unix(1_758_288_001, 0).Add(-6000 * time.Millisecond)
		require.Equal(t, boot.Add(1000*time.Millisecond).UTC(), got.FirstSwitched)
		require.Equal(t, boot.Add(2500*time.Millisecond).UTC(), got.LastSwitched)
	})
}

func TestNTPToTime(t *testing.T) {
	// IPFIX's flowStart/EndMicroseconds fields use the 64-bit NTP format:
	// seconds since 1900-01-01 in the high 32 bits, a binary fraction of a
	// second in the low 32 bits. 2208988800 is the NTP-to-Unix epoch offset.
	const ntpEpochOffset = 2208988800

	t.Run("whole second, no fraction", func(t *testing.T) {
		want := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
		v := uint64(want.Unix()+ntpEpochOffset) << 32
		require.Equal(t, want, ntpToTime(v))
	})

	t.Run("quarter-second fraction", func(t *testing.T) {
		want := time.Date(2026, 9, 19, 12, 0, 0, 250_000_000, time.UTC)
		v := uint64(want.Unix()+ntpEpochOffset)<<32 | uint64(1<<30) // 0.25 * 2^32
		require.Equal(t, want, ntpToTime(v))
	})
}

func TestDecodeRejectsUnknownVersion(t *testing.T) {
	d := newDecoder()
	_, err := d.decode([]byte{0, 1, 0, 0}, netip.MustParseAddr("10.0.0.1"), time.Now())
	require.Error(t, err)
	_, err = d.decode([]byte{5}, netip.MustParseAddr("10.0.0.1"), time.Now())
	require.ErrorIs(t, err, errShort)
}
