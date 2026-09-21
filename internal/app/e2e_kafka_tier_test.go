package app

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
)

// The compose redpanda service, as in the kafka source and sink tests.
var kafkaBrokers = []string{"127.0.0.1:19092"}

// startApp runs a until it is ready and returns a stop function that waits
// for a clean exit.
func startApp(t *testing.T, cfg config.Config) (*App, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	a, err := New(ctx, cfg)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	select {
	case <-a.Ready():
	case err := <-done:
		t.Fatalf("app stopped before ready: %v", err)
	case <-time.After(60 * time.Second):
		t.Fatal("app did not become ready")
	}
	return a, func() {
		cancel()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(15 * time.Second):
			t.Fatal("app did not stop")
		}
	}
}

// TestTieredDeployment is the reason the kafka sink exists: the same binary
// as an edge tier (NFC_SOURCES=netflow NFC_SINKS=kafka, no database) and a
// central tier (NFC_SOURCES=kafka NFC_SINKS=postgres, no UDP). One datagram
// into the edge comes out of the central tier's database with every field
// intact and a dedup key, because it crossed a transport that can redeliver.
func TestTieredDeployment(t *testing.T) {
	topic := fmt.Sprintf("nfc-test-tier-%d", time.Now().UnixNano())

	edgeCfg := testConfig("")
	edgeCfg.Sinks = []string{config.SinkKafka}
	edgeCfg.PostgresEnabled = false
	edgeCfg.KafkaSinkEnabled, edgeCfg.KafkaEnabled = true, true
	edgeCfg.KafkaBrokers, edgeCfg.KafkaTopic = kafkaBrokers, topic
	edgeCfg.HTTPAddr = "127.0.0.1:0"
	edgeCfg.APIKeys = []string{"edge-key"}
	// New pings the broker, which may still be settling after compose --wait.
	var edge *App
	var stopEdge func()
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := New(ctx, edgeCfg)
		return err == nil
	}, 30*time.Second, time.Second, "redpanda at %v not reachable", kafkaBrokers)
	edge, stopEdge = startApp(t, edgeCfg)
	defer stopEdge()

	centralCfg := testConfig(sinktest.StartTimescale(t))
	centralCfg.Sources = []string{config.SourceKafka}
	centralCfg.NetFlowEnabled, centralCfg.NetFlowAddr = false, ""
	centralCfg.KafkaSourceEnabled, centralCfg.KafkaEnabled = true, true
	centralCfg.KafkaBrokers, centralCfg.KafkaTopic, centralCfg.KafkaGroup = kafkaBrokers, topic, topic+"-group"
	central, stopCentral := startApp(t, centralCfg)
	defer stopCentral()

	t.Run("the edge tier serves ops endpoints but has nothing to query", func(t *testing.T) {
		base := "http://" + edge.HTTPAddr().String()
		resp, err := http.Get(base + "/readyz")
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusOK, resp.StatusCode, "the kafka sink answers readiness through Ping")

		req, _ := http.NewRequest(http.MethodGet, base+"/v1/flows?start=2026-01-01T00:00:00Z&end=2027-01-01T00:00:00Z", nil)
		req.Header.Set("Authorization", "Bearer edge-key")
		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	})

	src, dst := netip.MustParseAddr("203.0.113.10"), netip.MustParseAddr("198.51.100.200")
	conn, err := net.Dial("udp", edge.NetFlowAddr().String())
	require.NoError(t, err)
	_, err = conn.Write(v5Datagram(src, dst, 51514, 443, 6, 12, 9000))
	require.NoError(t, err)
	require.NoError(t, conn.Close())

	querier := central.Backend(config.SinkPostgres)
	require.NotNil(t, querier)
	q := flow.FlowQuery{Start: time.Now().Add(-time.Minute), End: time.Now().Add(time.Minute), Limit: 10}
	var got flow.FlowRecord
	require.Eventually(t, func() bool {
		page, err := querier.Query(context.Background(), q)
		if err != nil || len(page.Records) == 0 {
			return false
		}
		got = page.Records[0]
		return true
	}, 30*time.Second, 100*time.Millisecond, "the record must cross both tiers")

	require.Equal(t, flow.FlowTypeNetFlow5, got.FlowType)
	require.Equal(t, src, got.SrcAddr)
	require.Equal(t, dst, got.DstAddr)
	require.Equal(t, uint16(51514), got.SrcPort)
	require.Equal(t, uint16(443), got.DstPort)
	require.Equal(t, uint8(6), got.Protocol)
	require.Equal(t, uint64(12), got.Packets)
	require.Equal(t, uint64(9000), got.Bytes)
	require.Equal(t, netip.MustParseAddr("127.0.0.1"), got.ExporterAddr, "the exporter is the edge's peer, not the central tier's")
	require.WithinDuration(t, time.Now(), got.ReceivedAt, time.Minute)
	require.Len(t, got.DedupKey, 16, "a record that crossed kafka carries the dedup key the UDP path does not")
}

// TestTieredDeploymentSharesTemplates is the Kubernetes case: two edge
// instances behind one address, no affinity. A v9 template reaches one
// instance and the data reaches the other, and the record still comes out
// of the central tier — because the edge instances share templates through
// NFC_KAFKA_TEMPLATE_TOPIC.
func TestTieredDeploymentSharesTemplates(t *testing.T) {
	stamp := time.Now().UnixNano()
	topic := fmt.Sprintf("nfc-test-tier-%d", stamp)
	templates := fmt.Sprintf("nfc-test-templates-%d", stamp)

	edgeCfg := testConfig("")
	edgeCfg.Sinks = []string{config.SinkKafka}
	edgeCfg.PostgresEnabled = false
	edgeCfg.KafkaSinkEnabled, edgeCfg.KafkaEnabled = true, true
	edgeCfg.KafkaBrokers, edgeCfg.KafkaTopic, edgeCfg.KafkaTemplateTopic = kafkaBrokers, topic, templates
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := New(ctx, edgeCfg)
		return err == nil
	}, 30*time.Second, time.Second, "redpanda at %v not reachable", kafkaBrokers)
	edgeA, stopA := startApp(t, edgeCfg)
	defer stopA()
	edgeB, stopB := startApp(t, edgeCfg)
	defer stopB()

	centralCfg := testConfig(sinktest.StartTimescale(t))
	centralCfg.Sources = []string{config.SourceKafka}
	centralCfg.NetFlowEnabled, centralCfg.NetFlowAddr = false, ""
	centralCfg.KafkaSourceEnabled, centralCfg.KafkaEnabled = true, true
	centralCfg.KafkaBrokers, centralCfg.KafkaTopic, centralCfg.KafkaGroup = kafkaBrokers, topic, topic+"-group"
	central, stopCentral := startApp(t, centralCfg)
	defer stopCentral()

	// The template goes to A only.
	sendUDP(t, edgeA.NetFlowAddr(), v9TemplateDatagram(5000, 1_758_288_000))

	// The data goes to B only, repeatedly, until B has the template: the
	// store is asynchronous and B decodes nothing until it arrives.
	src, dst := netip.MustParseAddr("203.0.113.77"), netip.MustParseAddr("198.51.100.201")
	querier := central.Backend(config.SinkPostgres)
	q := flow.FlowQuery{Start: time.Now().Add(-time.Minute), End: time.Now().Add(time.Minute), Limit: 10}
	var got flow.FlowRecord
	require.Eventually(t, func() bool {
		sendUDP(t, edgeB.NetFlowAddr(), v9DataDatagram(6000, 1_758_288_001, src, dst, 51600, 443))
		page, err := querier.Query(context.Background(), q)
		if err != nil || len(page.Records) == 0 {
			return false
		}
		got = page.Records[0]
		return true
	}, 45*time.Second, 250*time.Millisecond, "instance B must decode with the template instance A received")

	require.Equal(t, flow.FlowTypeNetFlow9, got.FlowType)
	require.Equal(t, src, got.SrcAddr)
	require.Equal(t, dst, got.DstAddr)
	require.Equal(t, uint16(51600), got.SrcPort)
	require.Equal(t, uint16(443), got.DstPort)
}

func sendUDP(t *testing.T, to netip.AddrPort, b []byte) {
	t.Helper()
	conn, err := net.Dial("udp", to.String())
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write(b)
	require.NoError(t, err)
}

// v9 fixtures, built from the wire layout: one template flowset carrying
// template 256, and one data flowset using it.
var v9Fields = [][2]uint16{{8, 4}, {12, 4}, {7, 2}, {11, 2}, {4, 1}, {6, 1}, {1, 4}, {2, 4}, {22, 4}, {21, 4}}

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
	b = binary.BigEndian.AppendUint16(b, 0) // flowset id 0 = template
	b = binary.BigEndian.AppendUint16(b, uint16(4+4+4*len(v9Fields)))
	b = binary.BigEndian.AppendUint16(b, 256)
	b = binary.BigEndian.AppendUint16(b, uint16(len(v9Fields)))
	for _, f := range v9Fields {
		b = binary.BigEndian.AppendUint16(b, f[0])
		b = binary.BigEndian.AppendUint16(b, f[1])
	}
	return b
}

func v9DataDatagram(sysUptime, unixSecs uint32, src, dst netip.Addr, srcPort, dstPort uint16) []byte {
	b := v9Header(1, sysUptime, unixSecs)
	b = binary.BigEndian.AppendUint16(b, 256)
	var recLen uint16
	for _, f := range v9Fields {
		recLen += f[1]
	}
	b = binary.BigEndian.AppendUint16(b, 4+recLen)
	b = append(b, src.AsSlice()...)
	b = append(b, dst.AsSlice()...)
	b = binary.BigEndian.AppendUint16(b, srcPort)
	b = binary.BigEndian.AppendUint16(b, dstPort)
	b = append(b, 6, 24)                       // proto, tcp flags
	b = binary.BigEndian.AppendUint32(b, 9000) // octets
	b = binary.BigEndian.AppendUint32(b, 12)   // packets
	b = binary.BigEndian.AppendUint32(b, 1000) // first
	b = binary.BigEndian.AppendUint32(b, 2500) // last
	return b
}

// testConfigKafka is the kafka source's configuration against the compose
// broker, with a group unique to the topic.
func testConfigKafka(topic string) config.Config {
	cfg := testConfig("")
	cfg.Sources = []string{config.SourceKafka}
	cfg.NetFlowEnabled, cfg.NetFlowAddr = false, ""
	cfg.KafkaSourceEnabled, cfg.KafkaEnabled = true, true
	cfg.KafkaBrokers, cfg.KafkaTopic, cfg.KafkaGroup = kafkaBrokers, topic, topic+"-group"
	return cfg
}
