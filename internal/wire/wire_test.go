package wire_test

import (
	"encoding/json"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/sink/sinktest"
	"github.com/bogie5464/netflow-collector/internal/wire"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 19, 12, 0, 0, 123456789, time.UTC)
	cases := map[string]flow.FlowRecord{
		"ipv4 with next hop":    sinktest.Fixture(0, netip.MustParseAddr("192.0.2.1"), at, false),
		"ipv6 without next hop": sinktest.Fixture(1, netip.MustParseAddr("2001:db8::1"), at, false),
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			body, err := wire.Encode(in)
			require.NoError(t, err)
			out, err := wire.Decode(body, time.Time{})
			require.NoError(t, err)
			want := in
			want.ReceivedAt = want.ReceivedAt.Truncate(time.Microsecond)
			want.DedupKey = nil // the wire carries no dedup key; the consuming transport decides
			require.Equal(t, want, out)
		})
	}
}

func TestEncodeCarriesReceivedAtAndNullNextHop(t *testing.T) {
	rec := sinktest.Fixture(1, netip.MustParseAddr("2001:db8::1"), time.Now().UTC(), false)
	require.False(t, rec.NextHop.IsValid())
	body, err := wire.Encode(rec)
	require.NoError(t, err)
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(body, &m))
	require.Equal(t, "null", string(m["next_hop"]))
	require.NotEqual(t, "null", string(m["received_at"]), "received_at is always sent so a consumer never falls back to its own clock")
}

func TestDecodeDefaultsAndRejects(t *testing.T) {
	kt := time.Date(2026, 9, 19, 12, 30, 0, 0, time.UTC)
	valid := `{"exporter_addr":"1.2.3.4","flow_type":"ipfix","first_switched":"2026-09-19T12:00:00Z","last_switched":"2026-09-19T12:00:01Z","src_addr":"10.0.0.1","dst_addr":"10.0.0.2"`

	rec, err := wire.Decode([]byte(valid+`,"future_field":42}`), kt)
	require.NoError(t, err, "unknown fields are ignored")
	require.Equal(t, kt, rec.ReceivedAt, "no received_at in the body: the transport's timestamp")
	require.EqualValues(t, 1, rec.SamplingRate, "sampling_rate 0 means unsampled")

	for name, body := range map[string]string{
		"not json":              `not json`,
		"no exporter":           `{"flow_type":"ipfix"}`,
		"unknown flow type":     `{"exporter_addr":"1.2.3.4","flow_type":"sflow","first_switched":"2026-09-19T12:00:00Z","last_switched":"2026-09-19T12:00:01Z","src_addr":"10.0.0.1","dst_addr":"10.0.0.2"}`,
		"no timestamp anywhere": valid + `}`,
	} {
		t.Run(name, func(t *testing.T) {
			ts := kt
			if name == "no timestamp anywhere" {
				ts = time.Time{}
			}
			_, err := wire.Decode([]byte(body), ts)
			require.Error(t, err)
		})
	}
}
