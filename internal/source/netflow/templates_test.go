package netflow

import (
	"context"
	"encoding/json"
	"net/netip"
	"sync"
	"testing"
	"time"

	nf "github.com/netsampler/goflow2/v2/decoders/netflow"
	"github.com/stretchr/testify/require"

	"github.com/bogie5464/netflow-collector/internal/flow"
)

// memStore is a TemplateStore in one process: a map plus subscribers. It is
// what a compacted topic is, minus the network.
type memStore struct {
	mu      sync.Mutex
	entries map[string][]byte
	order   []string
	watch   []chan entry
	puts    int
}

func newMemStore() *memStore { return &memStore{entries: map[string][]byte{}} }

func (m *memStore) Put(_ context.Context, key string, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.entries[key]; !ok {
		m.order = append(m.order, key)
	}
	m.entries[key] = value
	m.puts++
	for _, w := range m.watch {
		w <- entry{key, value}
	}
	return nil
}

func (m *memStore) Watch(ctx context.Context, apply func(string, []byte), synced func()) error {
	ch := make(chan entry, 64)
	m.mu.Lock()
	for _, k := range m.order {
		apply(k, m.entries[k])
	}
	m.watch = append(m.watch, ch)
	m.mu.Unlock()
	synced()
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-ch:
			apply(e.key, e.value)
		}
	}
}

func TestTemplateEnvelopeRoundTrip(t *testing.T) {
	cases := map[string]any{
		"template":      nf.TemplateRecord{TemplateId: 256, FieldCount: 2, Fields: []nf.Field{{Type: 8, Length: 4}, {Type: 12, Length: 4}}},
		"nfv9 options":  nf.NFv9OptionsTemplateRecord{TemplateId: 257, ScopeLength: 4, Scopes: []nf.Field{{Type: 1, Length: 4}}},
		"ipfix options": nf.IPFIXOptionsTemplateRecord{TemplateId: 258, FieldCount: 1, ScopeFieldCount: 1, Scopes: []nf.Field{{Type: 149, Length: 4}}},
	}
	for name, in := range cases {
		t.Run(name, func(t *testing.T) {
			b, err := encodeTemplate(in)
			require.NoError(t, err)
			out, err := decodeTemplate(b)
			require.NoError(t, err)
			require.Equal(t, in, out)
		})
	}
	_, err := encodeTemplate("not a template")
	require.Error(t, err)
	_, err = decodeTemplate([]byte(`{"kind":"mystery","record":{}}`))
	require.Error(t, err)
}

func TestTemplateKeyRoundTrip(t *testing.T) {
	exp := netip.MustParseAddr("2001:db8::1")
	key := templateKey(exp, 10, 7, 300)
	require.Equal(t, "2001:db8::1/10/7/300", key)
	e, v, d, id, err := parseTemplateKey(key)
	require.NoError(t, err)
	require.Equal(t, exp, e)
	require.EqualValues(t, 10, v)
	require.EqualValues(t, 7, d)
	require.EqualValues(t, 300, id)
	for _, bad := range []string{"", "a/b", "nope/9/0/256", "1.2.3.4/x/0/256", "1.2.3.4/9/0/70000"} {
		_, _, _, _, err := parseTemplateKey(bad)
		require.Error(t, err, bad)
	}
}

// TestTemplatesAreSharedAcrossInstances is the reason the store exists: the
// template goes to one listener, the data to another, and the second
// decodes it.
func TestTemplatesAreSharedAcrossInstances(t *testing.T) {
	store := newMemStore()
	_, addrA, _, _ := startSource(t, nil, WithTemplateStore(store))
	_, addrB, outB, _ := startSource(t, nil, WithTemplateStore(store))

	send(t, addrA, v9TemplateDatagram(5000, 1_758_288_000))
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.puts == 1
	}, 2*time.Second, 10*time.Millisecond, "instance A publishes the template it learned")

	require.Eventually(t, func() bool {
		send(t, addrB, v9DataDatagram(6000, 1_758_288_001, recA))
		select {
		case got := <-outB:
			require.Equal(t, flow.FlowTypeNetFlow9, got.FlowType)
			require.Equal(t, recA.src, got.SrcAddr)
			require.Equal(t, recA.dstPort, got.DstPort)
			return true
		case <-time.After(100 * time.Millisecond):
			return false
		}
	}, 3*time.Second, 50*time.Millisecond, "instance B decodes with the template A received")

	store.mu.Lock()
	puts := store.puts
	store.mu.Unlock()
	require.Equal(t, 1, puts, "a template applied from the store is not published again")
}

func TestNewInstanceHydratesBeforeListening(t *testing.T) {
	store := newMemStore()
	_, addrA, _, _ := startSource(t, nil, WithTemplateStore(store))
	send(t, addrA, v9TemplateDatagram(5000, 1_758_288_000))
	require.Eventually(t, func() bool {
		store.mu.Lock()
		defer store.mu.Unlock()
		return store.puts == 1
	}, 2*time.Second, 10*time.Millisecond)

	// A listener started afterwards decodes the very first data datagram it
	// receives: the replay finished before the socket was bound.
	_, addrB, outB, _ := startSource(t, nil, WithTemplateStore(store))
	send(t, addrB, v9DataDatagram(6000, 1_758_288_001, recA))
	got := recv(t, outB, 1)[0]
	require.Equal(t, recA.src, got.SrcAddr)
}

func TestStoreDeliversTheLatestTemplateForAKey(t *testing.T) {
	// The wire can only carry a template once per datagram here, so exercise
	// apply directly: a second value for the same key replaces the first.
	store := newMemStore()
	src, _, _, _ := startSource(t, nil, WithTemplateStore(store))
	exp := netip.MustParseAddr("192.0.2.9")
	key := templateKey(exp, 9, 0, 256)
	first, err := encodeTemplate(nf.TemplateRecord{TemplateId: 256, FieldCount: 1, Fields: []nf.Field{{Type: 8, Length: 4}}})
	require.NoError(t, err)
	second, err := encodeTemplate(nf.TemplateRecord{TemplateId: 256, FieldCount: 2, Fields: []nf.Field{{Type: 8, Length: 4}, {Type: 12, Length: 4}}})
	require.NoError(t, err)
	src.share.apply(src.dec, key, first)
	src.share.apply(src.dec, key, second)
	got, err := src.dec.templatesFor(exp).GetTemplate(9, 0, 256)
	require.NoError(t, err)
	require.EqualValues(t, 2, got.(nf.TemplateRecord).FieldCount)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(second, &raw))
	require.Contains(t, raw, "kind")
}
