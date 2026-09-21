package templatestore_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bogie5464/netflow-collector/internal/templatestore"
)

// The compose redpanda service, as in every other Kafka test.
var brokers = []string{"127.0.0.1:19092"}

func newStore(t *testing.T, topic string) *templatestore.Kafka {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s, err := templatestore.NewKafka(ctx, brokers, topic)
		cancel()
		if err == nil {
			t.Cleanup(func() { require.NoError(t, s.Close()) })
			return s
		}
		require.Less(t, time.Now(), deadline, "redpanda at %v not reachable within 30s: %v", brokers, err)
		time.Sleep(time.Second)
	}
}

type recorder struct {
	mu      sync.Mutex
	got     map[string]string
	applied int
	synced  chan struct{}
}

func newRecorder() *recorder { return &recorder{got: map[string]string{}, synced: make(chan struct{})} }

func (r *recorder) apply(key string, value []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got[key] = string(value)
	r.applied++
}

func (r *recorder) markSynced() { close(r.synced) }

func (r *recorder) snapshot() map[string]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]string, len(r.got))
	for k, v := range r.got {
		out[k] = v
	}
	return out
}

func TestKafkaStoreCreatesACompactedTopic(t *testing.T) {
	topic := fmt.Sprintf("nfc-test-templates-%d", time.Now().UnixNano())
	newStore(t, topic)
	newStore(t, topic) // a second instance finds it and does not fail

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer cl.Close()
	cfgs, err := kadm.NewClient(cl).DescribeTopicConfigs(context.Background(), topic)
	require.NoError(t, err)
	require.Len(t, cfgs, 1)
	var policy string
	for _, c := range cfgs[0].Configs {
		if c.Key == "cleanup.policy" && c.Value != nil {
			policy = *c.Value
		}
	}
	require.Equal(t, "compact", policy)
}

func TestKafkaStoreReplaysThenStreams(t *testing.T) {
	topic := fmt.Sprintf("nfc-test-templates-%d", time.Now().UnixNano())
	s := newStore(t, topic)
	ctx := context.Background()
	require.NoError(t, s.Put(ctx, "10.0.0.1/9/0/256", []byte("v1")))
	require.NoError(t, s.Put(ctx, "10.0.0.1/9/0/257", []byte("x")))
	require.NoError(t, s.Put(ctx, "10.0.0.1/9/0/256", []byte("v2"))) // same key, newer value

	rec := newRecorder()
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- s.Watch(wctx, rec.apply, rec.markSynced) }()

	select {
	case <-rec.synced:
	case <-time.After(30 * time.Second):
		t.Fatal("watch never reported synced")
	}
	require.Equal(t, map[string]string{"10.0.0.1/9/0/256": "v2", "10.0.0.1/9/0/257": "x"}, rec.snapshot(),
		"replay delivers everything stored, latest value per key winning")

	// A template published after the watch started arrives live.
	require.NoError(t, s.Put(ctx, "10.0.0.2/10/3/300", []byte("live")))
	require.Eventually(t, func() bool { return rec.snapshot()["10.0.0.2/10/3/300"] == "live" },
		10*time.Second, 50*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "cancellation is a clean exit")
	case <-time.After(10 * time.Second):
		t.Fatal("watch did not stop")
	}
}

func TestKafkaStoreSyncsImmediatelyOnAnEmptyTopic(t *testing.T) {
	topic := fmt.Sprintf("nfc-test-templates-%d", time.Now().UnixNano())
	s := newStore(t, topic)
	rec := newRecorder()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.Watch(ctx, rec.apply, rec.markSynced) }()
	select {
	case <-rec.synced:
	case <-time.After(30 * time.Second):
		t.Fatal("an empty topic must report synced without waiting for a record")
	}
	require.NoError(t, s.Ping(ctx))
}

func TestKafkaStoreRejectsMissingConfig(t *testing.T) {
	_, err := templatestore.NewKafka(context.Background(), nil, "t")
	require.Error(t, err)
	_, err = templatestore.NewKafka(context.Background(), brokers, "")
	require.Error(t, err)
}
