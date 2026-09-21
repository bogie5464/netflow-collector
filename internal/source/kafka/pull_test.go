package kafka_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
	"github.com/bogie5464/netflow-collector/internal/source/kafka"
)

// fakeIntake stands in for the pipeline: it numbers offers, records them,
// and answers Written however the test scripts it.
type fakeIntake struct {
	mu      sync.Mutex
	offered []flow.FlowRecord
	next    uint64
	written func(seq uint64) error // nil means "written immediately"
	release chan struct{}          // when non-nil, Written blocks until closed
}

func (f *fakeIntake) Offer(_ context.Context, rec flow.FlowRecord) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	f.offered = append(f.offered, rec)
	return f.next, nil
}

func (f *fakeIntake) Written(ctx context.Context, seq uint64) error {
	if f.release != nil {
		select {
		case <-f.release:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if f.written != nil {
		return f.written(seq)
	}
	return nil
}

func (f *fakeIntake) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.offered)
}

func startPull(t *testing.T, topic, group string, in flow.Intake) func() error {
	t.Helper()
	cfg := config.Config{KafkaBrokers: brokers, KafkaTopic: topic, KafkaGroup: group}
	src, err := kafka.New(cfg)
	require.NoError(t, err)
	pull, ok := src.(flow.PullSource)
	require.True(t, ok, "the kafka source must offer StartPull")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pull.StartPull(ctx, in) }()
	stopped := false
	stop := func() error {
		if stopped {
			return nil
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			return err
		case <-time.After(15 * time.Second):
			t.Fatal("StartPull did not return after cancel")
			return nil
		}
	}
	t.Cleanup(func() { _ = stop() })
	return stop
}

// committed returns the group's committed offset for the topic's partition 0,
// or -1 when nothing is committed.
func committed(t *testing.T, cl *kgo.Client, group, topic string) int64 {
	t.Helper()
	resp, err := kadm.NewClient(cl).FetchOffsets(context.Background(), group)
	require.NoError(t, err)
	if o, ok := resp.Lookup(topic, 0); ok && o.Err == nil {
		return o.At
	}
	return -1
}

func TestPullCommitsOnlyAfterThePipelineReportsWritten(t *testing.T) {
	cl := producer(t)
	topic, group := uniqueName(t, "topic"), uniqueName(t, "group")
	at := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	for i := range 3 {
		produce(t, cl, topic, time.Time{}, withReceivedAt(sample, at.Add(time.Duration(i)*time.Second)))
	}

	in := &fakeIntake{release: make(chan struct{})}
	stop := startPull(t, topic, group, in)
	require.Eventually(t, func() bool { return in.count() == 3 }, 30*time.Second, 50*time.Millisecond, "all three are offered")

	// Offered is not written: nothing may be committed yet.
	time.Sleep(500 * time.Millisecond)
	require.Equal(t, int64(-1), committed(t, cl, group, topic), "no commit before the pipeline reports the write")

	close(in.release)
	require.Eventually(t, func() bool { return committed(t, cl, group, topic) == 3 }, 30*time.Second, 100*time.Millisecond,
		"once written, the poll's offsets are committed")
	require.NoError(t, stop())
}

func TestPullRewindsAndRedeliversAfterARejectedBatch(t *testing.T) {
	cl := producer(t)
	topic, group := uniqueName(t, "topic"), uniqueName(t, "group")
	at := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	for i := range 2 {
		produce(t, cl, topic, time.Time{}, withReceivedAt(sample, at.Add(time.Duration(i)*time.Second)))
	}
	rewindsBefore := testutil.ToFloat64(obs.SourceRewinds.WithLabelValues("kafka"))

	var failures sync.Once
	in := &fakeIntake{}
	in.written = func(uint64) error {
		var err error
		failures.Do(func() { err = flow.ErrWriteFailed })
		return err
	}
	stop := startPull(t, topic, group, in)

	// The first poll is rejected, the source rewinds to the committed
	// offset (none: the start), and every record is offered a second time.
	require.Eventually(t, func() bool { return in.count() == 4 }, 60*time.Second, 100*time.Millisecond,
		"both records delivered twice: once before the failure, once after the rewind")
	require.Eventually(t, func() bool { return committed(t, cl, group, topic) == 2 }, 30*time.Second, 100*time.Millisecond,
		"the redelivery is written and committed")
	require.Equal(t, rewindsBefore+1, testutil.ToFloat64(obs.SourceRewinds.WithLabelValues("kafka")))
	require.NoError(t, stop())
}

func TestPullCommitsPastAPoisonMessage(t *testing.T) {
	cl := producer(t)
	topic, group := uniqueName(t, "topic"), uniqueName(t, "group")
	produce(t, cl, topic, time.Time{}, []byte(`{"this is": "not a flow"}`))
	produce(t, cl, topic, time.Time{}, withReceivedAt(sample, time.Date(2026, 9, 19, 12, 0, 1, 0, time.UTC)))

	in := &fakeIntake{}
	stop := startPull(t, topic, group, in)
	require.Eventually(t, func() bool { return committed(t, cl, group, topic) == 2 }, 30*time.Second, 100*time.Millisecond,
		"the poison message is committed past, the good one delivered")
	require.Equal(t, 1, in.count())
	require.NoError(t, stop())
}
