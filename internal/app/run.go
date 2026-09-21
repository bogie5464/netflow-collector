// Package app is the only wiring layer: the one place concrete sources and
// backends are constructed and connected to the pipeline. Everything below it
// talks through internal/flow.
package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/bogie5464/netflow-collector/internal/api"
	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
	"github.com/bogie5464/netflow-collector/internal/pipeline"
	"github.com/bogie5464/netflow-collector/internal/sink/clickhouse"
	kafkasink "github.com/bogie5464/netflow-collector/internal/sink/kafka"
	"github.com/bogie5464/netflow-collector/internal/sink/mariadb"
	"github.com/bogie5464/netflow-collector/internal/sink/postgres"
	"github.com/bogie5464/netflow-collector/internal/source/kafka"
	"github.com/bogie5464/netflow-collector/internal/source/netflow"
)

// Sentinel errors main maps onto the process exit codes: ErrConfig is 2,
// ErrMigration is 3, anything else is 1.
var (
	ErrConfig    = errors.New("invalid configuration")
	ErrMigration = errors.New("storage boot failed")
)

// shutdownTimeout bounds the flush after cancellation; a hung backend must
// not keep the process alive past the orchestrator's patience.
const shutdownTimeout = 10 * time.Second

// Run loads configuration, initialises logging and telemetry before
// anything else, builds the app and runs it until SIGINT/SIGTERM.
func Run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}
	obs.SetupLogging(os.Stderr, cfg.LogLevel, obs.ServiceName)
	stopMetrics, err := obs.SetupMetrics()
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = stopMetrics(flushCtx)
	}()

	a, err := New(ctx, *cfg)
	if err != nil {
		return err
	}
	ctx, stop := signalContext(ctx)
	defer stop()
	return a.Run(ctx)
}

// signalContext cancels ctx on SIGINT or SIGTERM.
func signalContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
}

// CheckNames rejects a source or sink this binary does not implement, and a
// configuration that would serve /v1 without a key. It is pure so
// -validate-config can run it without touching a database.
func CheckNames(cfg config.Config) error {
	if err := api.CheckConfig(cfg); err != nil {
		return fmt.Errorf("%w: %w", ErrConfig, err)
	}
	for _, s := range cfg.Sinks {
		switch s {
		case config.SinkPostgres, config.SinkMariaDB, config.SinkClickHouse, config.SinkKafka:
		default:
			return fmt.Errorf("%w: NFC_SINKS: backend %q is not implemented by this binary", ErrConfig, s)
		}
	}
	for _, s := range cfg.Sources {
		switch s {
		case config.SourceNetFlow, config.SourceKafka:
		default:
			return fmt.Errorf("%w: NFC_SOURCES: source %q is not implemented by this binary", ErrConfig, s)
		}
	}
	return nil
}

// App is one configured collector. Build it with New, drive it with Run.
type App struct {
	cfg      config.Config
	log      *slog.Logger
	sinks    map[string]flow.Sink    // every NFC_SINKS entry
	backends map[string]flow.Backend // the sinks that are also storage: migrated, queried, listed
	sources  map[string]flow.Source
	pipe     *pipeline.Pipeline

	mu        sync.Mutex
	listening netip.AddrPort
	httpAddr  netip.AddrPort
	ready     chan struct{}
	readyOnce sync.Once
}

// New constructs every backend and source named in cfg. A name the binary
// does not implement is ErrConfig; a backend that cannot be reached is
// ErrMigration, because it is the boot-time storage failure class.
func New(ctx context.Context, cfg config.Config) (*App, error) {
	if err := CheckNames(cfg); err != nil {
		return nil, err
	}
	obs.Prime(cfg.Sources, cfg.Sinks)
	a := &App{
		cfg:      cfg,
		log:      slog.Default().With("component", "app"),
		sinks:    map[string]flow.Sink{},
		backends: map[string]flow.Backend{},
		sources:  map[string]flow.Source{},
		ready:    make(chan struct{}),
	}
	for _, name := range cfg.Sinks {
		s, err := newSink(ctx, name, cfg)
		if err != nil {
			a.closeSinks()
			return nil, fmt.Errorf("%w: %s: %w", ErrMigration, name, err)
		}
		a.sinks[name] = s
		if b, ok := s.(flow.Backend); ok {
			a.backends[name] = b
		}
	}
	for _, name := range cfg.Sources {
		src, err := newSource(name, cfg)
		if err != nil {
			a.closeSinks()
			return nil, fmt.Errorf("%w: %s: %w", ErrConfig, name, err)
		}
		a.sources[name] = src
	}
	sinks := make([]pipeline.Sink, 0, len(a.sinks))
	for name, s := range a.sinks {
		sinks = append(sinks, pipeline.Sink{Name: name, Sink: s})
	}
	pipe, err := pipeline.New(pipeline.Options{
		BufferSize:    cfg.PipelineBuffer,
		BatchSize:     cfg.BatchSize,
		BatchInterval: cfg.BatchInterval,
		Workers:       cfg.Workers,
	}, sinks...)
	if err != nil {
		a.closeSinks()
		return nil, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	a.pipe = pipe
	return a, nil
}

// newSink is the sink switch: the one place a sink package is named. A
// storage engine returns a flow.Backend; a transport returns a plain
// flow.Sink, and New sorts them by type assertion.
func newSink(ctx context.Context, name string, cfg config.Config) (flow.Sink, error) {
	switch name {
	case config.SinkPostgres:
		return postgres.New(ctx, cfg.PostgresDSN, cfg.RetentionDays)
	case config.SinkMariaDB:
		return mariadb.New(ctx, cfg.MariaDBDSN, cfg.RetentionDays)
	case config.SinkClickHouse:
		return clickhouse.New(ctx, cfg.ClickHouseDSN, cfg.RetentionDays)
	case config.SinkKafka:
		return kafkasink.New(ctx, cfg.KafkaBrokers, cfg.KafkaTopic)
	default:
		return nil, fmt.Errorf("%w: unknown sink %q", ErrConfig, name)
	}
}

// newSource is the source switch: the one place a source package is named.
func newSource(name string, cfg config.Config) (flow.Source, error) {
	switch name {
	case config.SourceNetFlow:
		return netflow.New(cfg)
	case config.SourceKafka:
		return kafka.New(cfg)
	default:
		return nil, fmt.Errorf("%w: unknown source %q", ErrConfig, name)
	}
}

// Backend returns a constructed storage backend by its NFC_SINKS name, for
// tests. A sink that is not storage (kafka) is not found here.
func (a *App) Backend(name string) flow.Backend { return a.backends[name] }

// Ready is closed once every listener is bound.
func (a *App) Ready() <-chan struct{} { return a.ready }

// HTTPAddr is the bound API address, valid after Ready; zero when the API
// is disabled (empty NFC_HTTP_ADDR).
func (a *App) HTTPAddr() netip.AddrPort {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.httpAddr
}

// OpenAPI is the generated API document, for -dump-openapi.
func OpenAPI() []byte { return api.OpenAPI() }

// NetFlowAddr is the bound UDP address, valid after Ready.
func (a *App) NetFlowAddr() netip.AddrPort {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.listening
}

// Run migrates every backend before anything listens, then runs the pipeline
// and the sources until ctx is cancelled. Shutdown order: sources stop, the
// pipeline drains and flushes, backends close.
func (a *App) Run(ctx context.Context) error {
	for name, b := range a.backends {
		if err := b.Migrate(ctx); err != nil {
			a.closeSinks()
			return fmt.Errorf("%w: %s: %w", ErrMigration, name, err)
		}
		a.log.Info("migrations applied", "backend", name)
	}

	pipeCtx, stopPipe := context.WithCancel(context.WithoutCancel(ctx))
	pipeDone := make(chan error, 1)
	go func() { pipeDone <- a.pipe.Run(pipeCtx) }()

	httpSrv, httpDone, err := a.serveHTTP()
	if err != nil {
		stopPipe()
		<-pipeDone
		a.closeSinks()
		return err
	}

	srcCtx, stopSources := context.WithCancel(ctx)
	var (
		wg     sync.WaitGroup
		srcErr = make(chan error, len(a.sources))
	)
	for name, src := range a.sources {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.pipe.RunSource(srcCtx, name, src); err != nil {
				srcErr <- err
			}
		}()
		a.awaitBound(srcCtx, name, src)
	}
	a.readyOnce.Do(func() { close(a.ready) })

	// Wait for cancellation or a source that died on its own.
	var runErr error
	select {
	case <-ctx.Done():
	case err := <-srcErr:
		runErr = err
	}
	stopSources()
	wg.Wait()

	if httpSrv != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			a.log.Error("http shutdown failed", "err", err)
		}
		cancel()
		<-httpDone
	}

	stopPipe()
	select {
	case <-pipeDone:
	case <-time.After(shutdownTimeout):
		a.log.Error("pipeline did not drain in time", "timeout", shutdownTimeout)
	}
	a.closeSinks()
	a.log.Info("collector stopped")
	return runErr
}

// serveHTTP binds NFC_HTTP_ADDR and serves the API. The querier is the first
// configured sink that is storage — nil on an edge tier with no database,
// where /v1 answers 503 and /healthz, /readyz and /metrics still work. Every
// sink is handed over for readiness checks.
func (a *App) serveHTTP() (*http.Server, <-chan struct{}, error) {
	if a.cfg.HTTPAddr == "" {
		return nil, nil, nil
	}
	var querier flow.Querier
	for _, name := range a.cfg.Sinks {
		if b, ok := a.backends[name]; ok {
			querier = b
			break
		}
	}
	ln, err := net.Listen("tcp", a.cfg.HTTPAddr)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: NFC_HTTP_ADDR: listen %s: %w", ErrConfig, a.cfg.HTTPAddr, err)
	}
	if ta, ok := ln.Addr().(*net.TCPAddr); ok {
		a.mu.Lock()
		a.httpAddr = ta.AddrPort()
		a.mu.Unlock()
	}
	srv := &http.Server{
		Handler:           api.New(querier, a.sinks, a.cfg),
		ReadHeaderTimeout: 10 * time.Second,
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			a.log.Error("http server stopped", "err", err)
		}
	}()
	a.log.Info("api listening", "addr", ln.Addr().String())
	return srv, done, nil
}

// awaitBound records the listen address of a source that reports one, so
// tests and logs can find an ephemeral port. Sources without one are fine.
func (a *App) awaitBound(ctx context.Context, name string, src flow.Source) {
	r, ok := src.(interface {
		Ready() <-chan struct{}
		LocalAddr() netip.AddrPort
	})
	if !ok {
		return
	}
	select {
	case <-r.Ready():
		a.mu.Lock()
		a.listening = r.LocalAddr()
		a.mu.Unlock()
		a.log.Info("source listening", "source", name, "addr", r.LocalAddr())
	case <-ctx.Done():
	}
}

func (a *App) closeSinks() {
	for name, s := range a.sinks {
		if c, ok := s.(io.Closer); ok {
			if err := c.Close(); err != nil {
				a.log.Error("close sink failed", "sink", name, "err", err)
			}
		}
	}
	// Prevent a double close from a construction failure followed by Run.
	a.sinks = map[string]flow.Sink{}
	a.backends = map[string]flow.Backend{}
}
