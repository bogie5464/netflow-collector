// Package netflow is the UDP input adapter: it listens on NFC_NETFLOW_ADDR,
// decodes NetFlow v5, NetFlow v9 and IPFIX datagrams with goflow2, and emits
// flow.FlowRecord values. It implements flow.Source and nothing else — no
// batching, no retry, no writes. Backpressure belongs to the pipeline.
package netflow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/bogie5464/netflow-collector/internal/config"
	"github.com/bogie5464/netflow-collector/internal/flow"
	"github.com/bogie5464/netflow-collector/internal/obs"
)

// maxDatagram is the largest UDP payload; every read uses a buffer this size.
const maxDatagram = 65535

// sourceLabel is the value of the source label on every metric this package
// touches.
const sourceLabel = "netflow"

// Source is a NetFlow/IPFIX listener. Create it with New.
type Source struct {
	addr string
	log  *slog.Logger
	now  func() time.Time
	dec  *decoder

	mu    sync.Mutex
	local netip.AddrPort
	ready chan struct{}
}

var _ flow.Source = (*Source)(nil)

// New builds a Source listening on cfg.NetFlowAddr.
func New(cfg config.Config) (flow.Source, error) {
	if cfg.NetFlowAddr == "" {
		return nil, errors.New("netflow: NFC_NETFLOW_ADDR must be set")
	}
	return newSource(cfg.NetFlowAddr), nil
}

func newSource(addr string) *Source {
	return &Source{
		addr:  addr,
		log:   slog.Default().With("source", sourceLabel),
		now:   time.Now,
		dec:   newDecoder(),
		ready: make(chan struct{}),
	}
}

// Ready is closed once the socket is bound; LocalAddr is valid after that.
func (s *Source) Ready() <-chan struct{} { return s.ready }

// LocalAddr is the bound address, useful when addr asked for port 0.
func (s *Source) LocalAddr() netip.AddrPort {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.local
}

// Start binds the socket and reads datagrams until ctx is cancelled, then
// closes the socket and returns nil. A non-nil return means the listener
// could not be established.
func (s *Source) Start(ctx context.Context, out chan<- flow.FlowRecord) error {
	var lc net.ListenConfig
	conn, err := lc.ListenPacket(ctx, "udp", s.addr)
	if err != nil {
		return fmt.Errorf("netflow: listen %s: %w", s.addr, err)
	}
	if ua, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		s.mu.Lock()
		s.local = ua.AddrPort()
		s.mu.Unlock()
	}
	close(s.ready)
	s.log.Info("netflow listener started", "addr", conn.LocalAddr().String())

	// Closing the socket is what unblocks ReadFrom when ctx is cancelled.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()
	defer close(done)
	defer func() { _ = conn.Close() }()

	buf := make([]byte, maxDatagram)
	for {
		n, from, err := conn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			s.log.Warn("netflow read failed", "err", err)
			continue
		}
		exporter := exporterAddr(from)
		receivedAt := s.now().UTC().Truncate(time.Microsecond)
		records, err := s.dec.decode(buf[:n], exporter, receivedAt)
		if err != nil {
			obs.DecodeErrors.WithLabelValues(sourceLabel).Inc()
			s.log.Debug("netflow decode failed", "exporter", exporter, "len", n, "err", err)
			continue
		}
		for _, rec := range records {
			select {
			case out <- rec:
			case <-ctx.Done():
				return nil
			}
		}
	}
}

// exporterAddr extracts the sender's IP, unmapped so an IPv4 exporter seen on
// a dual-stack socket keys the same as on an IPv4-only one.
func exporterAddr(a net.Addr) netip.Addr {
	if ua, ok := a.(*net.UDPAddr); ok {
		if ip, ok := netip.AddrFromSlice(ua.IP); ok {
			return ip.Unmap()
		}
	}
	return netip.Addr{}
}
