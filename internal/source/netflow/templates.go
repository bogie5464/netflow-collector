package netflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/netip"
	"strconv"
	"strings"

	nf "github.com/netsampler/goflow2/v2/decoders/netflow"
)

// TemplateStore shares v9/IPFIX templates between collector instances. A
// template arrives once, from one exporter, at one instance; every later
// datagram from that exporter is undecodable without it. With a store, the
// instance that receives a template publishes it and every other instance
// applies it, so exporters may be spread across instances by any load
// balancer — no affinity, no decode gap when a pod is replaced.
//
// Keys are unique per (exporter, version, observation domain, template id)
// and the latest value wins, which is what a compacted Kafka topic gives.
// The interface is bytes in, bytes out, so the source owns the encoding and
// a store owns only the transport.
type TemplateStore interface {
	// Put records one template. It must not block the caller for long; the
	// source calls it off the read loop but templates are still time-critical.
	Put(ctx context.Context, key string, value []byte) error
	// Watch replays every stored template, calls synced once the replay has
	// reached the store's end as of the call, then streams new templates
	// until ctx is cancelled. It returns nil on cancellation.
	Watch(ctx context.Context, apply func(key string, value []byte), synced func()) error
}

// publishQueue bounds templates waiting to be published. Templates are rare
// (one per exporter per refresh interval) and every exporter re-sends them,
// so a full queue drops the newest with a log line rather than blocking.
const publishQueue = 1024

// templateKey is the store key: exporter/version/domain/id.
func templateKey(exporter netip.Addr, version uint16, domain uint32, id uint16) string {
	return fmt.Sprintf("%s/%d/%d/%d", exporter, version, domain, id)
}

func parseTemplateKey(key string) (exporter netip.Addr, version uint16, domain uint32, id uint16, err error) {
	parts := strings.Split(key, "/")
	if len(parts) != 4 {
		return exporter, 0, 0, 0, fmt.Errorf("template key %q: want exporter/version/domain/id", key)
	}
	if exporter, err = netip.ParseAddr(parts[0]); err != nil {
		return exporter, 0, 0, 0, fmt.Errorf("template key %q: %w", key, err)
	}
	v, err := strconv.ParseUint(parts[1], 10, 16)
	if err != nil {
		return exporter, 0, 0, 0, fmt.Errorf("template key %q: version: %w", key, err)
	}
	d, err := strconv.ParseUint(parts[2], 10, 32)
	if err != nil {
		return exporter, 0, 0, 0, fmt.Errorf("template key %q: domain: %w", key, err)
	}
	i, err := strconv.ParseUint(parts[3], 10, 16)
	if err != nil {
		return exporter, 0, 0, 0, fmt.Errorf("template key %q: id: %w", key, err)
	}
	return exporter, uint16(v), uint32(d), uint16(i), nil
}

// templateEnvelope is the stored value. goflow2 hands the decoder one of
// three record types; kind says which so the bytes can be decoded back into
// the right one.
type templateEnvelope struct {
	Kind   string          `json:"kind"`
	Record json.RawMessage `json:"record"`
}

const (
	kindTemplate     = "template"
	kindNFv9Options  = "nfv9-options"
	kindIPFIXOptions = "ipfix-options"
)

func encodeTemplate(tpl any) ([]byte, error) {
	var kind string
	switch tpl.(type) {
	case nf.TemplateRecord:
		kind = kindTemplate
	case nf.NFv9OptionsTemplateRecord:
		kind = kindNFv9Options
	case nf.IPFIXOptionsTemplateRecord:
		kind = kindIPFIXOptions
	default:
		return nil, fmt.Errorf("unknown template type %T", tpl)
	}
	rec, err := json.Marshal(tpl)
	if err != nil {
		return nil, err
	}
	return json.Marshal(templateEnvelope{Kind: kind, Record: rec})
}

func decodeTemplate(b []byte) (any, error) {
	var env templateEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, err
	}
	switch env.Kind {
	case kindTemplate:
		var r nf.TemplateRecord
		return r, json.Unmarshal(env.Record, &r)
	case kindNFv9Options:
		var r nf.NFv9OptionsTemplateRecord
		return r, json.Unmarshal(env.Record, &r)
	case kindIPFIXOptions:
		var r nf.IPFIXOptionsTemplateRecord
		return r, json.Unmarshal(env.Record, &r)
	default:
		return nil, fmt.Errorf("unknown template kind %q", env.Kind)
	}
}

// sharing wraps the decoder's per-exporter template systems so every
// template learned from the wire is published, and every template the
// store delivers is applied — directly to the inner system, so nothing an
// instance receives from the store is published again.
type sharing struct {
	store TemplateStore
	log   *slog.Logger
	queue chan entry
}

type entry struct {
	key   string
	value []byte
}

func newSharing(store TemplateStore, log *slog.Logger) *sharing {
	return &sharing{store: store, log: log, queue: make(chan entry, publishQueue)}
}

// system returns a template system for exporter that publishes on
// AddTemplate.
func (s *sharing) system(exporter netip.Addr) nf.NetFlowTemplateSystem {
	return &publishingSystem{inner: nf.CreateTemplateSystem(), exporter: exporter, sharing: s}
}

// enqueue hands a template to the publisher without blocking the read loop.
func (s *sharing) enqueue(key string, tpl any) {
	value, err := encodeTemplate(tpl)
	if err != nil {
		s.log.Warn("template not shared", "key", key, "err", err)
		return
	}
	select {
	case s.queue <- entry{key: key, value: value}:
	default:
		s.log.Warn("template publish queue full, template not shared", "key", key)
	}
}

// publish drains the queue into the store until ctx is cancelled.
func (s *sharing) publish(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-s.queue:
			if err := s.store.Put(ctx, e.key, e.value); err != nil && ctx.Err() == nil {
				s.log.Warn("template publish failed", "key", e.key, "err", err)
			}
		}
	}
}

// apply installs one stored template into the decoder, bypassing the
// publishing hook.
func (s *sharing) apply(d *decoder, key string, value []byte) {
	exporter, version, domain, id, err := parseTemplateKey(key)
	if err != nil {
		s.log.Warn("ignoring stored template", "err", err)
		return
	}
	tpl, err := decodeTemplate(value)
	if err != nil {
		s.log.Warn("ignoring stored template", "key", key, "err", err)
		return
	}
	ps, ok := d.templatesFor(exporter).(*publishingSystem)
	if !ok {
		return
	}
	if err := ps.inner.AddTemplate(version, domain, id, tpl); err != nil {
		s.log.Warn("stored template rejected", "key", key, "err", err)
	}
}

type publishingSystem struct {
	inner    nf.NetFlowTemplateSystem
	exporter netip.Addr
	sharing  *sharing
}

func (p *publishingSystem) AddTemplate(version uint16, obsDomainID uint32, templateID uint16, template any) error {
	if err := p.inner.AddTemplate(version, obsDomainID, templateID, template); err != nil {
		return err
	}
	p.sharing.enqueue(templateKey(p.exporter, version, obsDomainID, templateID), template)
	return nil
}

func (p *publishingSystem) GetTemplate(version uint16, obsDomainID uint32, templateID uint16) (any, error) {
	return p.inner.GetTemplate(version, obsDomainID, templateID)
}

func (p *publishingSystem) RemoveTemplate(version uint16, obsDomainID uint32, templateID uint16) (any, error) {
	return p.inner.RemoveTemplate(version, obsDomainID, templateID)
}

func (p *publishingSystem) GetTemplates() nf.FlowBaseTemplateSet {
	return p.inner.GetTemplates()
}
