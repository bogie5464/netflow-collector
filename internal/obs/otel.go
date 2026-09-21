package obs

import (
	"context"
	"errors"
	"fmt"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// ServiceName labels every log line and telemetry resource.
const ServiceName = "netflow-collector"

// Shutdown flushes and stops a provider set up by this package.
type Shutdown func(ctx context.Context) error

// SetupMetrics builds the OpenTelemetry meter provider whose reader is the
// Prometheus exporter registered on client_golang's default registry — the
// same registry the six instruments in metrics.go live on, so one /metrics
// scrape serves both. It touches no network.
func SetupMetrics() (Shutdown, error) {
	exporter, err := otelprom.New(otelprom.WithRegisterer(prometheus.DefaultRegisterer))
	if err != nil {
		// A second call in the same process re-registers the same collector;
		// that is not a failure of this call.
		var are prometheus.AlreadyRegisteredError
		if !errors.As(err, &are) {
			return nil, fmt.Errorf("obs: prometheus exporter: %w", err)
		}
	}
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter), sdkmetric.WithResource(res()))
	otel.SetMeterProvider(mp)
	return mp.Shutdown, nil
}

func res() *resource.Resource {
	return resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(ServiceName))
}

// Prime creates the zero-valued series for every configured label so each
// instrument is present on /metrics from the first scrape, before any
// record has flowed. A labelled counter with no series is invisible to a
// scraper, and an absent metric looks like a broken exporter.
func Prime(sources, sinks []string) {
	for _, s := range sources {
		RecordsIngested.WithLabelValues(s)
		SourceRewinds.WithLabelValues(s)
	}
	for _, r := range []string{DropBufferFull, DropShutdown} {
		RecordsDropped.WithLabelValues(r)
	}
	for _, s := range sinks {
		BatchWriteDuration.WithLabelValues(s)
		BatchWriteErrors.WithLabelValues(s)
	}
}
