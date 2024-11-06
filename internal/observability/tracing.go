package observability

import (
	"context"
	"net/url"
	"time"

	"github.com/kudak/gateway/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var Propagator = propagation.TraceContext{}

func SetupTracing(ctx context.Context, c config.Tracing) (*sdktrace.TracerProvider, error) {
	return SetupTracingNamed(ctx, c, "gateway")
}
func SetupTracingNamed(ctx context.Context, c config.Tracing, name string) (*sdktrace.TracerProvider, error) {
	opts := []sdktrace.TracerProviderOption{sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(c.SampleRatio))), sdktrace.WithResource(resource.NewWithAttributes("", attribute.String("service.name", name)))}
	if c.Endpoint != "" {
		endpoint, err := url.Parse(c.Endpoint)
		if err != nil {
			return nil, err
		}
		if endpoint.Path == "" || endpoint.Path == "/" {
			endpoint.Path = "/v1/traces"
		}
		e, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint.String()), otlptracehttp.WithTimeout(2*time.Second))
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(e, sdktrace.WithMaxQueueSize(2048), sdktrace.WithExportTimeout(2*time.Second)))
	}
	p := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(p)
	return p, nil
}
func Tracer() trace.Tracer { return otel.Tracer("gateway") }
