package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/kudak/gateway/internal/config"
	"go.opentelemetry.io/otel"
)

func TestOTLPBaseEndpointIncludesTracePath(t *testing.T) {
	old := otel.GetTracerProvider()
	defer otel.SetTracerProvider(old)
	var received atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/traces" || r.Method != "POST" {
			t.Errorf("unexpected exporter request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		received.Add(1)
		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(200)
	}))
	defer server.Close()
	p, e := SetupTracing(context.Background(), config.Tracing{Endpoint: server.URL, SampleRatio: 1})
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = p.Shutdown(context.Background()) }()
	_, span := Tracer().Start(context.Background(), "export test")
	span.End()
	if e = p.ForceFlush(context.Background()); e != nil {
		t.Fatal(e)
	}
	if received.Load() != 1 {
		t.Fatal("trace not exported")
	}
}
