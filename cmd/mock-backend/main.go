// mock-backend is deliberately a failure-injection demo, not a production service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/kudak/gateway/internal/config"
	"github.com/kudak/gateway/internal/observability"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	addr := flag.String("address", ":8081", "listen address")
	id := flag.String("id", "backend", "instance ID")
	delay := flag.Duration("delay", 0, "response delay")
	prob := flag.Float64("failure-probability", 0.25, "flaky failure probability")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	provider, e := observability.SetupTracingNamed(ctx, config.Tracing{Endpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"), SampleRatio: 1}, *id)
	if e != nil {
		logger.Error("tracing setup", "error", e)
		os.Exit(1)
	}
	var unhealthy, failing atomic.Bool
	var latency atomic.Int64
	latency.Store(int64(*delay))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if unhealthy.Load() {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("POST /control", func(w http.ResponseWriter, r *http.Request) {
		if v := r.URL.Query().Get("unhealthy"); v != "" {
			unhealthy.Store(v == "true")
		}
		if v := r.URL.Query().Get("fail"); v != "" {
			failing.Store(v == "true")
		}
		if v := r.URL.Query().Get("delay"); v != "" {
			d, e := time.ParseDuration(v)
			if e != nil || d < 0 || d > time.Minute {
				http.Error(w, "invalid delay", 400)
				return
			}
			latency.Store(int64(d))
		}
		w.WriteHeader(204)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		pctx := observability.Propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		_, span := observability.Tracer().Start(pctx, "backend request", trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attribute.String("backend.id", *id)))
		defer span.End()
		d := time.Duration(latency.Load())
		if r.URL.Path == "/slow" {
			d = 5 * time.Second
		}
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
		status := 200
		if failing.Load() || r.URL.Path == "/fail" || (r.URL.Path == "/flaky" && rand.Float64() < *prob) {
			status = 500
			if r.URL.Path == "/flaky" {
				status = 503
			}
		}
		if v := r.URL.Query().Get("status"); v != "" {
			n, e := strconv.Atoi(v)
			if e == nil && n >= 200 && n <= 599 {
				status = n
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Backend-Id", *id)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"backend": *id, "path": r.URL.EscapedPath(), "requestId": r.Header.Get("X-Request-Id"), "status": status})
	})
	server := &http.Server{Addr: *addr, Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	errs := make(chan error, 1)
	go func() { errs <- server.ListenAndServe() }()
	select {
	case <-ctx.Done():
	case e := <-errs:
		if e != http.ErrServerClosed {
			logger.Error("serve", "error", e)
		}
	}
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e := server.Shutdown(sctx); e != nil {
		_ = server.Close()
	}
	_ = provider.Shutdown(sctx)
}
