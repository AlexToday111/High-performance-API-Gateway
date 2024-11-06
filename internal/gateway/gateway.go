// Package gateway assembles immutable routing snapshots and the public request flow.
package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kudak/gateway/internal/config"
	"github.com/kudak/gateway/internal/observability"
	"github.com/kudak/gateway/internal/proxy"
	"github.com/kudak/gateway/internal/resilience"
	"github.com/kudak/gateway/internal/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type snapshot struct {
	config config.Config
	table  *routing.Table
}
type Gateway struct {
	current   atomic.Pointer[snapshot]
	reloadMu  sync.Mutex
	routeIDs  map[string]bool
	transport *http.Transport
	proxy     *proxy.Proxy
	Metrics   *observability.Metrics
	logger    *slog.Logger
	draining  atomic.Bool
}

func New(c config.Config, logger *slog.Logger) (*Gateway, error) {
	if e := c.Validate(); e != nil {
		return nil, e
	}
	g := &Gateway{logger: logger, routeIDs: map[string]bool{}}
	g.Metrics = observability.NewMetrics(g.endpointStates)
	g.transport = proxy.NewTransport(c.Transport)
	g.proxy = proxy.New(g.transport, g.Metrics, logger)
	if e := g.Reload(c); e != nil {
		return nil, e
	}
	return g, nil
}
func (g *Gateway) Reload(c config.Config) error {
	g.reloadMu.Lock()
	defer g.reloadMu.Unlock()
	reject := func(e error) error { g.Metrics.ReloadErrors.Inc(); return e }
	if e := c.Validate(); e != nil {
		return reject(e)
	}
	old := g.current.Load()
	if old != nil {
		a, b := old.config, c
		a.Routes = nil
		b.Routes = nil
		if !reflect.DeepEqual(a, b) {
			return reject(errors.New("hot reload only supports routes; listener, transport, health, tracing and intervals require restart"))
		}
	}
	ids := len(g.routeIDs)
	for _, r := range c.Routes {
		if !g.routeIDs[r.ID] {
			ids++
		}
	}
	if ids > 1024 {
		return reject(errors.New("process lifetime route ID budget exceeded"))
	}
	// Copy caller-owned slices: publication never exposes mutable input configuration.
	c.Routes = slices.Clone(c.Routes)
	for i := range c.Routes {
		c.Routes[i].Upstreams = slices.Clone(c.Routes[i].Upstreams)
	}
	routes := make([]*routing.Route, 0, len(c.Routes))
	for _, rc := range c.Routes {
		var reuse *routing.Route
		if old != nil {
			for _, r := range old.table.Routes {
				if reflect.DeepEqual(r.Config, rc) && r.APIKey == getAPIKey(rc) {
					reuse = r
					break
				}
			}
		}
		if reuse == nil {
			reuse = routing.NewRoute(rc, c.HealthCheck)
		}
		routes = append(routes, reuse)
		g.routeIDs[rc.ID] = true
	}
	g.current.Store(&snapshot{config: c, table: routing.New(routes)})
	if old != nil {
		g.Metrics.Reloads.Inc()
	}
	return nil
}
func (g *Gateway) Config() config.Config {
	c := g.current.Load().config
	c.Routes = slices.Clone(c.Routes)
	for i := range c.Routes {
		c.Routes[i].Upstreams = slices.Clone(c.Routes[i].Upstreams)
	}
	return c
}
func (g *Gateway) Close() { g.transport.CloseIdleConnections() }
func (g *Gateway) Drain() { g.draining.Store(true) }
func (g *Gateway) Ready() bool {
	if g.draining.Load() {
		return false
	}
	for _, r := range g.current.Load().table.Routes {
		ok := false
		for _, u := range r.Upstreams {
			if u.Healthy() && u.Breaker.Available(time.Now()) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	return true
}
func (g *Gateway) endpointStates() []observability.EndpointState {
	var result []observability.EndpointState
	s := g.current.Load()
	if s == nil {
		return result
	}
	for _, r := range s.table.Routes {
		for _, u := range r.Upstreams {
			st := u.Status()
			result = append(result, observability.EndpointState{Route: r.Config.ID, ID: u.ID, Healthy: st.Healthy, Active: st.Active, Circuit: st.Circuit})
		}
	}
	return result
}
func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	d := &proxy.Details{}
	d.RequestID = requestID(r.Header.Get("X-Request-Id"))
	w.Header().Set("X-Request-Id", d.RequestID)
	ctx := observability.Propagator.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
	ctx, span := observability.Tracer().Start(ctx, "gateway request", trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	r = r.WithContext(proxy.WithDetails(ctx, d))
	sw := &statusWriter{ResponseWriter: w}
	g.Metrics.Active.Inc()
	defer g.Metrics.Active.Dec()
	defer func() {
		recovered := recover()
		if recovered != nil {
			if recovered == http.ErrAbortHandler {
				d.Error = "response stream aborted"
			} else {
				d.Error = "handler panic"
				g.logger.Error("handler panic", "request_id", d.RequestID, "panic", fmt.Sprint(recovered))
				if sw.status == 0 {
					proxy.WriteError(sw, 500, "INTERNAL_ERROR", "internal error", d.RequestID)
				}
			}
		}
		status := sw.status
		if status == 0 {
			status = 200
		}
		route := "unmatched"
		if d.Route != nil {
			route = d.Route.Config.ID
		}
		method := observability.Method(r.Method)
		g.Metrics.Requests.WithLabelValues(route, method, observability.Class(status)).Inc()
		g.Metrics.Duration.WithLabelValues(route, method).Observe(time.Since(start).Seconds())
		span.SetAttributes(attribute.String("gateway.route", route), attribute.String("http.request.method", method), attribute.Int("http.response.status_code", status))
		if status >= 500 || d.Error != "" {
			span.SetStatus(codes.Error, "request failed")
		}
		host, _, _ := net.SplitHostPort(r.RemoteAddr)
		g.logger.Info("request", "request_id", d.RequestID, "method", method, "path", r.URL.EscapedPath(), "route", route, "status", status, "duration_ms", float64(time.Since(start).Microseconds())/1000, "upstream", d.Upstream, "retry_count", d.Retries, "error", d.Error, "client_ip", host)
		if recovered == http.ErrAbortHandler {
			panic(recovered)
		}
	}()
	fail := func(status int, code string) {
		proxy.WriteError(sw, status, code, http.StatusText(status), d.RequestID)
	}
	if g.draining.Load() {
		fail(503, "DRAINING")
		return
	}
	s := g.current.Load()
	_, lookup := observability.Tracer().Start(ctx, "route lookup")
	route := s.table.Match(r.URL.Path)
	lookup.End()
	d.Route = route
	if route == nil {
		fail(404, "ROUTE_NOT_FOUND")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), route.Config.Timeout.D())
	defer cancel()
	r = r.WithContext(ctx)
	if route.APIKey != "" {
		actual := sha256.Sum256([]byte(r.Header.Get("X-Api-Key")))
		expected := sha256.Sum256([]byte(route.APIKey))
		if subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
			fail(401, "UNAUTHORIZED")
			return
		}
	}
	if r.ContentLength > s.config.Server.MaxBodyBytes {
		fail(413, "BODY_TOO_LARGE")
		return
	}
	if r.Body != nil && r.Body != http.NoBody {
		r.Body = http.MaxBytesReader(sw, r.Body, s.config.Server.MaxBodyBytes)
		// Context deadlines alone do not interrupt an inbound HTTP/1 body Read.
		// Bound upload/replay reads too; net/http resets deadlines for the next request.
		_ = http.NewResponseController(sw).SetReadDeadline(time.Now().Add(min(route.Config.Timeout.D(), s.config.Server.ReadTimeout.D())))
	}
	if route.Slots != nil {
		select {
		case route.Slots <- struct{}{}:
			defer func() { <-route.Slots }()
		default:
			fail(503, "OVERLOADED")
			return
		}
	}
	key, _, _ := net.SplitHostPort(r.RemoteAddr)
	_, limitSpan := observability.Tracer().Start(ctx, "rate limit")
	result := route.Limiter.Allow(key, time.Now())
	limitSpan.End()
	if route.Config.RateLimit.RequestsPerSecond > 0 {
		sw.Header().Set("X-RateLimit-Limit", strconv.FormatFloat(route.Config.RateLimit.RequestsPerSecond, 'f', -1, 64))
		sw.Header().Set("X-RateLimit-Remaining", strconv.Itoa(result.Remaining))
	}
	if !result.Allowed {
		sw.Header().Set("Retry-After", strconv.Itoa(result.RetryAfter))
		g.Metrics.RateLimited.WithLabelValues(route.Config.ID).Inc()
		fail(429, "RATE_LIMITED")
		return
	}
	// Replay only bounded safe-method bodies. Larger bodies stream once, without retries.
	if route.Config.Retry.Enabled && resilience.SafeMethod(r.Method) && r.Body != nil && r.Body != http.NoBody {
		capBytes := route.Config.Retry.MaxBodyBytes
		b, e := io.ReadAll(io.LimitReader(r.Body, capBytes+1))
		if e != nil {
			d.Error = e.Error()
			var size *http.MaxBytesError
			if errors.As(e, &size) {
				fail(413, "BODY_TOO_LARGE")
			} else if ctx.Err() != nil || isTimeout(e) {
				fail(504, "GATEWAY_TIMEOUT")
			} else {
				fail(400, "INVALID_BODY")
			}
			return
		}
		original := r.Body
		if int64(len(b)) <= capBytes {
			_ = original.Close()
			r.Body = io.NopCloser(bytes.NewReader(b))
			r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
		} else {
			r.Body = &joinedBody{Reader: io.MultiReader(bytes.NewReader(b), original), closer: original}
			r.GetBody = nil
		}
	}
	g.proxy.ServeHTTP(sw, r)
}

func isTimeout(err error) bool { var e net.Error; return errors.As(err, &e) && e.Timeout() }

type joinedBody struct {
	io.Reader
	closer io.Closer
}

func (b *joinedBody) Close() error { return b.closer.Close() }

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *statusWriter) WriteHeader(s int) {
	if s >= 100 && s < 200 {
		w.ResponseWriter.WriteHeader(s)
		return
	}
	if w.status == 0 {
		w.status = s
		w.ResponseWriter.WriteHeader(s)
	}
}
func (w *statusWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(b)
}
func (w *statusWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
func requestID(s string) string {
	if len(s) > 0 && len(s) <= 128 && !strings.ContainsFunc(s, func(r rune) bool {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			return false
		default:
			return true
		}
	}) {
		return s
	}
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
