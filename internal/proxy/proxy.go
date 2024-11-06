package proxy

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/kudak/gateway/internal/balancing"
	"github.com/kudak/gateway/internal/observability"
	"github.com/kudak/gateway/internal/resilience"
	"github.com/kudak/gateway/internal/routing"
	"github.com/kudak/gateway/internal/upstream"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

type Details struct {
	Route               *routing.Route
	RequestID, Upstream string
	Retries             int
	Error               string
}
type detailsKey struct{}

func WithDetails(ctx context.Context, d *Details) context.Context {
	return context.WithValue(ctx, detailsKey{}, d)
}
func DetailsFrom(ctx context.Context) *Details { return ctx.Value(detailsKey{}).(*Details) }

type Proxy struct{ handler *httputil.ReverseProxy }

func New(t http.RoundTripper, m *observability.Metrics, logger *slog.Logger) *Proxy {
	p := &Proxy{}
	p.handler = &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			d := DetailsFrom(r.In.Context())
			r.Out.URL.Path, r.Out.URL.RawPath = routing.Rewrite(r.In.URL.Path, r.In.URL.RawPath, d.Route.Config)
			// Rewrite mode strips untrusted forwarded headers before this callback.
			r.Out.Header.Del("Forwarded")
			r.Out.Header.Del("X-Api-Key")
			for k := range r.Out.Header {
				if strings.HasPrefix(strings.ToLower(k), "x-internal-") {
					r.Out.Header.Del(k)
				}
			}
			r.SetXForwarded()
			r.Out.Header.Set("X-Request-Id", d.RequestID)
		},
		Transport: &attemptTransport{base: t, metrics: m}, BufferPool: &bufferPool{}, FlushInterval: -1,
		ModifyResponse: func(r *http.Response) error {
			r.Header.Set("X-Request-Id", DetailsFrom(r.Request.Context()).RequestID)
			return nil
		},
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			d := DetailsFrom(r.Context())
			d.Error = err.Error()
			status, code := 502, "BAD_GATEWAY"
			var ne net.Error
			var size *http.MaxBytesError
			if errors.Is(err, balancing.ErrUnavailable) {
				status, code = 503, "UPSTREAM_UNAVAILABLE"
			} else if errors.As(err, &size) {
				status, code = 413, "BODY_TOO_LARGE"
			} else if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
				status, code = 504, "GATEWAY_TIMEOUT"
			} else if errors.Is(err, context.Canceled) {
				status, code = 499, "CLIENT_CANCELED"
			}
			WriteError(w, status, code, http.StatusText(status), d.RequestID)
		},
		ErrorLog: slog.NewLogLogger(logger.Handler(), slog.LevelWarn),
	}
	return p
}
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) { p.handler.ServeHTTP(w, r) }

type attemptTransport struct {
	base    http.RoundTripper
	metrics *observability.Metrics
}

func (t *attemptTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	d := DetailsFrom(r.Context())
	route := d.Route
	c := route.Config
	attempts := 1
	if c.Retry.Enabled && resilience.SafeMethod(r.Method) && (r.Body == nil || r.Body == http.NoBody || r.GetBody != nil) {
		attempts = c.Retry.MaxAttempts
	}
	excluded := map[*upstream.Upstream]bool{}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		var u *upstream.Upstream
		var permit *resilience.Permit
		for tries := 0; tries < len(route.Upstreams); tries++ {
			var err error
			u, err = route.Balancer.Next(route.Upstreams, excluded)
			if err != nil {
				if lastErr != nil {
					return nil, lastErr
				}
				return nil, err
			}
			var ok bool
			permit, ok = u.Breaker.Acquire(time.Now())
			if ok {
				break
			}
			excluded[u] = true
			u = nil
		}
		if u == nil {
			return nil, balancing.ErrUnavailable
		}
		excluded[u] = true
		// Retrying a single endpoint is allowed only after every other endpoint was tried.
		if len(excluded) == len(route.Upstreams) {
			excluded = map[*upstream.Upstream]bool{}
		}
		ctx, span := observability.Tracer().Start(r.Context(), "upstream attempt", trace.WithSpanKind(trace.SpanKindClient), trace.WithAttributes(attribute.String("gateway.route", c.ID), attribute.String("gateway.upstream", u.ID), attribute.Int("retry.attempt", attempt)))
		out := r.Clone(ctx)
		copyURL := *r.URL
		out.URL = &copyURL
		target := *u.URL
		out.URL.Scheme = target.Scheme
		out.URL.Host = target.Host
		out.Host = target.Host
		joinTargetPath(out, target.Path, target.RawPath)
		if attempt > 0 && r.GetBody != nil {
			body, err := r.GetBody()
			if err != nil {
				permit.Done(nil)
				span.End()
				return nil, err
			}
			out.Body = body
		}
		observability.Propagator.Inject(ctx, propagation.HeaderCarrier(out.Header))
		d.Upstream = u.ID
		d.Retries = attempt
		u.Active.Add(1)
		start := time.Now()
		resp, err := t.base.RoundTrip(out)
		success := err == nil && resp.StatusCode < 500
		canceled := errors.Is(r.Context().Err(), context.Canceled)
		var size *http.MaxBytesError
		inputError := errors.As(err, &size)
		var outcome *bool
		if !canceled && !inputError {
			outcome = &success
		}
		opened := permit.Done(outcome)
		if opened {
			t.metrics.CircuitOpened.WithLabelValues(c.ID, u.ID).Inc()
		}
		if !success && !canceled && !inputError {
			t.metrics.UpstreamErrors.WithLabelValues(c.ID, u.ID).Inc()
		}
		if !canceled && !inputError {
			u.Passive(success)
		}
		status := 0
		if resp != nil {
			status = resp.StatusCode
		}
		t.metrics.UpstreamRequests.WithLabelValues(c.ID, u.ID, observability.Class(status)).Inc()
		finish := func() {
			u.Active.Add(-1)
			t.metrics.UpstreamDuration.WithLabelValues(c.ID, u.ID).Observe(time.Since(start).Seconds())
			span.End()
		}
		retryable := resilience.RetryError(err) && !canceled && !inputError
		if resp != nil {
			retryable = resilience.RetryStatus(resp.StatusCode)
		}
		if !success {
			span.SetStatus(codes.Error, "upstream attempt failed")
		}
		if err != nil {
			span.RecordError(err)
		}
		if retryable && attempt+1 < attempts && r.Context().Err() == nil {
			if resp != nil { // Do not unboundedly drain failed responses: closing trades reuse for bounded latency.
				_ = resp.Body.Close()
			}
			finish()
			t.metrics.Retries.WithLabelValues(c.ID).Inc()
			lastErr = err
			if e := resilience.Wait(r.Context(), c.Retry.Backoff.D(), attempt); e != nil {
				return nil, e
			}
			continue
		}
		if err != nil {
			finish()
			return nil, err
		}
		resp.Request = out
		resp.Body = &trackedBody{ReadCloser: resp.Body, finish: finish}
		return resp, nil
	}
	return nil, lastErr
}
func joinTargetPath(r *http.Request, path, raw string) {
	if path == "" || path == "/" {
		return
	}
	// Use the standard escaped-path-aware join instead of collapsing repeated
	// separators or dropping encoded slash bytes in the decoded/raw pair.
	target := &url.URL{Scheme: r.URL.Scheme, Host: r.URL.Host, Path: path, RawPath: raw}
	pr := &httputil.ProxyRequest{Out: r}
	pr.SetURL(target)
	r.Host = target.Host
}

type trackedBody struct {
	io.ReadCloser
	once   sync.Once
	finish func()
}

func (b *trackedBody) Close() error { e := b.ReadCloser.Close(); b.once.Do(b.finish); return e }

type bufferPool struct{ pool sync.Pool }

func (b *bufferPool) Get() []byte {
	if v := b.pool.Get(); v != nil {
		return v.(*[32 << 10]byte)[:]
	}
	return make([]byte, 32<<10)
}
func (b *bufferPool) Put(v []byte) {
	if cap(v) == 32<<10 {
		b.pool.Put((*[32 << 10]byte)(v[:32<<10]))
	}
}
