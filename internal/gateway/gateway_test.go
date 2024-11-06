package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kudak/gateway/internal/config"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func testConfig(t testing.TB, urls ...string) config.Config {
	t.Helper()
	raw := `routes:
 - id: test
   pathPrefix: /api
   stripPrefix: /api
   upstreams: ["` + strings.Join(urls, `", "`) + `"]
`
	c, e := config.Parse([]byte(raw))
	if e != nil {
		t.Fatal(e)
	}
	return c
}
func newGateway(t testing.TB, c config.Config) *Gateway {
	t.Helper()
	g, e := New(c, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(g.Close)
	return g
}
func response(t testing.TB, g *Gateway, method, path string, body io.Reader) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	g.ServeHTTP(w, httptest.NewRequest(method, path, body))
	return w
}
func TestProxyRewriteAndHeaders(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"path": r.URL.EscapedPath(), "query": r.URL.RawQuery, "host": r.Host, "headers": r.Header})
	}))
	defer backend.Close()
	c := testConfig(t, backend.URL+"/base")
	c.Routes[0].RewritePrefix = "/v1"
	g := newGateway(t, c)
	r := httptest.NewRequest("GET", "/api/a%2Fb?q=1", nil)
	r.RemoteAddr = "192.0.2.10:1234"
	r.Header.Set("X-Forwarded-For", "evil")
	r.Header.Set("X-Forwarded-Host", "evil")
	r.Header.Set("Forwarded", "for=evil")
	r.Header.Set("Connection", "X-Hop")
	r.Header.Set("X-Hop", "secret")
	r.Header.Set("X-Internal-User", "admin")
	r.Header.Set("X-Request-Id", "request-42")
	r.Header.Set("X-Api-Key", "secret")
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	var got struct {
		Path, Query, Host string
		Headers           http.Header
	}
	if e := json.Unmarshal(w.Body.Bytes(), &got); e != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	if got.Path != "/base/v1/a%2Fb" || got.Query != "q=1" || got.Headers.Get("X-Forwarded-For") != "192.0.2.10" || got.Headers.Get("X-Forwarded-Host") == "evil" || got.Headers.Get("Forwarded") != "" || got.Headers.Get("X-Hop") != "" || got.Headers.Get("X-Internal-User") != "" || got.Headers.Get("X-Api-Key") != "" || got.Headers.Get("X-Request-Id") != "request-42" {
		t.Fatalf("%+v", got)
	}
}
func TestRetryFailoverAndUnsafeMethod(t *testing.T) {
	var failures atomic.Int64
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { failures.Add(1); w.WriteHeader(503) }))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		_, _ = w.Write(append([]byte("ok:"), body...))
	}))
	defer b.Close()
	c := testConfig(t, a.URL, b.URL)
	c.Routes[0].Retry.Enabled = true
	g := newGateway(t, c)
	w := response(t, g, "GET", "/api/x", strings.NewReader("body"))
	if w.Code != 200 || w.Body.String() != "ok:body" {
		t.Fatal(w.Code, w.Body.String())
	}
	g = newGateway(t, c)
	w = response(t, g, "POST", "/api/x", strings.NewReader("body"))
	if w.Code != 503 || failures.Load() != 2 {
		t.Fatal(w.Code, failures.Load())
	}
}
func TestTimeoutCancellationAndCounter(t *testing.T) {
	done := make(chan struct{}, 2)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done(); done <- struct{}{} }))
	defer backend.Close()
	c := testConfig(t, backend.URL)
	c.Routes[0].Timeout = config.Duration(30 * time.Millisecond)
	g := newGateway(t, c)
	if w := response(t, g, "GET", "/api/slow", nil); w.Code != 504 {
		t.Fatal(w.Code)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("upstream not canceled")
	}
	if a := g.current.Load().table.Routes[0].Upstreams[0].Active.Load(); a != 0 {
		t.Fatal(a)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("GET", "/api/x", nil).WithContext(ctx)
	cancel()
	w := httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 499 {
		t.Fatal(w.Code)
	}
}
func TestRateLimitAuthAndSizes(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(w, r.Body) }))
	defer backend.Close()
	c := testConfig(t, backend.URL)
	c.Routes[0].RateLimit = config.Rate{RequestsPerSecond: 0.001, Burst: 1, MaxClients: 10, ClientTTL: config.Duration(time.Minute)}
	g := newGateway(t, c)
	if response(t, g, "GET", "/api", nil).Code != 200 {
		t.Fatal("first")
	}
	w := response(t, g, "GET", "/api", nil)
	if w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatal(w.Code)
	}
	c = testConfig(t, backend.URL)
	c.Server.MaxBodyBytes = 10
	c.Routes[0].Retry.MaxBodyBytes = 5
	t.Setenv("GATEWAY_TEST_KEY", "abc")
	c.Routes[0].Auth.APIKeyEnv = "GATEWAY_TEST_KEY"
	g = newGateway(t, c)
	if response(t, g, "GET", "/api", nil).Code != 401 {
		t.Fatal("auth")
	}
	r := httptest.NewRequest("POST", "/api", strings.NewReader(strings.Repeat("x", 11)))
	r.Header.Set("X-Api-Key", "abc")
	w = httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatal(w.Code)
	}
	r = httptest.NewRequest("GET", "/api", strings.NewReader(strings.Repeat("x", 11)))
	r.ContentLength = -1
	r.Header.Set("X-Api-Key", "abc")
	c.Routes[0].Retry.Enabled = true
	c.Routes[0].Retry.MaxBodyBytes = 10
	g = newGateway(t, c)
	w = httptest.NewRecorder()
	g.ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatal("chunked body", w.Code, w.Body.String())
	}
}
func TestHealthAndBreaker(t *testing.T) {
	var healthy atomic.Bool
	healthy.Store(true)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" && healthy.Load() {
			w.WriteHeader(200)
		} else {
			w.WriteHeader(500)
		}
	}))
	defer backend.Close()
	c := testConfig(t, backend.URL)
	c.HealthCheck.UnhealthyThreshold = 2
	c.HealthCheck.HealthyThreshold = 2
	c.Routes[0].CircuitBreaker.Enabled = true
	c.Routes[0].CircuitBreaker.FailureThreshold = 1
	c.Routes[0].CircuitBreaker.OpenDuration = config.Duration(20 * time.Millisecond)
	g := newGateway(t, c)
	if response(t, g, "GET", "/api/fail", nil).Code != 500 {
		t.Fatal("backend error")
	}
	if response(t, g, "GET", "/api/fail", nil).Code != 503 {
		t.Fatal("breaker not open")
	}
	healthy.Store(false)
	g.CheckHealth(context.Background())
	g.CheckHealth(context.Background())
	if g.Ready() {
		t.Fatal("unhealthy ready")
	}
	if response(t, g, "GET", "/api", nil).Code != 503 {
		t.Fatal("unhealthy selected")
	}
	healthy.Store(true)
	g.CheckHealth(context.Background())
	g.CheckHealth(context.Background())
	if !g.current.Load().table.Routes[0].Upstreams[0].Healthy() {
		t.Fatal("no recovery")
	}
}
func TestConcurrentReloadAndDistribution(t *testing.T) {
	var aCount, bCount atomic.Int64
	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { aCount.Add(1); _, _ = w.Write([]byte("a")) }))
	defer a.Close()
	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { bCount.Add(1); _, _ = w.Write([]byte("b")) }))
	defer b.Close()
	c := testConfig(t, a.URL, b.URL)
	g := newGateway(t, c)
	var wg sync.WaitGroup
	errs := make(chan int, 1000)
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if w := response(t, g, "GET", "/api/fast", nil); w.Code != 200 {
				errs <- w.Code
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if e := g.Reload(c); e != nil {
				t.Error(e)
			}
		}
	}()
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if aCount.Load() != 500 || bCount.Load() != 500 {
		t.Fatal(aCount.Load(), bCount.Load())
	}
	for _, u := range g.current.Load().table.Routes[0].Upstreams {
		if u.Active.Load() != 0 {
			t.Fatal("leaked count")
		}
	}
	bad := g.Config()
	bad.Routes[0].Upstreams = nil
	if g.Reload(bad) == nil {
		t.Fatal("invalid reload")
	}
	if response(t, g, "GET", "/api", nil).Code != 200 {
		t.Fatal("old table lost")
	}
	next := g.Config()
	next.Routes[0].PathPrefix = "/new"
	next.Routes[0].StripPrefix = "/new"
	if e := g.Reload(next); e != nil {
		t.Fatal(e)
	}
	if response(t, g, "GET", "/api", nil).Code != 404 || response(t, g, "GET", "/new", nil).Code != 200 {
		t.Fatal("table not swapped")
	}
	next.Routes[0].Upstreams[0] = "corrupted"
	if !strings.HasPrefix(g.Config().Routes[0].Upstreams[0], "http") {
		t.Fatal("snapshot aliased")
	}
}
func TestMetricsAndTracePropagation(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	provider := trace.NewTracerProvider(trace.WithSyncer(exporter))
	otel.SetTracerProvider(provider)
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(trace.NewTracerProvider()) })
	var parent string
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { parent = r.Header.Get("Traceparent"); w.WriteHeader(200) }))
	defer backend.Close()
	g := newGateway(t, testConfig(t, backend.URL))
	r := httptest.NewRequest("GET", "/api/123", nil)
	r.Header.Set("Traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	g.ServeHTTP(httptest.NewRecorder(), r)
	if !strings.Contains(parent, "4bf92f3577b34da6a3ce929d0e0e4736") {
		t.Fatal(parent)
	}
	if len(exporter.GetSpans()) < 4 {
		t.Fatal("missing spans")
	}
	w := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), `route="test"`) || strings.Contains(w.Body.String(), "/api/123") {
		t.Fatal("labels")
	}
	if response(t, g, "GET", "/metrics", nil).Code != 404 {
		t.Fatal("admin public")
	}
}
func TestConcurrencyLimitAndStreaming(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("first\n"))
		w.(http.Flusher).Flush()
		close(started)
		<-release
		_, _ = w.Write([]byte("last\n"))
	}))
	defer backend.Close()
	c := testConfig(t, backend.URL)
	c.Routes[0].MaxConcurrentRequests = 1
	g := newGateway(t, c)
	server := httptest.NewServer(g)
	defer server.Close()
	resp, e := http.Get(server.URL + "/api/stream")
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	<-started
	buf := make([]byte, 6)
	if _, e := io.ReadFull(resp.Body, buf); e != nil || string(buf) != "first\n" {
		t.Fatal(string(buf), e)
	}
	if w := response(t, g, "GET", "/api/other", nil); w.Code != 503 {
		t.Fatal("overload", w.Code)
	}
	if g.current.Load().table.Routes[0].Upstreams[0].Active.Load() != 1 {
		t.Fatal("counter not held through streaming")
	}
	close(release)
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
}
func TestFileWatcherKeepsInvalidConfig(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer backend.Close()
	c := testConfig(t, backend.URL)
	c.ReloadInterval = config.Duration(5 * time.Millisecond)
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	raw := fmt.Sprintf("reloadInterval: 5ms\nroutes:\n - id: test\n   pathPrefix: /api\n   upstreams: [%s]\n", backend.URL)
	if e := os.WriteFile(path, []byte(raw), 0600); e != nil {
		t.Fatal(e)
	}
	g := newGateway(t, c)
	stop := g.StartBackground(context.Background(), path)
	defer stop()
	time.Sleep(20 * time.Millisecond)
	_ = os.WriteFile(path, []byte("invalid: ["), 0600)
	time.Sleep(30 * time.Millisecond)
	if response(t, g, "GET", "/api", nil).Code != 200 {
		t.Fatal("invalid replaced table")
	}
	_ = os.WriteFile(path, []byte(strings.Replace(raw, "/api", "/new", 1)), 0600)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if response(t, g, "GET", "/new", nil).Code == 200 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("watcher did not reload")
}
func BenchmarkProxyMiddlewareChain(b *testing.B) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("ok")) }))
	defer backend.Close()
	g := newGateway(b, testConfig(b, backend.URL))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		w := response(b, g, "GET", "/api/x", nil)
		if w.Code != 200 {
			b.Fatal(w.Code)
		}
	}
}
func TestRequestIDValidation(t *testing.T) {
	if requestID("safe_123") != "safe_123" || requestID("bad\n") == "bad\n" || len(requestID("")) != 32 {
		t.Fatal("request ID")
	}
}
func TestReplayBoundFallsBackToStreaming(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(w, r.Body) }))
	defer backend.Close()
	c := testConfig(t, backend.URL)
	c.Routes[0].Retry.Enabled = true
	c.Routes[0].Retry.MaxBodyBytes = 3
	g := newGateway(t, c)
	w := response(t, g, "GET", "/api", bytes.NewBufferString("larger body"))
	if w.Code != 200 || w.Body.String() != "larger body" {
		t.Fatal(w.Code, w.Body.String())
	}
}
