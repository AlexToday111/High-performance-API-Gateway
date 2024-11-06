package gateway

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.uber.org/goleak"
)

func freeAddress(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	a := l.Addr().String()
	_ = l.Close()
	return a
}
func TestRunDrainsAdmittedRequest(t *testing.T) {
	oldProvider := otel.GetTracerProvider()
	defer otel.SetTracerProvider(oldProvider)
	started, release := make(chan struct{}), make(chan struct{})
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(200)
			return
		}
		close(started)
		select {
		case <-release:
			_, _ = w.Write([]byte("done"))
		case <-r.Context().Done():
		}
	}))
	defer backend.Close()
	public, admin := freeAddress(t), freeAddress(t)
	path := filepath.Join(t.TempDir(), "gateway.yaml")
	raw := fmt.Sprintf("server:\n address: %s\n adminAddress: %s\n shutdownTimeout: 1s\nroutes:\n - id: test\n   pathPrefix: /\n   upstreams: [%s]\n", public, admin, backend.URL)
	if e := os.WriteFile(path, []byte(raw), 0600); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- Run(ctx, path, slog.New(slog.NewJSONHandler(io.Discard, nil))) }()
	deadline := time.Now().Add(time.Second)
	ready := false
	for time.Now().Before(deadline) {
		r, e := http.Get("http://" + admin + "/health/ready")
		if e == nil {
			_ = r.Body.Close()
			if r.StatusCode == 200 {
				ready = true
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !ready {
		t.Fatal("server did not start")
	}
	result := make(chan int, 1)
	go func() {
		r, e := http.Get("http://" + public + "/fast")
		if e != nil {
			result <- 0
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		result <- r.StatusCode
	}()
	<-started
	cancel()
	select {
	case e := <-done:
		t.Fatal("exited before drain", e)
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if s := <-result; s != 200 {
		t.Fatal("request not drained", s)
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown stuck")
	}
}
func TestBackgroundWorkersAreJoined(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())
	entered := make(chan struct{}, 16)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { entered <- struct{}{}; <-r.Context().Done() }))
	defer backend.Close()
	g := newGateway(t, testConfig(t, backend.URL))
	defer g.Close()
	path := filepath.Join(t.TempDir(), "missing.yaml")
	stop := g.StartBackground(context.Background(), path)
	<-entered
	done := make(chan struct{})
	go func() { stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("background cancellation stuck")
	}
	stop()
}
