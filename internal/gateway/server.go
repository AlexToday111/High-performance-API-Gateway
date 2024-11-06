package gateway

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/kudak/gateway/internal/config"
	"github.com/kudak/gateway/internal/observability"
)

func HTTPServer(c config.Server, address string, h http.Handler) *http.Server {
	return &http.Server{Addr: address, Handler: h, ReadHeaderTimeout: c.ReadHeaderTimeout.D(), ReadTimeout: c.ReadTimeout.D(), WriteTimeout: c.WriteTimeout.D(), IdleTimeout: c.IdleTimeout.D(), MaxHeaderBytes: c.MaxHeaderBytes}
}

// Run binds both listeners before serving. Shutdown drains requests before cancelling
// background work, then flushes telemetry and closes pooled connections.
func Run(ctx context.Context, path string, logger *slog.Logger) error {
	c, e := config.Load(path)
	if e != nil {
		return e
	}
	g, e := New(c, logger)
	if e != nil {
		return e
	}
	defer g.Close()
	public, e := net.Listen("tcp", c.Server.Address)
	if e != nil {
		return e
	}
	defer public.Close()
	admin, e := net.Listen("tcp", c.Server.AdminAddress)
	if e != nil {
		return e
	}
	defer admin.Close()
	provider, e := observability.SetupTracing(ctx, c.Tracing)
	if e != nil {
		return e
	}
	baseCtx, baseCancel := context.WithCancel(context.Background())
	defer baseCancel()
	ps := HTTPServer(c.Server, c.Server.Address, g)
	as := HTTPServer(c.Server, c.Server.AdminAddress, g.AdminHandler())
	ps.BaseContext = func(net.Listener) context.Context { return baseCtx }
	as.BaseContext = ps.BaseContext
	stop := g.StartBackground(baseCtx, path)
	defer stop()
	errs := make(chan error, 2)
	var wg sync.WaitGroup
	for _, entry := range []struct {
		s *http.Server
		l net.Listener
	}{{ps, public}, {as, admin}} {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- entry.s.Serve(entry.l) }()
	}
	logger.Info("gateway started", "public", public.Addr().String(), "admin", admin.Addr().String())
	var serveErr error
	select {
	case <-ctx.Done():
	case serveErr = <-errs:
	}
	g.Drain()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), c.Server.ShutdownTimeout.D())
	defer cancel()
	if e = ps.Shutdown(shutdownCtx); e != nil {
		logger.Warn("public drain timed out", "error", e)
		baseCancel()
		_ = ps.Close()
	}
	if e = as.Shutdown(shutdownCtx); e != nil {
		_ = as.Close()
	}
	baseCancel()
	stop()
	wg.Wait()
	flushCtx, flushCancel := context.WithTimeout(context.Background(), c.Server.ShutdownTimeout.D())
	defer flushCancel()
	if e = provider.Shutdown(flushCtx); e != nil {
		logger.Warn("telemetry flush failed", "error", e)
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", serveErr)
	}
	return nil
}
