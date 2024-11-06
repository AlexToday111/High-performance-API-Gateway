package gateway

import (
	"context"
	"crypto/sha256"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/kudak/gateway/internal/config"
	"github.com/kudak/gateway/internal/upstream"
)

func getAPIKey(c config.Route) string { return os.Getenv(c.Auth.APIKeyEnv) }

// StartBackground starts two owned loops. The returned function cancels and joins them.
func (g *Gateway) StartBackground(parent context.Context, path string) func() {
	ctx, cancel := context.WithCancel(parent)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		g.CheckHealth(ctx)
		ticker := time.NewTicker(g.current.Load().config.HealthCheck.Interval.D())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				g.CheckHealth(ctx)
			}
		}
	}()
	go func() {
		defer wg.Done()
		data, _ := os.ReadFile(path)
		digest := sha256.Sum256(data)
		ticker := time.NewTicker(g.current.Load().config.ReloadInterval.D())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b, e := os.ReadFile(path)
				if e != nil {
					g.logger.Warn("config read failed", "error", e)
					continue
				}
				next := sha256.Sum256(b)
				if next == digest {
					continue
				}
				digest = next
				c, e := config.Parse(b)
				if e != nil {
					g.Metrics.ReloadErrors.Inc()
				} else {
					e = g.Reload(c)
				}
				if e != nil {
					g.logger.Warn("config reload rejected", "error", e)
				} else {
					g.logger.Info("config reloaded")
				}
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { cancel(); wg.Wait() }) }
}

// CheckHealth uses at most eight workers; it never spawns a goroutine per endpoint.
func (g *Gateway) CheckHealth(ctx context.Context) {
	s := g.current.Load()
	jobs := make(chan *upstream.Upstream)
	var wg sync.WaitGroup
	client := &http.Client{Transport: g.transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range jobs {
				pctx, cancel := context.WithTimeout(ctx, s.config.HealthCheck.Timeout.D())
				target := *u.URL
				target.Path = s.config.HealthCheck.Path
				target.RawPath = ""
				req, e := http.NewRequestWithContext(pctx, "GET", target.String(), nil)
				if e != nil {
					cancel()
					continue
				}
				resp, e := client.Do(req)
				ok := e == nil && resp.StatusCode >= 200 && resp.StatusCode < 300
				if resp != nil {
					_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
					_ = resp.Body.Close()
				}
				if ctx.Err() == nil {
					u.Probe(ok)
				}
				cancel()
			}
		}()
	}
loop:
	for _, r := range s.table.Routes {
		for _, u := range r.Upstreams {
			select {
			case jobs <- u:
			case <-ctx.Done():
				break loop
			}
		}
	}
	close(jobs)
	wg.Wait()
}
