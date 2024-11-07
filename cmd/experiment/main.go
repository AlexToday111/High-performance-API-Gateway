package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/kudak/gateway/internal/config"
	"github.com/kudak/gateway/internal/gateway"
	"github.com/kudak/gateway/internal/loadtest"
)

func main() {
	duration := flag.Duration("duration", 3*time.Second, "duration per scenario")
	concurrency := flag.Int("concurrency", 32, "workers")
	flag.Parse()
	if *duration <= 0 || *concurrency < 1 || *concurrency > 10000 {
		os.Exit(2)
	}
	type backend struct {
		server             *httptest.Server
		count, connections *atomic.Int64
	}
	makeBackend := func(delay time.Duration, flaky bool) backend {
		var count, connections atomic.Int64
		s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			count.Add(1)
			if delay > 0 {
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-r.Context().Done():
					return
				case <-timer.C:
				}
			}
			if flaky && rand.IntN(4) == 0 {
				w.WriteHeader(503)
				return
			}
			_, _ = w.Write([]byte("ok"))
		}))
		s.Config.ConnState = func(_ net.Conn, state http.ConnState) {
			if state == http.StateNew {
				connections.Add(1)
			}
		}
		s.Start()
		return backend{s, &count, &connections}
	}
	a, b, slow, flaky := makeBackend(0, false), makeBackend(0, false), makeBackend(50*time.Millisecond, false), makeBackend(0, true)
	fast := makeBackend(10*time.Millisecond, false)
	defer a.server.Close()
	defer b.server.Close()
	defer slow.server.Close()
	defer flaky.server.Close()
	defer fast.server.Close()
	encoder := json.NewEncoder(os.Stdout)
	_ = encoder.Encode(map[string]any{"environment": map[string]any{"go": runtime.Version(), "os": runtime.GOOS, "arch": runtime.GOARCH, "logical_cpus": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0), "duration": duration.String(), "note": "client, gateway and httptest backends share one Linux process; CPU is combined; no tracing exporter; JSON logging to discard"}})
	scenarios := []struct {
		name        string
		us          []backend
		strategy    string
		noKeepalive bool
		limit       bool
		retry       bool
		arrival     int
	}{
		{name: "direct", us: []backend{a}},
		{name: "proxy_keepalive", us: []backend{a, b}, strategy: "round_robin"},
		{name: "proxy_no_keepalive", us: []backend{a, b}, strategy: "round_robin", noKeepalive: true},
		{name: "round_robin_10ms_50ms", us: []backend{fast, slow}, strategy: "round_robin"},
		{name: "least_connections_10ms_50ms", us: []backend{fast, slow}, strategy: "least_connections"},
		{name: "rate_100_arrival_500", us: []backend{a}, strategy: "round_robin", limit: true, arrival: 500},
		{name: "flaky_without_retry", us: []backend{flaky, b}, strategy: "round_robin"},
		{name: "flaky_with_retry", us: []backend{flaky, b}, strategy: "round_robin", retry: true},
	}
	for _, sc := range scenarios {
		urls := make([]string, len(sc.us))
		beforeConns := int64(0)
		beforeCounts := make([]int64, len(sc.us))
		for i, u := range sc.us {
			urls[i] = u.server.URL
			beforeConns += u.connections.Load()
			beforeCounts[i] = u.count.Load()
		}
		var g *gateway.Gateway
		var server *httptest.Server
		url := urls[0] + "/fast"
		if sc.strategy != "" {
			c, e := config.Parse([]byte(fmt.Sprintf("routes:\n - id: experiment\n   pathPrefix: /\n   upstreams: [%s]\n", joinURLs(urls))))
			if e != nil {
				panic(e)
			}
			c.Transport.DisableKeepAlives = sc.noKeepalive
			c.Routes[0].LoadBalancer = sc.strategy
			c.Routes[0].Retry.Enabled = sc.retry
			c.Routes[0].Retry.Backoff = config.Duration(time.Millisecond)
			c.HealthCheck.UnhealthyThreshold = 1_000_000 // isolate retry/balancing from health ejection
			if sc.limit {
				c.Routes[0].RateLimit.RequestsPerSecond = 100
				c.Routes[0].RateLimit.Burst = 50
			}
			g, e = gateway.New(c, slog.New(slog.NewJSONHandler(io.Discard, nil)))
			if e != nil {
				panic(e)
			}
			server = httptest.NewServer(g)
			url = server.URL + "/fast"
		}
		runtime.GC()
		cpuStart := cpuSeconds()
		result := loadtest.Run(url, *concurrency, *duration, sc.arrival)
		result.Scenario = sc.name
		result.CPUSeconds = cpuSeconds() - cpuStart
		result.CPUPercent = result.CPUSeconds / result.Seconds * 100
		for i, u := range sc.us {
			result.BackendConnections += u.connections.Load()
			result.BackendDistribution = append(result.BackendDistribution, u.count.Load()-beforeCounts[i])
		}
		result.BackendConnections -= beforeConns
		_ = encoder.Encode(result)
		if server != nil {
			server.Close()
			g.Close()
		}
	}
}
func joinURLs(urls []string) string {
	result := ""
	for i, u := range urls {
		if i > 0 {
			result += ", "
		}
		result += u
	}
	return result
}
