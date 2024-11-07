// Package loadtest provides a bounded, repeatable load generator for local experiments.
package loadtest

import (
	"context"
	"io"
	"net/http"
	"slices"
	"sync"
	"time"
)

type Result struct {
	Scenario            string      `json:"scenario"`
	Concurrency         int         `json:"concurrency"`
	Seconds             float64     `json:"seconds"`
	Requests            int         `json:"requests"`
	RPS                 float64     `json:"rps"`
	P50                 float64     `json:"p50_ms"`
	P95                 float64     `json:"p95_ms"`
	P99                 float64     `json:"p99_ms"`
	ErrorPercent        float64     `json:"error_percent"`
	Status              map[int]int `json:"status_counts"`
	BackendConnections  int64       `json:"backend_connections"`
	BackendDistribution []int64     `json:"backend_distribution,omitempty"`
	CPUSeconds          float64     `json:"process_cpu_seconds"`
	CPUPercent          float64     `json:"process_cpu_percent_one_core"`
	LatencySamples      int         `json:"latency_samples"`
}

func Run(url string, concurrency int, duration time.Duration, rps int) Result {
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	transport := &http.Transport{MaxIdleConns: concurrency, MaxIdleConnsPerHost: concurrency, MaxConnsPerHost: concurrency}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}
	var tokens chan struct{}
	var scheduler sync.WaitGroup
	if rps > 0 {
		tokens = make(chan struct{}, concurrency)
		scheduler.Add(1)
		go func() {
			defer scheduler.Done()
			ticker := time.NewTicker(time.Second / time.Duration(rps))
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					select {
					case tokens <- struct{}{}:
					default:
					}
				}
			}
		}()
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	latencies := make([]float64, 0, 10000)
	counts := map[int]int{}
	total, errors := 0, 0
	started := time.Now()
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				if tokens != nil {
					select {
					case <-ctx.Done():
						return
					case <-tokens:
					}
				}
				req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
				start := time.Now()
				resp, e := client.Do(req)
				status := 0
				if resp != nil {
					status = resp.StatusCode
					_, copyErr := io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
					if e == nil {
						e = copyErr
					}
				}
				elapsed := time.Since(start).Seconds() * 1000
				if ctx.Err() != nil {
					return
				}
				mu.Lock()
				total++
				counts[status]++
				if e != nil || status >= 400 {
					errors++
				}
				if len(latencies) < 1_000_000 {
					latencies = append(latencies, elapsed)
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	scheduler.Wait()
	seconds := time.Since(started).Seconds()
	slices.Sort(latencies)
	percentile := func(q float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(q*float64(len(latencies)-1))]
	}
	errorPct := 0.
	if total > 0 {
		errorPct = 100 * float64(errors) / float64(total)
	}
	return Result{Concurrency: concurrency, Seconds: seconds, Requests: total, RPS: float64(total) / seconds, P50: percentile(.5), P95: percentile(.95), P99: percentile(.99), ErrorPercent: errorPct, Status: counts, LatencySamples: len(latencies)}
}
