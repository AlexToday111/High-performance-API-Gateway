// Package upstream owns health state and per-endpoint in-flight request accounting.
package upstream

import (
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kudak/gateway/internal/config"
	"github.com/kudak/gateway/internal/resilience"
)

type Upstream struct {
	ID                     string
	URL                    *url.URL
	Active                 atomic.Int64
	Breaker                *resilience.Breaker
	mu                     sync.Mutex
	healthy                bool
	failures, successes    int
	passiveFailures        int
	failureCount           uint64
	lastFailure, lastCheck time.Time
	health                 config.Health
}
type Status struct {
	ID           string    `json:"id"`
	URL          string    `json:"url"`
	Healthy      bool      `json:"healthy"`
	Active       int64     `json:"activeConnections"`
	FailureCount uint64    `json:"failureCount"`
	LastFailure  time.Time `json:"lastFailure"`
	LastCheck    time.Time `json:"lastHealthCheck"`
	Circuit      string    `json:"circuit"`
}

func New(id, raw string, h config.Health, b config.Breaker) *Upstream {
	u, _ := url.Parse(raw)
	return &Upstream{ID: id, URL: u, healthy: true, health: h, Breaker: resilience.New(b)}
}
func (u *Upstream) Healthy() bool { u.mu.Lock(); defer u.mu.Unlock(); return u.healthy }

// Only active probes restore health; normal traffic cannot erase probe failure streaks.
func (u *Upstream) Probe(ok bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.lastCheck = time.Now()
	if ok {
		u.failures = 0
		u.successes++
		if u.successes >= u.health.HealthyThreshold {
			u.healthy = true
		}
	} else {
		u.failLocked()
	}
}
func (u *Upstream) Failure() { u.Passive(false) }

// Passive tracks request failures independently of probe failures. Successful
// traffic resets this streak but never restores an ejected endpoint.
func (u *Upstream) Passive(ok bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if ok {
		u.passiveFailures = 0
		return
	}
	u.passiveFailures++
	u.failureCount++
	u.lastFailure = time.Now()
	if u.passiveFailures >= u.health.UnhealthyThreshold {
		u.healthy = false
		u.successes = 0
	}
}
func (u *Upstream) failLocked() {
	u.successes = 0
	u.failures++
	u.failureCount++
	u.lastFailure = time.Now()
	if u.failures >= u.health.UnhealthyThreshold {
		u.healthy = false
	}
}
func (u *Upstream) Status() Status {
	u.mu.Lock()
	s := Status{ID: u.ID, URL: u.URL.String(), Healthy: u.healthy, Active: u.Active.Load(), FailureCount: u.failureCount, LastFailure: u.lastFailure, LastCheck: u.lastCheck}
	u.mu.Unlock()
	s.Circuit = u.Breaker.State()
	return s
}
