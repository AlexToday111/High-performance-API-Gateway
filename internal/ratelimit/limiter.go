// Package ratelimit implements token buckets with bounded per-client storage.
package ratelimit

import (
	"math"
	"sync"
	"time"

	"github.com/kudak/gateway/internal/config"
)

type bucket struct {
	tokens         float64
	last, timeUsed time.Time
}
type Limiter struct {
	mu          sync.Mutex
	cfg         config.Rate
	route       bucket
	clients     map[string]*bucket
	nextCleanup time.Time
}
type Result struct {
	Allowed    bool
	Remaining  int
	RetryAfter int
}

func New(c config.Rate) *Limiter {
	now := time.Now()
	return &Limiter{cfg: c, route: bucket{tokens: float64(c.Burst), last: now}, clients: make(map[string]*bucket), nextCleanup: now.Add(min(time.Minute, c.ClientTTL.D()))}
}
func (l *Limiter) Allow(key string, now time.Time) Result {
	if l.cfg.RequestsPerSecond == 0 {
		return Result{Allowed: true}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	b := &l.route
	if l.cfg.PerClient {
		if !now.Before(l.nextCleanup) {
			for k, v := range l.clients {
				if now.Sub(v.timeUsed) >= l.cfg.ClientTTL.D() {
					delete(l.clients, k)
				}
			}
			l.nextCleanup = now.Add(min(time.Minute, l.cfg.ClientTTL.D()))
		}
		var ok bool
		b, ok = l.clients[key]
		if !ok {
			if len(l.clients) >= l.cfg.MaxClients {
				return Result{RetryAfter: 1}
			}
			b = &bucket{tokens: float64(l.cfg.Burst), last: now}
			l.clients[key] = b
		}
	}
	// Callers can capture time before locking and arrive out of order. Never move
	// the refill clock backward, which would mint duplicate elapsed-time tokens.
	if now.Before(b.last) {
		now = b.last
	}
	b.tokens = math.Min(float64(l.cfg.Burst), b.tokens+now.Sub(b.last).Seconds()*l.cfg.RequestsPerSecond)
	b.last = now
	b.timeUsed = now
	if b.tokens >= 1 {
		b.tokens--
		return Result{Allowed: true, Remaining: int(b.tokens)}
	}
	return Result{Remaining: int(b.tokens), RetryAfter: max(1, int(math.Ceil((1-b.tokens)/l.cfg.RequestsPerSecond)))}
}
