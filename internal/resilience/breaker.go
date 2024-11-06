// Package resilience implements bounded retries and generation-safe circuit breakers.
package resilience

import (
	"sync"
	"time"

	"github.com/kudak/gateway/internal/config"
)

type Breaker struct {
	mu                          sync.Mutex
	cfg                         config.Breaker
	state                       string
	failures, probes, successes int
	generation                  uint64
	opened                      time.Time
}
type Permit struct {
	b          *Breaker
	generation uint64
	once       sync.Once
}

func New(c config.Breaker) *Breaker { return &Breaker{cfg: c, state: "closed"} }
func (b *Breaker) Available(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.cfg.Enabled || b.state == "closed" || (b.state == "open" && now.Sub(b.opened) >= b.cfg.OpenDuration.D()) || (b.state == "half_open" && b.probes < b.cfg.HalfOpenRequests)
}
func (b *Breaker) Acquire(now time.Time) (*Permit, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cfg.Enabled {
		if b.state == "open" {
			if now.Sub(b.opened) < b.cfg.OpenDuration.D() {
				return nil, false
			}
			b.state = "half_open"
			b.generation++
			b.probes = 0
			b.successes = 0
		}
		if b.state == "half_open" {
			if b.probes >= b.cfg.HalfOpenRequests {
				return nil, false
			}
			b.probes++
		}
	}
	return &Permit{b: b, generation: b.generation}, true
}

// Done reports one result; nil means cancellation by the client, not an upstream failure.
// The returned bool reports a transition into OPEN for metrics.
func (p *Permit) Done(success *bool) (opened bool) {
	p.once.Do(func() {
		b := p.b
		b.mu.Lock()
		defer b.mu.Unlock()
		if !b.cfg.Enabled || p.generation != b.generation {
			return
		}
		if b.state == "half_open" {
			b.probes--
		}
		if success == nil {
			return
		}
		if *success {
			if b.state == "closed" {
				b.failures = 0
			}
			if b.state == "half_open" {
				b.successes++
				if b.successes >= b.cfg.HalfOpenRequests && b.probes == 0 {
					b.state = "closed"
					b.failures = 0
					b.generation++
				}
			}
			return
		}
		b.failures++
		if b.state == "half_open" || b.failures >= b.cfg.FailureThreshold {
			b.state = "open"
			b.opened = time.Now()
			b.generation++
			opened = true
		}
	})
	return
}
func (b *Breaker) State() string { b.mu.Lock(); defer b.mu.Unlock(); return b.state }
