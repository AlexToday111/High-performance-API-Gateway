// Package balancing selects healthy endpoints without a global request-path lock.
package balancing

import (
	"errors"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/kudak/gateway/internal/upstream"
)

var ErrUnavailable = errors.New("no healthy upstream available")

type Balancer struct {
	strategy string
	next     atomic.Uint64
}

func New(strategy string) *Balancer { return &Balancer{strategy: strategy} }
func (b *Balancer) Next(us []*upstream.Upstream, excluded map[*upstream.Upstream]bool) (*upstream.Upstream, error) {
	if len(us) == 0 {
		return nil, ErrUnavailable
	}
	start := int((b.next.Add(1) - 1) % uint64(len(us)))
	if b.strategy == "random" {
		start = rand.IntN(len(us))
	}
	var best *upstream.Upstream
	for i := 0; i < len(us); i++ {
		u := us[(start+i)%len(us)]
		if excluded[u] || !u.Healthy() || !u.Breaker.Available(time.Now()) {
			continue
		}
		if b.strategy != "least_connections" {
			return u, nil
		}
		if best == nil || u.Active.Load() < best.Active.Load() {
			best = u
		}
	}
	if best == nil {
		return nil, ErrUnavailable
	}
	return best, nil
}
