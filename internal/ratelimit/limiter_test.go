package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kudak/gateway/internal/config"
)

func TestBurstRefillAndConcurrent(t *testing.T) {
	l := New(config.Rate{RequestsPerSecond: 10, Burst: 10})
	now := time.Now()
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if l.Allow("", now).Allowed {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 10 {
		t.Fatal(admitted.Load())
	}
	if !l.Allow("", now.Add(time.Second)).Allowed {
		t.Fatal("no refill")
	}
}
func TestBoundedClientsExpire(t *testing.T) {
	l := New(config.Rate{RequestsPerSecond: 1, Burst: 1, PerClient: true, MaxClients: 1, ClientTTL: config.Duration(time.Second)})
	now := time.Now()
	if !l.Allow("a", now).Allowed || l.Allow("b", now).Allowed {
		t.Fatal("capacity")
	}
	if !l.Allow("b", now.Add(2*time.Minute)).Allowed || len(l.clients) != 1 {
		t.Fatal("cleanup")
	}
}
func BenchmarkTokenBucket(b *testing.B) {
	l := New(config.Rate{RequestsPerSecond: 1e9, Burst: 1000})
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Allow("", time.Now())
		}
	})
}
