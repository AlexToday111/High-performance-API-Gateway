package resilience

import (
	"context"
	"testing"
	"time"

	"github.com/kudak/gateway/internal/config"
)

func TestBreakerTransitionsAndStaleResults(t *testing.T) {
	b := New(config.Breaker{Enabled: true, FailureThreshold: 1, OpenDuration: config.Duration(time.Millisecond), HalfOpenRequests: 2})
	now := time.Now()
	old, _ := b.Acquire(now)
	p, _ := b.Acquire(now)
	fail, ok := false, true
	if !p.Done(&fail) || b.State() != "open" {
		t.Fatal("did not open")
	}
	old.Done(&ok)
	if b.State() != "open" {
		t.Fatal("stale success closed circuit")
	}
	if _, yes := b.Acquire(now); yes {
		t.Fatal("open admitted")
	}
	p1, _ := b.Acquire(now.Add(time.Second))
	p2, _ := b.Acquire(now.Add(time.Second))
	if _, yes := b.Acquire(now.Add(time.Second)); yes {
		t.Fatal("too many probes")
	}
	p1.Done(&ok)
	if b.State() != "half_open" {
		t.Fatal("closed prematurely")
	}
	p2.Done(&ok)
	if b.State() != "closed" {
		t.Fatal("did not recover")
	}
}
func TestCanceledProbeReleasesPermit(t *testing.T) {
	b := New(config.Breaker{Enabled: true, FailureThreshold: 1, OpenDuration: 1, HalfOpenRequests: 1})
	p, _ := b.Acquire(time.Now())
	bad := false
	p.Done(&bad)
	p, _ = b.Acquire(time.Now().Add(time.Second))
	p.Done(nil)
	if _, ok := b.Acquire(time.Now()); !ok {
		t.Fatal("leaked permit")
	}
}
func TestRetrySafety(t *testing.T) {
	for _, m := range []string{"POST", "PUT", "PATCH", "DELETE"} {
		if SafeMethod(m) {
			t.Fatal(m)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if e := Wait(ctx, time.Second, 0); e != context.Canceled {
		t.Fatal(e)
	}
}
