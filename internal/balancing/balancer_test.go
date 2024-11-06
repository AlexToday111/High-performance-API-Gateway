package balancing

import (
	"testing"

	"github.com/kudak/gateway/internal/config"
	"github.com/kudak/gateway/internal/upstream"
)

func endpoints() []*upstream.Upstream {
	return []*upstream.Upstream{upstream.New("a", "http://localhost:1", config.Health{UnhealthyThreshold: 1}, config.Breaker{}), upstream.New("b", "http://localhost:2", config.Health{UnhealthyThreshold: 1}, config.Breaker{})}
}
func TestSelection(t *testing.T) {
	us := endpoints()
	rr := New("round_robin")
	for i := 0; i < 10; i++ {
		u, e := rr.Next(us, nil)
		if e != nil || u != us[i%2] {
			t.Fatal(i, u, e)
		}
	}
	us[0].Active.Store(10)
	u, _ := New("least_connections").Next(us, nil)
	if u != us[1] {
		t.Fatal("wrong least connections")
	}
	us[1].Probe(false)
	u, _ = rr.Next(us, nil)
	if u != us[0] {
		t.Fatal("unhealthy selected")
	}
	us[0].Probe(false)
	if _, e := rr.Next(us, nil); e != ErrUnavailable {
		t.Fatal(e)
	}
}
func BenchmarkRoundRobin(b *testing.B)       { bench(b, "round_robin") }
func BenchmarkLeastConnections(b *testing.B) { bench(b, "least_connections") }
func BenchmarkRandom(b *testing.B)           { bench(b, "random") }
func bench(b *testing.B, s string) {
	us := endpoints()
	bal := New(s)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = bal.Next(us, nil)
		}
	})
}
