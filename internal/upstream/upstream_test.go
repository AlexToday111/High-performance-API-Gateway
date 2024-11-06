package upstream

import (
	"testing"

	"github.com/kudak/gateway/internal/config"
)

func TestHealthThresholds(t *testing.T) {
	u := New("test", "http://localhost", config.Health{UnhealthyThreshold: 3, HealthyThreshold: 2}, config.Breaker{})
	u.Probe(false)
	u.Probe(false)
	if !u.Healthy() {
		t.Fatal("early failure")
	}
	u.Probe(false)
	if u.Healthy() {
		t.Fatal("threshold")
	}
	u.Probe(true)
	if u.Healthy() {
		t.Fatal("early recovery")
	}
	u.Probe(true)
	if !u.Healthy() {
		t.Fatal("recovery")
	}
	u.Failure()
	if u.Status().FailureCount != 4 {
		t.Fatal(u.Status())
	}
}

func TestPassiveStreakIsIndependentAndCannotRestoreHealth(t *testing.T) {
	u := New("test", "http://localhost", config.Health{UnhealthyThreshold: 3, HealthyThreshold: 2}, config.Breaker{})
	u.Passive(false)
	u.Passive(false)
	u.Passive(true)
	u.Passive(false)
	if !u.Healthy() {
		t.Fatal("non-consecutive failures ejected endpoint")
	}
	u.Probe(false)
	u.Probe(false)
	u.Passive(true)
	u.Probe(false)
	if u.Healthy() {
		t.Fatal("traffic success erased active probe failures")
	}
	u.Passive(true)
	if u.Healthy() {
		t.Fatal("passive success restored health")
	}
	u.Probe(true)
	u.Probe(true)
	if !u.Healthy() {
		t.Fatal("active recovery failed")
	}
}
