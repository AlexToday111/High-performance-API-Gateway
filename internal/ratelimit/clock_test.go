package ratelimit

import (
	"testing"
	"time"

	"github.com/kudak/gateway/internal/config"
)

func TestOutOfOrderAdmissionTimesDoNotMintTokens(t *testing.T) {
	l := New(config.Rate{RequestsPerSecond: 1, Burst: 1})
	now := time.Now()
	if !l.Allow("", now).Allowed {
		t.Fatal("initial burst")
	}
	if !l.Allow("", now.Add(time.Second)).Allowed {
		t.Fatal("refill")
	}
	if l.Allow("", now).Allowed {
		t.Fatal("older time allowed")
	}
	if l.Allow("", now.Add(time.Second)).Allowed {
		t.Fatal("duplicate elapsed time refilled")
	}
}
