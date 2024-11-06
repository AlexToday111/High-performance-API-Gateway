package resilience

import (
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"syscall"
	"time"
)

func SafeMethod(m string) bool {
	return m == http.MethodGet || m == http.MethodHead || m == http.MethodOptions
}
func RetryStatus(s int) bool { return s == 502 || s == 503 || s == 504 }
func RetryError(err error) bool {
	var n net.Error
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || (errors.As(err, &n) && n.Timeout())
}
func Wait(ctx context.Context, base time.Duration, attempt int) error {
	d := base * time.Duration(1<<min(attempt, 5))
	if d > time.Second {
		d = time.Second
	}
	if d > 0 {
		d = d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
