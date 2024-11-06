package gateway

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kudak/gateway/internal/config"
)

func TestSlowUploadRespectsRouteDeadline(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.Copy(io.Discard, r.Body) }))
	defer backend.Close()
	c := testConfig(t, backend.URL)
	c.Routes[0].Retry.Enabled = true
	c.Routes[0].Timeout = config.Duration(30 * time.Millisecond)
	g := newGateway(t, c)
	server := httptest.NewServer(g)
	defer server.Close()
	conn, e := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Second))
	_, _ = fmt.Fprint(conn, "GET /api/slow HTTP/1.1\r\nHost: example\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n")
	started := time.Now()
	resp, e := http.ReadResponse(bufio.NewReader(conn), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 504 || time.Since(started) > 500*time.Millisecond {
		t.Fatal(resp.StatusCode, time.Since(started))
	}
}
