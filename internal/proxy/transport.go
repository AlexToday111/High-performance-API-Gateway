// Package proxy executes upstream attempts through one reusable HTTP transport.
package proxy

import (
	"net"
	"net/http"

	"github.com/kudak/gateway/internal/config"
)

func NewTransport(c config.Transport) *http.Transport {
	return &http.Transport{
		Proxy:             nil, // Configured targets never inherit ambient HTTP_PROXY destinations.
		DialContext:       (&net.Dialer{Timeout: c.DialTimeout.D(), KeepAlive: c.IdleConnTimeout.D()}).DialContext,
		ForceAttemptHTTP2: true, MaxIdleConns: c.MaxIdleConns, MaxIdleConnsPerHost: c.MaxIdleConnsPerHost, MaxConnsPerHost: c.MaxConnsPerHost, IdleConnTimeout: c.IdleConnTimeout.D(), TLSHandshakeTimeout: c.TLSHandshakeTimeout.D(), ExpectContinueTimeout: c.ExpectContinueTimeout.D(), ResponseHeaderTimeout: c.ResponseHeaderTimeout.D(), DisableKeepAlives: c.DisableKeepAlives,
	}
}
