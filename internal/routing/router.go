// Package routing builds immutable byte-prefix tries with path-segment boundaries.
package routing

import (
	"net/url"
	"os"
	"strings"

	"github.com/kudak/gateway/internal/balancing"
	"github.com/kudak/gateway/internal/config"
	"github.com/kudak/gateway/internal/ratelimit"
	"github.com/kudak/gateway/internal/upstream"
)

type Route struct {
	Config    config.Route
	Upstreams []*upstream.Upstream
	Balancer  *balancing.Balancer
	Limiter   *ratelimit.Limiter
	Slots     chan struct{}
	APIKey    string
}

func NewRoute(c config.Route, h config.Health) *Route {
	r := &Route{Config: c, Balancer: balancing.New(c.LoadBalancer), Limiter: ratelimit.New(c.RateLimit), APIKey: os.Getenv(c.Auth.APIKeyEnv)}
	if c.MaxConcurrentRequests > 0 {
		r.Slots = make(chan struct{}, c.MaxConcurrentRequests)
	}
	for i, raw := range c.Upstreams {
		r.Upstreams = append(r.Upstreams, upstream.New(c.ID+"/"+string(rune('a'+i)), raw, h, c.CircuitBreaker))
	}
	return r
}

type node struct {
	children map[byte]*node
	route    *Route
}
type Table struct {
	root   node
	Routes []*Route
}

func New(rs []*Route) *Table {
	t := &Table{Routes: rs}
	for _, r := range rs {
		n := &t.root
		for _, c := range []byte(r.Config.PathPrefix) {
			if n.children == nil {
				n.children = map[byte]*node{}
			}
			if n.children[c] == nil {
				n.children[c] = &node{}
			}
			n = n.children[c]
		}
		n.route = r
	}
	return t
}
func (t *Table) Match(path string) *Route {
	n := &t.root
	var best *Route
	for i := 0; i < len(path); i++ {
		n = n.children[path[i]]
		if n == nil {
			break
		}
		if n.route != nil && (n.route.Config.PathPrefix == "/" || i+1 == len(path) || path[i+1] == '/') {
			best = n.route
		}
	}
	return best
}

// Rewrite transforms decoded and escaped paths together, preserving escaped slashes.
func Rewrite(path, rawPath string, c config.Route) (string, string) {
	rewrite := func(p string, prefix string) string {
		if c.StripPrefix != "" {
			p = strings.TrimPrefix(p, prefix)
			if p == "" {
				p = "/"
			}
			if !strings.HasPrefix(p, "/") {
				p = "/" + p
			}
			if c.RewritePrefix != "" {
				p = strings.TrimRight(c.RewritePrefix, "/") + p
			}
		}
		return p
	}
	rawPrefix := c.StripPrefix
	if rawPath != "" && c.StripPrefix != "" {
		// RawPath may encode characters inside the matched prefix (/%61pi).
		// Locate its decoded byte boundary, rather than trimming the plain prefix.
		decodedBytes, end := 0, 0
		for end < len(rawPath) && decodedBytes < len(c.StripPrefix) {
			if rawPath[end] == '%' && end+2 < len(rawPath) {
				end += 3
			} else {
				end++
			}
			decodedBytes++
		}
		rawPrefix = rawPath[:end]
	}
	path = rewrite(path, c.StripPrefix)
	if rawPath != "" {
		originalRewrite := c.RewritePrefix
		c.RewritePrefix = (&url.URL{Path: originalRewrite}).EscapedPath()
		rawPath = rewrite(rawPath, rawPrefix)
		// An escaped leading slash remains data after stripping. Inserting a
		// literal origin-form slash must also be reflected in the decoded path.
		if decoded, err := url.PathUnescape(rawPath); err == nil {
			path = decoded
		}
	}
	return path, rawPath
}
