package routing

import (
	"fmt"
	"strings"
	"testing"

	"github.com/kudak/gateway/internal/config"
)

func TestLongestBoundaryPrefix(t *testing.T) {
	rs := []*Route{{Config: config.Route{PathPrefix: "/"}}, {Config: config.Route{PathPrefix: "/api"}}, {Config: config.Route{PathPrefix: "/api/users"}}}
	table := New(rs)
	for path, want := range map[string]*Route{"/api/users/1": rs[2], "/api/users": rs[2], "/api/usersX": rs[1], "/apix": rs[0], "/other": rs[0]} {
		if got := table.Match(path); got != want {
			t.Errorf("%s matched %+v", path, got)
		}
	}
}
func TestRewrite(t *testing.T) {
	c := config.Route{StripPrefix: "/api", RewritePrefix: "/v1"}
	p, r := Rewrite("/api/a/b", "/api/a%2Fb", c)
	if p != "/v1/a/b" || r != "/v1/a%2Fb" {
		t.Fatal(p, r)
	}
}
func TestEncodedPrefixRewrite(t *testing.T) {
	p, raw := Rewrite("/api/a/b", "/%61pi/a%2Fb", config.Route{StripPrefix: "/api", RewritePrefix: "/v1"})
	if p != "/v1/a/b" || raw != "/v1/a%2Fb" {
		t.Fatal(p, raw)
	}
}
func TestEscapedBoundaryPreservesRawPair(t *testing.T) {
	p, raw := Rewrite("/api/x/y", "/api%2Fx%2Fy", config.Route{StripPrefix: "/api"})
	if p != "//x/y" || raw != "/%2Fx%2Fy" {
		t.Fatal(p, raw)
	}
}
func benchmarkRoutes() *Table {
	rs := make([]*Route, 500)
	for i := range rs {
		rs[i] = &Route{Config: config.Route{PathPrefix: fmt.Sprintf("/api/service/%03d", i)}}
	}
	return New(rs)
}
func BenchmarkRouteLookup(b *testing.B) {
	t := benchmarkRoutes()
	b.ReportAllocs()
	for b.Loop() {
		_ = t.Match("/api/service/250/item")
	}
}
func BenchmarkSortedPrefixLookup(b *testing.B) {
	t := benchmarkRoutes()
	b.ReportAllocs()
	for b.Loop() {
		for i := len(t.Routes) - 1; i >= 0; i-- {
			p := t.Routes[i].Config.PathPrefix
			if strings.HasPrefix("/api/service/250/item", p) {
				break
			}
		}
	}
}
