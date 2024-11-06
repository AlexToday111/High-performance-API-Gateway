package proxy

import (
	"net/http"
	"net/url"
	"testing"
)

func TestTargetJoinPreservesRepeatedAndEscapedSlashes(t *testing.T) {
	for _, tc := range []struct{ path, raw, want string }{{"//x", "", "/base//x"}, {"/x/y", "/x%2Fy", "/base/x%2Fy"}, {"//x", "/%2Fx", "/base/%2Fx"}} {
		r := &http.Request{URL: &url.URL{Scheme: "http", Host: "backend", Path: tc.path, RawPath: tc.raw, RawQuery: "q=1"}}
		joinTargetPath(r, "/base", "")
		if r.URL.EscapedPath() != tc.want || r.URL.RawQuery != "q=1" {
			t.Fatal(r.URL.String(), tc.want)
		}
		if p, e := url.PathUnescape(r.URL.EscapedPath()); e != nil || p != r.URL.Path {
			t.Fatal("inconsistent raw path", r.URL)
		}
	}
}
