package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/pprof"
)

func (g *Gateway) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", g.Metrics.Handler())
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		if !g.Ready() {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("GET /admin/routes", func(w http.ResponseWriter, r *http.Request) {
		type route struct {
			ID       string `json:"id"`
			Prefix   string `json:"pathPrefix"`
			Balancer string `json:"loadBalancer"`
		}
		rs := []route{}
		for _, v := range g.current.Load().table.Routes {
			rs = append(rs, route{v.Config.ID, v.Config.PathPrefix, v.Config.LoadBalancer})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rs)
	})
	mux.HandleFunc("GET /admin/upstreams", func(w http.ResponseWriter, r *http.Request) {
		type item struct {
			Route  string `json:"route"`
			Status any    `json:"status"`
		}
		us := []item{}
		for _, v := range g.current.Load().table.Routes {
			for _, u := range v.Upstreams {
				us = append(us, item{v.Config.ID, u.Status()})
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(us)
	})
	if g.current.Load().config.Server.Profiling {
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
	}
	return mux
}
