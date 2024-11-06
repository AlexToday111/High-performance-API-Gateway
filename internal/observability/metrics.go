package observability

import (
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/common/expfmt"
)

type Metrics struct {
	Registry                                                                        *prometheus.Registry
	Requests, UpstreamRequests, UpstreamErrors, RateLimited, Retries, CircuitOpened *prometheus.CounterVec
	Duration, UpstreamDuration                                                      *prometheus.HistogramVec
	Active                                                                          prometheus.Gauge
	Reloads, ReloadErrors                                                           prometheus.Counter
}
type EndpointState struct {
	Route, ID string
	Healthy   bool
	Active    int64
	Circuit   string
}

func NewMetrics(states func() []EndpointState) *Metrics {
	m := &Metrics{Registry: prometheus.NewRegistry()}
	counter := func(name string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: name}, labels)
		m.Registry.MustRegister(c)
		return c
	}
	hist := func(name string, labels ...string) *prometheus.HistogramVec {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: name, Buckets: prometheus.DefBuckets}, labels)
		m.Registry.MustRegister(h)
		return h
	}
	m.Requests = counter("gateway_http_requests_total", "route", "method", "status_class")
	m.Duration = hist("gateway_http_request_duration_seconds", "route", "method")
	m.UpstreamRequests = counter("gateway_upstream_requests_total", "route", "upstream", "status_class")
	m.UpstreamErrors = counter("gateway_upstream_errors_total", "route", "upstream")
	m.UpstreamDuration = hist("gateway_upstream_request_duration_seconds", "route", "upstream")
	m.RateLimited = counter("gateway_rate_limited_total", "route")
	m.Retries = counter("gateway_retries_total", "route")
	m.CircuitOpened = counter("gateway_circuit_breaker_open_total", "route", "upstream")
	m.Active = prometheus.NewGauge(prometheus.GaugeOpts{Name: "gateway_active_requests", Help: "Active public requests"})
	m.Reloads = prometheus.NewCounter(prometheus.CounterOpts{Name: "gateway_config_reload_total", Help: "Successful config reloads"})
	m.ReloadErrors = prometheus.NewCounter(prometheus.CounterOpts{Name: "gateway_config_reload_errors_total", Help: "Rejected config reloads"})
	m.Registry.MustRegister(m.Active, m.Reloads, m.ReloadErrors, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), &stateCollector{states: states})
	return m
}
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		families, e := m.Registry.Gather()
		if e != nil {
			http.Error(w, "metrics gathering failed", 500)
			return
		}
		w.Header().Set("Content-Type", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
		encoder := expfmt.NewEncoder(w, expfmt.NewFormat(expfmt.TypeTextPlain))
		for _, f := range families {
			if e := encoder.Encode(f); e != nil {
				return
			}
		}
	})
}
func Method(s string) string {
	switch s {
	case "GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "CONNECT", "TRACE":
		return s
	default:
		return "OTHER"
	}
}
func Class(s int) string {
	if s < 100 {
		return "error"
	}
	return strconv.Itoa(s/100) + "xx"
}

type stateCollector struct{ states func() []EndpointState }

var healthDesc = prometheus.NewDesc("gateway_upstream_healthy", "Health of configured endpoints", []string{"route", "upstream"}, nil)
var activeDesc = prometheus.NewDesc("gateway_upstream_active_connections", "In-flight upstream requests including response transfer", []string{"route", "upstream"}, nil)
var circuitDesc = prometheus.NewDesc("gateway_circuit_breaker_state", "0 closed, 1 open, 2 half-open", []string{"route", "upstream"}, nil)

func (s *stateCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- healthDesc
	ch <- activeDesc
	ch <- circuitDesc
}
func (s *stateCollector) Collect(ch chan<- prometheus.Metric) {
	for _, v := range s.states() {
		h := 0.
		if v.Healthy {
			h = 1
		}
		b := 0.
		switch v.Circuit {
		case "open":
			b = 1
		case "half_open":
			b = 2
		}
		ch <- prometheus.MustNewConstMetric(healthDesc, prometheus.GaugeValue, h, v.Route, v.ID)
		ch <- prometheus.MustNewConstMetric(activeDesc, prometheus.GaugeValue, float64(v.Active), v.Route, v.ID)
		ch <- prometheus.MustNewConstMetric(circuitDesc, prometheus.GaugeValue, b, v.Route, v.ID)
	}
}
