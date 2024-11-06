// Package config parses strict YAML and validates configuration before publication.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Duration time.Duration

func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	v, e := time.ParseDuration(n.Value)
	*d = Duration(v)
	return e
}
func (d Duration) D() time.Duration { return time.Duration(d) }

type Server struct {
	Address           string   `yaml:"address"`
	AdminAddress      string   `yaml:"adminAddress"`
	ReadHeaderTimeout Duration `yaml:"readHeaderTimeout"`
	ReadTimeout       Duration `yaml:"readTimeout"`
	WriteTimeout      Duration `yaml:"writeTimeout"`
	IdleTimeout       Duration `yaml:"idleTimeout"`
	ShutdownTimeout   Duration `yaml:"shutdownTimeout"`
	MaxHeaderBytes    int      `yaml:"maxHeaderBytes"`
	MaxBodyBytes      int64    `yaml:"maxBodyBytes"`
	Profiling         bool     `yaml:"profiling"`
}
type Transport struct {
	DialTimeout           Duration `yaml:"dialTimeout"`
	MaxIdleConns          int      `yaml:"maxIdleConns"`
	MaxIdleConnsPerHost   int      `yaml:"maxIdleConnsPerHost"`
	MaxConnsPerHost       int      `yaml:"maxConnsPerHost"`
	IdleConnTimeout       Duration `yaml:"idleConnTimeout"`
	TLSHandshakeTimeout   Duration `yaml:"tlsHandshakeTimeout"`
	ExpectContinueTimeout Duration `yaml:"expectContinueTimeout"`
	ResponseHeaderTimeout Duration `yaml:"responseHeaderTimeout"`
	DisableKeepAlives     bool     `yaml:"disableKeepAlives"`
}
type Health struct {
	Path               string   `yaml:"path"`
	Interval           Duration `yaml:"interval"`
	Timeout            Duration `yaml:"timeout"`
	UnhealthyThreshold int      `yaml:"unhealthyThreshold"`
	HealthyThreshold   int      `yaml:"healthyThreshold"`
}
type Retry struct {
	Enabled      bool     `yaml:"enabled"`
	MaxAttempts  int      `yaml:"maxAttempts"`
	Backoff      Duration `yaml:"backoff"`
	MaxBodyBytes int64    `yaml:"maxBodyBytes"`
}
type Breaker struct {
	Enabled          bool     `yaml:"enabled"`
	FailureThreshold int      `yaml:"failureThreshold"`
	OpenDuration     Duration `yaml:"openDuration"`
	HalfOpenRequests int      `yaml:"halfOpenRequests"`
}
type Rate struct {
	RequestsPerSecond float64  `yaml:"requestsPerSecond"`
	Burst             int      `yaml:"burst"`
	PerClient         bool     `yaml:"perClient"`
	MaxClients        int      `yaml:"maxClients"`
	ClientTTL         Duration `yaml:"clientTTL"`
}
type Auth struct {
	APIKeyEnv string `yaml:"apiKeyEnv"`
}
type Route struct {
	ID                    string   `yaml:"id"`
	PathPrefix            string   `yaml:"pathPrefix"`
	StripPrefix           string   `yaml:"stripPrefix"`
	RewritePrefix         string   `yaml:"rewritePrefix"`
	Upstreams             []string `yaml:"upstreams"`
	LoadBalancer          string   `yaml:"loadBalancer"`
	Timeout               Duration `yaml:"timeout"`
	Retry                 Retry    `yaml:"retry"`
	RateLimit             Rate     `yaml:"rateLimit"`
	CircuitBreaker        Breaker  `yaml:"circuitBreaker"`
	MaxConcurrentRequests int      `yaml:"maxConcurrentRequests"`
	Auth                  Auth     `yaml:"auth"`
}
type Tracing struct {
	Endpoint    string  `yaml:"endpoint"`
	SampleRatio float64 `yaml:"sampleRatio"`
}
type Config struct {
	Server         Server    `yaml:"server"`
	Transport      Transport `yaml:"transport"`
	HealthCheck    Health    `yaml:"healthCheck"`
	ReloadInterval Duration  `yaml:"reloadInterval"`
	Tracing        Tracing   `yaml:"tracing"`
	Routes         []Route   `yaml:"routes"`
}

func Defaults() Config {
	return Config{
		Server:         Server{Address: ":8080", AdminAddress: "127.0.0.1:9091", ReadHeaderTimeout: Duration(2 * time.Second), ReadTimeout: Duration(10 * time.Second), WriteTimeout: Duration(15 * time.Second), IdleTimeout: Duration(60 * time.Second), ShutdownTimeout: Duration(30 * time.Second), MaxHeaderBytes: 1 << 20, MaxBodyBytes: 10 << 20},
		Transport:      Transport{DialTimeout: Duration(2 * time.Second), MaxIdleConns: 256, MaxIdleConnsPerHost: 64, MaxConnsPerHost: 128, IdleConnTimeout: Duration(90 * time.Second), TLSHandshakeTimeout: Duration(3 * time.Second), ExpectContinueTimeout: Duration(time.Second), ResponseHeaderTimeout: Duration(5 * time.Second)},
		HealthCheck:    Health{Path: "/health", Interval: Duration(5 * time.Second), Timeout: Duration(time.Second), UnhealthyThreshold: 3, HealthyThreshold: 2},
		ReloadInterval: Duration(time.Second), Tracing: Tracing{SampleRatio: 0.1},
	}
}
func Load(path string) (Config, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return Config{}, e
	}
	return Parse(b)
}
func Parse(b []byte) (Config, error) {
	c := Defaults()
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if e := dec.Decode(&c); e != nil {
		return c, e
	}
	var extra any
	if e := dec.Decode(&extra); e != io.EOF {
		return c, errors.New("configuration must contain one YAML document")
	}
	for i := range c.Routes {
		r := &c.Routes[i]
		if r.LoadBalancer == "" {
			r.LoadBalancer = "round_robin"
		}
		if r.Timeout == 0 {
			r.Timeout = Duration(3 * time.Second)
		}
		if r.Retry.MaxAttempts == 0 {
			r.Retry.MaxAttempts = 2
		}
		if r.Retry.Backoff == 0 {
			r.Retry.Backoff = Duration(25 * time.Millisecond)
		}
		if r.Retry.MaxBodyBytes == 0 {
			r.Retry.MaxBodyBytes = 1 << 20
		}
		if r.CircuitBreaker.FailureThreshold == 0 {
			r.CircuitBreaker.FailureThreshold = 5
		}
		if r.CircuitBreaker.OpenDuration == 0 {
			r.CircuitBreaker.OpenDuration = Duration(10 * time.Second)
		}
		if r.CircuitBreaker.HalfOpenRequests == 0 {
			r.CircuitBreaker.HalfOpenRequests = 1
		}
		if r.RateLimit.MaxClients == 0 {
			r.RateLimit.MaxClients = 4096
		}
		if r.RateLimit.ClientTTL == 0 {
			r.RateLimit.ClientTTL = Duration(5 * time.Minute)
		}
	}
	for name, dst := range map[string]*Duration{"HTTP_READ_HEADER_TIMEOUT": &c.Server.ReadHeaderTimeout, "HTTP_READ_TIMEOUT": &c.Server.ReadTimeout, "HTTP_WRITE_TIMEOUT": &c.Server.WriteTimeout, "HTTP_IDLE_TIMEOUT": &c.Server.IdleTimeout, "SHUTDOWN_TIMEOUT": &c.Server.ShutdownTimeout} {
		if v, ok := os.LookupEnv(name); ok {
			d, e := time.ParseDuration(v)
			if e != nil {
				return c, fmt.Errorf("%s: %w", name, e)
			}
			*dst = Duration(d)
		}
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	for _, a := range []string{c.Server.Address, c.Server.AdminAddress} {
		if _, _, e := net.SplitHostPort(a); e != nil {
			return fmt.Errorf("listener: %w", e)
		}
	}
	if c.Server.Address == c.Server.AdminAddress {
		return errors.New("public and admin listeners must differ")
	}
	for _, d := range []Duration{c.Server.ReadHeaderTimeout, c.Server.ReadTimeout, c.Server.WriteTimeout, c.Server.IdleTimeout, c.Server.ShutdownTimeout, c.Transport.DialTimeout, c.Transport.IdleConnTimeout, c.Transport.TLSHandshakeTimeout, c.Transport.ExpectContinueTimeout, c.Transport.ResponseHeaderTimeout, c.HealthCheck.Interval, c.HealthCheck.Timeout, c.ReloadInterval} {
		if d <= 0 {
			return errors.New("timeouts and intervals must be positive")
		}
	}
	if c.Server.MaxHeaderBytes < 1024 || c.Server.MaxBodyBytes <= 0 || c.Transport.MaxIdleConns <= 0 || c.Transport.MaxIdleConnsPerHost <= 0 || c.Transport.MaxConnsPerHost <= 0 {
		return errors.New("resource limits must be positive")
	}
	if !strings.HasPrefix(c.HealthCheck.Path, "/") || strings.ContainsAny(c.HealthCheck.Path, "?#") || c.HealthCheck.UnhealthyThreshold < 1 || c.HealthCheck.HealthyThreshold < 1 {
		return errors.New("invalid health check")
	}
	if math.IsNaN(c.Tracing.SampleRatio) || c.Tracing.SampleRatio < 0 || c.Tracing.SampleRatio > 1 {
		return errors.New("invalid trace sample ratio")
	}
	if c.Tracing.Endpoint != "" {
		u, e := url.Parse(c.Tracing.Endpoint)
		if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("invalid OTLP HTTP endpoint")
		}
	}
	if len(c.Routes) == 0 || len(c.Routes) > 1024 {
		return errors.New("configure between 1 and 1024 routes")
	}
	ids, prefixes := map[string]bool{}, map[string]bool{}
	for _, r := range c.Routes {
		if r.ID == "" || len(r.ID) > 64 || ids[r.ID] || prefixes[r.PathPrefix] {
			return errors.New("empty/duplicate route ID or prefix")
		}
		ids[r.ID] = true
		prefixes[r.PathPrefix] = true
		if !strings.HasPrefix(r.PathPrefix, "/") || strings.ContainsAny(r.PathPrefix, "?#%") || strings.Contains(r.PathPrefix, "//") || (len(r.PathPrefix) > 1 && strings.HasSuffix(r.PathPrefix, "/")) {
			return fmt.Errorf("route %s: invalid prefix", r.ID)
		}
		if r.StripPrefix != "" && r.StripPrefix != r.PathPrefix {
			return errors.New("stripPrefix must equal pathPrefix")
		}
		if r.RewritePrefix != "" && (r.StripPrefix == "" || !strings.HasPrefix(r.RewritePrefix, "/") || strings.ContainsAny(r.RewritePrefix, "?#%")) {
			return errors.New("rewritePrefix requires stripPrefix and an absolute path")
		}
		if r.Timeout <= 0 || r.Timeout >= c.Server.WriteTimeout || r.MaxConcurrentRequests < 0 {
			return errors.New("route timeout must be positive and below writeTimeout; concurrency cannot be negative")
		}
		if r.LoadBalancer != "round_robin" && r.LoadBalancer != "least_connections" && r.LoadBalancer != "random" {
			return errors.New("unsupported load balancer")
		}
		if len(r.Upstreams) == 0 || len(r.Upstreams) > 64 {
			return errors.New("route requires 1..64 upstreams")
		}
		seen := map[string]bool{}
		for _, raw := range r.Upstreams {
			u, e := url.Parse(raw)
			if e != nil || u.Host == "" || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || seen[raw] {
				return fmt.Errorf("invalid/duplicate upstream %q", raw)
			}
			seen[raw] = true
			if p := u.Port(); p != "" {
				if _, e = net.LookupPort("tcp", p); e != nil {
					return e
				}
			}
		}
		if r.Retry.MaxAttempts < 1 || r.Retry.MaxAttempts > 5 || r.Retry.Backoff < 0 || r.Retry.Backoff > Duration(time.Second) || r.Retry.MaxBodyBytes < 1 || r.Retry.MaxBodyBytes > c.Server.MaxBodyBytes {
			return errors.New("invalid retry limits")
		}
		if r.CircuitBreaker.FailureThreshold < 1 || r.CircuitBreaker.OpenDuration <= 0 || r.CircuitBreaker.HalfOpenRequests < 1 || r.CircuitBreaker.HalfOpenRequests > 16 {
			return errors.New("invalid circuit breaker")
		}
		rate := r.RateLimit
		if math.IsNaN(rate.RequestsPerSecond) || math.IsInf(rate.RequestsPerSecond, 0) || rate.RequestsPerSecond < 0 || rate.Burst < 0 || (rate.RequestsPerSecond > 0) != (rate.Burst > 0) || rate.MaxClients < 1 || rate.MaxClients > 100000 || rate.ClientTTL <= 0 {
			return errors.New("invalid rate limit")
		}
		if r.Auth.APIKeyEnv != "" && os.Getenv(r.Auth.APIKeyEnv) == "" {
			return fmt.Errorf("API key environment variable %s is empty", r.Auth.APIKeyEnv)
		}
	}
	return nil
}
