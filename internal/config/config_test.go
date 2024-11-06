package config

import (
	"strings"
	"testing"
)

const valid = `routes:
  - id: test
    pathPrefix: /api
    upstreams: [http://127.0.0.1:8081]
`

func TestStrictConfiguration(t *testing.T) {
	if _, e := Parse([]byte(valid)); e != nil {
		t.Fatal(e)
	}
	for _, input := range []string{valid + "unknown: true\n", valid + "---\nfoo: bar\n", strings.Replace(valid, "http://127.0.0.1:8081", "file:///etc/passwd", 1), strings.Replace(valid, "/api", "api", 1), valid + "    timeout: -1s\n", valid + "    loadBalancer: magic\n", valid + "    rateLimit: {requestsPerSecond: -1, burst: 2}\n", valid + "    retry: {maxAttempts: 100}\n", valid + "    auth: {apiKeyEnv: MISSING_GATEWAY_TEST_KEY}\n"} {
		if _, e := Parse([]byte(input)); e == nil {
			t.Errorf("accepted invalid config: %s", input)
		}
	}
}
func TestEnvironment(t *testing.T) {
	t.Setenv("HTTP_IDLE_TIMEOUT", "12s")
	c, e := Parse([]byte(valid))
	if e != nil || c.Server.IdleTimeout.D().Seconds() != 12 {
		t.Fatalf("%+v %v", c.Server, e)
	}
}
