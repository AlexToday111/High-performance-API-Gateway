package main

import (
	"encoding/json"
	"flag"
	"os"
	"time"

	"github.com/kudak/gateway/internal/loadtest"
)

func main() {
	url := flag.String("url", "http://localhost:8080/api/users/fast", "target URL")
	concurrency := flag.Int("concurrency", 50, "workers")
	duration := flag.Duration("duration", 30*time.Second, "duration")
	rps := flag.Int("rps", 0, "arrival rate, zero for closed loop")
	flag.Parse()
	if *concurrency < 1 || *concurrency > 10000 || *duration <= 0 || *rps < 0 || *rps > 1000000 {
		os.Exit(2)
	}
	r := loadtest.Run(*url, *concurrency, *duration, *rps)
	r.Scenario = *url
	_ = json.NewEncoder(os.Stdout).Encode(r)
}
