.PHONY: build run test test-race lint benchmark profile docker-up docker-down load-test experiments
build:
	go build -o bin/gateway ./cmd/gateway
	go build -o bin/mock-backend ./cmd/mock-backend
run:
	go run ./cmd/gateway -config configs/gateway.yaml
test:
	go test ./...
test-race:
	go test -race ./...
lint:
	go vet ./...
	golangci-lint run
benchmark:
	go test -run '^$$' -bench=. -benchmem ./...
profile:
	go test ./internal/gateway -run '^$$' -bench=BenchmarkProxyMiddlewareChain -cpuprofile=cpu.prof -memprofile=heap.prof
docker-up:
	docker compose up --build -d
docker-down:
	docker compose down
load-test:
	docker compose run --rm k6
experiments:
	go run ./cmd/experiment -duration 3s
