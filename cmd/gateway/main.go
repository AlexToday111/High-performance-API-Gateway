package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/kudak/gateway/internal/gateway"
)

func main() {
	path := flag.String("config", "configs/gateway.yaml", "YAML configuration path")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if e := gateway.Run(ctx, *path, logger); e != nil {
		logger.Error("gateway failed", "error", e)
		os.Exit(1)
	}
}
