package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"rlaas/src/internal/protocol"
	"rlaas/src/internal/service"
	"rlaas/src/internal/transport/tcp"
)

func main() {
	logLevel := new(slog.LevelVar)
	if err := logLevel.UnmarshalText([]byte(environmentValue("RLAAS_LOG_LEVEL", "info"))); err != nil {
		fmt.Fprintln(os.Stderr, "configure logging:", err)
		os.Exit(1)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	handler := service.NewBasicHandler()
	codec := protocol.NewJSONCodec()
	config := tcp.DefaultServerConfig("0.0.0.0:6342")
	config.Logger = logger
	server, err := tcp.NewServer(config, codec, handler)
	if err != nil {
		logger.Error("server configuration failed", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("server starting", "address", config.Address, "log_level", logLevel.Level())
	if err := server.ListenAndServe(ctx); err != nil {
		logger.Error("server stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("server stopped")
}

func environmentValue(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
