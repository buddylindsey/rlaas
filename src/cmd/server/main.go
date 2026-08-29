package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"rlaas/src/internal/protocol"
	"rlaas/src/internal/service"
	"rlaas/src/internal/transport/tcp"
)

func main() {
	handler := service.NewBasicHandler()
	codec := protocol.NewJSONCodec()
	config := tcp.DefaultServerConfig("0.0.0.0:6342")
	server, err := tcp.NewServer(config, codec, handler)
	if err != nil {
		fmt.Fprintln(os.Stderr, "configure server:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("RLAAS server starting on 0.0.0.0:6342")
	if err := server.ListenAndServe(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "server stopped:", err)
		os.Exit(1)
	}
}
