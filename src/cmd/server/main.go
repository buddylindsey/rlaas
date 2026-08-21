// Command server starts the RLAAS TCP server.
package main

import (
	"fmt"
	"os"

	"rlaas/src/internal/protocol"
	"rlaas/src/internal/service"
	"rlaas/src/internal/transport/tcp"
)

func main() {
	handler := service.NewBasicHandler()
	codec := protocol.NewJSONCodec()
	server := tcp.NewServer("0.0.0.0:6342", codec, handler)

	fmt.Println("RLAAS server starting on 0.0.0.0:6342")
	if err := server.ListenAndServe(); err != nil {
		fmt.Fprintln(os.Stderr, "server stopped:", err)
		os.Exit(1)
	}
}
