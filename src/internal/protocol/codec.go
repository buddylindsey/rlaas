// Package protocol defines how wire payloads are translated to and from the
// application's request and response types.
package protocol

import (
	"rlaas/src/internal/service"
)

// Codec translates between framed wire data and application messages.
// It does not read from or write to a network connection.
type Codec interface {
	Decode(payload []byte) (service.Request, error)
	Encode(response service.Response) ([]byte, error)
}
