// Package protocol defines how wire payloads are translated to and from the
// application's request and response types.
package protocol

import (
	"rlaas/src/internal/service"
)

// DecodeError reports a request decoding failure after a request ID was
// successfully decoded from the envelope.
type DecodeError struct {
	RequestID string
	Err       error
}

func (e *DecodeError) Error() string { return e.Err.Error() }
func (e *DecodeError) Unwrap() error { return e.Err }

// Codec translates between framed wire data and application messages.
// It does not read from or write to a network connection.
type Codec interface {
	Decode(payload []byte) (service.Request, error)
	Encode(response service.Response) ([]byte, error)
}
