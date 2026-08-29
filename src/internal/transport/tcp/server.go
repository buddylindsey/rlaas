// Package tcp provides the TCP transport for the RLAAS server.
package tcp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"rlaas/src/internal/protocol"
	"rlaas/src/internal/service"
)

const (
	frameLengthSize = 4
	maxFrameSize    = 1 << 20 // 1 MiB
)

// Server accepts TCP connections and delegates each received message to a Handler.
type Server struct {
	address string
	codec   protocol.Codec
	handler service.Handler
}

// NewServer creates a TCP server that listens on address.
func NewServer(address string, codec protocol.Codec, handler service.Handler) *Server {
	return &Server{address: address, codec: codec, handler: handler}
}

// ListenAndServe listens on the configured address and serves connections until
// the listener encounters an error.
func (s *Server) ListenAndServe() error {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return err
	}
	defer listener.Close()

	return s.Serve(listener)
}

// Serve accepts connections from listener. It is exported to allow alternate
// listener setup in tests and future application configuration.
func (s *Server) Serve(listener net.Listener) error {
	for {
		connection, err := listener.Accept()
		if err != nil {
			return err
		}

		go s.serveConnection(connection)
	}
}

func (s *Server) serveConnection(connection net.Conn) {
	defer connection.Close()

	for {
		payload, err := s.readFrame(connection)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				fmt.Println("error reading client message:", err)
			}
			return
		}

		request, err := s.codec.Decode(payload)
		if err != nil {
			if !s.writeError(connection, "", "invalid_request", err) {
				return
			}
			continue
		}

		response, err := s.handler.Handle(context.Background(), request)
		if err != nil {
			if !s.writeError(connection, request.RequestID, serviceErrorCode(err), err) {
				return
			}
			continue
		}

		payload, err = s.codec.Encode(response)
		if err != nil {
			if !s.writeError(connection, request.RequestID, "response_encoding_failed", err) {
				return
			}
			continue
		}

		if err := s.writeFrame(connection, payload); err != nil {
			fmt.Println("error writing client response:", err)
			return
		}
	}
}

// readFrame reads a 4-byte unsigned, big-endian payload length followed by the
// payload bytes themselves.
func (s *Server) readFrame(connection net.Conn) ([]byte, error) {
	var header [frameLengthSize]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return nil, err
	}

	length := binary.BigEndian.Uint32(header[:])
	if length > maxFrameSize {
		return nil, fmt.Errorf("frame length %d exceeds maximum of %d bytes", length, maxFrameSize)
	}

	payload := make([]byte, int(length))
	if _, err := io.ReadFull(connection, payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// writeFrame writes a 4-byte unsigned, big-endian payload length followed by
// the payload bytes themselves.
func (s *Server) writeFrame(connection net.Conn, payload []byte) error {
	if len(payload) > maxFrameSize {
		return fmt.Errorf("frame length %d exceeds maximum of %d bytes", len(payload), maxFrameSize)
	}

	var header [frameLengthSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeAll(connection, header[:]); err != nil {
		return err
	}
	return writeAll(connection, payload)
}

func writeAll(connection net.Conn, payload []byte) error {
	for len(payload) > 0 {
		written, err := connection.Write(payload)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func (s *Server) writeError(connection net.Conn, requestID, code string, cause error) bool {
	payload, err := s.codec.Encode(service.Response{
		RequestID: requestID,
		Status:    service.StatusError,
		Error: &service.ResponseError{
			Code:    code,
			Message: cause.Error(),
		},
	})
	if err != nil {
		fmt.Println("error encoding client error response:", err)
		return false
	}
	if writeErr := s.writeFrame(connection, payload); writeErr != nil {
		fmt.Println("error writing client response:", writeErr)
		return false
	}
	return true
}

func serviceErrorCode(err error) string {
	switch {
	case errors.Is(err, service.ErrLimiterNotFound):
		return "limiter_not_found"
	case errors.Is(err, service.ErrLimiterConfigurationConflict):
		return "limiter_configuration_conflict"
	case errors.Is(err, context.Canceled):
		return "request_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "request_deadline_exceeded"
	default:
		return "request_failed"
	}
}
