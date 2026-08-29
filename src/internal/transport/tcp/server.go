// Package tcp provides the TCP transport for the RLAAS server.
package tcp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"rlaas/src/internal/protocol"
	"rlaas/src/internal/service"
)

const (
	frameLengthSize = 4
	maxFrameSize    = 1 << 20 // 1 MiB
)

// Server accepts TCP connections and delegates each received message to a Handler.
type Server struct {
	config  ServerConfig
	codec   protocol.Codec
	handler service.Handler

	mu           sync.Mutex
	connections  map[net.Conn]*connectionState
	shuttingDown bool
	forceCancel  context.CancelFunc
	wg           sync.WaitGroup
}

type connectionState struct {
	active bool
}

// ServerConfig controls TCP connection capacity and lifecycle deadlines.
type ServerConfig struct {
	Address         string
	IdleTimeout     time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	ShutdownTimeout time.Duration
	MaxConnections  int
}

// DefaultServerConfig returns production-oriented defaults for address.
func DefaultServerConfig(address string) ServerConfig {
	return ServerConfig{
		Address:         address,
		IdleTimeout:     60 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		ShutdownTimeout: 10 * time.Second,
		MaxConnections:  1_024,
	}
}

// NewServer creates a TCP server with validated lifecycle configuration.
func NewServer(config ServerConfig, codec protocol.Codec, handler service.Handler) (*Server, error) {
	if config.IdleTimeout <= 0 {
		return nil, errors.New("idle timeout must be greater than zero")
	}
	if config.ReadTimeout <= 0 {
		return nil, errors.New("read timeout must be greater than zero")
	}
	if config.WriteTimeout <= 0 {
		return nil, errors.New("write timeout must be greater than zero")
	}
	if config.ShutdownTimeout <= 0 {
		return nil, errors.New("shutdown timeout must be greater than zero")
	}
	if config.MaxConnections <= 0 {
		return nil, errors.New("maximum connections must be greater than zero")
	}
	if codec == nil {
		return nil, errors.New("codec is required")
	}
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	return &Server{
		config:      config,
		codec:       codec,
		handler:     handler,
		connections: make(map[net.Conn]*connectionState),
	}, nil
}

// ListenAndServe listens until ctx is canceled or the listener fails.
func (s *Server) ListenAndServe(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.config.Address)
	if err != nil {
		return err
	}
	defer listener.Close()

	return s.Serve(ctx, listener)
}

// Serve accepts connections until ctx is canceled or listener fails.
func (s *Server) Serve(ctx context.Context, listener net.Listener) error {
	workCtx, forceCancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.forceCancel = forceCancel
	s.mu.Unlock()
	defer forceCancel()

	shutdownStarted := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.beginShutdown(listener)
		case <-shutdownStarted:
		}
	}()

	for {
		connection, err := listener.Accept()
		if err != nil {
			close(shutdownStarted)
			if ctx.Err() != nil {
				return s.waitForShutdown()
			}
			s.forceShutdown()
			return err
		}

		if !s.addConnection(connection) {
			connection.Close()
			continue
		}

		s.wg.Add(1)
		go s.serveConnection(workCtx, connection)
	}
}

func (s *Server) serveConnection(ctx context.Context, connection net.Conn) {
	defer func() {
		connection.Close()
		s.removeConnection(connection)
		s.wg.Done()
	}()

	for {
		payload, err := s.readFrame(connection)
		if err != nil {
			if !isExpectedDisconnect(err) {
				fmt.Println("error reading client message:", err)
			}
			return
		}
		if !s.markActive(connection) {
			return
		}

		request, err := s.codec.Decode(payload)
		if err != nil {
			if !s.writeError(connection, "", "invalid_request", err) {
				return
			}
			if !s.markIdle(connection) {
				return
			}
			continue
		}

		response, err := s.handler.Handle(ctx, request)
		if err != nil {
			if !s.writeError(connection, request.RequestID, serviceErrorCode(err), err) {
				return
			}
			if !s.markIdle(connection) {
				return
			}
			continue
		}

		payload, err = s.codec.Encode(response)
		if err != nil {
			if !s.writeError(connection, request.RequestID, "response_encoding_failed", err) {
				return
			}
			if !s.markIdle(connection) {
				return
			}
			continue
		}

		if err := s.writeFrame(connection, payload); err != nil {
			if !isExpectedDisconnect(err) {
				fmt.Println("error writing client response:", err)
			}
			return
		}
		if !s.markIdle(connection) {
			return
		}
	}
}

// readFrame reads a 4-byte unsigned, big-endian payload length followed by the
// payload bytes themselves.
func (s *Server) readFrame(connection net.Conn) ([]byte, error) {
	if err := connection.SetReadDeadline(time.Now().Add(s.config.IdleTimeout)); err != nil {
		return nil, err
	}
	var header [frameLengthSize]byte
	if _, err := io.ReadFull(connection, header[:1]); err != nil {
		return nil, err
	}
	if err := connection.SetReadDeadline(time.Now().Add(s.config.ReadTimeout)); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(connection, header[1:]); err != nil {
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
	if err := connection.SetReadDeadline(time.Time{}); err != nil {
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
	if err := connection.SetWriteDeadline(time.Now().Add(s.config.WriteTimeout)); err != nil {
		return err
	}

	var header [frameLengthSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if err := writeAll(connection, header[:]); err != nil {
		return err
	}
	if err := writeAll(connection, payload); err != nil {
		return err
	}
	return connection.SetWriteDeadline(time.Time{})
}

func (s *Server) addConnection(connection net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.shuttingDown || len(s.connections) >= s.config.MaxConnections {
		return false
	}
	s.connections[connection] = &connectionState{}
	return true
}

func (s *Server) removeConnection(connection net.Conn) {
	s.mu.Lock()
	delete(s.connections, connection)
	s.mu.Unlock()
}

func (s *Server) markActive(connection net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, exists := s.connections[connection]
	if !exists || s.shuttingDown {
		return false
	}
	state.active = true
	return true
}

func (s *Server) markIdle(connection net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state, exists := s.connections[connection]
	if !exists {
		return false
	}
	state.active = false
	return !s.shuttingDown
}

func (s *Server) beginShutdown(listener net.Listener) {
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		return
	}
	s.shuttingDown = true
	listener.Close()
	for connection, state := range s.connections {
		if !state.active {
			connection.Close()
		}
	}
	s.mu.Unlock()
}

func (s *Server) waitForShutdown() error {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	timer := time.NewTimer(s.config.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		s.forceShutdown()
		return errors.New("server shutdown timed out")
	}
}

func (s *Server) forceShutdown() {
	s.mu.Lock()
	if s.forceCancel != nil {
		s.forceCancel()
	}
	for connection := range s.connections {
		connection.Close()
	}
	s.mu.Unlock()
}

func isExpectedDisconnect(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
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
	case errors.Is(err, service.ErrInvalidLimiterConfiguration):
		return "invalid_limiter_configuration"
	case errors.Is(err, context.Canceled):
		return "request_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "request_deadline_exceeded"
	default:
		return "request_failed"
	}
}
