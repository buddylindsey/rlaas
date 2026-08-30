package tcp

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"rlaas/src/internal/service"
)

func TestNewServerRejectsInvalidConfig(t *testing.T) {
	valid := DefaultServerConfig("")
	tests := []ServerConfig{
		func() ServerConfig { config := valid; config.IdleTimeout = 0; return config }(),
		func() ServerConfig { config := valid; config.ReadTimeout = 0; return config }(),
		func() ServerConfig { config := valid; config.WriteTimeout = 0; return config }(),
		func() ServerConfig { config := valid; config.ShutdownTimeout = 0; return config }(),
		func() ServerConfig { config := valid; config.MaxConnections = 0; return config }(),
	}
	for _, config := range tests {
		if _, err := NewServer(config, echoCodec{}, echoHandler{}); err == nil {
			t.Errorf("NewServer(%#v) error = nil, want validation error", config)
		}
	}
}

func TestIdleConnectionTimesOut(t *testing.T) {
	config := shortTimeoutConfig()
	client := startTestConnectionWithConfig(t, config, echoCodec{}, echoHandler{})
	defer client.Close()

	assertConnectionClosed(t, client)
}

func TestPartialFramePayloadTimesOut(t *testing.T) {
	config := shortTimeoutConfig()
	client := startTestConnectionWithConfig(t, config, echoCodec{}, echoHandler{})
	defer client.Close()

	header := []byte{0, 0, 0, 10}
	if _, err := client.Write(append(header, 'x')); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	assertConnectionClosed(t, client)
}

func TestPartialFrameHeaderTimesOut(t *testing.T) {
	config := shortTimeoutConfig()
	client := startTestConnectionWithConfig(t, config, echoCodec{}, echoHandler{})
	defer client.Close()

	if _, err := client.Write([]byte{0}); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	assertConnectionClosed(t, client)
}

func TestResponseWriteTimesOut(t *testing.T) {
	config := shortTimeoutConfig()
	client := startTestConnectionWithConfig(t, config, echoCodec{}, echoHandler{})
	defer client.Close()

	if err := writeTestFrame(client, []byte("response")); err != nil {
		t.Fatalf("writeTestFrame() error = %v", err)
	}
	time.Sleep(2 * config.WriteTimeout)
	assertConnectionClosed(t, client)
}

func TestServerRejectsConnectionsOverLimit(t *testing.T) {
	config := DefaultServerConfig("")
	config.MaxConnections = 1
	server, err := NewServer(config, echoCodec{}, echoHandler{})
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	listener := newQueuedListener()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- server.Serve(ctx, listener) }()

	serverOne, clientOne := net.Pipe()
	defer clientOne.Close()
	listener.connections <- serverOne
	waitForConnectionCount(t, server, 1)

	serverTwo, clientTwo := net.Pipe()
	defer clientTwo.Close()
	listener.connections <- serverTwo
	assertConnectionClosed(t, clientTwo)

	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
}

func TestGracefulShutdownAllowsActiveRequestToFinish(t *testing.T) {
	config := DefaultServerConfig("")
	config.ShutdownTimeout = time.Second
	handler := &blockingHandler{started: make(chan struct{}), release: make(chan struct{})}
	server, err := NewServer(config, echoCodec{}, handler)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	listener := newQueuedListener()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- server.Serve(ctx, listener) }()

	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()
	listener.connections <- serverConnection
	if err := writeTestFrame(clientConnection, []byte("request")); err != nil {
		t.Fatalf("writeTestFrame() error = %v", err)
	}
	<-handler.started
	cancel()
	close(handler.release)

	payload, err := readTestFrame(clientConnection)
	if err != nil {
		t.Fatalf("readTestFrame() error = %v", err)
	}
	if string(payload) != "request" {
		t.Errorf("response = %q, want request", payload)
	}
	if err := <-result; err != nil {
		t.Fatalf("Serve() error = %v", err)
	}
}

func TestForcedShutdownCancelsActiveRequest(t *testing.T) {
	config := shortTimeoutConfig()
	handler := &cancelHandler{started: make(chan struct{}), canceled: make(chan struct{})}
	server, err := NewServer(config, echoCodec{}, handler)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	listener := newQueuedListener()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- server.Serve(ctx, listener) }()

	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()
	listener.connections <- serverConnection
	if err := writeTestFrame(clientConnection, []byte("request")); err != nil {
		t.Fatalf("writeTestFrame() error = %v", err)
	}
	<-handler.started
	cancel()

	select {
	case <-handler.canceled:
	case <-time.After(time.Second):
		t.Fatal("handler context was not canceled")
	}
	if err := <-result; err == nil {
		t.Fatal("Serve() error = nil, want shutdown timeout")
	}
}

func shortTimeoutConfig() ServerConfig {
	config := DefaultServerConfig("")
	config.IdleTimeout = 20 * time.Millisecond
	config.ReadTimeout = 20 * time.Millisecond
	config.WriteTimeout = 20 * time.Millisecond
	config.ShutdownTimeout = 20 * time.Millisecond
	return config
}

func startTestConnectionWithConfig(t *testing.T, config ServerConfig, codec echoCodec, handler service.Handler) net.Conn {
	t.Helper()
	serverConnection, clientConnection := net.Pipe()
	server, err := NewServer(config, codec, handler)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	if !server.addConnection(serverConnection) {
		t.Fatal("addConnection() = false, want true")
	}
	server.wg.Add(1)
	go server.serveConnection(context.Background(), serverConnection)
	return clientConnection
}

func assertConnectionClosed(t *testing.T, connection net.Conn) {
	t.Helper()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		if errors.Is(err, io.ErrClosedPipe) || errors.Is(err, net.ErrClosed) {
			return
		}
		t.Fatalf("SetReadDeadline() error = %v", err)
	}
	var buffer [1]byte
	_, err := connection.Read(buffer[:])
	if err == nil {
		t.Fatal("Read() error = nil, want closed connection")
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		t.Fatalf("connection remained open: %v", err)
	}
}

func waitForConnectionCount(t *testing.T, server *Server, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		server.mu.Lock()
		got := len(server.connections)
		server.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("connection count did not reach %d", want)
}

type queuedListener struct {
	connections chan net.Conn
	closed      chan struct{}
	once        sync.Once
}

func newQueuedListener() *queuedListener {
	return &queuedListener{connections: make(chan net.Conn), closed: make(chan struct{})}
}

func (l *queuedListener) Accept() (net.Conn, error) {
	select {
	case connection := <-l.connections:
		return connection, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l *queuedListener) Close() error {
	l.once.Do(func() { close(l.closed) })
	return nil
}

func (l *queuedListener) Addr() net.Addr { return testAddr("test") }

type testAddr string

func (a testAddr) Network() string { return string(a) }
func (a testAddr) String() string  { return string(a) }

type blockingHandler struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *blockingHandler) Handle(_ context.Context, request service.Request) (service.Response, error) {
	h.once.Do(func() { close(h.started) })
	<-h.release
	return service.Response{RequestID: request.RequestID, Status: service.StatusOK, Body: request.Body}, nil
}

type cancelHandler struct {
	started  chan struct{}
	canceled chan struct{}
	once     sync.Once
}

func (h *cancelHandler) Handle(ctx context.Context, _ service.Request) (service.Response, error) {
	h.once.Do(func() { close(h.started) })
	<-ctx.Done()
	close(h.canceled)
	return service.Response{}, ctx.Err()
}
