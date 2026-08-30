package tcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"sync"
	"testing"

	"rlaas/src/internal/protocol"
	"rlaas/src/internal/service"
)

func TestRequestLifecycleLogsPreserveRequestID(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	config := DefaultServerConfig("")
	config.Logger = logger

	serverConnection, clientConnection := net.Pipe()
	server, err := NewServer(config, protocol.NewJSONCodec(), service.NewBasicHandler())
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	if !server.addConnection(serverConnection) {
		t.Fatal("addConnection() = false, want true")
	}
	go server.serveConnection(context.Background(), serverConnection)

	request := []byte(`{
		"request_id":"01JTRACE1",
		"operation":"acquire",
		"body":{"name":"missing"}
	}`)
	if err := writeTestFrame(clientConnection, request); err != nil {
		t.Fatalf("writeTestFrame() error = %v", err)
	}
	response := readJSONErrorResponse(t, clientConnection)
	if response.Error.Code != "limiter_not_found" {
		t.Fatalf("error code = %q, want limiter_not_found", response.Error.Code)
	}
	if err := clientConnection.Close(); err != nil {
		t.Fatalf("clientConnection.Close() error = %v", err)
	}
	server.wg.Wait()

	records := decodeLogRecords(t, output.Bytes())
	for _, message := range []string{"request decoded", "request handling failed", "error response sent"} {
		record := findLogRecord(t, records, message)
		if record["request_id"] != "01JTRACE1" {
			t.Errorf("%q request_id = %v, want 01JTRACE1", message, record["request_id"])
		}
		if record["operation"] != "acquire" {
			t.Errorf("%q operation = %v, want acquire", message, record["operation"])
		}
		if record["connection_id"] == nil {
			t.Errorf("%q has no connection_id", message)
		}
	}

	failed := findLogRecord(t, records, "request handling failed")
	if failed["error_code"] != "limiter_not_found" {
		t.Errorf("error_code = %v, want limiter_not_found", failed["error_code"])
	}
	completed := findLogRecord(t, records, "error response sent")
	if _, ok := completed["duration_ms"]; !ok {
		t.Error("error response log has no duration_ms")
	}
}

func TestHandlerPanicIsContainedAndLogged(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	config := DefaultServerConfig("")
	config.Logger = logger
	handler := &panicOnceHandler{}
	server, err := NewServer(config, echoCodec{}, handler)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}

	firstServerConnection, firstClientConnection := net.Pipe()
	if !server.addConnection(firstServerConnection) {
		t.Fatal("first addConnection() = false, want true")
	}
	go server.serveConnection(context.Background(), firstServerConnection)
	if err := writeTestFrame(firstClientConnection, []byte("first")); err != nil {
		t.Fatalf("first writeTestFrame() error = %v", err)
	}
	assertConnectionClosed(t, firstClientConnection)
	if err := firstClientConnection.Close(); err != nil {
		t.Fatalf("firstClientConnection.Close() error = %v", err)
	}
	server.wg.Wait()

	secondServerConnection, secondClientConnection := net.Pipe()
	if !server.addConnection(secondServerConnection) {
		t.Fatal("second addConnection() = false, want true")
	}
	go server.serveConnection(context.Background(), secondServerConnection)
	if err := writeTestFrame(secondClientConnection, []byte("second")); err != nil {
		t.Fatalf("second writeTestFrame() error = %v", err)
	}
	response, err := readTestFrame(secondClientConnection)
	if err != nil {
		t.Fatalf("second readTestFrame() error = %v", err)
	}
	if string(response) != "second" {
		t.Errorf("second response = %q, want second", response)
	}
	if err := secondClientConnection.Close(); err != nil {
		t.Fatalf("secondClientConnection.Close() error = %v", err)
	}
	server.wg.Wait()

	record := findLogRecord(t, decodeLogRecords(t, output.Bytes()), "request panic recovered")
	if record["request_id"] != "test-request" {
		t.Errorf("request_id = %v, want test-request", record["request_id"])
	}
	if record["operation"] != "echo" {
		t.Errorf("operation = %v, want echo", record["operation"])
	}
	if record["panic"] != "handler panic" {
		t.Errorf("panic = %v, want handler panic", record["panic"])
	}
	if record["stack_trace"] == "" {
		t.Error("stack_trace is empty")
	}
}

type panicOnceHandler struct {
	once sync.Once
}

func (h *panicOnceHandler) Handle(_ context.Context, request service.Request) (service.Response, error) {
	panicked := false
	h.once.Do(func() { panicked = true })
	if panicked {
		panic("handler panic")
	}
	return service.Response{RequestID: request.RequestID, Status: service.StatusOK, Body: request.Body}, nil
}

func decodeLogRecords(t *testing.T, payload []byte) []map[string]any {
	t.Helper()
	var records []map[string]any
	scanner := bufio.NewScanner(bytes.NewReader(payload))
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("decode log record %q: %v", scanner.Bytes(), err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan logs: %v", err)
	}
	return records
}

func findLogRecord(t *testing.T, records []map[string]any, message string) map[string]any {
	t.Helper()
	for _, record := range records {
		if record["msg"] == message {
			return record
		}
	}
	t.Fatalf("log message %q not found in %#v", message, records)
	return nil
}
