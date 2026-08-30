package tcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
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
	server.wg.Add(1)
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
