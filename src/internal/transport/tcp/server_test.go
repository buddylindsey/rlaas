package tcp

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"

	"rlaas/src/internal/protocol"
	"rlaas/src/internal/service"
)

func TestServeConnectionHandlesMultipleMessages(t *testing.T) {
	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()

	server := NewServer("", echoCodec{}, service.NewBasicHandler())
	go server.serveConnection(serverConnection)

	for _, message := range []string{"first", "second"} {
		if err := writeTestFrame(clientConnection, []byte(message)); err != nil {
			t.Fatalf("writeTestFrame() error = %v", err)
		}

		got, err := readTestFrame(clientConnection)
		if err != nil {
			t.Fatalf("readTestFrame() error = %v", err)
		}
		if got, want := string(got), message; got != want {
			t.Errorf("response = %q, want %q", got, want)
		}
	}
}

func TestServeConnectionCreatesLimiterAndReturnsJSON(t *testing.T) {
	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()

	server := NewServer("", protocol.NewJSONCodec(), service.NewBasicHandler())
	go server.serveConnection(serverConnection)

	request := []byte(`{
		"request_id":"01JCREATE1",
		"operation":"create_limiter",
		"body":{"name":"github-api","type":"fixed_window","time_window_ms":1000,"budget":10}
	}`)
	if err := writeTestFrame(clientConnection, request); err != nil {
		t.Fatalf("writeTestFrame() error = %v", err)
	}
	payload, err := readTestFrame(clientConnection)
	if err != nil {
		t.Fatalf("readTestFrame() error = %v", err)
	}

	var response struct {
		RequestID string `json:"request_id"`
		Status    string `json:"status"`
		Body      struct {
			Name    string `json:"name"`
			Created bool   `json:"created"`
			Message string `json:"message"`
		} `json:"body"`
	}
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode response %q: %v", payload, err)
	}
	if response.RequestID != "01JCREATE1" || response.Status != "ok" {
		t.Errorf("response envelope = %#v", response)
	}
	if response.Body.Name != "github-api" || !response.Body.Created || response.Body.Message != "limiter created" {
		t.Errorf("response body = %#v", response.Body)
	}

	acquire := []byte(`{
		"request_id":"01JACQUIRE1",
		"operation":"acquire",
		"body":{"name":"github-api"}
	}`)
	if err := writeTestFrame(clientConnection, acquire); err != nil {
		t.Fatalf("write acquire frame error = %v", err)
	}
	payload, err = readTestFrame(clientConnection)
	if err != nil {
		t.Fatalf("read acquire frame error = %v", err)
	}
	var acquireResponse struct {
		RequestID string `json:"request_id"`
		Status    string `json:"status"`
		Body      struct {
			Allowed      bool   `json:"allowed"`
			Remaining    uint64 `json:"remaining"`
			RetryAfterMs uint64 `json:"retry_after_ms"`
		} `json:"body"`
	}
	if err := json.Unmarshal(payload, &acquireResponse); err != nil {
		t.Fatalf("decode acquire response %q: %v", payload, err)
	}
	if acquireResponse.RequestID != "01JACQUIRE1" || acquireResponse.Status != "ok" || !acquireResponse.Body.Allowed {
		t.Errorf("acquire response = %#v", acquireResponse)
	}
	if acquireResponse.Body.Remaining != 9 || acquireResponse.Body.RetryAfterMs != 0 {
		t.Errorf("acquire response body = %#v", acquireResponse.Body)
	}
}

func TestServeConnectionReturnsJSONForInvalidRequest(t *testing.T) {
	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()

	server := NewServer("", protocol.NewJSONCodec(), service.NewBasicHandler())
	go server.serveConnection(serverConnection)

	if err := writeTestFrame(clientConnection, []byte(`not JSON`)); err != nil {
		t.Fatalf("writeTestFrame() error = %v", err)
	}
	response := readJSONErrorResponse(t, clientConnection)
	if response.Status != "error" || response.Error.Code != "invalid_request" {
		t.Errorf("response = %#v, want invalid_request error", response)
	}
	if response.Error.Message == "" {
		t.Error("error message is empty")
	}
}

func TestServeConnectionReturnsJSONWhenLimiterIsMissing(t *testing.T) {
	serverConnection, clientConnection := net.Pipe()
	defer clientConnection.Close()

	server := NewServer("", protocol.NewJSONCodec(), service.NewBasicHandler())
	go server.serveConnection(serverConnection)

	request := []byte(`{
		"request_id":"01JMISSING1",
		"operation":"acquire",
		"body":{"name":"missing"}
	}`)
	if err := writeTestFrame(clientConnection, request); err != nil {
		t.Fatalf("writeTestFrame() error = %v", err)
	}
	response := readJSONErrorResponse(t, clientConnection)
	if response.RequestID != "01JMISSING1" {
		t.Errorf("RequestID = %q, want 01JMISSING1", response.RequestID)
	}
	if response.Status != "error" || response.Error.Code != "limiter_not_found" {
		t.Errorf("response = %#v, want limiter_not_found error", response)
	}
}

type jsonErrorResponse struct {
	RequestID string `json:"request_id"`
	Status    string `json:"status"`
	Error     struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func readJSONErrorResponse(t *testing.T, connection net.Conn) jsonErrorResponse {
	t.Helper()
	payload, err := readTestFrame(connection)
	if err != nil {
		t.Fatalf("readTestFrame() error = %v", err)
	}
	var response jsonErrorResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		t.Fatalf("decode JSON error response %q: %v", payload, err)
	}
	return response
}

type echoCodec struct{}

func (echoCodec) Decode(payload []byte) (service.Request, error) {
	return service.Request{
		RequestID: "test-request",
		Type:      service.RequestType("echo"),
		Body:      string(payload),
	}, nil
}

func (echoCodec) Encode(response service.Response) ([]byte, error) {
	return []byte(response.Body.(string)), nil
}

func writeTestFrame(connection net.Conn, payload []byte) error {
	var header [frameLengthSize]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := connection.Write(header[:]); err != nil {
		return err
	}
	_, err := connection.Write(payload)
	return err
}

func readTestFrame(connection net.Conn) ([]byte, error) {
	var header [frameLengthSize]byte
	if _, err := io.ReadFull(connection, header[:]); err != nil {
		return nil, err
	}

	payload := make([]byte, binary.BigEndian.Uint32(header[:]))
	if _, err := io.ReadFull(connection, payload); err != nil {
		return nil, err
	}
	return payload, nil
}
