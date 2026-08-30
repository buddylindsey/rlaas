package tcp

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"testing"

	"rlaas/src/internal/protocol"
	"rlaas/src/internal/service"
)

func TestServeConnectionHandlesMultipleMessages(t *testing.T) {
	clientConnection := startTestConnection(t, echoCodec{}, echoHandler{})
	defer clientConnection.Close()

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
	clientConnection := startTestConnection(t, protocol.NewJSONCodec(), service.NewBasicHandler())
	defer clientConnection.Close()

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
	clientConnection := startTestConnection(t, protocol.NewJSONCodec(), service.NewBasicHandler())
	defer clientConnection.Close()

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

func TestServeConnectionPreservesRequestIDForInvalidBody(t *testing.T) {
	clientConnection := startTestConnection(t, protocol.NewJSONCodec(), service.NewBasicHandler())
	defer clientConnection.Close()

	request := []byte(`{
		"request_id":"01JINVALIDBODY",
		"operation":"acquire",
		"body":{}
	}`)
	if err := writeTestFrame(clientConnection, request); err != nil {
		t.Fatalf("writeTestFrame() error = %v", err)
	}
	response := readJSONErrorResponse(t, clientConnection)
	if response.RequestID != "01JINVALIDBODY" {
		t.Errorf("RequestID = %q, want 01JINVALIDBODY", response.RequestID)
	}
	if response.Status != "error" || response.Error.Code != "invalid_request" {
		t.Errorf("response = %#v, want invalid_request error", response)
	}
}

func TestServeConnectionRemainsUsableAfterUnknownField(t *testing.T) {
	clientConnection := startTestConnection(t, protocol.NewJSONCodec(), service.NewBasicHandler())
	defer clientConnection.Close()

	invalid := []byte(`{
		"request_id":"01JUNKNOWNFIELD",
		"operation":"acquire",
		"body":{"name":"missing","unexpected":true}
	}`)
	if err := writeTestFrame(clientConnection, invalid); err != nil {
		t.Fatalf("write invalid frame error = %v", err)
	}
	response := readJSONErrorResponse(t, clientConnection)
	if response.RequestID != "01JUNKNOWNFIELD" || response.Error.Code != "invalid_request" {
		t.Fatalf("invalid response = %#v", response)
	}

	valid := []byte(`{
		"request_id":"01JAFTERINVALID",
		"operation":"acquire",
		"body":{"name":"missing"}
	}`)
	if err := writeTestFrame(clientConnection, valid); err != nil {
		t.Fatalf("write valid frame error = %v", err)
	}
	response = readJSONErrorResponse(t, clientConnection)
	if response.RequestID != "01JAFTERINVALID" || response.Error.Code != "limiter_not_found" {
		t.Errorf("response after invalid request = %#v", response)
	}
}

func TestServeConnectionReturnsJSONWhenLimiterIsMissing(t *testing.T) {
	clientConnection := startTestConnection(t, protocol.NewJSONCodec(), service.NewBasicHandler())
	defer clientConnection.Close()

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

func TestServeConnectionReturnsJSONForInvalidLimiterConfiguration(t *testing.T) {
	clientConnection := startTestConnection(t, protocol.NewJSONCodec(), service.NewBasicHandler())
	defer clientConnection.Close()

	request := []byte(`{
		"request_id":"01JINVALID1",
		"operation":"create_limiter",
		"body":{"name":"api","type":"token_bucket","time_window_ms":1000,"budget":10}
	}`)
	if err := writeTestFrame(clientConnection, request); err != nil {
		t.Fatalf("writeTestFrame() error = %v", err)
	}
	response := readJSONErrorResponse(t, clientConnection)
	if response.RequestID != "01JINVALID1" {
		t.Errorf("RequestID = %q, want 01JINVALID1", response.RequestID)
	}
	if response.Status != "error" || response.Error.Code != "invalid_limiter_configuration" {
		t.Errorf("response = %#v, want invalid_limiter_configuration error", response)
	}
}

func TestServeConnectionDeletesLimiter(t *testing.T) {
	clientConnection := startTestConnection(t, protocol.NewJSONCodec(), service.NewBasicHandler())
	defer clientConnection.Close()

	create := []byte(`{
		"request_id":"01JCREATEDELETE",
		"operation":"create_limiter",
		"body":{"name":"github-api","type":"fixed_window","time_window_ms":1000,"budget":10}
	}`)
	if err := writeTestFrame(clientConnection, create); err != nil {
		t.Fatalf("write create frame error = %v", err)
	}
	if _, err := readTestFrame(clientConnection); err != nil {
		t.Fatalf("read create frame error = %v", err)
	}

	deleteRequest := []byte(`{
		"request_id":"01JDELETE1",
		"operation":"delete_limiter",
		"body":{"name":" GITHUB-API "}
	}`)
	if err := writeTestFrame(clientConnection, deleteRequest); err != nil {
		t.Fatalf("write delete frame error = %v", err)
	}
	payload, err := readTestFrame(clientConnection)
	if err != nil {
		t.Fatalf("read delete frame error = %v", err)
	}
	var deleteResponse struct {
		RequestID string `json:"request_id"`
		Status    string `json:"status"`
		Body      struct {
			Name    string `json:"name"`
			Deleted bool   `json:"deleted"`
		} `json:"body"`
	}
	if err := json.Unmarshal(payload, &deleteResponse); err != nil {
		t.Fatalf("decode delete response %q: %v", payload, err)
	}
	if deleteResponse.RequestID != "01JDELETE1" || deleteResponse.Status != "ok" {
		t.Errorf("delete response envelope = %#v", deleteResponse)
	}
	if deleteResponse.Body.Name != "github-api" || !deleteResponse.Body.Deleted {
		t.Errorf("delete response body = %#v", deleteResponse.Body)
	}

	acquire := []byte(`{
		"request_id":"01JACQUIREDELETED",
		"operation":"acquire",
		"body":{"name":"github-api"}
	}`)
	if err := writeTestFrame(clientConnection, acquire); err != nil {
		t.Fatalf("write acquire frame error = %v", err)
	}
	errorResponse := readJSONErrorResponse(t, clientConnection)
	if errorResponse.RequestID != "01JACQUIREDELETED" || errorResponse.Error.Code != "limiter_not_found" {
		t.Errorf("acquire after delete response = %#v", errorResponse)
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

type echoHandler struct{}

func (echoHandler) Handle(_ context.Context, request service.Request) (service.Response, error) {
	return service.Response{RequestID: request.RequestID, Status: service.StatusOK, Body: request.Body}, nil
}

func startTestConnection(t *testing.T, codec protocol.Codec, handler service.Handler) net.Conn {
	t.Helper()
	serverConnection, clientConnection := net.Pipe()
	server, err := NewServer(DefaultServerConfig(""), codec, handler)
	if err != nil {
		t.Fatalf("NewServer() error = %v", err)
	}
	if !server.addConnection(serverConnection) {
		t.Fatal("addConnection() = false, want true")
	}
	go server.serveConnection(context.Background(), serverConnection)
	return clientConnection
}

func (echoCodec) Decode(payload []byte) (service.Request, error) {
	return service.Request{
		RequestID: "test-request",
		Type:      service.RequestType("echo"),
		Body:      string(payload),
	}, nil
}

func (echoCodec) Encode(response service.Response) ([]byte, error) {
	if body, ok := response.Body.(string); ok {
		return []byte(body), nil
	}
	return []byte("error"), nil
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
