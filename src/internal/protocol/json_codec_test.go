package protocol

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"rlaas/src/internal/service"
)

func TestJSONCodecDecodeAcquire(t *testing.T) {
	codec := NewJSONCodec()

	request, err := codec.Decode([]byte(`{
		"request_id": "01JXYZ456",
		"operation": "acquire",
		"body": {"name": "github-api"}
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if request.RequestID != "01JXYZ456" {
		t.Errorf("RequestID = %q, want %q", request.RequestID, "01JXYZ456")
	}
	if request.Type != service.RequestAcquire {
		t.Errorf("Type = %q, want %q", request.Type, service.RequestAcquire)
	}

	body, ok := request.Body.(service.AcquireRequest)
	if !ok {
		t.Fatalf("Body type = %T, want service.AcquireRequest", request.Body)
	}
	if body.Name != "github-api" {
		t.Errorf("Body.Name = %q, want %q", body.Name, "github-api")
	}
}

func TestJSONCodecDecodeCreateLimiter(t *testing.T) {
	codec := NewJSONCodec()

	request, err := codec.Decode([]byte(`{
		"request_id": "01JCREATE1",
		"operation": "create_limiter",
		"body": {
			"name": "github-api",
			"type": "fixed_window",
			"time_window_ms": 3600000,
			"budget": 200
		}
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if request.Type != service.RequestCreateLimiter {
		t.Errorf("Type = %q, want %q", request.Type, service.RequestCreateLimiter)
	}

	body, ok := request.Body.(service.CreateLimiterRequest)
	if !ok {
		t.Fatalf("Body type = %T, want service.CreateLimiterRequest", request.Body)
	}
	if body.LimiterType != "fixed_window" || body.TimeWindowMs != 3600000 || body.Budget != 200 {
		t.Errorf("Body = %#v, want mapped create-limiter fields", body)
	}
}

func TestJSONCodecDecodeDeleteLimiter(t *testing.T) {
	codec := NewJSONCodec()

	request, err := codec.Decode([]byte(`{
		"request_id": "01JDELETE1",
		"operation": "delete_limiter",
		"body": {"name": "github-api-v1"}
	}`))
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if request.Type != service.RequestDeleteLimiter {
		t.Errorf("Type = %q, want %q", request.Type, service.RequestDeleteLimiter)
	}

	body, ok := request.Body.(service.DeleteLimiterRequest)
	if !ok {
		t.Fatalf("Body type = %T, want service.DeleteLimiterRequest", request.Body)
	}
	if body.Name != "github-api-v1" {
		t.Errorf("Body.Name = %q, want %q", body.Name, "github-api-v1")
	}
}

func TestJSONCodecDecodeRejectsInvalidRequests(t *testing.T) {
	codec := NewJSONCodec()

	for _, payload := range [][]byte{
		[]byte(`{"operation":"acquire","body":{"name":"github-api"}}`),
		[]byte(`{"request_id":"01JXYZ456","body":{"name":"github-api"}}`),
		[]byte(`{"request_id":"01JXYZ456","operation":"acquire","body":{}}`),
		[]byte(`{"request_id":"01JXYZ456","operation":"unknown","body":{"name":"github-api"}}`),
		[]byte(`{"request_id":"01JXYZ456","operation":"delete_limiter","body":{}}`),
		[]byte(`not JSON`),
	} {
		if _, err := codec.Decode(payload); err == nil {
			t.Errorf("Decode(%s) error = nil, want error", payload)
		}
	}
}

func TestJSONCodecDecodeErrorPreservesRequestID(t *testing.T) {
	codec := NewJSONCodec()
	_, err := codec.Decode([]byte(`{
		"request_id":"01JINVALIDBODY",
		"operation":"acquire",
		"body":{}
	}`))
	var decodeErr *DecodeError
	if !errors.As(err, &decodeErr) {
		t.Fatalf("Decode() error = %v, want *DecodeError", err)
	}
	if decodeErr.RequestID != "01JINVALIDBODY" {
		t.Errorf("RequestID = %q, want 01JINVALIDBODY", decodeErr.RequestID)
	}
}

func TestJSONCodecMalformedJSONHasNoDecodeError(t *testing.T) {
	codec := NewJSONCodec()
	_, err := codec.Decode([]byte(`not JSON`))
	var decodeErr *DecodeError
	if errors.As(err, &decodeErr) {
		t.Errorf("Decode() error = %#v, want error without request ID", decodeErr)
	}
}

func TestJSONCodecEncodeCreateLimiterResponse(t *testing.T) {
	codec := NewJSONCodec()

	payload, err := codec.Encode(service.Response{
		RequestID: "01JCREATE1",
		Status:    service.StatusOK,
		Body: service.CreateLimiterResponse{
			Name:    "github-api",
			Created: false,
			Message: "limiter already exists",
		},
	})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	assertJSONEqual(t, payload, []byte(`{
		"request_id":"01JCREATE1",
		"status":"ok",
		"body":{
			"name":"github-api",
			"created":false,
			"message":"limiter already exists"
		}
	}`))
}

func TestJSONCodecEncodeErrorResponse(t *testing.T) {
	codec := NewJSONCodec()

	payload, err := codec.Encode(service.Response{
		RequestID: "01JERROR1",
		Status:    service.StatusError,
		Error: &service.ResponseError{
			Code:    "limiter_not_found",
			Message: "limiter does not exist",
		},
	})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}

	assertJSONEqual(t, payload, []byte(`{
		"request_id":"01JERROR1",
		"status":"error",
		"error":{
			"code":"limiter_not_found",
			"message":"limiter does not exist"
		}
	}`))
}

func TestJSONCodecEncodeRejectsUnsupportedBody(t *testing.T) {
	codec := NewJSONCodec()

	if _, err := codec.Encode(service.Response{Body: struct{}{}}); err == nil {
		t.Fatal("Encode() error = nil, want unsupported body error")
	}
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("decode actual JSON: %v", err)
	}
	var wantValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("decode expected JSON: %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Errorf("JSON = %s, want %s", got, want)
	}
}
