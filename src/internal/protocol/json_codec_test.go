package protocol

import (
	"errors"
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
			"type": "token_bucket",
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
	if body.LimiterType != "token_bucket" || body.TimeWindowMs != 3600000 || body.Budget != 200 {
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

func TestJSONCodecEncodeIsExplicitlyUnimplemented(t *testing.T) {
	codec := NewJSONCodec()

	_, err := codec.Encode(service.Response{})
	if !errors.Is(err, ErrJSONResponseEncodingNotImplemented) {
		t.Fatalf("Encode() error = %v, want ErrJSONResponseEncodingNotImplemented", err)
	}
}
