package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"rlaas/src/internal/service"
)

type JSONCodec struct{}

// ErrJSONResponseEncodingNotImplemented is returned until JSON response
// encoding is added as the next protocol exercise.
var ErrJSONResponseEncodingNotImplemented = errors.New("JSON response encoding is not implemented")

// NewJSONCodec creates the JSON request codec.
func NewJSONCodec() *JSONCodec {
	return &JSONCodec{}
}

// Decode converts a JSON request envelope to a typed service request.
func (c *JSONCodec) Decode(payload []byte) (service.Request, error) {
	envelope, err := decodeEnvelope(payload)
	if err != nil {
		return service.Request{}, err
	}

	switch envelope.Operation {
	case service.RequestAcquire:
		return decodeAcquire(envelope)
	case service.RequestCreateLimiter:
		return decodeCreateLimiter(envelope)
	case service.RequestDeleteLimiter:
		return decodeDeleteLimiter(envelope)
	default:
		return service.Request{}, fmt.Errorf("unsupported operation %q", envelope.Operation)
	}
}

func decodeEnvelope(payload []byte) (jsonRequestEnvelope, error) {
	var envelope jsonRequestEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return jsonRequestEnvelope{}, fmt.Errorf("decode JSON request: %w", err)
	}
	if strings.TrimSpace(envelope.RequestID) == "" {
		return jsonRequestEnvelope{}, errors.New("request_id is required")
	}
	if envelope.Operation == "" {
		return jsonRequestEnvelope{}, errors.New("operation is required")
	}
	if len(envelope.Body) == 0 {
		return jsonRequestEnvelope{}, errors.New("body is required")
	}
	return envelope, nil
}

func decodeAcquire(envelope jsonRequestEnvelope) (service.Request, error) {
	var body service.AcquireRequest
	if err := json.Unmarshal(envelope.Body, &body); err != nil {
		return service.Request{}, fmt.Errorf("decode acquire body: %w", err)
	}
	if strings.TrimSpace(body.Name) == "" {
		return service.Request{}, errors.New("acquire body.name is required")
	}
	return service.Request{RequestID: envelope.RequestID, Type: service.RequestAcquire, Body: body}, nil
}

func decodeCreateLimiter(envelope jsonRequestEnvelope) (service.Request, error) {
	var wireBody jsonCreateLimiterBody
	if err := json.Unmarshal(envelope.Body, &wireBody); err != nil {
		return service.Request{}, fmt.Errorf("decode create limiter body: %w", err)
	}
	body := service.CreateLimiterRequest{
		Name:         wireBody.Name,
		LimiterType:  wireBody.LimiterType,
		TimeWindowMs: wireBody.TimeWindowMs,
		Budget:       wireBody.Budget,
	}

	var required []string
	if strings.TrimSpace(body.Name) == "" {
		required = append(required, "body.name is required")
	}
	if strings.TrimSpace(body.LimiterType) == "" {
		required = append(required, "body.type is required")
	}
	if body.TimeWindowMs == 0 {
		required = append(required, "body.time_window_ms is required")
	}
	if body.Budget == 0 {
		required = append(required, "body.budget is required")
	}
	if len(required) > 0 {
		return service.Request{}, errors.New("create_limiter requires " + strings.Join(required, ", "))
	}

	return service.Request{RequestID: envelope.RequestID, Type: service.RequestCreateLimiter, Body: body}, nil
}

func decodeDeleteLimiter(envelope jsonRequestEnvelope) (service.Request, error) {
	var body service.DeleteLimiterRequest
	if err := json.Unmarshal(envelope.Body, &body); err != nil {
		return service.Request{}, fmt.Errorf("decode delete limiter body: %w", err)
	}
	if strings.TrimSpace(body.Name) == "" {
		return service.Request{}, errors.New("delete_limiter body.name is required")
	}
	return service.Request{RequestID: envelope.RequestID, Type: service.RequestDeleteLimiter, Body: body}, nil
}

// Encode will be implemented after the remaining request types are added.
func (c *JSONCodec) Encode(service.Response) ([]byte, error) {
	return nil, ErrJSONResponseEncodingNotImplemented
}

type jsonRequestEnvelope struct {
	RequestID string              `json:"request_id"`
	Operation service.RequestType `json:"operation"`
	Body      json.RawMessage     `json:"body"`
}

// jsonCreateLimiterBody is the JSON representation of a create-limiter body.
// Keeping it here prevents JSON field names from leaking into service types.
type jsonCreateLimiterBody struct {
	Name         string `json:"name"`
	LimiterType  string `json:"type"`
	TimeWindowMs uint64 `json:"time_window_ms"`
	Budget       uint64 `json:"budget"`
}
