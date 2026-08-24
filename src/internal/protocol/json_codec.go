package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"rlaas/src/internal/service"
)

type JSONCodec struct{}

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

// Encode converts a typed service response to a JSON response envelope.
func (c *JSONCodec) Encode(response service.Response) ([]byte, error) {
	body, err := encodeResponseBody(response.Body)
	if err != nil {
		return nil, err
	}

	envelope := jsonResponseEnvelope{
		RequestID: response.RequestID,
		Status:    response.Status,
		Body:      body,
	}
	if response.Error != nil {
		envelope.Error = &jsonResponseError{
			Code:    response.Error.Code,
			Message: response.Error.Message,
		}
	}

	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("encode JSON response: %w", err)
	}
	return payload, nil
}

func encodeResponseBody(body any) (any, error) {
	switch body := body.(type) {
	case nil:
		return nil, nil
	case service.CreateLimiterResponse:
		return jsonCreateLimiterResponseBody{
			Name:    body.Name,
			Created: body.Created,
			Message: body.Message,
		}, nil
	case service.AcquireResponse:
		return jsonAcquireResponseBody{
			Allowed:      body.Allowed,
			Remaining:    body.Remaining,
			RetryAfterMs: body.RetryAfterMs,
		}, nil
	case service.DeleteLimiterResponse:
		return jsonDeleteLimiterResponseBody{
			Name:    body.Name,
			Deleted: body.Deleted,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported response body type %T", body)
	}
}

type jsonRequestEnvelope struct {
	RequestID string              `json:"request_id"`
	Operation service.RequestType `json:"operation"`
	Body      json.RawMessage     `json:"body"`
}

type jsonResponseEnvelope struct {
	RequestID string                 `json:"request_id"`
	Status    service.ResponseStatus `json:"status"`
	Body      any                    `json:"body,omitempty"`
	Error     *jsonResponseError     `json:"error,omitempty"`
}

type jsonResponseError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type jsonCreateLimiterResponseBody struct {
	Name    string `json:"name"`
	Created bool   `json:"created"`
	Message string `json:"message"`
}

type jsonAcquireResponseBody struct {
	Allowed      bool   `json:"allowed"`
	Remaining    uint64 `json:"remaining"`
	RetryAfterMs uint64 `json:"retry_after_ms"`
}

type jsonDeleteLimiterResponseBody struct {
	Name    string `json:"name"`
	Deleted bool   `json:"deleted"`
}

// jsonCreateLimiterBody is the JSON representation of a create-limiter body.
// Keeping it here prevents JSON field names from leaking into service types.
type jsonCreateLimiterBody struct {
	Name         string `json:"name"`
	LimiterType  string `json:"type"`
	TimeWindowMs uint64 `json:"time_window_ms"`
	Budget       uint64 `json:"budget"`
}
