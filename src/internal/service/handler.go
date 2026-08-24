package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// Handler processes one decoded request and returns a response.
type Handler interface {
	Handle(ctx context.Context, request Request) (Response, error)
}

// BasicHandler coordinates application behavior through a LimiterStore.
type BasicHandler struct {
	limiters LimiterStore
}

// NewBasicHandler creates a handler backed by an in-memory limiter store.
func NewBasicHandler() *BasicHandler {
	return NewBasicHandlerWithStore(NewMemoryLimiterStore())
}

// NewBasicHandlerWithStore creates a handler backed by the supplied store.
func NewBasicHandlerWithStore(limiters LimiterStore) *BasicHandler {
	return &BasicHandler{limiters: limiters}
}

// Handle applies the current business behavior and returns a Response.
func (h *BasicHandler) Handle(ctx context.Context, request Request) (Response, error) {
	switch request.Type {
	case RequestCreateLimiter:
		return h.createLimiter(ctx, request)
	case RequestAcquire:
		return h.acquire(ctx, request)
	default:
		return Response{
			RequestID: request.RequestID,
			Status:    StatusOK,
			Body:      request.Body,
		}, nil
	}
}

func (h *BasicHandler) createLimiter(ctx context.Context, request Request) (Response, error) {
	body, ok := request.Body.(CreateLimiterRequest)
	if !ok {
		return Response{}, fmt.Errorf("create limiter body has type %T, want service.CreateLimiterRequest", request.Body)
	}
	if err := validateCreateLimiter(body); err != nil {
		return Response{}, err
	}

	limiter := Limiter{
		Name:         body.Name,
		LimiterType:  body.LimiterType,
		TimeWindowMs: body.TimeWindowMs,
		Budget:       body.Budget,
	}
	if err := h.limiters.Create(ctx, limiter); err != nil {
		if errors.Is(err, ErrLimiterAlreadyExists) {
			return Response{
				RequestID: request.RequestID,
				Status:    StatusOK,
				Body: CreateLimiterResponse{
					Name:    limiter.Name,
					Created: false,
					Message: "limiter already exists",
				},
			}, nil
		}
		if errors.Is(err, ErrLimiterConfigurationConflict) {
			return Response{
				RequestID: request.RequestID,
				Status:    StatusError,
				Error: &ResponseError{
					Code:    "limiter_configuration_conflict",
					Message: fmt.Sprintf("limiter %q already exists with different configuration", limiter.Name),
				},
			}, nil
		}
		return Response{}, fmt.Errorf("create limiter %q: %w", limiter.Name, err)
	}

	return Response{
		RequestID: request.RequestID,
		Status:    StatusOK,
		Body: CreateLimiterResponse{
			Name:    limiter.Name,
			Created: true,
			Message: "limiter created",
		},
	}, nil
}

func (h *BasicHandler) acquire(ctx context.Context, request Request) (Response, error) {
	body, ok := request.Body.(AcquireRequest)
	if !ok {
		return Response{}, fmt.Errorf("acquire body has type %T, want service.AcquireRequest", request.Body)
	}
	if strings.TrimSpace(body.Name) == "" {
		return Response{}, errors.New("acquire limiter name is required")
	}

	result, err := h.limiters.Acquire(ctx, body.Name)
	if err != nil {
		return Response{}, fmt.Errorf("acquire limiter %q: %w", body.Name, err)
	}
	return Response{
		RequestID: request.RequestID,
		Status:    StatusOK,
		Body: AcquireResponse{
			Allowed:      result.Allowed,
			Remaining:    result.Remaining,
			RetryAfterMs: result.RetryAfterMs,
		},
	}, nil
}

func validateCreateLimiter(body CreateLimiterRequest) error {
	if strings.TrimSpace(body.Name) == "" {
		return errors.New("create limiter name is required")
	}
	if body.LimiterType != LimiterTypeFixedWindow {
		return fmt.Errorf("unsupported limiter type %q", body.LimiterType)
	}
	if body.TimeWindowMs == 0 {
		return errors.New("create limiter time window must be greater than zero")
	}
	maxDurationMs := uint64(math.MaxInt64 / int64(time.Millisecond))
	if body.TimeWindowMs > maxDurationMs {
		return errors.New("create limiter time window is too large")
	}
	if body.Budget == 0 {
		return errors.New("create limiter budget must be greater than zero")
	}
	return nil
}
