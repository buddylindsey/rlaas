package service

import (
	"context"
	"errors"
	"fmt"
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
	case RequestDeleteLimiter:
		return h.deleteLimiter(ctx, request)
	default:
		return Response{}, fmt.Errorf("unsupported request type %q", request.Type)
	}
}

func (h *BasicHandler) deleteLimiter(ctx context.Context, request Request) (Response, error) {
	body, ok := request.Body.(DeleteLimiterRequest)
	if !ok {
		return Response{}, fmt.Errorf("delete limiter body has type %T, want service.DeleteLimiterRequest", request.Body)
	}
	name := normalizeLimiterName(body.Name)
	if name == "" {
		return Response{}, errors.New("delete limiter name is required")
	}

	if err := h.limiters.Delete(ctx, name); err != nil {
		return Response{}, fmt.Errorf("delete limiter %q: %w", name, err)
	}
	return Response{
		RequestID: request.RequestID,
		Status:    StatusOK,
		Body: DeleteLimiterResponse{
			Name:    name,
			Deleted: true,
		},
	}, nil
}

func (h *BasicHandler) createLimiter(ctx context.Context, request Request) (Response, error) {
	body, ok := request.Body.(CreateLimiterRequest)
	if !ok {
		return Response{}, fmt.Errorf("create limiter body has type %T, want service.CreateLimiterRequest", request.Body)
	}
	limiter, err := NewLimiterConfiguration(
		body.Name,
		body.LimiterType,
		body.TimeWindowMs,
		body.Budget,
	)
	if err != nil {
		return Response{}, err
	}
	if err := h.limiters.Create(ctx, limiter); err != nil {
		if errors.Is(err, ErrLimiterAlreadyExists) {
			return Response{
				RequestID: request.RequestID,
				Status:    StatusOK,
				Body: CreateLimiterResponse{
					Name:    limiter.Name(),
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
					Message: fmt.Sprintf("limiter %q already exists with different configuration", limiter.Name()),
				},
			}, nil
		}
		return Response{}, fmt.Errorf("create limiter %q: %w", limiter.Name(), err)
	}

	return Response{
		RequestID: request.RequestID,
		Status:    StatusOK,
		Body: CreateLimiterResponse{
			Name:    limiter.Name(),
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
	name := normalizeLimiterName(body.Name)
	if name == "" {
		return Response{}, errors.New("acquire limiter name is required")
	}

	result, err := h.limiters.Acquire(ctx, name)
	if err != nil {
		return Response{}, fmt.Errorf("acquire limiter %q: %w", name, err)
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
