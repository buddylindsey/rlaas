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
