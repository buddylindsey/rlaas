package service

import (
	"context"
)

// Handler processes one decoded request and returns a response.
type Handler interface {
	Handle(ctx context.Context, request Request) (Response, error)
}

// BasicHandler is the initial application handler. It echoes the decoded body.
type BasicHandler struct{}

// NewBasicHandler creates the initial message handler.
func NewBasicHandler() *BasicHandler {
	return &BasicHandler{}
}

// Handle applies the current business behavior and returns a Response.
func (h *BasicHandler) Handle(_ context.Context, request Request) (Response, error) {
	return Response{
		RequestID: request.RequestID,
		Status:    StatusOK,
		Body:      request.Body,
	}, nil
}
