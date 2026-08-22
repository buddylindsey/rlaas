package service

import (
	"context"
	"testing"
)

func TestBasicHandlerEchoesRequestBody(t *testing.T) {
	handler := NewBasicHandler()
	request := Request{
		RequestID: "01JXYZ456",
		Type:      RequestAcquire,
		Body:      AcquireRequest{Name: "github-api"},
	}

	response, err := handler.Handle(context.Background(), request)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if response.RequestID != request.RequestID {
		t.Errorf("RequestID = %q, want %q", response.RequestID, request.RequestID)
	}
	if response.Status != StatusOK {
		t.Errorf("Status = %q, want %q", response.Status, StatusOK)
	}
	if response.Body != request.Body {
		t.Errorf("Body = %#v, want %#v", response.Body, request.Body)
	}
}

func TestBasicHandlerCreatesLimiterInStore(t *testing.T) {
	store := NewMemoryLimiterStore()
	handler := NewBasicHandlerWithStore(store)
	request := Request{
		RequestID: "01JCREATE1",
		Type:      RequestCreateLimiter,
		Body: CreateLimiterRequest{
			Name:         "github-api",
			LimiterType:  "token_bucket",
			TimeWindowMs: 1_000,
			Budget:       10,
		},
	}

	response, err := handler.Handle(context.Background(), request)
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if got, want := response.Body, (CreateLimiterResponse{Name: "github-api", Created: true, Message: "limiter created"}); got != want {
		t.Errorf("Body = %#v, want %#v", got, want)
	}

	limiter, err := store.Get(context.Background(), "github-api")
	if err != nil {
		t.Fatalf("store.Get() error = %v", err)
	}
	if limiter.Budget != 10 || limiter.TimeWindowMs != 1_000 || limiter.LimiterType != "token_bucket" {
		t.Errorf("stored limiter = %#v, want request configuration", limiter)
	}
}

func TestBasicHandlerTreatsExistingLimiterAsSuccess(t *testing.T) {
	store := NewMemoryLimiterStore()
	handler := NewBasicHandlerWithStore(store)
	request := Request{
		RequestID: "01JCREATE2",
		Type:      RequestCreateLimiter,
		Body: CreateLimiterRequest{
			Name:         "github-api",
			LimiterType:  "token_bucket",
			TimeWindowMs: 1_000,
			Budget:       10,
		},
	}

	if _, err := handler.Handle(context.Background(), request); err != nil {
		t.Fatalf("first Handle() error = %v", err)
	}
	request.Body = CreateLimiterRequest{
		Name:         "github-api",
		LimiterType:  "token_bucket",
		TimeWindowMs: 2_000,
		Budget:       99,
	}
	response, err := handler.Handle(context.Background(), request)
	if err != nil {
		t.Fatalf("second Handle() error = %v, want nil", err)
	}
	want := CreateLimiterResponse{Name: "github-api", Created: false, Message: "limiter already exists"}
	if response.Status != StatusOK {
		t.Errorf("Status = %q, want %q", response.Status, StatusOK)
	}
	if response.Body != want {
		t.Errorf("Body = %#v, want %#v", response.Body, want)
	}
	limiter, err := store.Get(context.Background(), "github-api")
	if err != nil {
		t.Fatalf("store.Get() error = %v", err)
	}
	if limiter.TimeWindowMs != 1_000 || limiter.Budget != 10 {
		t.Errorf("existing limiter was changed: %#v", limiter)
	}
}
