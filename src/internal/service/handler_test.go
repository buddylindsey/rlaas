package service

import (
	"context"
	"testing"
)

func TestBasicHandlerCreatesLimiterInStore(t *testing.T) {
	store := NewMemoryLimiterStore()
	handler := NewBasicHandlerWithStore(store)
	request := Request{
		RequestID: "01JCREATE1",
		Type:      RequestCreateLimiter,
		Body: CreateLimiterRequest{
			Name:         "github-api",
			LimiterType:  LimiterTypeFixedWindow,
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
	if limiter.Budget != 10 || limiter.TimeWindowMs != 1_000 || limiter.LimiterType != LimiterTypeFixedWindow {
		t.Errorf("stored limiter = %#v, want request configuration", limiter)
	}
}

func TestBasicHandlerRejectsUnsupportedLimiterType(t *testing.T) {
	handler := NewBasicHandler()
	_, err := handler.Handle(context.Background(), Request{
		RequestID: "01JCREATEINVALID",
		Type:      RequestCreateLimiter,
		Body: CreateLimiterRequest{
			Name:         "github-api",
			LimiterType:  "token_bucket",
			TimeWindowMs: 1_000,
			Budget:       10,
		},
	})
	if err == nil {
		t.Fatal("Handle() error = nil, want unsupported limiter type error")
	}
}

func TestBasicHandlerTreatsIdenticalLimiterAsSuccess(t *testing.T) {
	store := NewMemoryLimiterStore()
	handler := NewBasicHandlerWithStore(store)
	request := Request{
		RequestID: "01JCREATE2",
		Type:      RequestCreateLimiter,
		Body: CreateLimiterRequest{
			Name:         "github-api",
			LimiterType:  LimiterTypeFixedWindow,
			TimeWindowMs: 1_000,
			Budget:       10,
		},
	}

	if _, err := handler.Handle(context.Background(), request); err != nil {
		t.Fatalf("first Handle() error = %v", err)
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
}

func TestBasicHandlerRejectsLimiterConfigurationConflict(t *testing.T) {
	store := NewMemoryLimiterStore()
	handler := NewBasicHandlerWithStore(store)
	request := Request{
		RequestID: "01JCREATE3",
		Type:      RequestCreateLimiter,
		Body: CreateLimiterRequest{
			Name:         "github-api",
			LimiterType:  LimiterTypeFixedWindow,
			TimeWindowMs: 1_000,
			Budget:       10,
		},
	}
	if _, err := handler.Handle(context.Background(), request); err != nil {
		t.Fatalf("first Handle() error = %v", err)
	}
	request.Body = CreateLimiterRequest{
		Name:         "github-api",
		LimiterType:  LimiterTypeFixedWindow,
		TimeWindowMs: 2_000,
		Budget:       99,
	}
	response, err := handler.Handle(context.Background(), request)
	if err != nil {
		t.Fatalf("second Handle() error = %v, want nil", err)
	}
	if response.Status != StatusError || response.Error == nil {
		t.Fatalf("response = %#v, want structured configuration conflict", response)
	}
	if response.Error.Code != "limiter_configuration_conflict" {
		t.Errorf("error code = %q, want limiter_configuration_conflict", response.Error.Code)
	}
}

func TestBasicHandlerAcquiresPermit(t *testing.T) {
	handler := NewBasicHandler()
	create := Request{
		RequestID: "01JCREATE4",
		Type:      RequestCreateLimiter,
		Body: CreateLimiterRequest{
			Name:         "github-api",
			LimiterType:  LimiterTypeFixedWindow,
			TimeWindowMs: 1_000,
			Budget:       2,
		},
	}
	if _, err := handler.Handle(context.Background(), create); err != nil {
		t.Fatalf("create Handle() error = %v", err)
	}

	response, err := handler.Handle(context.Background(), Request{
		RequestID: "01JACQUIRE1",
		Type:      RequestAcquire,
		Body:      AcquireRequest{Name: "github-api"},
	})
	if err != nil {
		t.Fatalf("acquire Handle() error = %v", err)
	}
	want := AcquireResponse{Allowed: true, Remaining: 1, RetryAfterMs: 0}
	if response.RequestID != "01JACQUIRE1" || response.Body != want {
		t.Errorf("response = %#v, want body %#v", response, want)
	}
}
