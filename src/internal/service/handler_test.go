package service

import (
	"context"
	"errors"
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
	if limiter.Budget() != 10 || limiter.TimeWindowMs() != 1_000 || limiter.Type() != LimiterTypeFixedWindow {
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

func TestBasicHandlerUsesNormalizedLimiterName(t *testing.T) {
	store := NewMemoryLimiterStore()
	handler := NewBasicHandlerWithStore(store)
	createResponse, err := handler.Handle(context.Background(), Request{
		RequestID: "01JCREATE5",
		Type:      RequestCreateLimiter,
		Body: CreateLimiterRequest{
			Name:         "  GitHub-API  ",
			LimiterType:  LimiterTypeFixedWindow,
			TimeWindowMs: 1_000,
			Budget:       1,
		},
	})
	if err != nil {
		t.Fatalf("create Handle() error = %v", err)
	}
	wantCreate := CreateLimiterResponse{Name: "github-api", Created: true, Message: "limiter created"}
	if createResponse.Body != wantCreate {
		t.Errorf("create response body = %#v, want %#v", createResponse.Body, wantCreate)
	}

	acquireResponse, err := handler.Handle(context.Background(), Request{
		RequestID: "01JACQUIRE2",
		Type:      RequestAcquire,
		Body:      AcquireRequest{Name: " GITHUB-api "},
	})
	if err != nil {
		t.Fatalf("acquire Handle() error = %v", err)
	}
	if got, ok := acquireResponse.Body.(AcquireResponse); !ok || !got.Allowed {
		t.Errorf("acquire response body = %#v, want allowed", acquireResponse.Body)
	}

	duplicateResponse, err := handler.Handle(context.Background(), Request{
		RequestID: "01JCREATE6",
		Type:      RequestCreateLimiter,
		Body: CreateLimiterRequest{
			Name:         "GITHUB-API",
			LimiterType:  LimiterTypeFixedWindow,
			TimeWindowMs: 1_000,
			Budget:       1,
		},
	})
	if err != nil {
		t.Fatalf("duplicate create Handle() error = %v", err)
	}
	wantDuplicate := CreateLimiterResponse{Name: "github-api", Created: false, Message: "limiter already exists"}
	if duplicateResponse.Body != wantDuplicate {
		t.Errorf("duplicate response body = %#v, want %#v", duplicateResponse.Body, wantDuplicate)
	}
	limiters, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("store.List() error = %v", err)
	}
	if len(limiters) != 1 {
		t.Errorf("stored limiter count = %d, want 1", len(limiters))
	}
}

func TestBasicHandlerDeletesLimiter(t *testing.T) {
	store := NewMemoryLimiterStore()
	handler := NewBasicHandlerWithStore(store)
	configuration := newTestLimiterConfiguration(t, "github-api", 1_000, 1)
	if err := store.Create(context.Background(), configuration); err != nil {
		t.Fatalf("store.Create() error = %v", err)
	}

	response, err := handler.Handle(context.Background(), Request{
		RequestID: "01JDELETE1",
		Type:      RequestDeleteLimiter,
		Body:      DeleteLimiterRequest{Name: " GITHUB-API "},
	})
	if err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	want := DeleteLimiterResponse{Name: "github-api", Deleted: true}
	if response.RequestID != "01JDELETE1" || response.Status != StatusOK || response.Body != want {
		t.Errorf("response = %#v, want body %#v", response, want)
	}
	if _, err := store.Get(context.Background(), "github-api"); !errors.Is(err, ErrLimiterNotFound) {
		t.Errorf("store.Get() error = %v, want ErrLimiterNotFound", err)
	}
}

func TestBasicHandlerDeleteMissingLimiterReturnsNotFound(t *testing.T) {
	handler := NewBasicHandler()
	_, err := handler.Handle(context.Background(), Request{
		RequestID: "01JDELETE2",
		Type:      RequestDeleteLimiter,
		Body:      DeleteLimiterRequest{Name: "missing"},
	})
	if !errors.Is(err, ErrLimiterNotFound) {
		t.Fatalf("Handle() error = %v, want ErrLimiterNotFound", err)
	}
}

func TestBasicHandlerRejectsUnsupportedRequestType(t *testing.T) {
	handler := NewBasicHandler()
	if _, err := handler.Handle(context.Background(), Request{Type: RequestType("unknown")}); err == nil {
		t.Fatal("Handle() error = nil, want unsupported request error")
	}
}
