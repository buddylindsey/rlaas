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
