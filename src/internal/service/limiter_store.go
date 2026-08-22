package service

import (
	"context"
	"errors"
)

var (
	ErrLimiterAlreadyExists = errors.New("limiter already exists")
	ErrLimiterNotFound      = errors.New("limiter not found")
)

// LimiterStore owns limiter persistence. Implementations must be safe for use
// by multiple goroutines.
type LimiterStore interface {
	Create(ctx context.Context, limiter Limiter) error
	Get(ctx context.Context, name string) (Limiter, error)
	List(ctx context.Context) ([]Limiter, error)
	Delete(ctx context.Context, name string) error
}
