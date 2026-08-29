package service

import (
	"context"
	"errors"
)

var (
	ErrLimiterAlreadyExists         = errors.New("limiter already exists")
	ErrLimiterConfigurationConflict = errors.New("limiter configuration conflict")
	ErrLimiterNotFound              = errors.New("limiter not found")
)

// LimiterStore owns limiter persistence. Implementations must be safe for use
// by multiple goroutines.
type LimiterStore interface {
	Create(ctx context.Context, limiter LimiterConfiguration) error
	Get(ctx context.Context, name string) (LimiterConfiguration, error)
	List(ctx context.Context) ([]LimiterConfiguration, error)
	Delete(ctx context.Context, name string) error
	Acquire(ctx context.Context, name string) (AcquireResult, error)
}
