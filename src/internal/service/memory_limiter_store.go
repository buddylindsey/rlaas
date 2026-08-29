package service

import (
	"context"
	"sort"
	"sync"
)

// MemoryLimiterStore stores limiter configurations in process memory.
type MemoryLimiterStore struct {
	mu       sync.RWMutex
	limiters map[string]*FixedWindowLimiter
}

var _ LimiterStore = (*MemoryLimiterStore)(nil)

func NewMemoryLimiterStore() *MemoryLimiterStore {
	return &MemoryLimiterStore{limiters: make(map[string]*FixedWindowLimiter)}
}

func (s *MemoryLimiterStore) Create(ctx context.Context, limiter LimiterConfiguration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := limiter.Validate(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.limiters[limiter.Name()]; exists {
		if limiterConfiguration(existing) == limiter {
			return ErrLimiterAlreadyExists
		}
		return ErrLimiterConfigurationConflict
	}
	s.limiters[limiter.Name()] = newFixedWindowLimiter(limiter)
	return nil
}

func (s *MemoryLimiterStore) Get(ctx context.Context, name string) (LimiterConfiguration, error) {
	if err := ctx.Err(); err != nil {
		return LimiterConfiguration{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	limiter, exists := s.limiters[name]
	if !exists {
		return LimiterConfiguration{}, ErrLimiterNotFound
	}
	return limiterConfiguration(limiter), nil
}

func (s *MemoryLimiterStore) List(ctx context.Context) ([]LimiterConfiguration, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	limiters := make([]LimiterConfiguration, 0, len(s.limiters))
	for _, limiter := range s.limiters {
		limiters = append(limiters, limiterConfiguration(limiter))
	}
	s.mu.RUnlock()

	sort.Slice(limiters, func(i, j int) bool {
		return limiters[i].Name() < limiters[j].Name()
	})
	return limiters, nil
}

func (s *MemoryLimiterStore) Acquire(ctx context.Context, name string) (AcquireResult, error) {
	if err := ctx.Err(); err != nil {
		return AcquireResult{}, err
	}
	s.mu.RLock()
	limiter, exists := s.limiters[name]
	s.mu.RUnlock()
	if !exists {
		return AcquireResult{}, ErrLimiterNotFound
	}
	return limiter.Acquire(), nil
}

func limiterConfiguration(limiter *FixedWindowLimiter) LimiterConfiguration {
	return LimiterConfiguration{
		name:        limiter.Name,
		limiterType: LimiterTypeFixedWindow,
		timeWindow:  limiter.TimeWindow,
		budget:      limiter.Budget,
	}
}

func (s *MemoryLimiterStore) Delete(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.limiters[name]; !exists {
		return ErrLimiterNotFound
	}
	delete(s.limiters, name)
	return nil
}
