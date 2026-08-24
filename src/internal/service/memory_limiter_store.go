package service

import (
	"context"
	"sort"
	"sync"
	"time"
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

func (s *MemoryLimiterStore) Create(ctx context.Context, limiter Limiter) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, exists := s.limiters[limiter.Name]; exists {
		if existing.Budget == limiter.Budget && existing.TimeWindow == time.Duration(limiter.TimeWindowMs)*time.Millisecond {
			return ErrLimiterAlreadyExists
		}
		return ErrLimiterConfigurationConflict
	}
	s.limiters[limiter.Name] = newFixedWindowLimiter(limiter)
	return nil
}

func (s *MemoryLimiterStore) Get(ctx context.Context, name string) (Limiter, error) {
	if err := ctx.Err(); err != nil {
		return Limiter{}, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	limiter, exists := s.limiters[name]
	if !exists {
		return Limiter{}, ErrLimiterNotFound
	}
	return limiterConfiguration(limiter), nil
}

func (s *MemoryLimiterStore) List(ctx context.Context) ([]Limiter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	limiters := make([]Limiter, 0, len(s.limiters))
	for _, limiter := range s.limiters {
		limiters = append(limiters, limiterConfiguration(limiter))
	}
	s.mu.RUnlock()

	sort.Slice(limiters, func(i, j int) bool {
		return limiters[i].Name < limiters[j].Name
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

func limiterConfiguration(limiter *FixedWindowLimiter) Limiter {
	return Limiter{
		Name:         limiter.Name,
		LimiterType:  LimiterTypeFixedWindow,
		TimeWindowMs: uint64(limiter.TimeWindow / time.Millisecond),
		Budget:       limiter.Budget,
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
