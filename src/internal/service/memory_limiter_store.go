package service

import (
	"context"
	"sort"
	"sync"
)

// MemoryLimiterStore stores limiter configurations in process memory.
type MemoryLimiterStore struct {
	mu       sync.RWMutex
	limiters map[string]Limiter
}

var _ LimiterStore = (*MemoryLimiterStore)(nil)

func NewMemoryLimiterStore() *MemoryLimiterStore {
	return &MemoryLimiterStore{limiters: make(map[string]Limiter)}
}

func (s *MemoryLimiterStore) Create(ctx context.Context, limiter Limiter) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.limiters[limiter.Name]; exists {
		return ErrLimiterAlreadyExists
	}
	s.limiters[limiter.Name] = limiter
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
	return limiter, nil
}

func (s *MemoryLimiterStore) List(ctx context.Context) ([]Limiter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	limiters := make([]Limiter, 0, len(s.limiters))
	for _, limiter := range s.limiters {
		limiters = append(limiters, limiter)
	}
	s.mu.RUnlock()

	sort.Slice(limiters, func(i, j int) bool {
		return limiters[i].Name < limiters[j].Name
	})
	return limiters, nil
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
