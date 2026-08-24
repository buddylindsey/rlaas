package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestMemoryLimiterStoreCRUD(t *testing.T) {
	store := NewMemoryLimiterStore()
	ctx := context.Background()
	limiter := Limiter{Name: "github-api", LimiterType: LimiterTypeFixedWindow, TimeWindowMs: 1_000, Budget: 10}

	if err := store.Create(ctx, limiter); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if err := store.Create(ctx, limiter); !errors.Is(err, ErrLimiterAlreadyExists) {
		t.Fatalf("second Create() error = %v, want ErrLimiterAlreadyExists", err)
	}

	got, err := store.Get(ctx, limiter.Name)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got != limiter {
		t.Errorf("Get() = %#v, want %#v", got, limiter)
	}

	if err := store.Delete(ctx, limiter.Name); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := store.Get(ctx, limiter.Name); !errors.Is(err, ErrLimiterNotFound) {
		t.Fatalf("Get() after delete error = %v, want ErrLimiterNotFound", err)
	}
	if err := store.Delete(ctx, limiter.Name); !errors.Is(err, ErrLimiterNotFound) {
		t.Fatalf("second Delete() error = %v, want ErrLimiterNotFound", err)
	}
}

func TestMemoryLimiterStoreListIsSorted(t *testing.T) {
	store := NewMemoryLimiterStore()
	ctx := context.Background()
	for _, name := range []string{"charlie", "alpha", "bravo"} {
		if err := store.Create(ctx, Limiter{Name: name}); err != nil {
			t.Fatalf("Create(%q) error = %v", name, err)
		}
	}

	limiters, err := store.List(ctx)
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	for i, want := range []string{"alpha", "bravo", "charlie"} {
		if limiters[i].Name != want {
			t.Errorf("List()[%d].Name = %q, want %q", i, limiters[i].Name, want)
		}
	}
}

func TestMemoryLimiterStoreConcurrentCreateIsAtomic(t *testing.T) {
	store := NewMemoryLimiterStore()
	const workers = 100

	var created atomic.Int32
	var unexpected atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := store.Create(context.Background(), Limiter{Name: "shared"})
			switch {
			case err == nil:
				created.Add(1)
			case errors.Is(err, ErrLimiterAlreadyExists):
			default:
				unexpected.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := created.Load(); got != 1 {
		t.Errorf("successful creates = %d, want 1", got)
	}
	if got := unexpected.Load(); got != 0 {
		t.Errorf("unexpected errors = %d, want 0", got)
	}
}

func TestMemoryLimiterStoreConcurrentAccess(t *testing.T) {
	store := NewMemoryLimiterStore()
	const workers = 100

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			name := fmt.Sprintf("limiter-%03d", i)
			if err := store.Create(context.Background(), Limiter{Name: name}); err != nil {
				t.Errorf("Create(%q) error = %v", name, err)
				return
			}
			if _, err := store.Get(context.Background(), name); err != nil {
				t.Errorf("Get(%q) error = %v", name, err)
			}
		}()
	}
	wg.Wait()

	limiters, err := store.List(context.Background())
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(limiters) != workers {
		t.Errorf("List() length = %d, want %d", len(limiters), workers)
	}
}
