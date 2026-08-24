package service

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFixedWindowFirstAcquireSucceeds(t *testing.T) {
	limiter := &FixedWindowLimiter{Name: "api", Budget: 2, TimeWindow: time.Second}
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)

	result := limiter.acquireAt(now)
	if !result.Allowed || result.Remaining != 1 || result.RetryAfterMs != 0 {
		t.Errorf("Acquire() = %#v, want allowed with one remaining", result)
	}
	if !limiter.WindowStart.Equal(now) {
		t.Errorf("WindowStart = %v, want %v", limiter.WindowStart, now)
	}
}

func TestFixedWindowExhaustsBudgetAndDeniesNextAcquire(t *testing.T) {
	limiter := &FixedWindowLimiter{Name: "api", Budget: 2, TimeWindow: time.Second}
	start := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)

	first := limiter.acquireAt(start)
	second := limiter.acquireAt(start.Add(100 * time.Millisecond))
	denied := limiter.acquireAt(start.Add(250 * time.Millisecond))
	if !first.Allowed || !second.Allowed {
		t.Fatalf("allowed results = %#v, %#v", first, second)
	}
	if second.Remaining != 0 {
		t.Errorf("second Remaining = %d, want 0", second.Remaining)
	}
	if denied.Allowed || denied.Remaining != 0 || denied.RetryAfterMs != 750 {
		t.Errorf("denied Acquire() = %#v, want retry after 750ms", denied)
	}
}

func TestFixedWindowResetsAfterWindow(t *testing.T) {
	limiter := &FixedWindowLimiter{Name: "api", Budget: 1, TimeWindow: time.Second}
	start := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)

	if result := limiter.acquireAt(start); !result.Allowed {
		t.Fatalf("first Acquire() = %#v, want allowed", result)
	}
	result := limiter.acquireAt(start.Add(time.Second))
	if !result.Allowed || result.Remaining != 0 || !limiter.WindowStart.Equal(start.Add(time.Second)) {
		t.Errorf("Acquire() in new window = %#v, start = %v", result, limiter.WindowStart)
	}
}

func TestFixedWindowSeparateLimitersHaveIndependentState(t *testing.T) {
	store := NewMemoryLimiterStore()
	for _, name := range []string{"api-a", "api-b"} {
		err := store.Create(context.Background(), Limiter{
			Name: name, LimiterType: LimiterTypeFixedWindow, TimeWindowMs: 60_000, Budget: 1,
		})
		if err != nil {
			t.Fatalf("Create(%q) error = %v", name, err)
		}
	}

	for _, name := range []string{"api-a", "api-b"} {
		result, err := store.Acquire(context.Background(), name)
		if err != nil || !result.Allowed {
			t.Errorf("Acquire(%q) = %#v, %v; want allowed", name, result, err)
		}
	}
	result, err := store.Acquire(context.Background(), "api-a")
	if err != nil || result.Allowed {
		t.Errorf("second Acquire(api-a) = %#v, %v; want denied", result, err)
	}
}

func TestFixedWindowConcurrentAcquiresDoNotExceedBudget(t *testing.T) {
	const budget = 25
	const workers = 200
	limiter := &FixedWindowLimiter{Name: "api", Budget: budget, TimeWindow: time.Minute}
	now := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)

	var allowed atomic.Uint64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if limiter.acquireAt(now).Allowed {
				allowed.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := allowed.Load(); got != budget {
		t.Errorf("allowed acquires = %d, want %d", got, budget)
	}
	if limiter.Count != budget {
		t.Errorf("Count = %d, want %d", limiter.Count, budget)
	}
}
