package service

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenBucketStartsFullAndExhaustsBudget(t *testing.T) {
	limiter := &TokenBucketLimiter{
		Name:       "api",
		Budget:     2,
		TimeWindow: time.Second,
		Tokens:     2,
	}
	start := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)

	first := limiter.acquireAt(start)
	second := limiter.acquireAt(start)
	denied := limiter.acquireAt(start)
	if !first.Allowed || first.Remaining != 1 {
		t.Errorf("first Acquire() = %#v, want allowed with one remaining", first)
	}
	if !second.Allowed || second.Remaining != 0 {
		t.Errorf("second Acquire() = %#v, want allowed with zero remaining", second)
	}
	if denied.Allowed || denied.Remaining != 0 || denied.RetryAfterMs != 500 {
		t.Errorf("denied Acquire() = %#v, want retry after 500ms", denied)
	}
}

func TestTokenBucketRefillsContinuously(t *testing.T) {
	limiter := &TokenBucketLimiter{
		Name:       "api",
		Budget:     4,
		TimeWindow: time.Second,
		Tokens:     4,
	}
	start := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	for range 4 {
		if result := limiter.acquireAt(start); !result.Allowed {
			t.Fatalf("initial Acquire() = %#v, want allowed", result)
		}
	}

	denied := limiter.acquireAt(start.Add(200 * time.Millisecond))
	if denied.Allowed || denied.RetryAfterMs != 50 {
		t.Errorf("Acquire() after 200ms = %#v, want retry after 50ms", denied)
	}
	refilled := limiter.acquireAt(start.Add(250 * time.Millisecond))
	if !refilled.Allowed || refilled.Remaining != 0 {
		t.Errorf("Acquire() after 250ms = %#v, want one refilled token", refilled)
	}
	moreRefilled := limiter.acquireAt(start.Add(750 * time.Millisecond))
	if !moreRefilled.Allowed || moreRefilled.Remaining != 1 {
		t.Errorf("Acquire() after 750ms = %#v, want allowed with one remaining", moreRefilled)
	}
}

func TestTokenBucketTranslatesBudgetAndWindowIntoRefillRate(t *testing.T) {
	limiter := &TokenBucketLimiter{
		Name:       "hourly-api",
		Budget:     200,
		TimeWindow: time.Hour,
		Tokens:     200,
	}
	start := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	for range 200 {
		if result := limiter.acquireAt(start); !result.Allowed {
			t.Fatalf("initial Acquire() = %#v, want all 200 permits allowed", result)
		}
	}

	denied := limiter.acquireAt(start)
	if denied.Allowed || denied.RetryAfterMs != 18_000 {
		t.Errorf("exhausted Acquire() = %#v, want one token every 18000ms", denied)
	}
	refilled := limiter.acquireAt(start.Add(18 * time.Second))
	if !refilled.Allowed {
		t.Errorf("Acquire() after 18s = %#v, want refilled permit", refilled)
	}
}

func TestTokenBucketRefillDoesNotExceedCapacity(t *testing.T) {
	limiter := &TokenBucketLimiter{
		Name:       "api",
		Budget:     2,
		TimeWindow: time.Second,
		Tokens:     2,
	}
	start := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)
	if result := limiter.acquireAt(start); !result.Allowed {
		t.Fatalf("first Acquire() = %#v, want allowed", result)
	}

	result := limiter.acquireAt(start.Add(10 * time.Second))
	if !result.Allowed || result.Remaining != 1 {
		t.Errorf("Acquire() after idle period = %#v, want full bucket before acquire", result)
	}
}

func TestTokenBucketConcurrentAcquiresDoNotExceedCapacity(t *testing.T) {
	const budget = 25
	const workers = 200
	limiter := &TokenBucketLimiter{
		Name:       "api",
		Budget:     budget,
		TimeWindow: time.Minute,
		Tokens:     budget,
	}
	now := time.Date(2026, time.August, 30, 12, 0, 0, 0, time.UTC)

	var allowed atomic.Uint64
	var wg sync.WaitGroup
	for range workers {
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
}
