package service

import (
	"sync"
	"time"
)

const LimiterTypeFixedWindow = "fixed_window"

// Limiter is the stored configuration for a named rate limiter.
type Limiter struct {
	Name         string
	LimiterType  string
	TimeWindowMs uint64
	Budget       uint64
}

// FixedWindowLimiter tracks permit usage during a fixed interval.
type FixedWindowLimiter struct {
	mu          sync.Mutex
	Name        string
	Budget      uint64
	TimeWindow  time.Duration
	Count       uint64
	WindowStart time.Time
}

type AcquireResult struct {
	Allowed      bool
	Remaining    uint64
	RetryAfterMs uint64
}

func newFixedWindowLimiter(configuration Limiter) *FixedWindowLimiter {
	return &FixedWindowLimiter{
		Name:       configuration.Name,
		Budget:     configuration.Budget,
		TimeWindow: time.Duration(configuration.TimeWindowMs) * time.Millisecond,
	}
}

// Acquire attempts to consume one permit at the current time.
func (l *FixedWindowLimiter) Acquire() AcquireResult {
	return l.acquireAt(time.Now())
}

func (l *FixedWindowLimiter) acquireAt(now time.Time) AcquireResult {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.WindowStart.IsZero() || now.Sub(l.WindowStart) >= l.TimeWindow {
		l.WindowStart = now
		l.Count = 0
	}

	if l.Count >= l.Budget {
		retryAfter := l.WindowStart.Add(l.TimeWindow).Sub(now)
		if retryAfter < 0 {
			retryAfter = 0
		}
		return AcquireResult{
			Allowed:      false,
			Remaining:    0,
			RetryAfterMs: durationMillisecondsCeil(retryAfter),
		}
	}

	l.Count++
	return AcquireResult{
		Allowed:   true,
		Remaining: l.Budget - l.Count,
	}
}

func durationMillisecondsCeil(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64((duration-1)/time.Millisecond + 1)
}
