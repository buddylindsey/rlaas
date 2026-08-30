package service

import (
	"sync"
	"time"
)

// FixedWindowLimiter tracks permit usage during a fixed interval.
type FixedWindowLimiter struct {
	mu          sync.Mutex
	Name        string
	Budget      uint64
	TimeWindow  time.Duration
	Count       uint64
	WindowStart time.Time
}

func newFixedWindowLimiter(configuration LimiterConfiguration) *FixedWindowLimiter {
	return &FixedWindowLimiter{
		Name:       configuration.Name(),
		Budget:     configuration.Budget(),
		TimeWindow: configuration.TimeWindow(),
	}
}

func (l *FixedWindowLimiter) Configuration() LimiterConfiguration {
	return LimiterConfiguration{
		name:        l.Name,
		limiterType: LimiterTypeFixedWindow,
		timeWindow:  l.TimeWindow,
		budget:      l.Budget,
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
