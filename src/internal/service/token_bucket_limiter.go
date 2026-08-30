package service

import (
	"math"
	"sync"
	"time"
)

// TokenBucketLimiter continuously replenishes permits at Budget per TimeWindow
// and allows accumulated permits to burst up to Budget.
type TokenBucketLimiter struct {
	mu         sync.Mutex
	Name       string
	Budget     uint64
	TimeWindow time.Duration

	Tokens     float64
	LastRefill time.Time
}

func newTokenBucketLimiter(configuration LimiterConfiguration) *TokenBucketLimiter {
	return &TokenBucketLimiter{
		Name:       configuration.Name(),
		Budget:     configuration.Budget(),
		TimeWindow: configuration.TimeWindow(),
		Tokens:     float64(configuration.Budget()),
	}
}

func (l *TokenBucketLimiter) Configuration() LimiterConfiguration {
	return LimiterConfiguration{
		name:        l.Name,
		limiterType: LimiterTypeTokenBucket,
		timeWindow:  l.TimeWindow,
		budget:      l.Budget,
	}
}

// Acquire attempts to consume one permit at the current time.
func (l *TokenBucketLimiter) Acquire() AcquireResult {
	return l.acquireAt(time.Now())
}

func (l *TokenBucketLimiter) acquireAt(now time.Time) AcquireResult {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.refill(now)
	if l.Tokens < 1 {
		return AcquireResult{
			Allowed:      false,
			Remaining:    0,
			RetryAfterMs: durationMillisecondsCeil(l.timeUntilNextToken()),
		}
	}

	l.Tokens--
	return AcquireResult{
		Allowed:   true,
		Remaining: uint64(math.Floor(l.Tokens)),
	}
}

func (l *TokenBucketLimiter) refill(now time.Time) {
	if l.LastRefill.IsZero() {
		l.LastRefill = now
		return
	}
	if !now.After(l.LastRefill) {
		return
	}

	elapsed := now.Sub(l.LastRefill)
	if elapsed >= l.TimeWindow {
		l.Tokens = float64(l.Budget)
	} else {
		refilled := float64(elapsed) * float64(l.Budget) / float64(l.TimeWindow)
		l.Tokens = math.Min(float64(l.Budget), l.Tokens+refilled)
	}
	l.LastRefill = now
}

func (l *TokenBucketLimiter) timeUntilNextToken() time.Duration {
	missing := 1 - l.Tokens
	if missing <= 0 {
		return 0
	}
	nanoseconds := math.Ceil(missing * float64(l.TimeWindow) / float64(l.Budget))
	if nanoseconds >= float64(math.MaxInt64) {
		return time.Duration(math.MaxInt64)
	}
	return time.Duration(nanoseconds)
}
