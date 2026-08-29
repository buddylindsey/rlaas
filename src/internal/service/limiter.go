package service

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// LimiterType identifies a rate-limiting algorithm.
type LimiterType string

// LimiterTypeFixedWindow applies a fixed budget within each time window.
const LimiterTypeFixedWindow LimiterType = "fixed_window"

// ErrInvalidLimiterConfiguration identifies a configuration that violates
// limiter domain invariants.
var ErrInvalidLimiterConfiguration = errors.New("invalid limiter configuration")

// LimiterConfiguration is a validated, immutable limiter configuration.
type LimiterConfiguration struct {
	name        string
	limiterType LimiterType
	timeWindow  time.Duration
	budget      uint64
}

// NewLimiterConfiguration normalizes and validates a limiter configuration.
func NewLimiterConfiguration(name string, limiterType LimiterType, timeWindowMs, budget uint64) (LimiterConfiguration, error) {
	if timeWindowMs > uint64(math.MaxInt64/int64(time.Millisecond)) {
		return LimiterConfiguration{}, fmt.Errorf("%w: time window is too large", ErrInvalidLimiterConfiguration)
	}

	configuration := LimiterConfiguration{
		name:        normalizeLimiterName(name),
		limiterType: limiterType,
		timeWindow:  time.Duration(timeWindowMs) * time.Millisecond,
		budget:      budget,
	}
	if err := configuration.Validate(); err != nil {
		return LimiterConfiguration{}, err
	}
	return configuration, nil
}

// Name returns the normalized limiter name.
func (c LimiterConfiguration) Name() string { return c.name }

// Type returns the configured limiter algorithm.
func (c LimiterConfiguration) Type() LimiterType { return c.limiterType }

// TimeWindow returns the configured window duration.
func (c LimiterConfiguration) TimeWindow() time.Duration { return c.timeWindow }

// TimeWindowMs returns the configured window duration in milliseconds.
func (c LimiterConfiguration) TimeWindowMs() uint64 {
	return uint64(c.timeWindow / time.Millisecond)
}

// Budget returns the number of permits available in each window.
func (c LimiterConfiguration) Budget() uint64 { return c.budget }

// Validate reports whether the configuration satisfies domain invariants.
func (c LimiterConfiguration) Validate() error {
	if c.name == "" || c.name != normalizeLimiterName(c.name) {
		return fmt.Errorf("%w: name is required", ErrInvalidLimiterConfiguration)
	}
	if c.limiterType != LimiterTypeFixedWindow {
		return fmt.Errorf("%w: unsupported limiter type %q", ErrInvalidLimiterConfiguration, c.limiterType)
	}
	if c.timeWindow <= 0 {
		return fmt.Errorf("%w: time window must be greater than zero", ErrInvalidLimiterConfiguration)
	}
	if c.budget == 0 {
		return fmt.Errorf("%w: budget must be greater than zero", ErrInvalidLimiterConfiguration)
	}
	return nil
}

func normalizeLimiterName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
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

func newFixedWindowLimiter(configuration LimiterConfiguration) *FixedWindowLimiter {
	return &FixedWindowLimiter{
		Name:       configuration.Name(),
		Budget:     configuration.Budget(),
		TimeWindow: configuration.TimeWindow(),
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
