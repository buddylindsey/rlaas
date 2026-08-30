package service

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

// LimiterType identifies a rate-limiting algorithm.
type LimiterType string

const (
	// LimiterTypeFixedWindow applies a fixed budget within each time window.
	LimiterTypeFixedWindow LimiterType = "fixed_window"
	// LimiterTypeTokenBucket continuously refills a bucket over the configured window.
	LimiterTypeTokenBucket LimiterType = "token_bucket"
)

// MaxLimiterNameLength is the maximum encoded length of a normalized limiter
// name. Limiter names are ASCII, so the byte and character limits are equal.
const MaxLimiterNameLength = 128

// maxTokenBucketBudget is the largest integer for which float64 can represent
// every whole-token transition exactly.
const maxTokenBucketBudget = uint64(1<<53 - 1)

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
	if err := validateNormalizedLimiterName(c.name); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidLimiterConfiguration, err)
	}
	switch c.limiterType {
	case LimiterTypeFixedWindow, LimiterTypeTokenBucket:
	default:
		return fmt.Errorf("%w: unsupported limiter type %q", ErrInvalidLimiterConfiguration, c.limiterType)
	}
	if c.timeWindow <= 0 {
		return fmt.Errorf("%w: time window must be greater than zero", ErrInvalidLimiterConfiguration)
	}
	if c.budget == 0 {
		return fmt.Errorf("%w: budget must be greater than zero", ErrInvalidLimiterConfiguration)
	}
	if c.limiterType == LimiterTypeTokenBucket && c.budget > maxTokenBucketBudget {
		return fmt.Errorf("%w: token bucket budget must not exceed %d", ErrInvalidLimiterConfiguration, maxTokenBucketBudget)
	}
	return nil
}

func normalizeLimiterName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func validateNormalizedLimiterName(name string) error {
	if name == "" {
		return errors.New("name is required")
	}
	if name != normalizeLimiterName(name) {
		return errors.New("name must be normalized")
	}
	if len(name) > MaxLimiterNameLength {
		return fmt.Errorf("name must not exceed %d bytes", MaxLimiterNameLength)
	}
	for _, character := range name {
		if !isLimiterNameCharacter(character) {
			return errors.New("name may contain only letters, numbers, periods, colons, underscores, and hyphens")
		}
	}
	return nil
}

func isLimiterNameCharacter(character rune) bool {
	return character >= 'a' && character <= 'z' ||
		character >= '0' && character <= '9' ||
		character == '.' || character == ':' || character == '_' || character == '-'
}

type AcquireResult struct {
	Allowed      bool
	Remaining    uint64
	RetryAfterMs uint64
}

type limiter interface {
	Configuration() LimiterConfiguration
	Acquire() AcquireResult
}

func newLimiter(configuration LimiterConfiguration) (limiter, error) {
	switch configuration.Type() {
	case LimiterTypeFixedWindow:
		return newFixedWindowLimiter(configuration), nil
	case LimiterTypeTokenBucket:
		return newTokenBucketLimiter(configuration), nil
	default:
		return nil, fmt.Errorf("%w: unsupported limiter type %q", ErrInvalidLimiterConfiguration, configuration.Type())
	}
}

func durationMillisecondsCeil(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64((duration-1)/time.Millisecond + 1)
}
