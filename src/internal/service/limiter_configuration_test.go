package service

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestNewLimiterConfigurationNormalizesName(t *testing.T) {
	configuration, err := NewLimiterConfiguration("  GitHub-API  ", LimiterTypeFixedWindow, 1_000, 10)
	if err != nil {
		t.Fatalf("NewLimiterConfiguration() error = %v", err)
	}
	if configuration.Name() != "github-api" {
		t.Errorf("Name() = %q, want github-api", configuration.Name())
	}
	if configuration.Type() != LimiterTypeFixedWindow {
		t.Errorf("Type() = %q, want %q", configuration.Type(), LimiterTypeFixedWindow)
	}
	if configuration.TimeWindow() != time.Second || configuration.TimeWindowMs() != 1_000 {
		t.Errorf("time window = %v/%dms, want 1s/1000ms", configuration.TimeWindow(), configuration.TimeWindowMs())
	}
	if configuration.Budget() != 10 {
		t.Errorf("Budget() = %d, want 10", configuration.Budget())
	}
}

func TestNewLimiterConfigurationRejectsInvalidValues(t *testing.T) {
	maxDurationMs := uint64(math.MaxInt64 / int64(time.Millisecond))
	tests := []struct {
		name         string
		limiterType  LimiterType
		timeWindowMs uint64
		budget       uint64
	}{
		{name: "   ", limiterType: LimiterTypeFixedWindow, timeWindowMs: 1_000, budget: 1},
		{name: "api", limiterType: "token_bucket", timeWindowMs: 1_000, budget: 1},
		{name: "api", limiterType: LimiterTypeFixedWindow, timeWindowMs: 0, budget: 1},
		{name: "api", limiterType: LimiterTypeFixedWindow, timeWindowMs: maxDurationMs + 1, budget: 1},
		{name: "api", limiterType: LimiterTypeFixedWindow, timeWindowMs: 1_000, budget: 0},
	}

	for _, test := range tests {
		_, err := NewLimiterConfiguration(test.name, test.limiterType, test.timeWindowMs, test.budget)
		if !errors.Is(err, ErrInvalidLimiterConfiguration) {
			t.Errorf("NewLimiterConfiguration(%q, %q, %d, %d) error = %v, want ErrInvalidLimiterConfiguration",
				test.name, test.limiterType, test.timeWindowMs, test.budget, err)
		}
	}
}
