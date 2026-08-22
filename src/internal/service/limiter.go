package service

// Limiter is the stored configuration for a named rate limiter.
type Limiter struct {
	Name         string
	LimiterType  string
	TimeWindowMs uint64
	Budget       uint64
}
