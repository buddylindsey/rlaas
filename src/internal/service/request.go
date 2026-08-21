package service

type RequestType string

const (
	RequestCreateLimiter RequestType = "create_limiter"
	RequestAcquire       RequestType = "acquire"
	RequestDeleteLimiter RequestType = "delete_limiter"
)

type Request struct {
	RequestID string
	Type      RequestType
	Body      any
}

type CreateLimiterRequest struct {
	Name         string
	LimiterType  string
	TimeWindowMs uint64
	Budget       uint64
}

type AcquireRequest struct {
	Name string
}

type DeleteLimiterRequest struct {
	Name string
}
