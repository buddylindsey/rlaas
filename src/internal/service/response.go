package service

type ResponseStatus string

const (
	StatusOK    ResponseStatus = "ok"
	StatusError ResponseStatus = "error"
)

type ResponseError struct {
	Code    string
	Message string
}

type Response struct {
	RequestID string
	Status    ResponseStatus
	Body      any
	Error     *ResponseError
}

type CreateLimiterResponse struct {
	Name    string
	Created bool
	Message string
}

type AcquireResponse struct {
	Allowed      bool
	Remaining    uint64
	RetryAfterMs uint64
}

type DeleteLimiterResponse struct {
	Name    string
	Deleted bool
}
