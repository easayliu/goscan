package wechat

import (
	"errors"
	"fmt"
)

// Define sentinel errors using errors.New to create immutable error instances
var (
	// ErrWebhookURLEmpty indicates webhook URL is empty
	ErrWebhookURLEmpty = errors.New("wechat work webhook URL not configured")

	// ErrInvalidConfig indicates invalid configuration
	ErrInvalidConfig = errors.New("invalid configuration parameters")

	// ErrMarshalMessage indicates message serialization failed
	ErrMarshalMessage = errors.New("failed to serialize message")

	// ErrCreateRequest indicates HTTP request creation failed
	ErrCreateRequest = errors.New("failed to create HTTP request")

	// ErrSendRequest indicates HTTP request sending failed
	ErrSendRequest = errors.New("failed to send HTTP request")

	// ErrReadResponse indicates response reading failed
	ErrReadResponse = errors.New("failed to read response")

	// ErrUnmarshalResponse indicates response parsing failed
	ErrUnmarshalResponse = errors.New("failed to parse response")

	// ErrAPIError indicates WeChat Work API error
	ErrAPIError = errors.New("wechat work API error")

	// ErrHTTPStatusError indicates HTTP status code error
	ErrHTTPStatusError = errors.New("HTTP request failed")

	// ErrRetryExceeded indicates retry attempts exceeded
	ErrRetryExceeded = errors.New("failed to send wechat work message after retries")
)

// APIError represents WeChat Work API error type
type APIError struct {
	Code    int    `json:"errcode"`
	Message string `json:"errmsg"`
}

// Error implements the error interface
func (e *APIError) Error() string {
	return fmt.Sprintf("wechat work API error: %d %s", e.Code, e.Message)
}

// HTTPError represents HTTP error type
type HTTPError struct {
	StatusCode int
	Status     string
	Body       string
}

// Error implements the error interface
func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP request failed: %d %s, response: %s",
		e.StatusCode, e.Status, e.Body)
}

// RetryError represents retry error type
type RetryError struct {
	Attempts int
	LastErr  error
}

// Error implements the error interface
func (e *RetryError) Error() string {
	return fmt.Sprintf("failed to send wechat work message after %d retries: %v",
		e.Attempts, e.LastErr)
}

// Unwrap supports errors.Unwrap
func (e *RetryError) Unwrap() error {
	return e.LastErr
}

// NewAPIError creates an API error
func NewAPIError(code int, message string) *APIError {
	return &APIError{
		Code:    code,
		Message: message,
	}
}

// NewHTTPError creates an HTTP error
func NewHTTPError(statusCode int, status, body string) *HTTPError {
	return &HTTPError{
		StatusCode: statusCode,
		Status:     status,
		Body:       body,
	}
}

// NewRetryError creates a retry error
func NewRetryError(attempts int, lastErr error) *RetryError {
	return &RetryError{
		Attempts: attempts,
		LastErr:  lastErr,
	}
}
