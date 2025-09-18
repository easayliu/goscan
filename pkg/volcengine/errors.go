package volcengine

import (
	"errors"
	"fmt"
	"strings"
)

// Package-level error variable definitions
var (
	// API related errors
	ErrRateLimited    = errors.New("volcengine: rate limit exceeded")
	ErrAuthFailed     = errors.New("volcengine: authentication failed")
	ErrInvalidRequest = errors.New("volcengine: invalid request parameters")
	ErrAPIUnavailable = errors.New("volcengine: api service unavailable")

	// Data validation errors
	ErrInvalidBillPeriod = errors.New("volcengine: invalid bill period format")
	ErrEmptyBillPeriod   = errors.New("volcengine: bill period cannot be empty")
	ErrFutureBillPeriod  = errors.New("volcengine: bill period cannot be in the future")
	ErrTooOldBillPeriod  = errors.New("volcengine: bill period is too old (before 2018)")

	// Data processing errors
	ErrNoData           = errors.New("volcengine: no data available")
	ErrDataProcessing   = errors.New("volcengine: data processing failed")
	ErrDataInconsistent = errors.New("volcengine: data inconsistency detected")

	// Configuration errors
	ErrMissingConfig    = errors.New("volcengine: configuration is missing")
	ErrInvalidConfig    = errors.New("volcengine: invalid configuration")
	ErrMissingAccessKey = errors.New("volcengine: access key is required")
	ErrMissingSecretKey = errors.New("volcengine: secret key is required")
)

// APIError represents an API call error
type APIError struct {
	Code    string
	Message string
	Details string
}

func (e *APIError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("volcengine api error: %s - %s (%s)", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("volcengine api error: %s - %s", e.Code, e.Message)
}

// IsRetryable determines if the error is retryable
func (e *APIError) IsRetryable() bool {
	retryableCodes := []string{
		"InternalError",
		"ServiceUnavailable",
		"Throttling",
		"RequestTimeout",
		"AccountFlowLimitExceeded",
	}

	code := strings.ToLower(e.Code)
	for _, retryable := range retryableCodes {
		if strings.Contains(code, strings.ToLower(retryable)) {
			return true
		}
	}
	return false
}

// IsAuthError determines if this is an authentication error
func (e *APIError) IsAuthError() bool {
	authCodes := []string{
		"InvalidAccessKey",
		"SignatureNotMatch",
		"InvalidToken",
		"Forbidden",
		"Unauthorized",
	}

	code := strings.ToLower(e.Code)
	for _, auth := range authCodes {
		if strings.Contains(code, strings.ToLower(auth)) {
			return true
		}
	}
	return false
}

// NewAPIError creates a new API error
func NewAPIError(code, message string) *APIError {
	return &APIError{
		Code:    code,
		Message: message,
	}
}

// NewAPIErrorWithDetails creates an API error with details
func NewAPIErrorWithDetails(code, message, details string) *APIError {
	return &APIError{
		Code:    code,
		Message: message,
		Details: details,
	}
}

// WrapError wraps an error and adds context
func WrapError(err error, context string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", context, err)
}

// IsRateLimitError checks if the error is a rate limiting error
func IsRateLimitError(err error) bool {
	if err == nil {
		return false
	}

	// Check if it's a predefined rate limiting error
	if errors.Is(err, ErrRateLimited) {
		return true
	}

	// Check rate limiting indicators in error message
	errStr := strings.ToLower(err.Error())
	rateLimitIndicators := []string{
		"accountflowlimitexceeded",
		"rate limit",
		"rate exceeded",
		"too many requests",
		"throttling",
		"flow control limit",
	}

	for _, indicator := range rateLimitIndicators {
		if strings.Contains(errStr, indicator) {
			return true
		}
	}

	// Check APIError type
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable() && strings.Contains(strings.ToLower(apiErr.Code), "limit")
	}

	return false
}

// IsRetryableError checks if the error is retryable
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check predefined retryable errors
	retryableErrors := []error{
		ErrRateLimited,
		ErrAPIUnavailable,
	}

	for _, retryable := range retryableErrors {
		if errors.Is(err, retryable) {
			return true
		}
	}

	// Check APIError type
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable()
	}

	// Check error message
	errStr := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"timeout",
		"connection",
		"network",
		"internal error",
		"service unavailable",
		"temporarily unavailable",
		"try again",
		"502", "503", "504",
	}

	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}
