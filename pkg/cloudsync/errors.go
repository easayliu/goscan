package cloudsync

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

// Common error types
var (
	ErrCredentialsNotConfigured = errors.New("credentials not configured")
	ErrSyncFailed               = errors.New("sync failed")
	ErrTableCreationFailed      = errors.New("table creation failed")
	ErrDataInconsistent         = errors.New("data inconsistent")
	ErrCleanupFailed            = errors.New("cleanup failed")
	ErrProviderNotSupported     = errors.New("provider not supported")
	ErrInvalidConfiguration     = errors.New("invalid configuration")
	ErrRateLimitExceeded        = errors.New("rate limit exceeded")
	ErrNetworkTimeout           = errors.New("network timeout")
	ErrServiceUnavailable       = errors.New("service unavailable")
)

// DefaultErrorHandler implements ErrorHandler interface
type DefaultErrorHandler struct {
	maxRetries     int
	baseDelay      time.Duration
	maxDelay       time.Duration
	backoffFactor  float64
	retryableCodes []int
}

// NewDefaultErrorHandler creates a new error handler
func NewDefaultErrorHandler() *DefaultErrorHandler {
	return &DefaultErrorHandler{
		maxRetries:     3,
		baseDelay:      time.Second,
		maxDelay:       30 * time.Second,
		backoffFactor:  2.0,
		retryableCodes: []int{429, 500, 502, 503, 504},
	}
}

// HandleError handles an error and determines if it should be retried
func (h *DefaultErrorHandler) HandleError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}

	// Log the error
	return fmt.Errorf("error occurred: %w", err)
}

// ShouldRetry determines if an error should be retried
func (h *DefaultErrorHandler) ShouldRetry(err error) bool {
	if err == nil {
		return false
	}

	return h.IsRetryableError(err)
}

// GetRetryDelay calculates the delay for a retry attempt
func (h *DefaultErrorHandler) GetRetryDelay(attempt int) time.Duration {
	if attempt <= 0 {
		return h.baseDelay
	}

	delay := h.baseDelay
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * h.backoffFactor)
		if delay > h.maxDelay {
			return h.maxDelay
		}
	}

	return delay
}

// IsRetryableError checks if an error is retryable
func (h *DefaultErrorHandler) IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for network errors
	if h.isNetworkError(err) {
		return true
	}

	// Check for HTTP errors
	if h.isRetryableHTTPError(err) {
		return true
	}

	// Check for specific error types
	errorStr := strings.ToLower(err.Error())
	retryableErrors := []string{
		"timeout",
		"connection reset",
		"connection refused",
		"rate limit",
		"service unavailable",
		"internal server error",
		"bad gateway",
		"gateway timeout",
		"too many requests",
	}

	for _, retryableError := range retryableErrors {
		if strings.Contains(errorStr, retryableError) {
			return true
		}
	}

	return false
}

// isNetworkError checks if error is a network-related error
func (h *DefaultErrorHandler) isNetworkError(err error) bool {
	// Check for net.Error types
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout()
	}

	// Check for common network error types
	var opErr *net.OpError
	var dnsErr *net.DNSError
	var addrErr *net.AddrError

	if errors.As(err, &opErr) || errors.As(err, &dnsErr) || errors.As(err, &addrErr) {
		return true
	}

	return false
}

// isRetryableHTTPError checks if HTTP error is retryable
func (h *DefaultErrorHandler) isRetryableHTTPError(err error) bool {
	// Try to extract HTTP status code from error
	errorStr := err.Error()

	// Look for HTTP status codes in error message
	for _, code := range h.retryableCodes {
		codeStr := fmt.Sprintf("%d", code)
		if strings.Contains(errorStr, codeStr) {
			return true
		}

		// Also check for descriptive error messages
		switch code {
		case http.StatusTooManyRequests:
			if strings.Contains(errorStr, "too many requests") || strings.Contains(errorStr, "rate limit") {
				return true
			}
		case http.StatusInternalServerError:
			if strings.Contains(errorStr, "internal server error") {
				return true
			}
		case http.StatusBadGateway:
			if strings.Contains(errorStr, "bad gateway") {
				return true
			}
		case http.StatusServiceUnavailable:
			if strings.Contains(errorStr, "service unavailable") {
				return true
			}
		case http.StatusGatewayTimeout:
			if strings.Contains(errorStr, "gateway timeout") {
				return true
			}
		}
	}

	return false
}

// CloudError represents a cloud provider specific error
type CloudError struct {
	Provider   string
	Code       string
	Message    string
	RequestID  string
	Retryable  bool
	StatusCode int
	Underlying error
}

// Error implements the error interface
func (e *CloudError) Error() string {
	if e.RequestID != "" {
		return fmt.Sprintf("[%s] %s: %s (RequestID: %s)", e.Provider, e.Code, e.Message, e.RequestID)
	}
	return fmt.Sprintf("[%s] %s: %s", e.Provider, e.Code, e.Message)
}

// Unwrap returns the underlying error
func (e *CloudError) Unwrap() error {
	return e.Underlying
}

// IsRetryable returns whether the error is retryable
func (e *CloudError) IsRetryable() bool {
	return e.Retryable
}

// NewCloudError creates a new cloud error
func NewCloudError(provider, code, message string) *CloudError {
	return &CloudError{
		Provider:  provider,
		Code:      code,
		Message:   message,
		Retryable: false,
	}
}

// NewRetryableCloudError creates a new retryable cloud error
func NewRetryableCloudError(provider, code, message string) *CloudError {
	return &CloudError{
		Provider:  provider,
		Code:      code,
		Message:   message,
		Retryable: true,
	}
}

// WrapCloudError wraps an existing error as a cloud error
func WrapCloudError(provider, code string, err error) *CloudError {
	return &CloudError{
		Provider:   provider,
		Code:       code,
		Message:    err.Error(),
		Underlying: err,
		Retryable:  false,
	}
}

// IsCloudError checks if an error is a CloudError
func IsCloudError(err error) bool {
	var cloudErr *CloudError
	return errors.As(err, &cloudErr)
}

// GetCloudError extracts CloudError from an error
func GetCloudError(err error) *CloudError {
	var cloudErr *CloudError
	if errors.As(err, &cloudErr) {
		return cloudErr
	}
	return nil
}

// IsRetryableError is a helper function to check if any error is retryable
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check if it's a cloud error with retryable flag
	if cloudErr := GetCloudError(err); cloudErr != nil {
		return cloudErr.IsRetryable()
	}

	// Use default error handler for other errors
	handler := NewDefaultErrorHandler()
	return handler.IsRetryableError(err)
}

// GetRetryDelay gets the retry delay for an error
func GetRetryDelay(err error, attempt int) time.Duration {
	handler := NewDefaultErrorHandler()
	return handler.GetRetryDelay(attempt)
}
