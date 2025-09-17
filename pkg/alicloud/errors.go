package alicloud

import (
	"errors"
	"fmt"
	"strings"
)

// Define package-level error variables (Sentinel Errors)
var (
	// API related errors
	ErrRateLimited    = errors.New("alicloud: rate limit exceeded")
	ErrAuthFailed     = errors.New("alicloud: authentication failed")
	ErrInvalidRequest = errors.New("alicloud: invalid request parameters")
	ErrAPIUnavailable = errors.New("alicloud: api service unavailable")
	ErrAccessDenied   = errors.New("alicloud: access denied")

	// Data validation errors
	ErrInvalidBillingCycle = errors.New("alicloud: invalid billing cycle format")
	ErrEmptyBillingCycle   = errors.New("alicloud: billing cycle cannot be empty")
	ErrFutureBillingCycle  = errors.New("alicloud: billing cycle cannot be in the future")
	ErrTooOldBillingCycle  = errors.New("alicloud: billing cycle is too old (before 2018)")
	ErrInvalidBillingDate  = errors.New("alicloud: invalid billing date format")
	ErrInvalidGranularity  = errors.New("alicloud: invalid granularity")

	// Data processing errors
	ErrNoData           = errors.New("alicloud: no data available")
	ErrDataProcessing   = errors.New("alicloud: data processing failed")
	ErrDataInconsistent = errors.New("alicloud: data inconsistency detected")
	ErrBatchFailed      = errors.New("alicloud: batch processing failed")
	ErrTransformFailed  = errors.New("alicloud: data transformation failed")

	// Configuration errors
	ErrMissingConfig    = errors.New("alicloud: configuration is missing")
	ErrInvalidConfig    = errors.New("alicloud: invalid configuration")
	ErrMissingAccessKey = errors.New("alicloud: access key is required")
	ErrMissingSecretKey = errors.New("alicloud: secret key is required")
	ErrMissingRegion    = errors.New("alicloud: region is required")

	// Table and database errors
	ErrTableNotExists   = errors.New("alicloud: table does not exist")
	ErrTableExists      = errors.New("alicloud: table already exists")
	ErrInvalidTableName = errors.New("alicloud: invalid table name")
	ErrSchemaError      = errors.New("alicloud: table schema error")

	// Pagination errors
	ErrInvalidPageSize  = errors.New("alicloud: invalid page size")
	ErrInvalidNextToken = errors.New("alicloud: invalid next token")
	ErrPaginationFailed = errors.New("alicloud: pagination failed")
)

// APIError represents API call error
type APIError struct {
	Code     string
	Message  string
	Details  string
	HTTPCode int
}

func (e *APIError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("alicloud api error: %s - %s (%s)", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("alicloud api error: %s - %s", e.Code, e.Message)
}

// IsRetryable determines if the error is retryable
func (e *APIError) IsRetryable() bool {
	retryableCodes := []string{
		"Throttling.User",    // User-level throttling
		"Throttling.Api",     // API-level throttling
		"ServiceUnavailable", // Service unavailable
		"InternalError",      // Internal error
		"SystemError",        // System error
	}

	for _, code := range retryableCodes {
		if e.Code == code {
			return true
		}
	}

	// HTTP 5xx errors are usually retryable
	return e.HTTPCode >= 500 && e.HTTPCode < 600
}

// IsRateLimit determines if it's a rate limit error
func (e *APIError) IsRateLimit() bool {
	return strings.Contains(e.Code, "Throttling") || e.Code == "QpsLimitExceeded"
}

// ValidationError represents parameter validation error
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("alicloud validation error: field '%s' with value '%v' - %s",
		e.Field, e.Value, e.Message)
}

// ProcessingError represents data processing error
type ProcessingError struct {
	Stage   string
	Records int
	Err     error
}

func (e *ProcessingError) Error() string {
	return fmt.Sprintf("alicloud processing error at stage '%s' (records: %d): %v",
		e.Stage, e.Records, e.Err)
}

func (e *ProcessingError) Unwrap() error {
	return e.Err
}

// ConfigError represents configuration error
type ConfigError struct {
	Key     string
	Value   interface{}
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("alicloud config error: key '%s' with value '%v' - %s",
		e.Key, e.Value, e.Message)
}

// Error constructor functions

// NewAPIError creates an API error
func NewAPIError(code, message, details string, httpCode int) *APIError {
	return &APIError{
		Code:     code,
		Message:  message,
		Details:  details,
		HTTPCode: httpCode,
	}
}

// NewValidationError creates a validation error
func NewValidationError(field string, value interface{}, message string) *ValidationError {
	return &ValidationError{
		Field:   field,
		Value:   value,
		Message: message,
	}
}

// NewProcessingError creates a processing error
func NewProcessingError(stage string, records int, err error) *ProcessingError {
	return &ProcessingError{
		Stage:   stage,
		Records: records,
		Err:     err,
	}
}

// NewConfigError creates a configuration error
func NewConfigError(key string, value interface{}, message string) *ConfigError {
	return &ConfigError{
		Key:     key,
		Value:   value,
		Message: message,
	}
}

// Error checking utility functions

// IsRetryableError determines if any error is retryable
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check APIError
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable()
	}

	// Check known retryable errors
	if errors.Is(err, ErrRateLimited) ||
		errors.Is(err, ErrAPIUnavailable) ||
		errors.Is(err, ErrBatchFailed) {
		return true
	}

	return false
}

// IsRateLimitError determines if it's a rate limit error
func IsRateLimitError(err error) bool {
	if err == nil {
		return false
	}

	// Check APIError
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRateLimit()
	}

	// Check sentinel error
	return errors.Is(err, ErrRateLimited)
}

// IsValidationError determines if it's a validation error
func IsValidationError(err error) bool {
	if err == nil {
		return false
	}

	var valErr *ValidationError
	return errors.As(err, &valErr) ||
		errors.Is(err, ErrInvalidBillingCycle) ||
		errors.Is(err, ErrInvalidBillingDate) ||
		errors.Is(err, ErrInvalidGranularity) ||
		errors.Is(err, ErrInvalidRequest)
}

// IsConfigError determines if it's a configuration error
func IsConfigError(err error) bool {
	if err == nil {
		return false
	}

	var confErr *ConfigError
	return errors.As(err, &confErr) ||
		errors.Is(err, ErrMissingConfig) ||
		errors.Is(err, ErrInvalidConfig) ||
		errors.Is(err, ErrMissingAccessKey) ||
		errors.Is(err, ErrMissingSecretKey) ||
		errors.Is(err, ErrMissingRegion)
}

// WrapError wraps an error and adds context
func WrapError(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	message := fmt.Sprintf(format, args...)
	return fmt.Errorf("%s: %w", message, err)
}

// CombineErrors combines multiple errors
func CombineErrors(errors ...error) error {
	var nonNilErrors []error
	for _, err := range errors {
		if err != nil {
			nonNilErrors = append(nonNilErrors, err)
		}
	}

	if len(nonNilErrors) == 0 {
		return nil
	}

	if len(nonNilErrors) == 1 {
		return nonNilErrors[0]
	}

	// Build combined error message
	var messages []string
	for _, err := range nonNilErrors {
		messages = append(messages, err.Error())
	}

	return fmt.Errorf("alicloud: multiple errors occurred: %s", strings.Join(messages, "; "))
}
