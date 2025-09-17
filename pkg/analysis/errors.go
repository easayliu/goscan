package analysis

import (
	"errors"
	"fmt"
)

// Sentinel errors - using sentinel error pattern
var (
	// ErrInvalidConfig invalid configuration error
	ErrInvalidConfig = errors.New("invalid configuration")

	// ErrConnectionNotInitialized connection not initialized error
	ErrConnectionNotInitialized = errors.New("connection not initialized")

	// ErrDirectConnectionFailed direct connection failed error
	ErrDirectConnectionFailed = errors.New("direct connection failed")

	// ErrQueryTimeout query timeout error
	ErrQueryTimeout = errors.New("query timeout")

	// ErrNoDataFound no data found error
	ErrNoDataFound = errors.New("no data found")

	// ErrInvalidTableInfo invalid table info error
	ErrInvalidTableInfo = errors.New("invalid table info")

	// ErrQueryExecutionFailed query execution failed error
	ErrQueryExecutionFailed = errors.New("query execution failed")

	// ErrDataScanFailed data scan failed error
	ErrDataScanFailed = errors.New("data scan failed")

	// ErrCacheOperationFailed cache operation failed error
	ErrCacheOperationFailed = errors.New("cache operation failed")

	// ErrResourceCleanupFailed resource cleanup failed error
	ErrResourceCleanupFailed = errors.New("resource cleanup failed")
)

// ConfigError configuration related error
type ConfigError struct {
	Field   string
	Message string
	Err     error
}

func (e *ConfigError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("configuration error [%s]: %s: %v", e.Field, e.Message, e.Err)
	}
	return fmt.Sprintf("configuration error [%s]: %s", e.Field, e.Message)
}

func (e *ConfigError) Unwrap() error {
	return e.Err
}

// NewConfigError creates configuration error
func NewConfigError(field, message string, err error) *ConfigError {
	return &ConfigError{
		Field:   field,
		Message: message,
		Err:     err,
	}
}

// QueryError query related error
type QueryError struct {
	Table   string
	Query   string
	Message string
	Err     error
}

func (e *QueryError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("query error [table: %s]: %s: %v", e.Table, e.Message, e.Err)
	}
	return fmt.Sprintf("query error [table: %s]: %s", e.Table, e.Message)
}

func (e *QueryError) Unwrap() error {
	return e.Err
}

// NewQueryError creates query error
func NewQueryError(table, query, message string, err error) *QueryError {
	return &QueryError{
		Table:   table,
		Query:   query,
		Message: message,
		Err:     err,
	}
}

// ConnectionError connection related error
type ConnectionError struct {
	Type    string // "direct", "legacy", "pool"
	Address string
	Message string
	Err     error
}

func (e *ConnectionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("connection error [%s:%s]: %s: %v", e.Type, e.Address, e.Message, e.Err)
	}
	return fmt.Sprintf("connection error [%s:%s]: %s", e.Type, e.Address, e.Message)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// NewConnectionError creates connection error
func NewConnectionError(connType, address, message string, err error) *ConnectionError {
	return &ConnectionError{
		Type:    connType,
		Address: address,
		Message: message,
		Err:     err,
	}
}

// ValidationError validation related error
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error [%s=%v]: %s", e.Field, e.Value, e.Message)
}

// NewValidationError creates validation error
func NewValidationError(field string, value interface{}, message string) *ValidationError {
	return &ValidationError{
		Field:   field,
		Value:   value,
		Message: message,
	}
}

// wrapError wraps error with context information
func wrapError(err error, message string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	if len(args) > 0 {
		message = fmt.Sprintf(message, args...)
	}

	return fmt.Errorf("%s: %w", message, err)
}

// isTemporaryError determines if error is temporary
func isTemporaryError(err error) bool {
	type temporary interface {
		Temporary() bool
	}

	if te, ok := err.(temporary); ok && te.Temporary() {
		return true
	}

	// Check common temporary errors
	switch {
	case errors.Is(err, ErrQueryTimeout):
		return true
	case errors.Is(err, ErrConnectionNotInitialized):
		return true
	default:
		return false
	}
}

// isRetryableError determines if error is retryable
func isRetryableError(err error) bool {
	if isTemporaryError(err) {
		return true
	}

	// Check retryable error types
	var queryErr *QueryError
	var connErr *ConnectionError

	switch {
	case errors.As(err, &queryErr):
		// Query timeout and similar situations can be retried
		return errors.Is(queryErr.Err, ErrQueryTimeout)
	case errors.As(err, &connErr):
		// Connection failures can be retried
		return true
	default:
		return false
	}
}
