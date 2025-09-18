package clickhouse

import (
	"errors"
	"fmt"
)

// Sentinel errors for common ClickHouse operations
var (
	// ErrConnectionFailed indicates a connection failure
	ErrConnectionFailed = errors.New("clickhouse connection failed")

	// ErrTableNotFound indicates that the requested table doesn't exist
	ErrTableNotFound = errors.New("table not found")

	// ErrInvalidTableName indicates an invalid table name
	ErrInvalidTableName = errors.New("invalid table name")

	// ErrEmptyData indicates that no data was provided for insertion
	ErrEmptyData = errors.New("no data provided for insertion")

	// ErrInvalidBatchSize indicates an invalid batch size
	ErrInvalidBatchSize = errors.New("invalid batch size")

	// ErrClusterNotConfigured indicates that cluster operations require cluster configuration
	ErrClusterNotConfigured = errors.New("cluster name is required for cluster operations")

	// ErrPartitionNotFound indicates that the specified partition doesn't exist
	ErrPartitionNotFound = errors.New("partition not found")

	// ErrUnsupportedPartitionCondition indicates an unsupported partition condition
	ErrUnsupportedPartitionCondition = errors.New("partition condition not supported")

	// ErrTimeout indicates a timeout occurred
	ErrTimeout = errors.New("operation timeout")

	// ErrRetryExhausted indicates that all retry attempts have been exhausted
	ErrRetryExhausted = errors.New("retry attempts exhausted")

	// ErrInvalidConfirmation indicates that the confirmation string is invalid
	ErrInvalidConfirmation = errors.New("invalid confirmation string")
)

// ErrorWrapper wraps errors with additional context
type ErrorWrapper struct {
	Operation string
	Table     string
	Err       error
}

// Error implements the error interface
func (e *ErrorWrapper) Error() string {
	if e.Table != "" {
		return fmt.Sprintf("%s failed for table '%s': %v", e.Operation, e.Table, e.Err)
	}
	return fmt.Sprintf("%s failed: %v", e.Operation, e.Err)
}

// Unwrap returns the wrapped error
func (e *ErrorWrapper) Unwrap() error {
	return e.Err
}

// WrapError wraps an error with operation and table context
func WrapError(operation, table string, err error) error {
	if err == nil {
		return nil
	}
	return &ErrorWrapper{
		Operation: operation,
		Table:     table,
		Err:       err,
	}
}

// WrapConnectionError wraps a connection-related error
func WrapConnectionError(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrConnectionFailed, err)
}

// WrapTableError wraps a table-related error
func WrapTableError(tableName string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("table '%s': %w", tableName, err)
}

// WrapBatchError wraps a batch operation error
func WrapBatchError(batchNum int, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("batch %d: %w", batchNum, err)
}

// WrapPartitionError wraps a partition-related error
func WrapPartitionError(partition string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("partition '%s': %w", partition, err)
}

// IsConnectionError checks if the error is a connection-related error
func IsConnectionError(err error) bool {
	return errors.Is(err, ErrConnectionFailed)
}

// IsTableNotFoundError checks if the error is a table not found error
func IsTableNotFoundError(err error) bool {
	return errors.Is(err, ErrTableNotFound)
}

// IsTimeoutError checks if the error is a timeout error
func IsTimeoutError(err error) bool {
	return errors.Is(err, ErrTimeout)
}

// IsRetryExhaustedError checks if the error is a retry exhausted error
func IsRetryExhaustedError(err error) bool {
	return errors.Is(err, ErrRetryExhausted)
}

// FormatBytesHumanReadable formats bytes into a human-readable string
func FormatBytesHumanReadable(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
