package resultutils

import (
	"fmt"
	"time"
)

// TaskResult represents a task execution result
// This is a common interface for all task results
type TaskResult struct {
	ID               string                 `json:"id"`
	Type             string                 `json:"type"`
	Status           string                 `json:"status"`
	RecordsProcessed int                    `json:"records_processed"`
	RecordsFetched   int                    `json:"records_fetched"`
	Duration         time.Duration          `json:"duration"`
	Success          bool                   `json:"success"`
	Message          string                 `json:"message"`
	Error            string                 `json:"error,omitempty"`
	StartedAt        time.Time              `json:"started_at"`
	CompletedAt      time.Time              `json:"completed_at"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

// TaskStatus constants for consistent status handling
const (
	StatusPending   = "pending"
	StatusRunning   = "running"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
	StatusSkipped   = "skipped"
)

// NewTaskResult creates a new task result with basic information
func NewTaskResult(id, taskType, message string) *TaskResult {
	return &TaskResult{
		ID:        id,
		Type:      taskType,
		Status:    StatusRunning,
		Message:   message,
		StartedAt: time.Now(),
		Metadata:  make(map[string]interface{}),
	}
}

// SetTaskFailed sets a task result to failed status with error information
func SetTaskFailed(result *TaskResult, err error) {
	if result == nil {
		return
	}

	result.Status = StatusFailed
	result.Success = false
	result.CompletedAt = time.Now()
	result.Duration = time.Since(result.StartedAt)

	if err != nil {
		result.Error = err.Error()
	}
}

// SetTaskFailedWithMessage sets a task result to failed status with custom message and error
func SetTaskFailedWithMessage(result *TaskResult, message string, err error) {
	SetTaskFailed(result, err)
	if result != nil && message != "" {
		result.Message = message
	}
}

// SetTaskSuccess sets a task result to successful status
func SetTaskSuccess(result *TaskResult, message string) {
	if result == nil {
		return
	}

	result.Status = StatusCompleted
	result.Success = true
	result.CompletedAt = time.Now()
	result.Duration = time.Since(result.StartedAt)

	if message != "" {
		result.Message = message
	}
}

// SetTaskSkipped sets a task result to skipped status
func SetTaskSkipped(result *TaskResult, reason string) {
	if result == nil {
		return
	}

	result.Status = StatusSkipped
	result.Success = true
	result.CompletedAt = time.Now()
	result.Duration = time.Since(result.StartedAt)

	if reason != "" {
		result.Message = reason
	}
}

// SetTaskRunning sets a task result to running status
func SetTaskRunning(result *TaskResult) {
	if result == nil {
		return
	}

	result.Status = StatusRunning
	result.Success = false
}

// UpdateRecordsProcessed updates the number of records processed
func UpdateRecordsProcessed(result *TaskResult, processed, fetched int) {
	if result == nil {
		return
	}

	result.RecordsProcessed = processed
	result.RecordsFetched = fetched
}

// AddMetadata adds metadata to the task result
func AddMetadata(result *TaskResult, key string, value interface{}) {
	if result == nil {
		return
	}

	if result.Metadata == nil {
		result.Metadata = make(map[string]interface{})
	}

	result.Metadata[key] = value
}

// GetMetadata retrieves metadata from the task result
func GetMetadata(result *TaskResult, key string) (interface{}, bool) {
	if result == nil || result.Metadata == nil {
		return nil, false
	}

	value, exists := result.Metadata[key]
	return value, exists
}

// WrapError wraps an error with additional context
func WrapError(err error, context string) error {
	if err == nil {
		return nil
	}

	if context == "" {
		return err
	}

	return fmt.Errorf("%s: %w", context, err)
}

// WrapErrorf wraps an error with formatted context
func WrapErrorf(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}

	context := fmt.Sprintf(format, args...)
	return fmt.Errorf("%s: %w", context, err)
}

// ValidationError represents a validation error with structured information
type ValidationError struct {
	Field   string      `json:"field"`
	Value   interface{} `json:"value"`
	Message string      `json:"message"`
}

// Error implements the error interface
func (ve *ValidationError) Error() string {
	return fmt.Sprintf("validation failed for field '%s' with value '%v': %s",
		ve.Field, ve.Value, ve.Message)
}

// NewValidationError creates a new validation error
func NewValidationError(field string, value interface{}, message string) error {
	return &ValidationError{
		Field:   field,
		Value:   value,
		Message: message,
	}
}

// IsValidationError checks if an error is a validation error
func IsValidationError(err error) bool {
	_, ok := err.(*ValidationError)
	return ok
}

// GetValidationError extracts validation error details if the error is a validation error
func GetValidationError(err error) (*ValidationError, bool) {
	ve, ok := err.(*ValidationError)
	return ve, ok
}

// CalculateSuccessRate calculates success rate as a percentage
func CalculateSuccessRate(successful, total int) float64 {
	if total == 0 {
		return 0.0
	}
	return float64(successful) / float64(total) * 100.0
}

// FormatDuration formats a duration for human-readable display
func FormatDuration(duration time.Duration) string {
	if duration < time.Second {
		return fmt.Sprintf("%.2fms", float64(duration.Nanoseconds())/1e6)
	}
	if duration < time.Minute {
		return fmt.Sprintf("%.2fs", duration.Seconds())
	}
	if duration < time.Hour {
		return fmt.Sprintf("%.2fm", duration.Minutes())
	}
	return fmt.Sprintf("%.2fh", duration.Hours())
}

// CreateTaskSummary creates a summary string for the task result
func CreateTaskSummary(result *TaskResult) string {
	if result == nil {
		return "No result available"
	}

	status := result.Status
	if result.Success {
		status = "successful"
	}

	return fmt.Sprintf("Task %s (%s) %s: processed %d/%d records in %s",
		result.ID, result.Type, status,
		result.RecordsProcessed, result.RecordsFetched,
		FormatDuration(result.Duration))
}

// IsTaskCompleted checks if a task has completed (successfully or failed)
func IsTaskCompleted(result *TaskResult) bool {
	if result == nil {
		return false
	}

	return result.Status == StatusCompleted ||
		result.Status == StatusFailed ||
		result.Status == StatusSkipped ||
		result.Status == StatusCancelled
}

// IsTaskSuccessful checks if a task completed successfully
func IsTaskSuccessful(result *TaskResult) bool {
	if result == nil {
		return false
	}

	return (result.Status == StatusCompleted || result.Status == StatusSkipped) &&
		result.Success
}

// IsTaskFailed checks if a task failed
func IsTaskFailed(result *TaskResult) bool {
	if result == nil {
		return false
	}

	return result.Status == StatusFailed ||
		(result.Status == StatusCompleted && !result.Success)
}

// CopyResult creates a copy of a task result
func CopyResult(source *TaskResult) *TaskResult {
	if source == nil {
		return nil
	}

	result := &TaskResult{
		ID:               source.ID,
		Type:             source.Type,
		Status:           source.Status,
		RecordsProcessed: source.RecordsProcessed,
		RecordsFetched:   source.RecordsFetched,
		Duration:         source.Duration,
		Success:          source.Success,
		Message:          source.Message,
		Error:            source.Error,
		StartedAt:        source.StartedAt,
		CompletedAt:      source.CompletedAt,
	}

	// Deep copy metadata
	if source.Metadata != nil {
		result.Metadata = make(map[string]interface{})
		for k, v := range source.Metadata {
			result.Metadata[k] = v
		}
	}

	return result
}

// MergeResults merges multiple task results into a summary result
func MergeResults(results []*TaskResult, summaryID, summaryType string) *TaskResult {
	if len(results) == 0 {
		return NewTaskResult(summaryID, summaryType, "No tasks to merge")
	}

	merged := NewTaskResult(summaryID, summaryType, "Merged task results")

	var totalProcessed, totalFetched int
	var allSuccessful = true
	var allMessages []string
	var earliestStart time.Time
	var latestEnd time.Time

	for i, result := range results {
		if result == nil {
			continue
		}

		totalProcessed += result.RecordsProcessed
		totalFetched += result.RecordsFetched

		if !result.Success {
			allSuccessful = false
		}

		if result.Message != "" {
			allMessages = append(allMessages, fmt.Sprintf("Task %d: %s", i+1, result.Message))
		}

		if earliestStart.IsZero() || result.StartedAt.Before(earliestStart) {
			earliestStart = result.StartedAt
		}

		if latestEnd.IsZero() || result.CompletedAt.After(latestEnd) {
			latestEnd = result.CompletedAt
		}
	}

	merged.RecordsProcessed = totalProcessed
	merged.RecordsFetched = totalFetched
	merged.Success = allSuccessful
	merged.StartedAt = earliestStart
	merged.CompletedAt = latestEnd

	if !latestEnd.IsZero() && !earliestStart.IsZero() {
		merged.Duration = latestEnd.Sub(earliestStart)
	}

	if allSuccessful {
		merged.Status = StatusCompleted
		merged.Message = fmt.Sprintf("Successfully merged %d tasks: processed %d records",
			len(results), totalProcessed)
	} else {
		merged.Status = StatusFailed
		merged.Message = fmt.Sprintf("Merged %d tasks with some failures: processed %d records",
			len(results), totalProcessed)
	}

	// Add individual messages as metadata
	if len(allMessages) > 0 {
		AddMetadata(merged, "individual_messages", allMessages)
	}

	return merged
}
