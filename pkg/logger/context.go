package logger

import (
	"context"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// contextKey is a custom type for context keys to avoid collisions
type contextKey string

const (
	// Context keys for logger fields
	taskIDKey   contextKey = "task_id"
	jobIDKey    contextKey = "job_id"
	providerKey contextKey = "provider"
	batchIDKey  contextKey = "batch_id"
	loggerKey   contextKey = "logger"
)

// WithTaskID adds task ID to context
func WithTaskID(ctx context.Context, taskID string) context.Context {
	return context.WithValue(ctx, taskIDKey, taskID)
}

// WithJobID adds job ID to context
func WithJobID(ctx context.Context, jobID string) context.Context {
	return context.WithValue(ctx, jobIDKey, jobID)
}

// WithProvider adds provider to context
func WithProvider(ctx context.Context, provider string) context.Context {
	return context.WithValue(ctx, providerKey, provider)
}

// WithBatchID adds batch ID to context
func WithBatchID(ctx context.Context, batchID string) context.Context {
	return context.WithValue(ctx, batchIDKey, batchID)
}

// FromContext extracts logger from context with all accumulated fields
func FromContext(ctx context.Context) *zap.Logger {
	// Try to get existing logger from context
	if l, ok := ctx.Value(loggerKey).(*zap.Logger); ok && l != nil {
		return l
	}

	// Build logger with context fields
	l := Logger
	if l == nil {
		// Fallback to a basic logger if not initialized
		l, _ = zap.NewProduction()
	}

	// Add context fields
	var fields []zap.Field

	if taskID, ok := ctx.Value(taskIDKey).(string); ok && taskID != "" {
		fields = append(fields, zap.String("task_id", taskID))
	}

	if jobID, ok := ctx.Value(jobIDKey).(string); ok && jobID != "" {
		fields = append(fields, zap.String("job_id", jobID))
	}

	if provider, ok := ctx.Value(providerKey).(string); ok && provider != "" {
		fields = append(fields, zap.String("provider", provider))
	}

	if batchID, ok := ctx.Value(batchIDKey).(string); ok && batchID != "" {
		fields = append(fields, zap.String("batch_id", batchID))
	}

	if len(fields) > 0 {
		l = l.With(fields...)
	}

	return l
}

// WithLogger adds logger to context
func WithLogger(ctx context.Context, logger *zap.Logger) context.Context {
	return context.WithValue(ctx, loggerKey, logger)
}

// Task-aware logging helpers

// WithTask creates a logger with task-related fields
func WithTask(logger *zap.Logger, taskID string) *zap.Logger {
	if logger == nil {
		logger = Logger
	}
	return logger.With(zap.String("task_id", taskID))
}

// WithJob creates a logger with job-related fields
func WithJob(logger *zap.Logger, jobID string) *zap.Logger {
	if logger == nil {
		logger = Logger
	}
	return logger.With(zap.String("job_id", jobID))
}

// WithProviderLogger creates a logger with provider field
func WithProviderLogger(logger *zap.Logger, provider string) *zap.Logger {
	if logger == nil {
		logger = Logger
	}
	return logger.With(zap.String("provider", provider))
}

// WithBatch creates a logger with batch ID
func WithBatch(logger *zap.Logger, batchID string) *zap.Logger {
	if logger == nil {
		logger = Logger
	}
	return logger.With(zap.String("batch_id", batchID))
}

// WithTable creates a logger with table name
func WithTable(logger *zap.Logger, table string) *zap.Logger {
	if logger == nil {
		logger = Logger
	}
	return logger.With(zap.String("table", table))
}

// Common field helpers

// ErrorField returns a zap field for errors
func ErrorField(err error) zap.Field {
	return zap.Error(err)
}

// DurationField returns a zap field for duration in milliseconds
func DurationField(durationMs int64) zap.Field {
	return zap.Int64("duration_ms", durationMs)
}

// CountField returns a zap field for count/row count
func CountField(count int) zap.Field {
	return zap.Int("row_count", count)
}

// PeriodField returns a zap field for billing period
func PeriodField(period string) zap.Field {
	return zap.String("period", period)
}

// Dynamic log level management

var currentLevel = zap.NewAtomicLevelAt(zap.InfoLevel)

// SetLogLevel dynamically changes the log level
func SetLogLevel(level string) error {
	var zapLevel zapcore.Level
	if err := zapLevel.UnmarshalText([]byte(level)); err != nil {
		return err
	}

	currentLevel.SetLevel(zapLevel)
	SetLevel(zapLevel) // Also update the main logger level
	return nil
}

// GetLogLevel returns the current log level
func GetLogLevel() string {
	return currentLevel.Level().String()
}
