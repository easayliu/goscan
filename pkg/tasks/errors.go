package tasks

import "errors"

// Package-level error variables for unified error handling
var (
	// ErrCredentialsNotConfigured indicates cloud provider credentials not configured
	ErrCredentialsNotConfigured = errors.New("cloud provider credentials not configured")

	// ErrTableCreationFailed indicates table creation failed
	ErrTableCreationFailed = errors.New("failed to create table")

	// ErrSyncFailed indicates data synchronization failed
	ErrSyncFailed = errors.New("data synchronization failed")

	// ErrTaskNotFound indicates task not found
	ErrTaskNotFound = errors.New("task not found")

	// ErrTooManyTasks indicates too many running tasks
	ErrTooManyTasks = errors.New("too many running tasks")

	// ErrInvalidTaskConfig indicates invalid task configuration
	ErrInvalidTaskConfig = errors.New("invalid task configuration")

	// ErrTaskAlreadyRunning indicates task already running
	ErrTaskAlreadyRunning = errors.New("task already running")

	// ErrTaskCancelled indicates task was cancelled
	ErrTaskCancelled = errors.New("task was cancelled")

	// ErrInvalidPeriod indicates invalid billing period
	ErrInvalidPeriod = errors.New("invalid billing period")

	// ErrDataValidationFailed indicates data validation failed
	ErrDataValidationFailed = errors.New("data validation failed")

	// ErrProviderNotSupported indicates provider not supported
	ErrProviderNotSupported = errors.New("provider not supported")

	// ErrContextCancelled indicates context cancelled
	ErrContextCancelled = errors.New("context cancelled")

	// ErrDatabaseConnection indicates database connection failed
	ErrDatabaseConnection = errors.New("database connection failed")

	// ErrNotificationFailed indicates notification delivery failed
	ErrNotificationFailed = errors.New("notification delivery failed")
)
