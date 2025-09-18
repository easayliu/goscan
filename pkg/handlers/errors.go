package handlers

import (
	"errors"
	"fmt"
	"net/http"

	"goscan/pkg/logger"
	"goscan/pkg/response"

	"go.uber.org/zap"
)

// Common error type definitions
var (
	// ErrInvalidParam indicates invalid parameter error
	ErrInvalidParam = errors.New("invalid parameter")

	// ErrResourceNotFound indicates resource not found error
	ErrResourceNotFound = errors.New("resource not found")

	// ErrServiceUnavailable indicates service unavailable error
	ErrServiceUnavailable = errors.New("service unavailable")

	// ErrUnauthorized indicates unauthorized error
	ErrUnauthorized = errors.New("unauthorized")

	// ErrInternalServer indicates internal server error
	ErrInternalServer = errors.New("internal server error")

	// ErrBadRequest indicates bad request error
	ErrBadRequest = errors.New("bad request")
)

// APIError represents a custom API error structure
type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
	Err     error  `json:"-"`
}

// Error implements the error interface
func (e *APIError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("API Error (Code: %d, Message: %s): %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("API Error (Code: %d, Message: %s)", e.Code, e.Message)
}

// Unwrap supports error wrapping
func (e *APIError) Unwrap() error {
	return e.Err
}

// NewAPIError creates a new API error
func NewAPIError(code int, message string, err error) *APIError {
	return &APIError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// NewBadRequestError creates a 400 Bad Request error
func NewBadRequestError(message string, err error) *APIError {
	return &APIError{
		Code:    http.StatusBadRequest,
		Message: message,
		Err:     err,
	}
}

// NewNotFoundError creates a 404 Not Found error
func NewNotFoundError(message string, err error) *APIError {
	return &APIError{
		Code:    http.StatusNotFound,
		Message: message,
		Err:     err,
	}
}

// NewInternalServerError creates a 500 Internal Server Error
func NewInternalServerError(message string, err error) *APIError {
	return &APIError{
		Code:    http.StatusInternalServerError,
		Message: message,
		Err:     err,
	}
}

// NewServiceUnavailableError creates a 503 Service Unavailable error
func NewServiceUnavailableError(message string, err error) *APIError {
	return &APIError{
		Code:    http.StatusServiceUnavailable,
		Message: message,
		Err:     err,
	}
}

// HandleError provides unified error handling
func HandleError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}

	var apiErr *APIError
	if errors.As(err, &apiErr) {
		// Log detailed error information
		if apiErr.Err != nil {
			logger.Error("API error occurred",
				zap.Int("code", apiErr.Code),
				zap.String("message", apiErr.Message),
				zap.Error(apiErr.Err))
		} else {
			logger.Warn("API error occurred",
				zap.Int("code", apiErr.Code),
				zap.String("message", apiErr.Message))
		}

		response.WriteErrorResponse(w, apiErr.Code, apiErr.Message, apiErr.Err)
		return
	}

	// Handle standard errors
	switch {
	case errors.Is(err, ErrInvalidParam):
		response.WriteErrorResponse(w, http.StatusBadRequest, "Invalid parameter", err)
	case errors.Is(err, ErrResourceNotFound):
		response.WriteErrorResponse(w, http.StatusNotFound, "Resource not found", err)
	case errors.Is(err, ErrServiceUnavailable):
		response.WriteErrorResponse(w, http.StatusServiceUnavailable, "Service unavailable", err)
	case errors.Is(err, ErrUnauthorized):
		response.WriteErrorResponse(w, http.StatusUnauthorized, "Unauthorized", err)
	case errors.Is(err, ErrBadRequest):
		response.WriteErrorResponse(w, http.StatusBadRequest, "Bad request", err)
	default:
		// Unknown error, log details and return generic 500 error
		logger.Error("Unexpected error occurred", zap.Error(err))
		response.WriteErrorResponse(w, http.StatusInternalServerError, "Internal server error", nil)
	}
}

// WrapError wraps an error and adds context information
func WrapError(err error, message string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}

// ValidateRequired validates required parameters
func ValidateRequired(value, fieldName string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidParam, fieldName)
	}
	return nil
}

// ValidateStringSlice validates string slice parameters
func ValidateStringSlice(slice []string, fieldName string) error {
	if len(slice) == 0 {
		return fmt.Errorf("%w: %s cannot be empty", ErrInvalidParam, fieldName)
	}
	return nil
}

// LogErrorWithContext logs errors with context information
func LogErrorWithContext(err error, context string, fields ...interface{}) {
	if err == nil {
		return
	}

	// Convert parameters to zap fields
	zapFields := []zap.Field{
		zap.Error(err),
		zap.String("context", context),
	}

	// Add incoming parameters to zap fields in pairs
	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key := fmt.Sprintf("%v", fields[i])
			value := fields[i+1]
			zapFields = append(zapFields, zap.Any(key, value))
		}
	}

	logger.Error("Error occurred", zapFields...)
}
