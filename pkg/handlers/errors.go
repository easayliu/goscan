package handlers

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"

	"goscan/pkg/response"
)

// 定义常见的错误类型
var (
	// ErrInvalidParam 参数无效错误
	ErrInvalidParam = errors.New("invalid parameter")
	
	// ErrResourceNotFound 资源未找到错误
	ErrResourceNotFound = errors.New("resource not found")
	
	// ErrServiceUnavailable 服务不可用错误
	ErrServiceUnavailable = errors.New("service unavailable")
	
	// ErrUnauthorized 未授权错误
	ErrUnauthorized = errors.New("unauthorized")
	
	// ErrInternalServer 内部服务器错误
	ErrInternalServer = errors.New("internal server error")
	
	// ErrBadRequest 请求错误
	ErrBadRequest = errors.New("bad request")
)

// APIError 自定义API错误结构
type APIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
	Err     error  `json:"-"`
}

// Error 实现error接口
func (e *APIError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("API Error (Code: %d, Message: %s): %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("API Error (Code: %d, Message: %s)", e.Code, e.Message)
}

// Unwrap 支持错误链
func (e *APIError) Unwrap() error {
	return e.Err
}

// NewAPIError 创建新的API错误
func NewAPIError(code int, message string, err error) *APIError {
	return &APIError{
		Code:    code,
		Message: message,
		Err:     err,
	}
}

// NewBadRequestError 创建400错误
func NewBadRequestError(message string, err error) *APIError {
	return &APIError{
		Code:    http.StatusBadRequest,
		Message: message,
		Err:     err,
	}
}

// NewNotFoundError 创建404错误
func NewNotFoundError(message string, err error) *APIError {
	return &APIError{
		Code:    http.StatusNotFound,
		Message: message,
		Err:     err,
	}
}

// NewInternalServerError 创建500错误
func NewInternalServerError(message string, err error) *APIError {
	return &APIError{
		Code:    http.StatusInternalServerError,
		Message: message,
		Err:     err,
	}
}

// NewServiceUnavailableError 创建503错误
func NewServiceUnavailableError(message string, err error) *APIError {
	return &APIError{
		Code:    http.StatusServiceUnavailable,
		Message: message,
		Err:     err,
	}
}

// HandleError 统一错误处理函数
func HandleError(w http.ResponseWriter, err error) {
	if err == nil {
		return
	}

	var apiErr *APIError
	if errors.As(err, &apiErr) {
		// 记录详细错误信息
		if apiErr.Err != nil {
			slog.Error("API error occurred",
				"code", apiErr.Code,
				"message", apiErr.Message,
				"details", apiErr.Err.Error())
		} else {
			slog.Warn("API error occurred",
				"code", apiErr.Code,
				"message", apiErr.Message)
		}
		
		response.WriteErrorResponse(w, apiErr.Code, apiErr.Message, apiErr.Err)
		return
	}

	// 处理标准错误
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
		// 未知错误，记录详细信息并返回通用500错误
		slog.Error("Unexpected error occurred", "error", err)
		response.WriteErrorResponse(w, http.StatusInternalServerError, "Internal server error", nil)
	}
}

// WrapError 包装错误，添加上下文信息
func WrapError(err error, message string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}

// ValidateRequired 验证必需参数
func ValidateRequired(value, fieldName string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrInvalidParam, fieldName)
	}
	return nil
}

// ValidateStringSlice 验证字符串切片参数
func ValidateStringSlice(slice []string, fieldName string) error {
	if len(slice) == 0 {
		return fmt.Errorf("%w: %s cannot be empty", ErrInvalidParam, fieldName)
	}
	return nil
}

// LogErrorWithContext 记录错误并添加上下文信息
func LogErrorWithContext(err error, context string, fields ...interface{}) {
	if err == nil {
		return
	}
	
	args := []interface{}{"error", err, "context", context}
	args = append(args, fields...)
	slog.Error("Error occurred", args...)
}