package volcengine

import (
	"errors"
	"fmt"
	"strings"
)

// 定义包级错误变量
var (
	// API 相关错误
	ErrRateLimited      = errors.New("volcengine: rate limit exceeded")
	ErrAuthFailed       = errors.New("volcengine: authentication failed")
	ErrInvalidRequest   = errors.New("volcengine: invalid request parameters")
	ErrAPIUnavailable   = errors.New("volcengine: api service unavailable")
	
	// 数据验证错误
	ErrInvalidBillPeriod = errors.New("volcengine: invalid bill period format")
	ErrEmptyBillPeriod   = errors.New("volcengine: bill period cannot be empty")
	ErrFutureBillPeriod  = errors.New("volcengine: bill period cannot be in the future")
	ErrTooOldBillPeriod  = errors.New("volcengine: bill period is too old (before 2018)")
	
	// 数据处理错误
	ErrNoData           = errors.New("volcengine: no data available")
	ErrDataProcessing   = errors.New("volcengine: data processing failed")
	ErrDataInconsistent = errors.New("volcengine: data inconsistency detected")
	
	// 配置错误
	ErrMissingConfig    = errors.New("volcengine: configuration is missing")
	ErrInvalidConfig    = errors.New("volcengine: invalid configuration")
	ErrMissingAccessKey = errors.New("volcengine: access key is required")
	ErrMissingSecretKey = errors.New("volcengine: secret key is required")
)

// APIError 表示 API 调用错误
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

// IsRetryable 判断错误是否可重试
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

// IsAuthError 判断是否为认证错误
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

// NewAPIError 创建新的 API 错误
func NewAPIError(code, message string) *APIError {
	return &APIError{
		Code:    code,
		Message: message,
	}
}

// NewAPIErrorWithDetails 创建带详情的 API 错误
func NewAPIErrorWithDetails(code, message, details string) *APIError {
	return &APIError{
		Code:    code,
		Message: message,
		Details: details,
	}
}

// WrapError 包装错误并添加上下文
func WrapError(err error, context string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", context, err)
}

// IsRateLimitError 检查是否为限流错误
func IsRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	
	// 检查是否为预定义的限流错误
	if errors.Is(err, ErrRateLimited) {
		return true
	}
	
	// 检查错误消息中的限流指示符
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
	
	// 检查 APIError 类型
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable() && strings.Contains(strings.ToLower(apiErr.Code), "limit")
	}
	
	return false
}

// IsRetryableError 检查错误是否可以重试
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	// 检查预定义的可重试错误
	retryableErrors := []error{
		ErrRateLimited,
		ErrAPIUnavailable,
	}
	
	for _, retryable := range retryableErrors {
		if errors.Is(err, retryable) {
			return true
		}
	}
	
	// 检查 APIError
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable()
	}
	
	// 检查错误消息
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