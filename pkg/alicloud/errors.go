package alicloud

import (
	"errors"
	"fmt"
	"strings"
)

// 定义包级错误变量 (Sentinel Errors)
var (
	// API 相关错误
	ErrRateLimited      = errors.New("alicloud: rate limit exceeded")
	ErrAuthFailed       = errors.New("alicloud: authentication failed")
	ErrInvalidRequest   = errors.New("alicloud: invalid request parameters")
	ErrAPIUnavailable   = errors.New("alicloud: api service unavailable")
	ErrAccessDenied     = errors.New("alicloud: access denied")
	
	// 数据验证错误
	ErrInvalidBillingCycle = errors.New("alicloud: invalid billing cycle format")
	ErrEmptyBillingCycle   = errors.New("alicloud: billing cycle cannot be empty")
	ErrFutureBillingCycle  = errors.New("alicloud: billing cycle cannot be in the future")
	ErrTooOldBillingCycle  = errors.New("alicloud: billing cycle is too old (before 2018)")
	ErrInvalidBillingDate  = errors.New("alicloud: invalid billing date format")
	ErrInvalidGranularity  = errors.New("alicloud: invalid granularity")
	
	// 数据处理错误
	ErrNoData           = errors.New("alicloud: no data available")
	ErrDataProcessing   = errors.New("alicloud: data processing failed")
	ErrDataInconsistent = errors.New("alicloud: data inconsistency detected")
	ErrBatchFailed      = errors.New("alicloud: batch processing failed")
	ErrTransformFailed  = errors.New("alicloud: data transformation failed")
	
	// 配置错误
	ErrMissingConfig    = errors.New("alicloud: configuration is missing")
	ErrInvalidConfig    = errors.New("alicloud: invalid configuration")
	ErrMissingAccessKey = errors.New("alicloud: access key is required")
	ErrMissingSecretKey = errors.New("alicloud: secret key is required")
	ErrMissingRegion    = errors.New("alicloud: region is required")
	
	// 表和数据库错误
	ErrTableNotExists   = errors.New("alicloud: table does not exist")
	ErrTableExists      = errors.New("alicloud: table already exists")
	ErrInvalidTableName = errors.New("alicloud: invalid table name")
	ErrSchemaError      = errors.New("alicloud: table schema error")
	
	// 分页错误
	ErrInvalidPageSize  = errors.New("alicloud: invalid page size")
	ErrInvalidNextToken = errors.New("alicloud: invalid next token")
	ErrPaginationFailed = errors.New("alicloud: pagination failed")
)

// APIError 表示 API 调用错误
type APIError struct {
	Code    string
	Message string
	Details string
	HTTPCode int
}

func (e *APIError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("alicloud api error: %s - %s (%s)", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("alicloud api error: %s - %s", e.Code, e.Message)
}

// IsRetryable 判断错误是否可重试
func (e *APIError) IsRetryable() bool {
	retryableCodes := []string{
		"Throttling.User",      // 用户级限流
		"Throttling.Api",       // API级限流
		"ServiceUnavailable",   // 服务不可用
		"InternalError",        // 内部错误
		"SystemError",          // 系统错误
	}
	
	for _, code := range retryableCodes {
		if e.Code == code {
			return true
		}
	}
	
	// HTTP 5xx 错误通常可重试
	return e.HTTPCode >= 500 && e.HTTPCode < 600
}

// IsRateLimit 判断是否为限流错误
func (e *APIError) IsRateLimit() bool {
	return strings.Contains(e.Code, "Throttling") || e.Code == "QpsLimitExceeded"
}

// ValidationError 表示参数验证错误
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("alicloud validation error: field '%s' with value '%v' - %s", 
		e.Field, e.Value, e.Message)
}

// ProcessingError 表示数据处理错误
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

// ConfigError 表示配置错误
type ConfigError struct {
	Key     string
	Value   interface{}
	Message string
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("alicloud config error: key '%s' with value '%v' - %s", 
		e.Key, e.Value, e.Message)
}

// 错误构造函数

// NewAPIError 创建 API 错误
func NewAPIError(code, message, details string, httpCode int) *APIError {
	return &APIError{
		Code:     code,
		Message:  message,
		Details:  details,
		HTTPCode: httpCode,
	}
}

// NewValidationError 创建验证错误
func NewValidationError(field string, value interface{}, message string) *ValidationError {
	return &ValidationError{
		Field:   field,
		Value:   value,
		Message: message,
	}
}

// NewProcessingError 创建处理错误
func NewProcessingError(stage string, records int, err error) *ProcessingError {
	return &ProcessingError{
		Stage:   stage,
		Records: records,
		Err:     err,
	}
}

// NewConfigError 创建配置错误
func NewConfigError(key string, value interface{}, message string) *ConfigError {
	return &ConfigError{
		Key:     key,
		Value:   value,
		Message: message,
	}
}

// 错误判断工具函数

// IsRetryableError 判断任意错误是否可重试
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}
	
	// 检查 APIError
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRetryable()
	}
	
	// 检查已知的可重试错误
	if errors.Is(err, ErrRateLimited) ||
		errors.Is(err, ErrAPIUnavailable) ||
		errors.Is(err, ErrBatchFailed) {
		return true
	}
	
	return false
}

// IsRateLimitError 判断是否为限流错误
func IsRateLimitError(err error) bool {
	if err == nil {
		return false
	}
	
	// 检查 APIError
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.IsRateLimit()
	}
	
	// 检查 sentinel error
	return errors.Is(err, ErrRateLimited)
}

// IsValidationError 判断是否为验证错误
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

// IsConfigError 判断是否为配置错误
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

// WrapError 包装错误并添加上下文
func WrapError(err error, format string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	
	message := fmt.Sprintf(format, args...)
	return fmt.Errorf("%s: %w", message, err)
}

// CombineErrors 合并多个错误
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
	
	// 构建组合错误消息
	var messages []string
	for _, err := range nonNilErrors {
		messages = append(messages, err.Error())
	}
	
	return fmt.Errorf("alicloud: multiple errors occurred: %s", strings.Join(messages, "; "))
}