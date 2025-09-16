package analysis

import (
	"errors"
	"fmt"
)

// Sentinel errors - 使用 sentinel error 模式
var (
	// ErrInvalidConfig 配置无效错误
	ErrInvalidConfig = errors.New("配置无效")
	
	// ErrConnectionNotInitialized 连接未初始化错误
	ErrConnectionNotInitialized = errors.New("连接未初始化")
	
	// ErrDirectConnectionFailed 直接连接失败错误
	ErrDirectConnectionFailed = errors.New("直接连接失败")
	
	// ErrQueryTimeout 查询超时错误
	ErrQueryTimeout = errors.New("查询超时")
	
	// ErrNoDataFound 未找到数据错误
	ErrNoDataFound = errors.New("未找到数据")
	
	// ErrInvalidTableInfo 表信息无效错误
	ErrInvalidTableInfo = errors.New("表信息无效")
	
	// ErrQueryExecutionFailed 查询执行失败错误
	ErrQueryExecutionFailed = errors.New("查询执行失败")
	
	// ErrDataScanFailed 数据扫描失败错误
	ErrDataScanFailed = errors.New("数据扫描失败")
	
	// ErrCacheOperationFailed 缓存操作失败错误
	ErrCacheOperationFailed = errors.New("缓存操作失败")
	
	// ErrResourceCleanupFailed 资源清理失败错误
	ErrResourceCleanupFailed = errors.New("资源清理失败")
)

// ConfigError 配置相关错误
type ConfigError struct {
	Field   string
	Message string
	Err     error
}

func (e *ConfigError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("配置错误 [%s]: %s: %v", e.Field, e.Message, e.Err)
	}
	return fmt.Sprintf("配置错误 [%s]: %s", e.Field, e.Message)
}

func (e *ConfigError) Unwrap() error {
	return e.Err
}

// NewConfigError 创建配置错误
func NewConfigError(field, message string, err error) *ConfigError {
	return &ConfigError{
		Field:   field,
		Message: message,
		Err:     err,
	}
}

// QueryError 查询相关错误
type QueryError struct {
	Table   string
	Query   string
	Message string
	Err     error
}

func (e *QueryError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("查询错误 [表: %s]: %s: %v", e.Table, e.Message, e.Err)
	}
	return fmt.Sprintf("查询错误 [表: %s]: %s", e.Table, e.Message)
}

func (e *QueryError) Unwrap() error {
	return e.Err
}

// NewQueryError 创建查询错误
func NewQueryError(table, query, message string, err error) *QueryError {
	return &QueryError{
		Table:   table,
		Query:   query,
		Message: message,
		Err:     err,
	}
}

// ConnectionError 连接相关错误
type ConnectionError struct {
	Type    string // "direct", "legacy", "pool"
	Address string
	Message string
	Err     error
}

func (e *ConnectionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("连接错误 [%s:%s]: %s: %v", e.Type, e.Address, e.Message, e.Err)
	}
	return fmt.Sprintf("连接错误 [%s:%s]: %s", e.Type, e.Address, e.Message)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

// NewConnectionError 创建连接错误
func NewConnectionError(connType, address, message string, err error) *ConnectionError {
	return &ConnectionError{
		Type:    connType,
		Address: address,
		Message: message,
		Err:     err,
	}
}

// ValidationError 验证相关错误
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("验证错误 [%s=%v]: %s", e.Field, e.Value, e.Message)
}

// NewValidationError 创建验证错误
func NewValidationError(field string, value interface{}, message string) *ValidationError {
	return &ValidationError{
		Field:   field,
		Value:   value,
		Message: message,
	}
}

// wrapError 包装错误，添加上下文信息
func wrapError(err error, message string, args ...interface{}) error {
	if err == nil {
		return nil
	}
	
	if len(args) > 0 {
		message = fmt.Sprintf(message, args...)
	}
	
	return fmt.Errorf("%s: %w", message, err)
}

// isTemporaryError 判断是否为临时性错误
func isTemporaryError(err error) bool {
	type temporary interface {
		Temporary() bool
	}
	
	if te, ok := err.(temporary); ok && te.Temporary() {
		return true
	}
	
	// 检查常见的临时性错误
	switch {
	case errors.Is(err, ErrQueryTimeout):
		return true
	case errors.Is(err, ErrConnectionNotInitialized):
		return true
	default:
		return false
	}
}

// isRetryableError 判断是否为可重试错误
func isRetryableError(err error) bool {
	if isTemporaryError(err) {
		return true
	}
	
	// 检查可重试的错误类型
	var queryErr *QueryError
	var connErr *ConnectionError
	
	switch {
	case errors.As(err, &queryErr):
		// 查询超时等情况可以重试
		return errors.Is(queryErr.Err, ErrQueryTimeout)
	case errors.As(err, &connErr):
		// 连接失败可以重试
		return true
	default:
		return false
	}
}