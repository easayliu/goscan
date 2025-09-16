package wechat

import (
	"errors"
	"fmt"
)

// 定义 sentinel errors，使用 errors.New 创建不可变的错误实例
var (
	// ErrWebhookURLEmpty webhook URL为空
	ErrWebhookURLEmpty = errors.New("企业微信webhook URL未配置")
	
	// ErrInvalidConfig 配置无效
	ErrInvalidConfig = errors.New("配置参数无效")
	
	// ErrMarshalMessage 序列化消息失败
	ErrMarshalMessage = errors.New("序列化消息失败")
	
	// ErrCreateRequest 创建HTTP请求失败
	ErrCreateRequest = errors.New("创建HTTP请求失败")
	
	// ErrSendRequest 发送HTTP请求失败
	ErrSendRequest = errors.New("发送HTTP请求失败")
	
	// ErrReadResponse 读取响应失败
	ErrReadResponse = errors.New("读取响应失败")
	
	// ErrUnmarshalResponse 解析响应失败
	ErrUnmarshalResponse = errors.New("解析响应失败")
	
	// ErrAPIError 企业微信API错误
	ErrAPIError = errors.New("企业微信API错误")
	
	// ErrHTTPStatusError HTTP状态码错误
	ErrHTTPStatusError = errors.New("HTTP请求失败")
	
	// ErrRetryExceeded 重试次数超限
	ErrRetryExceeded = errors.New("发送企业微信消息失败，已重试")
)

// APIError 企业微信API错误类型
type APIError struct {
	Code    int    `json:"errcode"`
	Message string `json:"errmsg"`
}

// Error 实现 error 接口
func (e *APIError) Error() string {
	return fmt.Sprintf("企业微信API错误: %d %s", e.Code, e.Message)
}

// HTTPError HTTP错误类型
type HTTPError struct {
	StatusCode int
	Status     string
	Body       string
}

// Error 实现 error 接口
func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP请求失败: %d %s, 响应: %s", 
		e.StatusCode, e.Status, e.Body)
}

// RetryError 重试错误类型
type RetryError struct {
	Attempts int
	LastErr  error
}

// Error 实现 error 接口
func (e *RetryError) Error() string {
	return fmt.Sprintf("发送企业微信消息失败，已重试 %d 次: %v", 
		e.Attempts, e.LastErr)
}

// Unwrap 支持 errors.Unwrap
func (e *RetryError) Unwrap() error {
	return e.LastErr
}

// NewAPIError 创建API错误
func NewAPIError(code int, message string) *APIError {
	return &APIError{
		Code:    code,
		Message: message,
	}
}

// NewHTTPError 创建HTTP错误
func NewHTTPError(statusCode int, status, body string) *HTTPError {
	return &HTTPError{
		StatusCode: statusCode,
		Status:     status,
		Body:       body,
	}
}

// NewRetryError 创建重试错误
func NewRetryError(attempts int, lastErr error) *RetryError {
	return &RetryError{
		Attempts: attempts,
		LastErr:  lastErr,
	}
}