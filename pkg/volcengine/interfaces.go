package volcengine

import (
	"context"
	"time"
)

// BillProvider 账单数据提供者接口
type BillProvider interface {
	// ListBillDetail 获取账单明细
	ListBillDetail(ctx context.Context, req *ListBillDetailRequest) (*ListBillDetailResponse, error)

	// ValidatePeriod 验证账期格式
	ValidatePeriod(billPeriod string) error

	// CalculateSmartPeriod 计算智能账期
	CalculateSmartPeriod() (string, string)
}

// DataProcessor 数据处理器接口
type DataProcessor interface {
	// Process 处理账单数据
	Process(ctx context.Context, data []BillDetail) error

	// ProcessWithResult 处理数据并返回结果
	ProcessWithResult(ctx context.Context, data []BillDetail) (*ProcessResult, error)

	// SetBatchSize 设置批处理大小
	SetBatchSize(size int)
}

// BillService 账单服务接口
type BillService interface {
	// CreateBillTable 创建账单表
	CreateBillTable(ctx context.Context) error

	// SyncBillData 同步账单数据
	SyncBillData(ctx context.Context, req *ListBillDetailRequest) (*SyncResult, error)

	// SmartSyncAllData 智能同步所有数据
	SmartSyncAllData(ctx context.Context, billPeriod string, tableName string, isDistributed bool) (*SyncResult, error)

	// GetAPIDataCount 获取API数据总数
	GetAPIDataCount(ctx context.Context, billPeriod string) (int32, error)

	// CheckMonthlyDataExists 检查月度数据是否存在及数量
	CheckMonthlyDataExists(ctx context.Context, tableName, billPeriod string) (bool, int64, error)
}

// Paginator 分页器接口
type Paginator interface {
	// PaginateBillDetails 分页处理账单明细
	PaginateBillDetails(ctx context.Context, req *ListBillDetailRequest) (*PaginateResult, error)

	// SetProgressCallback 设置进度回调
	SetProgressCallback(callback ProgressCallback)
}

// RateLimiter 限流器接口
type RateLimiter interface {
	// Wait 等待直到可以发送下一个请求
	Wait(ctx context.Context) error

	// OnSuccess 记录成功的请求
	OnSuccess()

	// OnRateLimit 记录被限流的请求
	OnRateLimit()

	// OnError 记录错误
	OnError(err error)
}

// SyncResult 同步结果
type SyncResult struct {
	TotalRecords    int           `json:"total_records"`
	FetchedRecords  int           `json:"fetched_records"`
	InsertedRecords int           `json:"inserted_records"`
	FailedRecords   int           `json:"failed_records"`
	StartTime       time.Time     `json:"start_time"`
	EndTime         time.Time     `json:"end_time"`
	Duration        time.Duration `json:"duration"`
	Error           error         `json:"error,omitempty"`
}

// DataPreCheckResult 数据预检查结果
type DataPreCheckResult struct {
	APICount      int32  `json:"api_count"`
	DatabaseCount int64  `json:"database_count"`
	BillPeriod    string `json:"bill_period"`
	NeedSync      bool   `json:"need_sync"`
	NeedCleanup   bool   `json:"need_cleanup"`
	Reason        string `json:"reason"`
}

// ProcessCallback 处理回调函数
type ProcessCallback func(processed, total int, duration time.Duration)

// ErrorHandler 错误处理函数
type ErrorHandler func(err error) bool // 返回 true 表示继续，false 表示停止

// RetryPolicy 重试策略接口
type RetryPolicy interface {
	// ShouldRetry 判断是否应该重试
	ShouldRetry(err error, attempt int) bool

	// GetDelay 获取重试延迟
	GetDelay(attempt int) time.Duration

	// MaxAttempts 最大尝试次数
	MaxAttempts() int
}

// ExponentialBackoff 指数退避重试策略
type ExponentialBackoff struct {
	InitialDelay time.Duration
	MaxDelay     time.Duration
	Multiplier   float64
	MaxRetries   int
}

// ShouldRetry 实现 RetryPolicy 接口
func (e *ExponentialBackoff) ShouldRetry(err error, attempt int) bool {
	if attempt >= e.MaxRetries {
		return false
	}
	return IsRetryableError(err)
}

// GetDelay 实现 RetryPolicy 接口
func (e *ExponentialBackoff) GetDelay(attempt int) time.Duration {
	delay := e.InitialDelay
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * e.Multiplier)
		if delay > e.MaxDelay {
			delay = e.MaxDelay
			break
		}
	}
	return delay
}

// MaxAttempts 实现 RetryPolicy 接口
func (e *ExponentialBackoff) MaxAttempts() int {
	return e.MaxRetries
}

// DefaultExponentialBackoff 创建默认的指数退避策略
func DefaultExponentialBackoff() *ExponentialBackoff {
	return &ExponentialBackoff{
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
		MaxRetries:   5,
	}
}
