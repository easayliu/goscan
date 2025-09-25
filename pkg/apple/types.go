package apple

import (
	"context"
	"time"
	"goscan/pkg/tasks"
)

// StockMonitor defines the interface for monitoring Apple stock
type StockMonitor interface {
	// StartMonitoring starts the stock monitoring process
	StartMonitoring(ctx context.Context, config *StockMonitorConfig) error

	// StopMonitoring stops the stock monitoring process
	StopMonitoring(ctx context.Context, configID uint) error

	// CheckStock checks stock for specific products and stores
	CheckStock(ctx context.Context, productIDs []uint, storeIDs []uint) (*StockCheckResult, error)

	// GetStockInfo retrieves current stock information
	GetStockInfo(ctx context.Context, productID, storeID uint) (*StockInfo, error)

	// UpdateStockInfo updates stock information and creates change events if needed
	UpdateStockInfo(ctx context.Context, stockInfo *StockInfo) (*StockChangeEvent, error)

	// GetStockHistory retrieves stock change history
	GetStockHistory(ctx context.Context, productID, storeID uint, limit int) ([]*StockChangeEvent, error)
}

// StockNotifier defines the interface for sending stock notifications
type StockNotifier interface {
	// SendStockAlert sends a stock availability alert
	SendStockAlert(ctx context.Context, event *StockChangeEvent, config *StockMonitorConfig) error

	// SendSummaryReport sends a summary report of stock monitoring
	SendSummaryReport(ctx context.Context, result *StockMonitorResult, config *StockMonitorConfig) error

	// ValidateNotificationConfig validates notification configuration
	ValidateNotificationConfig(ctx context.Context, config *StockMonitorConfig) error

	// GetNotificationStatus gets the status of sent notifications
	GetNotificationStatus(ctx context.Context, eventID uint) (*NotificationStatus, error)
}

// StockRepository defines the interface for stock data operations
type StockRepository interface {
	// Product operations
	CreateProduct(ctx context.Context, product *AppleProduct) error
	GetProduct(ctx context.Context, id uint) (*AppleProduct, error)
	GetProductByCode(ctx context.Context, productCode string) (*AppleProduct, error)
	GetActiveProducts(ctx context.Context) ([]*AppleProduct, error)
	UpdateProduct(ctx context.Context, product *AppleProduct) error
	DeleteProduct(ctx context.Context, id uint) error

	// Store operations
	CreateStore(ctx context.Context, store *AppleStoreInfo) error
	GetStore(ctx context.Context, id uint) (*AppleStoreInfo, error)
	GetStoreByCode(ctx context.Context, storeCode string) (*AppleStoreInfo, error)
	GetActiveStores(ctx context.Context) ([]*AppleStoreInfo, error)
	GetStoresByLocation(ctx context.Context, city, state, country string) ([]*AppleStoreInfo, error)
	UpdateStore(ctx context.Context, store *AppleStoreInfo) error
	DeleteStore(ctx context.Context, id uint) error

	// Stock operations
	CreateStockInfo(ctx context.Context, stockInfo *StockInfo) error
	GetStockInfo(ctx context.Context, productID, storeID uint) (*StockInfo, error)
	UpdateStockInfo(ctx context.Context, stockInfo *StockInfo) error
	GetStockInfoByProduct(ctx context.Context, productID uint) ([]*StockInfo, error)
	GetStockInfoByStore(ctx context.Context, storeID uint) ([]*StockInfo, error)
	GetAvailableStock(ctx context.Context) ([]*StockInfo, error)

	// Stock event operations
	CreateStockEvent(ctx context.Context, event *StockChangeEvent) error
	GetStockEvents(ctx context.Context, productID, storeID uint, limit int) ([]*StockChangeEvent, error)
	GetRecentStockEvents(ctx context.Context, hours int) ([]*StockChangeEvent, error)
	GetUnnotifiedEvents(ctx context.Context) ([]*StockChangeEvent, error)
	MarkEventNotified(ctx context.Context, eventID uint) error

	// Config operations
	CreateConfig(ctx context.Context, config *StockMonitorConfig) error
	GetConfig(ctx context.Context, id uint) (*StockMonitorConfig, error)
	GetConfigByName(ctx context.Context, name string) (*StockMonitorConfig, error)
	GetActiveConfigs(ctx context.Context) ([]*StockMonitorConfig, error)
	UpdateConfig(ctx context.Context, config *StockMonitorConfig) error
	DeleteConfig(ctx context.Context, id uint) error

	// Result operations
	CreateResult(ctx context.Context, result *StockMonitorResult) error
	GetResult(ctx context.Context, taskID string) (*StockMonitorResult, error)
	GetRecentResults(ctx context.Context, configID uint, limit int) ([]*StockMonitorResult, error)
	UpdateResult(ctx context.Context, result *StockMonitorResult) error
}

// AppleStoreAPI defines the interface for Apple Store API interactions
type AppleStoreAPI interface {
	// GetProductInfo retrieves product information from Apple Store
	GetProductInfo(ctx context.Context, productCode string) (*AppleProduct, error)

	// CheckProductAvailability checks product availability at specific stores
	CheckProductAvailability(ctx context.Context, productCode string, storeCodes []string) (map[string]*StockInfo, error)

	// GetStoreInfo retrieves store information
	GetStoreInfo(ctx context.Context, storeCode string) (*AppleStoreInfo, error)

	// SearchStores searches for stores by location
	SearchStores(ctx context.Context, query *StoreSearchQuery) ([]*AppleStoreInfo, error)

	// GetAvailableModels gets all available models for a product category
	GetAvailableModels(ctx context.Context, category string) ([]*AppleProduct, error)
}

// StockExecutor implements the TaskExecutor interface for stock monitoring tasks
type StockExecutor interface {
	tasks.TaskExecutor

	// ExecuteStockMonitor executes a stock monitoring task
	ExecuteStockMonitor(ctx context.Context, config *StockMonitorConfig) (*StockMonitorResult, error)

	// ValidateStockConfig validates stock monitoring configuration
	ValidateStockConfig(ctx context.Context, config *StockMonitorConfig) error
}

// Additional types for API operations
type StockCheckResult struct {
	ProductsChecked     int                    `json:"products_checked"`
	StoresChecked       int                    `json:"stores_checked"`
	TotalChecks         int                    `json:"total_checks"`
	AvailabilityChanges int                    `json:"availability_changes"`
	CheckTime           int64                  `json:"check_time_ms"`
	Results             []*StockInfo           `json:"results"`
	Events              []*StockChangeEvent    `json:"events"`
	Errors              []string               `json:"errors,omitempty"`
	Metadata            map[string]interface{} `json:"metadata,omitempty"`
}

type NotificationStatus struct {
	ID             uint                   `json:"id"`
	EventID        uint                   `json:"event_id"`
	Channel        NotificationChannel    `json:"channel"`
	Status         NotificationStatusType `json:"status"`
	SentAt         *time.Time             `json:"sent_at"`
	DeliveredAt    *time.Time             `json:"delivered_at"`
	FailedAt       *time.Time             `json:"failed_at"`
	RetryCount     int                    `json:"retry_count"`
	ErrorMessage   string                 `json:"error_message,omitempty"`
	Metadata       string                 `json:"metadata"` // JSON metadata
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
}

type NotificationChannel string

const (
	ChannelEmail   NotificationChannel = "email"
	ChannelSMS     NotificationChannel = "sms"
	ChannelWebhook NotificationChannel = "webhook"
	ChannelPush    NotificationChannel = "push"
	ChannelSlack   NotificationChannel = "slack"
	ChannelDiscord NotificationChannel = "discord"
)

type NotificationStatusType string

const (
	NotificationPending   NotificationStatusType = "pending"
	NotificationSending   NotificationStatusType = "sending"
	NotificationSent      NotificationStatusType = "sent"
	NotificationDelivered NotificationStatusType = "delivered"
	NotificationFailed    NotificationStatusType = "failed"
	NotificationRetrying  NotificationStatusType = "retrying"
)

type StoreSearchQuery struct {
	City       string  `json:"city,omitempty"`
	State      string  `json:"state,omitempty"`
	Country    string  `json:"country"`
	ZipCode    string  `json:"zip_code,omitempty"`
	Latitude   float64 `json:"latitude,omitempty"`
	Longitude  float64 `json:"longitude,omitempty"`
	Radius     float64 `json:"radius,omitempty"` // Search radius in kilometers
	StoreType  string  `json:"store_type,omitempty"`
	Limit      int     `json:"limit,omitempty"`
}

// ProductFilter defines filtering criteria for products
type ProductFilter struct {
	Categories  []string `json:"categories,omitempty"`  // e.g., ["iPhone", "iPad"]
	Models      []string `json:"models,omitempty"`      // e.g., ["iPhone15,2"]
	StorageMin  string   `json:"storage_min,omitempty"` // e.g., "128GB"
	StorageMax  string   `json:"storage_max,omitempty"` // e.g., "1TB"
	Colors      []string `json:"colors,omitempty"`      // e.g., ["Natural Titanium"]
	Carriers    []string `json:"carriers,omitempty"`    // e.g., ["Unlocked", "Verizon"]
	PriceMin    float64  `json:"price_min,omitempty"`   // Minimum price
	PriceMax    float64  `json:"price_max,omitempty"`   // Maximum price
	IsActive    *bool    `json:"is_active,omitempty"`   // Active products only
}

// StoreFilter defines filtering criteria for stores
type StoreFilter struct {
	Cities      []string    `json:"cities,omitempty"`      // e.g., ["New York", "Los Angeles"]
	States      []string    `json:"states,omitempty"`      // e.g., ["CA", "NY"]
	Countries   []string    `json:"countries,omitempty"`   // e.g., ["US", "CA"]
	StoreTypes  []StoreType `json:"store_types,omitempty"` // e.g., ["retail", "online"]
	SupportPickup   *bool   `json:"support_pickup,omitempty"`   // Stores that support pickup
	SupportDelivery *bool   `json:"support_delivery,omitempty"` // Stores that support delivery
	IsActive    *bool       `json:"is_active,omitempty"`        // Active stores only
}

// MonitoringStats provides statistics about monitoring operations
type MonitoringStats struct {
	TotalProducts       int     `json:"total_products"`
	ActiveProducts      int     `json:"active_products"`
	TotalStores         int     `json:"total_stores"`
	ActiveStores        int     `json:"active_stores"`
	TotalConfigs        int     `json:"total_configs"`
	ActiveConfigs       int     `json:"active_configs"`
	RunningTasks        int     `json:"running_tasks"`
	TotalChecksToday    int     `json:"total_checks_today"`
	AvailabilityChanges int     `json:"availability_changes_today"`
	NotificationsSent   int     `json:"notifications_sent_today"`
	AverageResponseTime float64 `json:"average_response_time_ms"`
	SuccessRate         float64 `json:"success_rate_percent"`
	LastCheckTime       string  `json:"last_check_time"`
	NextCheckTime       string  `json:"next_check_time"`
}

// APIResponse represents a generic API response
type APIResponse struct {
	Success   bool        `json:"success"`
	Message   string      `json:"message"`
	Data      interface{} `json:"data,omitempty"`
	Error     string      `json:"error,omitempty"`
	Timestamp string      `json:"timestamp"`
	RequestID string      `json:"request_id,omitempty"`
}

// RateLimiter defines interface for rate limiting API calls
type RateLimiter interface {
	// Allow checks if the operation is allowed under rate limits
	Allow() bool

	// Wait waits until the operation is allowed
	Wait(ctx context.Context) error

	// Remaining returns the number of remaining requests
	Remaining() int

	// Reset returns when the rate limiter will reset
	Reset() time.Time
}

// Cache defines interface for caching stock information
type Cache interface {
	// Get retrieves a value from cache
	Get(ctx context.Context, key string) (interface{}, error)

	// Set stores a value in cache with expiration
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error

	// Delete removes a value from cache
	Delete(ctx context.Context, key string) error

	// Clear clears all cached values
	Clear(ctx context.Context) error

	// Exists checks if a key exists in cache
	Exists(ctx context.Context, key string) (bool, error)
}

// HealthChecker defines interface for health checking
type HealthChecker interface {
	// CheckHealth performs a health check
	CheckHealth(ctx context.Context) *HealthStatus

	// GetHealthStatus returns current health status
	GetHealthStatus() *HealthStatus
}

type HealthStatus struct {
	Status      string            `json:"status"`      // "healthy", "unhealthy", "degraded"
	Timestamp   time.Time         `json:"timestamp"`
	Services    map[string]string `json:"services"`    // Service name -> status
	Metrics     map[string]float64 `json:"metrics"`    // Metric name -> value
	Errors      []string          `json:"errors,omitempty"`
	Version     string            `json:"version"`
	Uptime      time.Duration     `json:"uptime"`
}

// Metrics defines interface for collecting metrics
type Metrics interface {
	// IncrementCounter increments a counter metric
	IncrementCounter(name string, labels map[string]string)

	// SetGauge sets a gauge metric value
	SetGauge(name string, value float64, labels map[string]string)

	// RecordHistogram records a histogram value
	RecordHistogram(name string, value float64, labels map[string]string)

	// RecordTiming records timing information
	RecordTiming(name string, duration time.Duration, labels map[string]string)
}

// Logger defines interface for structured logging
type Logger interface {
	// Debug logs debug level message
	Debug(msg string, fields map[string]interface{})

	// Info logs info level message
	Info(msg string, fields map[string]interface{})

	// Warn logs warning level message
	Warn(msg string, fields map[string]interface{})

	// Error logs error level message
	Error(msg string, err error, fields map[string]interface{})

	// Fatal logs fatal level message and exits
	Fatal(msg string, err error, fields map[string]interface{})

	// With returns a logger with additional fields
	With(fields map[string]interface{}) Logger
}