package apple

import (
	"time"

	"gorm.io/gorm"
)

// AppleProduct represents an Apple product with detailed specifications
type AppleProduct struct {
	ID            uint      `json:"id" gorm:"primaryKey"`
	ProductCode   string    `json:"product_code" gorm:"uniqueIndex;not null"` // Apple internal product code
	ProductName   string    `json:"product_name" gorm:"not null"`             // e.g., "iPhone 15 Pro"
	Model         string    `json:"model" gorm:"not null"`                    // e.g., "iPhone15,2"
	Storage       string    `json:"storage"`                                  // e.g., "128GB", "256GB"
	Color         string    `json:"color"`                                    // e.g., "Natural Titanium"
	Carrier       string    `json:"carrier"`                                  // e.g., "Unlocked", "Verizon"
	Price         float64   `json:"price"`                                    // Price in dollars
	Currency      string    `json:"currency" gorm:"default:'USD'"`            // Currency code
	IsActive      bool      `json:"is_active" gorm:"default:true"`            // Whether to monitor this product
	Category      string    `json:"category"`                                 // e.g., "iPhone", "iPad", "Mac"
	ProductURL    string    `json:"product_url"`                              // Direct link to product page
	ImageURL      string    `json:"image_url"`                                // Product image URL
	Description   string    `json:"description"`                              // Product description
	LaunchDate    time.Time `json:"launch_date"`                              // Product launch date
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	DeletedAt     gorm.DeletedAt `json:"deleted_at" gorm:"index"`
}

// StockInfo represents current stock information for a product at a specific store
type StockInfo struct {
	ID                  uint                `json:"id" gorm:"primaryKey"`
	ProductID           uint                `json:"product_id" gorm:"not null;index"`
	Product             AppleProduct        `json:"product" gorm:"foreignKey:ProductID"`
	StoreID             uint                `json:"store_id" gorm:"not null;index"`
	Store               AppleStoreInfo      `json:"store" gorm:"foreignKey:StoreID"`
	IsAvailable         bool                `json:"is_available"`                    // Current availability
	AvailabilityStatus  AvailabilityStatus  `json:"availability_status"`             // Detailed status
	LastCheckedAt       time.Time           `json:"last_checked_at"`                 // Last check timestamp
	LastAvailableAt     *time.Time          `json:"last_available_at"`               // Last time it was available
	LastUnavailableAt   *time.Time          `json:"last_unavailable_at"`             // Last time it became unavailable
	EstimatedArrival    *time.Time          `json:"estimated_arrival"`               // Estimated restock date
	PickupAvailable     bool                `json:"pickup_available"`                // Store pickup availability
	DeliveryAvailable   bool                `json:"delivery_available"`              // Delivery availability
	MaxOrderQuantity    int                 `json:"max_order_quantity"`              // Maximum order quantity
	CheckCount          int                 `json:"check_count" gorm:"default:0"`    // Total number of checks
	AvailableCount      int                 `json:"available_count" gorm:"default:0"` // Number of times found available
	AvailabilityRate    float64             `json:"availability_rate"`               // Percentage of availability
	CreatedAt           time.Time           `json:"created_at"`
	UpdatedAt           time.Time           `json:"updated_at"`
	DeletedAt           gorm.DeletedAt      `json:"deleted_at" gorm:"index"`
}

// AppleStoreInfo represents an Apple Store location
type AppleStoreInfo struct {
	ID                uint      `json:"id" gorm:"primaryKey"`
	StoreCode         string    `json:"store_code" gorm:"uniqueIndex;not null"` // Apple store code
	StoreName         string    `json:"store_name" gorm:"not null"`             // Store display name
	StoreType         StoreType `json:"store_type"`                             // Store type
	Address           string    `json:"address"`                                // Full address
	City              string    `json:"city" gorm:"index"`                      // City name
	State             string    `json:"state" gorm:"index"`                     // State/Province
	Country           string    `json:"country" gorm:"index"`                   // Country code
	ZipCode           string    `json:"zip_code"`                               // Postal code
	Phone             string    `json:"phone"`                                  // Store phone number
	Email             string    `json:"email"`                                  // Store email
	Latitude          float64   `json:"latitude"`                               // GPS latitude
	Longitude         float64   `json:"longitude"`                              // GPS longitude
	TimeZone          string    `json:"time_zone"`                              // Store timezone
	OpeningHours      string    `json:"opening_hours"`                          // JSON string of opening hours
	IsActive          bool      `json:"is_active" gorm:"default:true"`          // Whether to monitor this store
	SupportPickup     bool      `json:"support_pickup" gorm:"default:true"`     // Whether store supports pickup
	SupportDelivery   bool      `json:"support_delivery" gorm:"default:false"`  // Whether store supports delivery
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
	DeletedAt         gorm.DeletedAt `json:"deleted_at" gorm:"index"`
}

// StockChangeEvent represents a stock availability change event
type StockChangeEvent struct {
	ID               uint               `json:"id" gorm:"primaryKey"`
	ProductID        uint               `json:"product_id" gorm:"not null;index"`
	Product          AppleProduct       `json:"product" gorm:"foreignKey:ProductID"`
	StoreID          uint               `json:"store_id" gorm:"not null;index"`
	Store            AppleStoreInfo     `json:"store" gorm:"foreignKey:StoreID"`
	EventType        StockEventType     `json:"event_type" gorm:"not null"`         // Type of change
	PreviousStatus   AvailabilityStatus `json:"previous_status"`                    // Previous availability status
	CurrentStatus    AvailabilityStatus `json:"current_status"`                     // Current availability status
	EventTime        time.Time          `json:"event_time" gorm:"not null;index"`   // When the change occurred
	DetectedAt       time.Time          `json:"detected_at" gorm:"not null"`        // When we detected the change
	NotificationSent bool               `json:"notification_sent" gorm:"default:false"` // Whether notification was sent
	NotifiedAt       *time.Time         `json:"notified_at"`                        // When notification was sent
	Metadata         string             `json:"metadata"`                           // Additional event data (JSON)
	CreatedAt        time.Time          `json:"created_at"`
	UpdatedAt        time.Time          `json:"updated_at"`
}

// StockMonitorConfig represents configuration for stock monitoring
type StockMonitorConfig struct {
	ID                     uint                   `json:"id" gorm:"primaryKey"`
	ConfigName             string                 `json:"config_name" gorm:"uniqueIndex;not null"` // Unique config name
	IsActive               bool                   `json:"is_active" gorm:"default:true"`           // Whether config is active
	MonitorInterval        int                    `json:"monitor_interval" gorm:"default:300"`     // Check interval in seconds
	EnableNotifications    bool                   `json:"enable_notifications" gorm:"default:true"` // Enable notifications
	NotificationChannels   string                 `json:"notification_channels"`                   // JSON array of channels
	NotificationFrequency  NotificationFrequency  `json:"notification_frequency" gorm:"default:1"` // Notification frequency
	QuietHoursStart        string                 `json:"quiet_hours_start"`                       // Quiet hours start time (HH:MM)
	QuietHoursEnd          string                 `json:"quiet_hours_end"`                         // Quiet hours end time (HH:MM)
	MaxDailyNotifications  int                    `json:"max_daily_notifications" gorm:"default:10"` // Max notifications per day
	ProductFilters         string                 `json:"product_filters"`                         // JSON filters for products
	StoreFilters           string                 `json:"store_filters"`                           // JSON filters for stores
	PriorityProducts       string                 `json:"priority_products"`                       // JSON array of priority product IDs
	WebhookURL             string                 `json:"webhook_url"`                             // Webhook endpoint for notifications
	EmailRecipients        string                 `json:"email_recipients"`                        // JSON array of email addresses
	SMSRecipients          string                 `json:"sms_recipients"`                          // JSON array of phone numbers
	UserAgent              string                 `json:"user_agent"`                              // Custom user agent for requests
	RequestTimeout         int                    `json:"request_timeout" gorm:"default:30"`       // HTTP request timeout in seconds
	MaxRetries             int                    `json:"max_retries" gorm:"default:3"`            // Max retry attempts
	RateLimitPerMinute     int                    `json:"rate_limit_per_minute" gorm:"default:10"` // Rate limit for API calls
	EnableDetailedLogging  bool                   `json:"enable_detailed_logging" gorm:"default:false"` // Enable detailed logging
	CreatedAt              time.Time              `json:"created_at"`
	UpdatedAt              time.Time              `json:"updated_at"`
	DeletedAt              gorm.DeletedAt         `json:"deleted_at" gorm:"index"`
}

// StockMonitorResult represents the result of a stock monitoring task
type StockMonitorResult struct {
	ID                    uint                    `json:"id" gorm:"primaryKey"`
	TaskID                string                  `json:"task_id" gorm:"uniqueIndex;not null"` // Reference to task system
	ConfigID              uint                    `json:"config_id" gorm:"not null;index"`     // Which config was used
	Config                StockMonitorConfig      `json:"config" gorm:"foreignKey:ConfigID"`
	StartTime             time.Time               `json:"start_time" gorm:"not null"`          // Task start time
	EndTime               time.Time               `json:"end_time"`                            // Task end time
	Duration              int64                   `json:"duration"`                            // Duration in milliseconds
	Status                TaskExecutionStatus     `json:"status" gorm:"not null"`              // Task execution status
	ProductsChecked       int                     `json:"products_checked" gorm:"default:0"`   // Number of products checked
	StoresChecked         int                     `json:"stores_checked" gorm:"default:0"`     // Number of stores checked
	TotalChecks           int                     `json:"total_checks" gorm:"default:0"`       // Total API calls made
	AvailabilityChanges   int                     `json:"availability_changes" gorm:"default:0"` // Number of availability changes detected
	NotificationsSent     int                     `json:"notifications_sent" gorm:"default:0"` // Number of notifications sent
	ErrorCount            int                     `json:"error_count" gorm:"default:0"`        // Number of errors encountered
	ErrorDetails          string                  `json:"error_details"`                       // JSON array of error details
	SuccessRate           float64                 `json:"success_rate"`                        // Success rate percentage
	AverageResponseTime   float64                 `json:"average_response_time"`               // Average API response time in ms
	APICallsUsed          int                     `json:"api_calls_used" gorm:"default:0"`     // Number of API calls consumed
	RateLimitHits         int                     `json:"rate_limit_hits" gorm:"default:0"`    // Number of rate limit hits
	Summary               string                  `json:"summary"`                             // Task execution summary
	NewlyAvailableItems   string                  `json:"newly_available_items"`               // JSON array of newly available items
	NewlyUnavailableItems string                  `json:"newly_unavailable_items"`             // JSON array of newly unavailable items
	CreatedAt             time.Time               `json:"created_at"`
	UpdatedAt             time.Time               `json:"updated_at"`
}

// Enum types
type AvailabilityStatus string

const (
	StatusAvailable         AvailabilityStatus = "available"          // In stock
	StatusUnavailable       AvailabilityStatus = "unavailable"        // Out of stock
	StatusLimitedStock      AvailabilityStatus = "limited_stock"      // Limited availability
	StatusPreOrder          AvailabilityStatus = "pre_order"          // Available for pre-order
	StatusBackOrder         AvailabilityStatus = "back_order"         // Back ordered
	StatusDiscontinued      AvailabilityStatus = "discontinued"       // No longer available
	StatusTemporarilyOffline AvailabilityStatus = "temporarily_offline" // Temporarily not available
	StatusUnknown           AvailabilityStatus = "unknown"            // Status could not be determined
)

type StoreType string

const (
	StoreTypeRetail   StoreType = "retail"   // Apple Store retail location
	StoreTypeOnline   StoreType = "online"   // Apple Online Store
	StoreTypeAuthorized StoreType = "authorized" // Authorized reseller
	StoreTypeCarrier  StoreType = "carrier"  // Carrier store (Verizon, AT&T, etc.)
)

type StockEventType string

const (
	EventStockIn       StockEventType = "stock_in"       // Item came into stock
	EventStockOut      StockEventType = "stock_out"      // Item went out of stock
	EventStockLimited  StockEventType = "stock_limited"  // Stock became limited
	EventStockUnlimited StockEventType = "stock_unlimited" // Stock became unlimited
	EventPriceChange   StockEventType = "price_change"   // Price changed
	EventDiscontinued  StockEventType = "discontinued"   // Item discontinued
	EventPreOrderStart StockEventType = "preorder_start" // Pre-order started
	EventPreOrderEnd   StockEventType = "preorder_end"   // Pre-order ended
)

type NotificationFrequency int

const (
	NotifyAlways      NotificationFrequency = 1  // Notify on every change
	NotifyHourly      NotificationFrequency = 60  // Notify at most once per hour
	NotifyDaily       NotificationFrequency = 1440 // Notify at most once per day
	NotifyWeekly      NotificationFrequency = 10080 // Notify at most once per week
)

type TaskExecutionStatus string

const (
	StatusPending   TaskExecutionStatus = "pending"   // Task is pending
	StatusRunning   TaskExecutionStatus = "running"   // Task is currently running
	StatusCompleted TaskExecutionStatus = "completed" // Task completed successfully
	StatusFailed    TaskExecutionStatus = "failed"    // Task failed
	StatusCancelled TaskExecutionStatus = "cancelled" // Task was cancelled
	StatusTimeout   TaskExecutionStatus = "timeout"   // Task timed out
)

// Helper methods for AppleProduct
func (p *AppleProduct) GetFullName() string {
	if p.Storage != "" && p.Color != "" {
		return p.ProductName + " " + p.Storage + " " + p.Color
	}
	if p.Storage != "" {
		return p.ProductName + " " + p.Storage
	}
	if p.Color != "" {
		return p.ProductName + " " + p.Color
	}
	return p.ProductName
}

func (p *AppleProduct) IsPhone() bool {
	return p.Category == "iPhone"
}

func (p *AppleProduct) IsPad() bool {
	return p.Category == "iPad"
}

func (p *AppleProduct) IsMac() bool {
	return p.Category == "Mac" || p.Category == "MacBook"
}

// Helper methods for StockInfo
func (s *StockInfo) UpdateAvailability(isAvailable bool, status AvailabilityStatus) {
	now := time.Now()
	s.LastCheckedAt = now
	s.CheckCount++
	
	if isAvailable != s.IsAvailable {
		if isAvailable {
			s.LastAvailableAt = &now
		} else {
			s.LastUnavailableAt = &now
		}
	}
	
	s.IsAvailable = isAvailable
	s.AvailabilityStatus = status
	
	if isAvailable {
		s.AvailableCount++
	}
	
	if s.CheckCount > 0 {
		s.AvailabilityRate = float64(s.AvailableCount) / float64(s.CheckCount) * 100
	}
}

// Helper methods for StockChangeEvent
func (e *StockChangeEvent) IsStockInEvent() bool {
	return e.EventType == EventStockIn
}

func (e *StockChangeEvent) IsStockOutEvent() bool {
	return e.EventType == EventStockOut
}

func (e *StockChangeEvent) IsPriceChangeEvent() bool {
	return e.EventType == EventPriceChange
}

// Helper methods for StockMonitorConfig
func (c *StockMonitorConfig) IsInQuietHours(t time.Time) bool {
	if c.QuietHoursStart == "" || c.QuietHoursEnd == "" {
		return false
	}
	
	currentTime := t.Format("15:04")
	return currentTime >= c.QuietHoursStart && currentTime <= c.QuietHoursEnd
}

func (c *StockMonitorConfig) ShouldNotify(lastNotification time.Time) bool {
	if !c.EnableNotifications {
		return false
	}
	
	if c.IsInQuietHours(time.Now()) {
		return false
	}
	
	switch c.NotificationFrequency {
	case NotifyAlways:
		return true
	case NotifyHourly:
		return time.Since(lastNotification) >= time.Hour
	case NotifyDaily:
		return time.Since(lastNotification) >= 24*time.Hour
	case NotifyWeekly:
		return time.Since(lastNotification) >= 7*24*time.Hour
	default:
		return true
	}
}

// TableName methods for custom table names
func (AppleProduct) TableName() string {
	return "apple_products"
}

func (StockInfo) TableName() string {
	return "apple_stock_info"
}

func (AppleStoreInfo) TableName() string {
	return "apple_stores"
}

func (StockChangeEvent) TableName() string {
	return "apple_stock_events"
}

func (StockMonitorConfig) TableName() string {
	return "apple_monitor_configs"
}

func (StockMonitorResult) TableName() string {
	return "apple_monitor_results"
}