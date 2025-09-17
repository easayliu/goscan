package alicloud

import (
	"fmt"
	"strings"
	"time"
)

// Implements Validator interface
type validator struct{}

// NewValidator 创建验证器
func NewValidator() Validator {
	return &validator{}
}

// ValidateBillingCycle 验证账期格式
func (v *validator) ValidateBillingCycle(billingCycle string) error {
	if billingCycle == "" {
		return ErrEmptyBillingCycle
	}

	// 验证格式 YYYY-MM
	_, err := time.Parse("2006-01", billingCycle)
	if err != nil {
		return fmt.Errorf("%w: expected YYYY-MM format, got %s",
			ErrInvalidBillingCycle, billingCycle)
	}

	// 验证不能是未来时间
	billingTime, _ := time.Parse("2006-01", billingCycle)
	now := time.Now()
	currentMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	if billingTime.After(currentMonth) {
		return fmt.Errorf("%w: billing cycle %s is in the future",
			ErrFutureBillingCycle, billingCycle)
	}

	// 验证不能太早（2018年之前）
	minTime := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	if billingTime.Before(minTime) {
		return fmt.Errorf("%w: billing cycle %s is before 2018",
			ErrTooOldBillingCycle, billingCycle)
	}

	return nil
}

// ValidateBillingDate 验证账单日期格式
func (v *validator) ValidateBillingDate(billingDate string) error {
	if billingDate == "" {
		return nil // 空日期是允许的（用于月度数据）
	}

	// 验证格式 YYYY-MM-DD
	_, err := time.Parse("2006-01-02", billingDate)
	if err != nil {
		return fmt.Errorf("%w: expected YYYY-MM-DD format, got %s",
			ErrInvalidBillingDate, billingDate)
	}

	// 验证不能是未来时间
	billingTime, _ := time.Parse("2006-01-02", billingDate)
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	if billingTime.After(today) {
		return fmt.Errorf("%w: billing date %s is in the future",
			ErrInvalidBillingDate, billingDate)
	}

	// 验证不能太早（2018年之前）
	minTime := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)
	if billingTime.Before(minTime) {
		return fmt.Errorf("%w: billing date %s is before 2018",
			ErrInvalidBillingDate, billingDate)
	}

	return nil
}

// ValidateGranularity 验证粒度参数
func (v *validator) ValidateGranularity(granularity string) error {
	switch strings.ToUpper(granularity) {
	case "MONTHLY", "DAILY", "BOTH":
		return nil
	default:
		return fmt.Errorf("%w: expected MONTHLY, DAILY, or BOTH, got %s",
			ErrInvalidGranularity, granularity)
	}
}

// ValidateRequest 验证请求参数
func (v *validator) ValidateRequest(req *DescribeInstanceBillRequest) error {
	if req == nil {
		return NewValidationError("request", nil, "request cannot be nil")
	}

	// 验证账期
	if err := v.ValidateBillingCycle(req.BillingCycle); err != nil {
		return err
	}

	// 验证粒度
	if req.Granularity != "" {
		if err := v.ValidateGranularity(req.Granularity); err != nil {
			return err
		}
	}

	// 验证账单日期
	if err := v.ValidateBillingDate(req.BillingDate); err != nil {
		return err
	}

	// 验证账单日期与粒度的一致性
	if req.Granularity == "DAILY" && req.BillingDate == "" {
		return NewValidationError("BillingDate", req.BillingDate,
			"BillingDate is required when Granularity is DAILY")
	}

	// 验证账单日期与账期的一致性
	if req.BillingDate != "" {
		billingDate, err := time.Parse("2006-01-02", req.BillingDate)
		if err == nil {
			expectedMonth := billingDate.Format("2006-01")
			if expectedMonth != req.BillingCycle {
				return NewValidationError("BillingDate", req.BillingDate,
					fmt.Sprintf("BillingDate (%s) must be in the same month as BillingCycle (%s)",
						req.BillingDate, req.BillingCycle))
			}
		}
	}

	// 验证分页参数
	if req.MaxResults != 0 && (req.MaxResults < 1 || req.MaxResults > 300) {
		return NewValidationError("MaxResults", req.MaxResults,
			"MaxResults must be between 1 and 300")
	}

	return nil
}

// ValidateBillDetail 验证账单明细
func (v *validator) ValidateBillDetail(bill *BillDetail) error {
	if bill == nil {
		return NewValidationError("bill", nil, "bill detail cannot be nil")
	}

	// 验证必需字段
	if bill.InstanceID == "" {
		return NewValidationError("InstanceID", bill.InstanceID, "InstanceID cannot be empty")
	}

	if bill.ProductCode == "" {
		return NewValidationError("ProductCode", bill.ProductCode, "ProductCode cannot be empty")
	}

	// 验证账单日期（如果存在）
	if bill.BillingDate != "" {
		if err := v.ValidateBillingDate(bill.BillingDate); err != nil {
			return err
		}
	}

	return nil
}

// ValidateTableName 验证表名格式
func (v *validator) ValidateTableName(tableName string) error {
	if tableName == "" {
		return fmt.Errorf("%w: table name cannot be empty", ErrInvalidTableName)
	}

	// 检查表名长度
	if len(tableName) > 100 {
		return fmt.Errorf("%w: table name too long (max 100 characters)", ErrInvalidTableName)
	}

	// 检查表名字符（允许字母、数字、下划线）
	for _, char := range tableName {
		if !((char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_') {
			return fmt.Errorf("%w: table name contains invalid character '%c'",
				ErrInvalidTableName, char)
		}
	}

	// 检查表名不能以数字开头
	if tableName[0] >= '0' && tableName[0] <= '9' {
		return fmt.Errorf("%w: table name cannot start with a number", ErrInvalidTableName)
	}

	return nil
}

// 便利函数，保持向后兼容

// ValidateBillingCycle 验证账期格式（包级函数）
func ValidateBillingCycle(billingCycle string) error {
	v := NewValidator()
	return v.ValidateBillingCycle(billingCycle)
}

// ValidateBillingDate 验证账单日期格式（包级函数）
func ValidateBillingDate(billingDate string) error {
	v := NewValidator()
	return v.ValidateBillingDate(billingDate)
}

// ValidateGranularity 验证粒度参数（包级函数）
func ValidateGranularity(granularity string) error {
	v := NewValidator()
	return v.ValidateGranularity(granularity)
}

// GenerateDatesInMonth 生成指定月份的所有日期
func GenerateDatesInMonth(billingCycle string) ([]string, error) {
	// 验证账期格式
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return nil, err
	}

	// 解析账期
	startTime, err := time.Parse("2006-01", billingCycle)
	if err != nil {
		return nil, fmt.Errorf("failed to parse billing cycle: %w", err)
	}

	// 获取月份的第一天和最后一天
	year := startTime.Year()
	month := startTime.Month()
	firstDay := time.Date(year, month, 1, 0, 0, 0, 0, time.UTC)
	lastDay := firstDay.AddDate(0, 1, -1)

	// 生成日期列表
	var dates []string
	current := firstDay

	for current.Day() <= lastDay.Day() {
		dates = append(dates, current.Format("2006-01-02"))
		current = current.AddDate(0, 0, 1)
	}

	return dates, nil
}

// IsValidBillingCycle 检查账期是否有效
func IsValidBillingCycle(billingCycle string) bool {
	return ValidateBillingCycle(billingCycle) == nil
}

// IsValidBillingDate 检查账单日期是否有效
func IsValidBillingDate(billingDate string) bool {
	return ValidateBillingDate(billingDate) == nil
}

// IsValidGranularity 检查粒度是否有效
func IsValidGranularity(granularity string) bool {
	return ValidateGranularity(granularity) == nil
}

// NormalizeBillingCycle 规范化账期格式
func NormalizeBillingCycle(billingCycle string) (string, error) {
	// 去除空格
	billingCycle = strings.TrimSpace(billingCycle)

	// 验证格式
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return "", err
	}

	// 解析并重新格式化
	t, err := time.Parse("2006-01", billingCycle)
	if err != nil {
		return "", err
	}

	return t.Format("2006-01"), nil
}

// NormalizeBillingDate 规范化账单日期格式
func NormalizeBillingDate(billingDate string) (string, error) {
	if billingDate == "" {
		return "", nil
	}

	// 去除空格
	billingDate = strings.TrimSpace(billingDate)

	// 验证格式
	if err := ValidateBillingDate(billingDate); err != nil {
		return "", err
	}

	// 解析并重新格式化
	t, err := time.Parse("2006-01-02", billingDate)
	if err != nil {
		return "", err
	}

	return t.Format("2006-01-02"), nil
}

// GetCurrentBillingCycle 获取当前账期
func GetCurrentBillingCycle() string {
	now := time.Now()
	// 通常账单数据会延迟，所以使用上个月
	lastMonth := now.AddDate(0, -1, 0)
	return lastMonth.Format("2006-01")
}

// GetPreviousBillingCycle 获取前一个账期
func GetPreviousBillingCycle(billingCycle string) (string, error) {
	t, err := time.Parse("2006-01", billingCycle)
	if err != nil {
		return "", fmt.Errorf("invalid billing cycle format: %w", err)
	}

	previous := t.AddDate(0, -1, 0)
	return previous.Format("2006-01"), nil
}

// GetNextBillingCycle 获取下一个账期
func GetNextBillingCycle(billingCycle string) (string, error) {
	t, err := time.Parse("2006-01", billingCycle)
	if err != nil {
		return "", fmt.Errorf("invalid billing cycle format: %w", err)
	}

	next := t.AddDate(0, 1, 0)
	return next.Format("2006-01"), nil
}
