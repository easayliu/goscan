package alicloud

import (
	"fmt"
	"goscan/pkg/utils/dateutils"
	"strings"
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

	// 使用通用日期工具验证
	if err := dateutils.ValidateBillingCycle(billingCycle); err != nil {
		// 根据具体错误类型返回对应的错误
		if strings.Contains(err.Error(), "future") {
			return fmt.Errorf("%w: %v", ErrFutureBillingCycle, err)
		}
		if strings.Contains(err.Error(), "before 2018") {
			return fmt.Errorf("%w: %v", ErrTooOldBillingCycle, err)
		}
		return fmt.Errorf("%w: %v", ErrInvalidBillingCycle, err)
	}

	return nil
}

// ValidateBillingDate 验证账单日期格式
func (v *validator) ValidateBillingDate(billingDate string) error {
	if billingDate == "" {
		return nil // 空日期是允许的（用于月度数据）
	}

	// 使用通用日期工具验证
	if err := dateutils.ValidateBillingDate(billingDate); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidBillingDate, err)
	}

	return nil
}

// ValidateGranularity 验证粒度参数
func (v *validator) ValidateGranularity(granularity string) error {
	// 转换为小写后使用通用验证
	lowerGranularity := strings.ToLower(granularity)
	if err := dateutils.ValidateGranularity(lowerGranularity); err != nil {
		return fmt.Errorf("%w: expected MONTHLY, DAILY, or BOTH, got %s",
			ErrInvalidGranularity, granularity)
	}
	return nil
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
	if err := dateutils.ValidatePeriodDateConsistency(req.BillingCycle, req.BillingDate); err != nil {
		return NewValidationError("BillingDate", req.BillingDate, err.Error())
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
	// 使用通用日期工具
	return dateutils.GenerateDatesInMonth(billingCycle)
}

// IsValidBillingCycle 检查账期是否有效
func IsValidBillingCycle(billingCycle string) bool {
	return dateutils.IsValidBillingCycle(billingCycle)
}

// IsValidBillingDate 检查账单日期是否有效
func IsValidBillingDate(billingDate string) bool {
	return dateutils.IsValidBillingDate(billingDate)
}

// IsValidGranularity 检查粒度是否有效
func IsValidGranularity(granularity string) bool {
	return dateutils.IsValidGranularity(strings.ToLower(granularity))
}

// NormalizeBillingCycle 规范化账期格式
func NormalizeBillingCycle(billingCycle string) (string, error) {
	return dateutils.NormalizeBillingCycle(billingCycle)
}

// NormalizeBillingDate 规范化账单日期格式
func NormalizeBillingDate(billingDate string) (string, error) {
	return dateutils.NormalizeBillingDate(billingDate)
}

// GetCurrentBillingCycle 获取当前账期
func GetCurrentBillingCycle() string {
	return dateutils.GetCurrentBillingCycle()
}

// GetPreviousBillingCycle 获取前一个账期
func GetPreviousBillingCycle(billingCycle string) (string, error) {
	return dateutils.GetPreviousBillingCycle(billingCycle)
}

// GetNextBillingCycle 获取下一个账期
func GetNextBillingCycle(billingCycle string) (string, error) {
	return dateutils.GetNextBillingCycle(billingCycle)
}
