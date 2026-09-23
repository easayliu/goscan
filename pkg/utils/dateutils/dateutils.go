package dateutils

import (
	"fmt"
	"strings"
	"time"
)

// Common date layouts used throughout the application
const (
	LayoutYearMonth = "2006-01"
	LayoutDate      = "2006-01-02"
	LayoutDateTime  = "2006-01-02 15:04:05"
	MinValidYear    = 2018
)

// Granularity types for bill periods
const (
	GranularityMonthly = "monthly"
	GranularityDaily   = "daily"
	GranularityBoth    = "both"
)

// BillPeriodInfo contains parsed bill period information
type BillPeriodInfo struct {
	Date        time.Time
	Granularity string
	Period      string
}

// ParseBillPeriod parses a bill period string and determines its granularity
// Supports formats: "2006-01" (monthly), "2006-01-02" (daily)
// Returns the parsed date, detected granularity, and any error
func ParseBillPeriod(period string) (time.Time, string, error) {
	if period == "" {
		return time.Time{}, "", fmt.Errorf("bill period cannot be empty")
	}

	period = strings.TrimSpace(period)

	// Try parsing as daily format first (2006-01-02)
	if date, err := time.Parse(LayoutDate, period); err == nil {
		if err := validateDateRange(date); err != nil {
			return time.Time{}, "", err
		}
		return date, GranularityDaily, nil
	}

	// Try parsing as monthly format (2006-01)
	if date, err := time.Parse(LayoutYearMonth, period); err == nil {
		if err := validateDateRange(date); err != nil {
			return time.Time{}, "", err
		}
		return date, GranularityMonthly, nil
	}

	return time.Time{}, "", fmt.Errorf("invalid bill period format: %s, expected YYYY-MM or YYYY-MM-DD", period)
}

// FormatBillPeriod formats a date according to the specified granularity
func FormatBillPeriod(date time.Time, granularity string) string {
	switch strings.ToLower(granularity) {
	case GranularityDaily:
		return date.Format(LayoutDate)
	case GranularityMonthly:
		return date.Format(LayoutYearMonth)
	default:
		// Default to monthly format
		return date.Format(LayoutYearMonth)
	}
}

// GetLastMonthPeriod returns the previous month in YYYY-MM format
func GetLastMonthPeriod() string {
	now := time.Now()
	lastMonth := now.AddDate(0, -1, 0)
	return lastMonth.Format(LayoutYearMonth)
}

// GetYesterdayPeriod returns yesterday's date in YYYY-MM-DD format
func GetYesterdayPeriod() string {
	yesterday := time.Now().AddDate(0, 0, -1)
	return yesterday.Format(LayoutDate)
}

// ParseFlexibleDate attempts to parse a date string with multiple possible formats
// Supports: "2006-01-02", "2006-01", "2006-01-02 15:04:05"
func ParseFlexibleDate(dateStr string) (time.Time, error) {
	if dateStr == "" {
		return time.Time{}, nil // Empty date is allowed in some contexts
	}

	dateStr = strings.TrimSpace(dateStr)

	// Try different formats in order of specificity
	formats := []string{
		LayoutDateTime,
		LayoutDate,
		LayoutYearMonth,
	}

	for _, format := range formats {
		if date, err := time.Parse(format, dateStr); err == nil {
			return date, nil
		}
	}

	return time.Time{}, fmt.Errorf("unable to parse date: %s", dateStr)
}

// ValidateBillingCycle validates a billing cycle format (YYYY-MM)
func ValidateBillingCycle(billingCycle string) error {
	if billingCycle == "" {
		return fmt.Errorf("billing cycle cannot be empty")
	}

	// Parse the billing cycle
	billingTime, err := time.Parse(LayoutYearMonth, billingCycle)
	if err != nil {
		return fmt.Errorf("invalid billing cycle format: expected YYYY-MM, got %s", billingCycle)
	}

	return validateDateRange(billingTime)
}

// ValidateBillingDate validates a billing date format (YYYY-MM-DD)
func ValidateBillingDate(billingDate string) error {
	if billingDate == "" {
		return nil // Empty date is allowed for monthly data
	}

	// Parse the billing date
	billingTime, err := time.Parse(LayoutDate, billingDate)
	if err != nil {
		return fmt.Errorf("invalid billing date format: expected YYYY-MM-DD, got %s", billingDate)
	}

	return validateDateRange(billingTime)
}

// ValidateGranularity validates granularity parameter
func ValidateGranularity(granularity string) error {
	switch strings.ToLower(granularity) {
	case GranularityMonthly, GranularityDaily, GranularityBoth:
		return nil
	default:
		return fmt.Errorf("invalid granularity: expected monthly, daily, or both, got %s", granularity)
	}
}

// GetCurrentBillingCycle returns the current billing cycle (usually previous month)
func GetCurrentBillingCycle() string {
	return GetLastMonthPeriod()
}

// GetPreviousBillingCycle returns the previous billing cycle for a given cycle
func GetPreviousBillingCycle(billingCycle string) (string, error) {
	date, err := time.Parse(LayoutYearMonth, billingCycle)
	if err != nil {
		return "", fmt.Errorf("invalid billing cycle format: %w", err)
	}

	previous := date.AddDate(0, -1, 0)
	return previous.Format(LayoutYearMonth), nil
}

// GetNextBillingCycle returns the next billing cycle for a given cycle
func GetNextBillingCycle(billingCycle string) (string, error) {
	date, err := time.Parse(LayoutYearMonth, billingCycle)
	if err != nil {
		return "", fmt.Errorf("invalid billing cycle format: %w", err)
	}

	next := date.AddDate(0, 1, 0)
	return next.Format(LayoutYearMonth), nil
}

// GenerateDatesInMonth generates all dates within a specified month
func GenerateDatesInMonth(billingCycle string) ([]string, error) {
	// Validate and parse the billing cycle
	if err := ValidateBillingCycle(billingCycle); err != nil {
		return nil, err
	}

	startTime, err := time.Parse(LayoutYearMonth, billingCycle)
	if err != nil {
		return nil, fmt.Errorf("failed to parse billing cycle: %w", err)
	}

	// Walk the days while they are still in the month. Comparing the day of
	// the month against the last day's, as this used to, does not stop at the
	// month's end: February ran on to 28 March, and a 31-day month never
	// stopped at all, since no day is ever past the 31st.
	month := startTime.Month()
	var dates []string
	for current := time.Date(startTime.Year(), month, 1, 0, 0, 0, 0, time.UTC); current.Month() == month; current = current.AddDate(0, 0, 1) {
		dates = append(dates, current.Format(LayoutDate))
	}

	return dates, nil
}

// NormalizeBillingCycle normalizes a billing cycle string to standard format
func NormalizeBillingCycle(billingCycle string) (string, error) {
	billingCycle = strings.TrimSpace(billingCycle)

	if err := ValidateBillingCycle(billingCycle); err != nil {
		return "", err
	}

	date, err := time.Parse(LayoutYearMonth, billingCycle)
	if err != nil {
		return "", err
	}

	return date.Format(LayoutYearMonth), nil
}

// NormalizeBillingDate normalizes a billing date string to standard format
func NormalizeBillingDate(billingDate string) (string, error) {
	if billingDate == "" {
		return "", nil
	}

	billingDate = strings.TrimSpace(billingDate)

	if err := ValidateBillingDate(billingDate); err != nil {
		return "", err
	}

	date, err := time.Parse(LayoutDate, billingDate)
	if err != nil {
		return "", err
	}

	return date.Format(LayoutDate), nil
}

// IsValidBillingCycle checks if a billing cycle is valid
func IsValidBillingCycle(billingCycle string) bool {
	return ValidateBillingCycle(billingCycle) == nil
}

// IsValidBillingDate checks if a billing date is valid
func IsValidBillingDate(billingDate string) bool {
	return ValidateBillingDate(billingDate) == nil
}

// IsValidGranularity checks if granularity is valid
func IsValidGranularity(granularity string) bool {
	return ValidateGranularity(granularity) == nil
}

// IsFutureDate checks if a date is in the future
func IsFutureDate(date time.Time) bool {
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	dateOnly := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	return dateOnly.After(today)
}

// IsFutureBillingCycle checks if a billing cycle is in the future
func IsFutureBillingCycle(billingCycle string) bool {
	billingTime, err := time.Parse(LayoutYearMonth, billingCycle)
	if err != nil {
		return false
	}

	now := time.Now()
	currentMonth := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	return billingTime.After(currentMonth)
}

// validateDateRange validates that a date is within acceptable range
func validateDateRange(date time.Time) error {
	// Check if date is too old (before 2018)
	minTime := time.Date(MinValidYear, 1, 1, 0, 0, 0, 0, time.UTC)
	if date.Before(minTime) {
		return fmt.Errorf("date %s is before %d", date.Format(LayoutDate), MinValidYear)
	}

	// Check if date is in the future
	if IsFutureDate(date) {
		return fmt.Errorf("date %s is in the future", date.Format(LayoutDate))
	}

	return nil
}

// DetermineGranularityFromPeriod automatically determines granularity from period format
func DetermineGranularityFromPeriod(period string) (string, error) {
	_, granularity, err := ParseBillPeriod(period)
	return granularity, err
}

// ConvertPeriodToDate converts a period string to time.Time based on its format
func ConvertPeriodToDate(period string) (time.Time, error) {
	date, _, err := ParseBillPeriod(period)
	return date, err
}

// ValidatePeriodDateConsistency validates that a billing date is within the billing cycle
func ValidatePeriodDateConsistency(billingCycle, billingDate string) error {
	if billingDate == "" {
		return nil // Empty date is allowed
	}

	date, err := time.Parse(LayoutDate, billingDate)
	if err != nil {
		return fmt.Errorf("invalid billing date format: %w", err)
	}

	expectedMonth := date.Format(LayoutYearMonth)
	if expectedMonth != billingCycle {
		return fmt.Errorf("billing date (%s) must be in the same month as billing cycle (%s)",
			billingDate, billingCycle)
	}

	return nil
}

// MaxPeriodsInRange caps how many periods one range may expand into. A manual
// backfill is a handful of months; a typo like 2018-01..2026-09 at daily
// granularity would otherwise queue thousands of API-bound periods into a
// single task that nobody can cancel halfway.
const MaxPeriodsInRange = 400

// ExpandPeriodRange turns an inclusive start..end range into the list of
// periods the sync should walk, one entry per period.
//
// The step follows the format of the endpoints: YYYY-MM walks months, and
// YYYY-MM-DD walks days. Both ends have to use the same one — a range from a
// month to a day has no single answer, and guessing it would silently sync
// something other than what was asked for. Either end may be left empty, in
// which case the other is the whole range.
func ExpandPeriodRange(start, end string) ([]string, error) {
	start, end = strings.TrimSpace(start), strings.TrimSpace(end)
	switch {
	case start == "" && end == "":
		return nil, nil
	case start == "":
		start = end
	case end == "":
		end = start
	}

	from, startGranularity, err := ParseBillPeriod(start)
	if err != nil {
		return nil, fmt.Errorf("invalid start period: %w", err)
	}
	to, endGranularity, err := ParseBillPeriod(end)
	if err != nil {
		return nil, fmt.Errorf("invalid end period: %w", err)
	}
	if startGranularity != endGranularity {
		return nil, fmt.Errorf("period range must use one format on both ends, got %s and %s", start, end)
	}
	if from.After(to) {
		return nil, fmt.Errorf("start period %s is after end period %s", start, end)
	}

	var periods []string
	for current := from; !current.After(to); {
		periods = append(periods, FormatBillPeriod(current, startGranularity))
		if len(periods) > MaxPeriodsInRange {
			return nil, fmt.Errorf("period range %s..%s expands to more than %d periods, narrow it down",
				start, end, MaxPeriodsInRange)
		}
		if startGranularity == GranularityDaily {
			current = current.AddDate(0, 0, 1)
		} else {
			current = current.AddDate(0, 1, 0)
		}
	}

	return periods, nil
}
