package alicloud

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/cloudsync"
	"goscan/pkg/config"
	"time"
)

// AliCloudProvider implements the CloudProvider interface for Alibaba Cloud
type AliCloudProvider struct {
	config      *config.AliCloudConfig
	chClient    *clickhouse.Client
	billService *BillService
}

// NewAliCloudProvider creates a new AliCloud provider
func NewAliCloudProvider(config *config.AliCloudConfig, chClient *clickhouse.Client) *AliCloudProvider {
	return &AliCloudProvider{
		config:   config,
		chClient: chClient,
	}
}

// GetProviderName returns the provider name
func (p *AliCloudProvider) GetProviderName() string {
	return "alicloud"
}

// ValidateCredentials validates the provider credentials
func (p *AliCloudProvider) ValidateCredentials(ctx context.Context) error {
	if p.config.AccessKeyID == "" || p.config.AccessKeySecret == "" {
		return fmt.Errorf("%w: alicloud access key id or secret", cloudsync.ErrCredentialsNotConfigured)
	}

	// Initialize bill service if not already done
	if err := p.initBillService(); err != nil {
		return fmt.Errorf("failed to initialize bill service: %w", err)
	}

	// Test connection
	return p.billService.TestConnection(ctx)
}

// Close closes the provider connection
func (p *AliCloudProvider) Close() error {
	if p.billService != nil {
		return p.billService.Close()
	}
	return nil
}

// GetAPIDataCount gets the count of data from API
func (p *AliCloudProvider) GetAPIDataCount(ctx context.Context, period, granularity string) (int64, error) {
	if err := p.initBillService(); err != nil {
		return 0, fmt.Errorf("failed to initialize bill service: %w", err)
	}

	if granularity == "monthly" {
		count, err := p.billService.GetMonthlyAPIDataCount(ctx, period)
		return int64(count), err
	} else if granularity == "daily" {
		count, err := p.billService.GetDailyAPIDataCount(ctx, period)
		return int64(count), err
	}

	return 0, fmt.Errorf("unsupported granularity: %s", granularity)
}

// FetchBillData fetches bill data from API
func (p *AliCloudProvider) FetchBillData(ctx context.Context, req *cloudsync.FetchRequest) (*cloudsync.FetchResult, error) {
	if err := p.initBillService(); err != nil {
		return nil, fmt.Errorf("failed to initialize bill service: %w", err)
	}

	// Convert cloudsync request to alicloud request
	aliReq := &DescribeInstanceBillRequest{
		BillingCycle: req.Period,
		Granularity:  req.Granularity,
		MaxResults:   req.Limit,
		NextToken:    fmt.Sprintf("%d", req.Offset),
	}

	// Fetch data
	resp, err := p.billService.aliClient.DescribeInstanceBill(ctx, aliReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch bill data: %w", err)
	}

	// Convert response
	return &cloudsync.FetchResult{
		Data:       resp.Data.Items,
		Total:      resp.Data.TotalCount,
		Fetched:    int32(len(resp.Data.Items)),
		HasMore:    resp.Data.NextToken != "",
		NextOffset: req.Offset + req.Limit,
	}, nil
}

// GetTableConfig returns table configuration for the specified granularity
func (p *AliCloudProvider) GetTableConfig(granularity string) *cloudsync.TableConfig {
	switch granularity {
	case "monthly":
		return &cloudsync.TableConfig{
			TableName:        p.billService.GetMonthlyTableName(),
			DistributedTable: "", // Will be determined by table name resolver
			PeriodField:      "billing_cycle",
			DateField:        "billing_date", // Used for NULL check in monthly data
			ProviderField:    "provider",
			Granularity:      "monthly",
		}
	case "daily":
		return &cloudsync.TableConfig{
			TableName:        p.billService.GetDailyTableName(),
			DistributedTable: "", // Will be determined by table name resolver
			PeriodField:      "billing_date",
			DateField:        "billing_date",
			ProviderField:    "provider",
			Granularity:      "daily",
		}
	default:
		return nil
	}
}

// GetPeriodField returns the period field name
func (p *AliCloudProvider) GetPeriodField() string {
	return "billing_cycle"
}

// CreateTables creates the necessary tables
func (p *AliCloudProvider) CreateTables(ctx context.Context, config *cloudsync.TableConfig) error {
	if err := p.initBillService(); err != nil {
		return fmt.Errorf("failed to initialize bill service: %w", err)
	}

	// Create monthly table
	if err := p.billService.CreateMonthlyBillTable(ctx); err != nil {
		return fmt.Errorf("failed to create monthly table: %w", err)
	}

	// Create daily table
	if err := p.billService.CreateDailyBillTable(ctx); err != nil {
		return fmt.Errorf("failed to create daily table: %w", err)
	}

	return nil
}

// SyncPeriodData synchronizes data for a specific period
func (p *AliCloudProvider) SyncPeriodData(ctx context.Context, period string, options *cloudsync.SyncOptions) error {
	if err := p.initBillService(); err != nil {
		return fmt.Errorf("failed to initialize bill service: %w", err)
	}

	// Convert cloudsync options to alicloud options
	aliOptions := &SyncOptions{
		BatchSize:        options.BatchSize,
		UseDistributed:   options.UseDistributed,
		EnableValidation: options.EnableValidation,
		MaxWorkers:       options.MaxWorkers,
	}

	// Determine granularity from period format
	var err error
	if _, parseErr := time.Parse("2006-01-02", period); parseErr == nil {
		// Daily format (YYYY-MM-DD)
		err = p.billService.SyncSpecificDayBillData(ctx, period, aliOptions)
	} else if _, parseErr := time.Parse("2006-01", period); parseErr == nil {
		// Monthly format (YYYY-MM)
		err = p.billService.SyncMonthlyBillData(ctx, period, aliOptions)
	} else {
		return fmt.Errorf("invalid period format: %s", period)
	}

	if err != nil {
		return fmt.Errorf("failed to sync period %s: %w", period, err)
	}

	return nil
}

// initBillService initializes the bill service if not already done
func (p *AliCloudProvider) initBillService() error {
	if p.billService != nil {
		return nil
	}

	billService, err := NewBillService(p.config, p.chClient)
	if err != nil {
		return fmt.Errorf("failed to create bill service: %w", err)
	}

	p.billService = billService
	return nil
}
