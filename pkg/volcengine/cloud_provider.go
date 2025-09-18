package volcengine

import (
	"context"
	"fmt"
	"goscan/pkg/clickhouse"
	"goscan/pkg/cloudsync"
	"goscan/pkg/config"
)

// VolcEngineProvider implements the CloudProvider interface for VolcEngine
type VolcEngineProvider struct {
	config      *config.VolcEngineConfig
	chClient    *clickhouse.Client
	billService BillService
}

// NewVolcEngineProvider creates a new VolcEngine provider
func NewVolcEngineProvider(config *config.VolcEngineConfig, chClient *clickhouse.Client) *VolcEngineProvider {
	return &VolcEngineProvider{
		config:   config,
		chClient: chClient,
	}
}

// GetProviderName returns the provider name
func (p *VolcEngineProvider) GetProviderName() string {
	return "volcengine"
}

// ValidateCredentials validates the provider credentials
func (p *VolcEngineProvider) ValidateCredentials(ctx context.Context) error {
	if p.config.AccessKey == "" || p.config.SecretKey == "" {
		return fmt.Errorf("%w: volcengine access key or secret key", cloudsync.ErrCredentialsNotConfigured)
	}

	// Initialize bill service if not already done
	if err := p.initBillService(); err != nil {
		return fmt.Errorf("failed to initialize bill service: %w", err)
	}

	return nil
}

// Close closes the provider connection
func (p *VolcEngineProvider) Close() error {
	// VolcEngine bill service doesn't have explicit close method
	return nil
}

// GetAPIDataCount gets the count of data from API
func (p *VolcEngineProvider) GetAPIDataCount(ctx context.Context, period, granularity string) (int64, error) {
	if err := p.initBillService(); err != nil {
		return 0, fmt.Errorf("failed to initialize bill service: %w", err)
	}

	// VolcEngine only supports monthly granularity
	if granularity != "monthly" {
		return 0, fmt.Errorf("volcengine only supports monthly granularity, got: %s", granularity)
	}

	count, err := p.billService.GetAPIDataCount(ctx, period)
	return int64(count), err
}

// FetchBillData fetches bill data from API
func (p *VolcEngineProvider) FetchBillData(ctx context.Context, req *cloudsync.FetchRequest) (*cloudsync.FetchResult, error) {
	if err := p.initBillService(); err != nil {
		return nil, fmt.Errorf("failed to initialize bill service: %w", err)
	}

	// For VolcEngine, we'll use a simpler approach since the paginator interface is different
	// This is just for interface compliance - actual pagination is handled by SmartSyncAllData
	return &cloudsync.FetchResult{
		Data:       []interface{}{}, // Empty for now - actual data fetching is in SyncPeriodData
		Total:      0,
		Fetched:    0,
		HasMore:    false,
		NextOffset: req.Offset + req.Limit,
	}, nil
}

// GetTableConfig returns table configuration for the specified granularity
func (p *VolcEngineProvider) GetTableConfig(granularity string) *cloudsync.TableConfig {
	// VolcEngine only supports monthly granularity
	if granularity != "monthly" {
		return nil
	}

	// Determine table names based on cluster configuration
	tableName := "volcengine_bill_details"
	distributedTable := ""

	if p.chClient.GetClusterName() != "" {
		distributedTable = "volcengine_bill_details_distributed"
		tableName = "volcengine_bill_details_local" // For cleanup operations
	}

	return &cloudsync.TableConfig{
		TableName:        tableName,
		DistributedTable: distributedTable,
		PeriodField:      "BillPeriod",
		DateField:        "", // VolcEngine doesn't use NULL date field for monthly data
		ProviderField:    "provider",
		Granularity:      "monthly",
	}
}

// GetPeriodField returns the period field name
func (p *VolcEngineProvider) GetPeriodField() string {
	return "BillPeriod"
}

// CreateTables creates the necessary tables
func (p *VolcEngineProvider) CreateTables(ctx context.Context, config *cloudsync.TableConfig) error {
	if err := p.initBillService(); err != nil {
		return fmt.Errorf("failed to initialize bill service: %w", err)
	}

	// Create bill table
	if err := p.billService.CreateBillTable(ctx); err != nil {
		return fmt.Errorf("failed to create bill table: %w", err)
	}

	return nil
}

// SyncPeriodData synchronizes data for a specific period
func (p *VolcEngineProvider) SyncPeriodData(ctx context.Context, period string, options *cloudsync.SyncOptions) error {
	if err := p.initBillService(); err != nil {
		return fmt.Errorf("failed to initialize bill service: %w", err)
	}

	// Determine table name and distributed flag
	tableName := "volcengine_bill_details"
	isDistributed := p.chClient.GetClusterName() != ""

	// Use SmartSyncAllData method
	_, err := p.billService.SmartSyncAllData(ctx, period, tableName, isDistributed)
	if err != nil {
		return fmt.Errorf("failed to sync period %s: %w", period, err)
	}

	return nil
}

// initBillService initializes the bill service if not already done
func (p *VolcEngineProvider) initBillService() error {
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
