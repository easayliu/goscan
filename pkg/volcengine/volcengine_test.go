package volcengine

import (
	"goscan/pkg/config"
	"testing"
	"time"
)

func TestValidatePeriod(t *testing.T) {
	// 创建一个测试客户端
	cfg := &config.VolcEngineConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
		Region:    "cn-north-1",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	tests := []struct {
		name        string
		billPeriod  string
		expectError bool
	}{
		{
			name:        "empty bill period",
			billPeriod:  "",
			expectError: true,
		},
		{
			name:        "invalid format",
			billPeriod:  "2025-8",
			expectError: true,
		},
		{
			name:        "invalid format with day",
			billPeriod:  "2025-08-01",
			expectError: true,
		},
		{
			name:        "current month",
			billPeriod:  time.Now().Format("2006-01"),
			expectError: false,
		},
		{
			name:        "last month",
			billPeriod:  time.Now().AddDate(0, -1, 0).Format("2006-01"),
			expectError: false,
		},
		{
			name:        "future month",
			billPeriod:  time.Now().AddDate(0, 1, 0).Format("2006-01"),
			expectError: true,
		},
		{
			name:        "too old month",
			billPeriod:  "2017-01",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := client.ValidatePeriod(tt.billPeriod)
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestCalculateSmartPeriod(t *testing.T) {
	// 创建一个测试客户端
	cfg := &config.VolcEngineConfig{
		AccessKey: "test-key",
		SecretKey: "test-secret",
		Region:    "cn-north-1",
	}
	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}

	selectedPeriod, dateRange := client.CalculateSmartPeriod()
	
	// 验证返回的账期格式
	_, err = time.Parse("2006-01", selectedPeriod)
	if err != nil {
		t.Errorf("Invalid period format: %s", selectedPeriod)
	}
	
	// 验证日期范围不为空
	if dateRange == "" {
		t.Error("Date range should not be empty")
	}
	
	// 验证账期是当前月或上个月
	now := time.Now()
	currentMonth := now.Format("2006-01")
	lastMonth := now.AddDate(0, -1, 0).Format("2006-01")
	
	if selectedPeriod != currentMonth && selectedPeriod != lastMonth {
		t.Errorf("Expected period to be %s or %s, got %s", currentMonth, lastMonth, selectedPeriod)
	}
}

func TestClient(t *testing.T) {
	cfg := &config.VolcEngineConfig{
		AccessKey: "test-access-key",
		SecretKey: "test-secret-key",
		Region:    "cn-north-1",
		Host:      "billing.volcengineapi.com",
		Timeout:   30,
	}

	client, err := NewClient(cfg)
	if err != nil {
		t.Fatalf("Failed to create client: %v", err)
	}
	if client == nil {
		t.Fatal("Client should not be nil")
	}

	if client.config != cfg {
		t.Error("Client config should match provided config")
	}

	if client.billingService == nil {
		t.Error("Billing service should not be nil")
	}
}

// TestBillDetailConversion was removed because BillDetail structure has been refactored
// The new structure uses VolcEngine API's actual field names and format
// TODO: Consider adding new tests for the updated BillDetail structure if needed

func TestListBillDetailRequest(t *testing.T) {
	tests := []struct {
		name    string
		request *ListBillDetailRequest
		valid   bool
	}{
		{
			name: "valid request with all fields",
			request: &ListBillDetailRequest{
				BillPeriod:  "2023-01",
				Product:     []string{"ECS"},
				BillingMode: []string{"PostPaid"},
				OwnerID:     []int64{123456},
				Limit:       100,
				Offset:      0,
			},
			valid: true,
		},
		{
			name: "valid request with minimum fields",
			request: &ListBillDetailRequest{
				Limit:  50,
				Offset: 0,
			},
			valid: true,
		},
		{
			name: "request with large limit",
			request: &ListBillDetailRequest{
				Limit:  1000,
				Offset: 0,
			},
			valid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.request.Limit <= 0 {
				t.Error("Limit should be greater than 0")
			}

			if tt.request.Limit > 1000 {
				t.Error("Limit should not exceed 1000")
			}

			if tt.request.Offset < 0 {
				t.Error("Offset should not be negative")
			}
		})
	}
}

// 集成测试（需要真实的API凭证）
func TestBillServiceIntegration(t *testing.T) {
	// 跳过集成测试，除非设置了环境变量
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// 集成测试需要真实的凭证和ClickHouse实例
	t.Skip("Integration test requires real credentials and ClickHouse instance")

	// 以下代码仅作为示例，实际运行需要真实环境
	/*
		cfg := &config.VolcEngineConfig{
			AccessKey: "your-access-key",
			SecretKey: "your-secret-key",
			Region:    "cn-north-1",
			Host:      "billing.volcengineapi.com",
			Timeout:   30,
		}

		chCfg := &config.ClickHouseConfig{
			Hosts:    []string{"localhost"},
			Port:     9000,
			Database: "default",
			Username: "default",
			Password: "",
		}

		chClient, err := clickhouse.NewClient(chCfg)
		if err != nil {
			t.Fatalf("Failed to create ClickHouse client: %v", err)
		}
		defer chClient.Close()

		billService, err := NewBillService(cfg, chClient)
		if err != nil {
			t.Fatalf("Failed to create bill service: %v", err)
		}

		ctx := context.Background()

		// 创建测试表
		if err := billService.CreateBillTable(ctx); err != nil {
			t.Fatalf("Failed to create bill table: %v", err)
		}

		// 测试同步数据
		req := &ListBillDetailRequest{
			BillPeriod: "2023-01",
			Limit:      10,
			Offset:     0,
		}

		result, err := billService.SyncBillData(ctx, req)
		if err != nil {
			t.Fatalf("Failed to sync bill data: %v", err)
		}

		t.Logf("Sync result: %+v", result)
	*/
}
