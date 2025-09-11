package volcengine

import (
	"goscan/pkg/config"
	"testing"
	"time"
)

func TestValidateBillPeriod(t *testing.T) {
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
			billPeriod:  "2023-01",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateBillPeriod(tt.billPeriod)
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Expected no error but got: %v", err)
			}
		})
	}
}

func TestGetValidBillPeriods(t *testing.T) {
	periods := GetValidBillPeriods()
	if len(periods) != 2 {
		t.Errorf("Expected 2 periods, got %d", len(periods))
	}

	now := time.Now()
	currentMonth := now.Format("2006-01")
	lastMonth := now.AddDate(0, -1, 0).Format("2006-01")

	if periods[0] != currentMonth {
		t.Errorf("Expected first period to be %s, got %s", currentMonth, periods[0])
	}

	if periods[1] != lastMonth {
		t.Errorf("Expected second period to be %s, got %s", lastMonth, periods[1])
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

	client := NewClient(cfg)
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

func TestBillDetailConversion(t *testing.T) {
	now := time.Now()

	bill := &BillDetail{
		ID:            "test-bill-001",
		OwnerID:       "123456",
		OwnerUserName: "test-user",
		Product:       "ECS",
		ProductZh:     "云服务器",
		BillingMode:   "PostPaid",
		ExpenseTime:   now,
		ExpenseDate:   now.Format("2006-01-02"),
		BillPeriod:    "2023-01",
		Amount:        123.45,
		Currency:      "CNY",
		Region:        "cn-north-1",
		Zone:          "cn-north-1a",
		InstanceID:    "i-123456",
		InstanceName:  "test-instance",
		Tags: []Tag{
			{Key: "Environment", Value: "test"},
			{Key: "Project", Value: "goscan"},
		},
		UsageStartTime: now.Add(-time.Hour),
		UsageEndTime:   now,
	}

	dbBill := bill.ToDBFormat()

	if dbBill.ID != bill.ID {
		t.Errorf("Expected ID %s, got %s", bill.ID, dbBill.ID)
	}

	if dbBill.Amount != bill.Amount {
		t.Errorf("Expected Amount %.2f, got %.2f", bill.Amount, dbBill.Amount)
	}

	if len(dbBill.Tags) != 2 {
		t.Errorf("Expected 2 tags, got %d", len(dbBill.Tags))
	}

	if dbBill.Tags["Environment"] != "test" {
		t.Errorf("Expected Environment tag to be 'test', got '%s'", dbBill.Tags["Environment"])
	}

	if dbBill.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}

	if dbBill.UpdatedAt.IsZero() {
		t.Error("UpdatedAt should not be zero")
	}
}

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
				Product:     "ECS",
				BillingMode: "PostPaid",
				OwnerID:     "123456",
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
