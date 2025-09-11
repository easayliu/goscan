package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// 测试默认配置
	cfg, err := LoadConfig("nonexistent.yaml")
	if err != nil {
		t.Fatalf("Failed to load default config: %v", err)
	}

	if cfg.ClickHouse == nil {
		t.Fatal("ClickHouse config should not be nil")
	}
	if cfg.App == nil {
		t.Fatal("App config should not be nil")
	}
}

func TestSaveAndLoadConfig(t *testing.T) {
	tempFile := "test_config.yaml"
	defer os.Remove(tempFile)

	// 创建测试配置
	originalConfig := &Config{
		ClickHouse: &ClickHouseConfig{
			Hosts:    []string{"test-host"},
			Port:     9001,
			Database: "test_db",
			Username: "test_user",
			Password: "test_pass",
			Debug:    true,
		},
		App: &AppConfig{
			LogLevel: "debug",
			LogFile:  "/tmp/test.log",
		},
	}

	// 保存配置
	if err := SaveConfig(originalConfig, tempFile); err != nil {
		t.Fatalf("Failed to save config: %v", err)
	}

	// 加载配置
	loadedConfig, err := LoadConfig(tempFile)
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// 验证配置
	if len(loadedConfig.ClickHouse.Hosts) == 0 || loadedConfig.ClickHouse.Hosts[0] != originalConfig.ClickHouse.Hosts[0] {
		t.Errorf("Expected host %v, got %v", originalConfig.ClickHouse.Hosts, loadedConfig.ClickHouse.Hosts)
	}
	if loadedConfig.ClickHouse.Port != originalConfig.ClickHouse.Port {
		t.Errorf("Expected port %d, got %d", originalConfig.ClickHouse.Port, loadedConfig.ClickHouse.Port)
	}
	if loadedConfig.App.LogLevel != originalConfig.App.LogLevel {
		t.Errorf("Expected log level %s, got %s", originalConfig.App.LogLevel, loadedConfig.App.LogLevel)
	}
}

func TestConfigWithEnvVars(t *testing.T) {
	// 设置环境变量
	os.Setenv("CLICKHOUSE_HOSTS", "env-host")
	os.Setenv("CLICKHOUSE_PORT", "9002")
	os.Setenv("LOG_LEVEL", "debug")
	defer func() {
		os.Unsetenv("CLICKHOUSE_HOSTS")
		os.Unsetenv("CLICKHOUSE_PORT")
		os.Unsetenv("LOG_LEVEL")
	}()

	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// 验证环境变量覆盖了默认值
	if len(cfg.ClickHouse.Hosts) == 0 || cfg.ClickHouse.Hosts[0] != "env-host" {
		t.Errorf("Expected host env-host, got %v", cfg.ClickHouse.Hosts)
	}
	if cfg.ClickHouse.Port != 9002 {
		t.Errorf("Expected port 9002, got %d", cfg.ClickHouse.Port)
	}
	if cfg.App.LogLevel != "debug" {
		t.Errorf("Expected log level debug, got %s", cfg.App.LogLevel)
	}
}
