package clickhouse

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ConnectionConfig 连接配置结构
type ConnectionConfig struct {
	DialTimeout          time.Duration
	MaxOpenConns         int
	MaxIdleConns         int
	ConnMaxLifetime      time.Duration
	BlockBufferSize      uint8
	MaxCompressionBuffer int
	MaxExecutionTime     int
}

// DefaultConnectionConfig 返回默认连接配置
func DefaultConnectionConfig() *ConnectionConfig {
	return &ConnectionConfig{
		DialTimeout:          30 * time.Second,
		MaxOpenConns:         10,
		MaxIdleConns:         5,
		ConnMaxLifetime:      time.Hour,
		BlockBufferSize:      10,
		MaxCompressionBuffer: 10240,
		MaxExecutionTime:     60,
	}
}

// connectionManager 连接管理器
type connectionManager struct {
	conn   driver.Conn
	config *config.ClickHouseConfig
}

// NewConnectionManager 创建连接管理器
func NewConnectionManager(cfg *config.ClickHouseConfig, connCfg *ConnectionConfig) (ConnectionManager, error) {
	if connCfg == nil {
		connCfg = DefaultConnectionConfig()
	}

	conn, err := createConnection(cfg, connCfg)
	if err != nil {
		return nil, WrapConnectionError(err)
	}

	// 验证连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, WrapConnectionError(fmt.Errorf("ping failed: %w", err))
	}

	return &connectionManager{
		conn:   conn,
		config: cfg,
	}, nil
}

// createConnection 创建ClickHouse连接
func createConnection(cfg *config.ClickHouseConfig, connCfg *ConnectionConfig) (driver.Conn, error) {
	opts := &clickhouse.Options{
		Addr: cfg.GetAddresses(),
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Debug:    cfg.Debug,
		Protocol: cfg.GetProtocol(),
		Settings: clickhouse.Settings{
			"max_execution_time": connCfg.MaxExecutionTime,
		},
		DialTimeout:          connCfg.DialTimeout,
		MaxOpenConns:         connCfg.MaxOpenConns,
		MaxIdleConns:         connCfg.MaxIdleConns,
		ConnMaxLifetime:      connCfg.ConnMaxLifetime,
		ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		BlockBufferSize:      connCfg.BlockBufferSize,
		MaxCompressionBuffer: connCfg.MaxCompressionBuffer,
	}

	// 只有在 Native 协议时才设置压缩，HTTP 协议不支持 LZ4 压缩
	if cfg.GetProtocol() == clickhouse.Native {
		opts.Compression = &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	return conn, nil
}

// Close 关闭连接
func (cm *connectionManager) Close() error {
	if cm.conn == nil {
		return nil
	}
	err := cm.conn.Close()
	if err != nil {
		return WrapConnectionError(err)
	}
	return nil
}

// Ping 检查连接状态
func (cm *connectionManager) Ping(ctx context.Context) error {
	if cm.conn == nil {
		return WrapConnectionError(fmt.Errorf("connection is nil"))
	}
	err := cm.conn.Ping(ctx)
	if err != nil {
		return WrapConnectionError(err)
	}
	return nil
}

// GetConnection 获取底层连接
func (cm *connectionManager) GetConnection() driver.Conn {
	return cm.conn
}

// HealthCheck 连接健康检查
func (cm *connectionManager) HealthCheck(ctx context.Context) error {
	// 检查连接是否为nil
	if cm.conn == nil {
		return WrapConnectionError(fmt.Errorf("connection is not initialized"))
	}

	// 执行ping检查
	if err := cm.Ping(ctx); err != nil {
		return err
	}

	// 执行简单查询验证连接可用性
	if err := cm.conn.Exec(ctx, "SELECT 1"); err != nil {
		return WrapConnectionError(fmt.Errorf("test query failed: %w", err))
	}

	return nil
}

// GetConnectionStats 获取连接统计信息
func (cm *connectionManager) GetConnectionStats() map[string]interface{} {
	if cm.conn == nil {
		return map[string]interface{}{
			"status": "not_connected",
		}
	}

	// 注意：clickhouse-go v2 可能不提供详细的连接统计信息
	// 这里返回基本状态信息
	return map[string]interface{}{
		"status":   "connected",
		"database": cm.config.Database,
		"protocol": cm.config.GetProtocol().String(),
		"cluster":  cm.config.Cluster,
	}
}

// IsConnected 检查是否已连接
func (cm *connectionManager) IsConnected() bool {
	if cm.conn == nil {
		return false
	}

	// 使用短超时进行快速检查
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return cm.Ping(ctx) == nil
}

// Reconnect 重新连接
func (cm *connectionManager) Reconnect() error {
	// 关闭现有连接
	if cm.conn != nil {
		cm.conn.Close()
	}

	// 创建新连接
	connCfg := DefaultConnectionConfig()
	conn, err := createConnection(cm.config, connCfg)
	if err != nil {
		return WrapConnectionError(err)
	}

	// 验证新连接
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return WrapConnectionError(fmt.Errorf("reconnect ping failed: %w", err))
	}

	cm.conn = conn
	return nil
}

// SetMaxOpenConns 设置最大打开连接数（这个方法在当前的clickhouse-go版本中可能不直接支持）
func (cm *connectionManager) SetMaxOpenConns(n int) {
	// 注意：clickhouse-go v2 的连接池配置在创建时设置，无法动态修改
	// 这个方法可能需要重新创建连接来实现
}

// SetMaxIdleConns 设置最大空闲连接数（这个方法在当前的clickhouse-go版本中可能不直接支持）
func (cm *connectionManager) SetMaxIdleConns(n int) {
	// 注意：clickhouse-go v2 的连接池配置在创建时设置，无法动态修改
	// 这个方法可能需要重新创建连接来实现
}