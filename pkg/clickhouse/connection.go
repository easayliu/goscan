package clickhouse

import (
	"context"
	"fmt"
	"goscan/pkg/config"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ConnectionConfig connection configuration structure
type ConnectionConfig struct {
	DialTimeout          time.Duration
	MaxOpenConns         int
	MaxIdleConns         int
	ConnMaxLifetime      time.Duration
	BlockBufferSize      uint8
	MaxCompressionBuffer int
	MaxExecutionTime     int
}

// DefaultConnectionConfig returns default connection configuration
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

// connectionManager connection manager
type connectionManager struct {
	conn   driver.Conn
	config *config.ClickHouseConfig
}

// NewConnectionManager creates connection manager
func NewConnectionManager(cfg *config.ClickHouseConfig, connCfg *ConnectionConfig) (ConnectionManager, error) {
	if connCfg == nil {
		connCfg = DefaultConnectionConfig()
	}

	conn, err := createConnection(cfg, connCfg)
	if err != nil {
		return nil, WrapConnectionError(err)
	}

	// verify connection
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

// createConnection creates ClickHouse connection
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

	// only set compression for Native protocol, HTTP protocol does not support LZ4 compression
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

// Close closes the connection
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

// Ping checks connection status
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

// GetConnection gets the underlying connection
func (cm *connectionManager) GetConnection() driver.Conn {
	return cm.conn
}

// HealthCheck connection health check
func (cm *connectionManager) HealthCheck(ctx context.Context) error {
	// check if connection is nil
	if cm.conn == nil {
		return WrapConnectionError(fmt.Errorf("connection is not initialized"))
	}

	// execute ping check
	if err := cm.Ping(ctx); err != nil {
		return err
	}

	// execute simple query to verify connection availability
	if err := cm.conn.Exec(ctx, "SELECT 1"); err != nil {
		return WrapConnectionError(fmt.Errorf("test query failed: %w", err))
	}

	return nil
}

// GetConnectionStats gets connection statistics
func (cm *connectionManager) GetConnectionStats() map[string]interface{} {
	if cm.conn == nil {
		return map[string]interface{}{
			"status": "not_connected",
		}
	}

	// note: clickhouse-go v2 may not provide detailed connection statistics
	// return basic status information here
	return map[string]interface{}{
		"status":   "connected",
		"database": cm.config.Database,
		"protocol": cm.config.GetProtocol().String(),
		"cluster":  cm.config.Cluster,
	}
}

// IsConnected checks if connected
func (cm *connectionManager) IsConnected() bool {
	if cm.conn == nil {
		return false
	}

	// use short timeout for quick check
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return cm.Ping(ctx) == nil
}

// Reconnect reconnects
func (cm *connectionManager) Reconnect() error {
	// close existing connection
	if cm.conn != nil {
		cm.conn.Close()
	}

	// create new connection
	connCfg := DefaultConnectionConfig()
	conn, err := createConnection(cm.config, connCfg)
	if err != nil {
		return WrapConnectionError(err)
	}

	// verify new connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return WrapConnectionError(fmt.Errorf("reconnect ping failed: %w", err))
	}

	cm.conn = conn
	return nil
}

// SetMaxOpenConns sets maximum open connections (this method may not be directly supported in current clickhouse-go version)
func (cm *connectionManager) SetMaxOpenConns(n int) {
	// note: clickhouse-go v2 connection pool configuration is set at creation time and cannot be modified dynamically
	// this method may need to recreate connection to implement
}

// SetMaxIdleConns sets maximum idle connections (this method may not be directly supported in current clickhouse-go version)
func (cm *connectionManager) SetMaxIdleConns(n int) {
	// note: clickhouse-go v2 connection pool configuration is set at creation time and cannot be modified dynamically
	// this method may need to recreate connection to implement
}
