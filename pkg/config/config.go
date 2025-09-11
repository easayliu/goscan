package config

import (
	"fmt"
	"net/url"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type ClickHouseConfig struct {
	Hosts    []string `json:"hosts" yaml:"hosts"`
	Port     int      `json:"port" yaml:"port"`
	Database string   `json:"database" yaml:"database"`
	Username string   `json:"username" yaml:"username"`
	Password string   `json:"password" yaml:"password"`
	Debug    bool     `json:"debug" yaml:"debug"`
	Cluster  string   `json:"cluster" yaml:"cluster"`
	Protocol string   `json:"protocol" yaml:"protocol"` // native, http
}

func NewClickHouseConfig() *ClickHouseConfig {
	hosts := []string{getEnv("CLICKHOUSE_HOST", "localhost")}
	if hostsEnv := os.Getenv("CLICKHOUSE_HOSTS"); hostsEnv != "" {
		hosts = parseHosts(hostsEnv)
	}

	// 根据协议设置默认端口
	protocol := getEnv("CLICKHOUSE_PROTOCOL", "native")
	defaultPort := 9000 // native 协议默认端口
	if protocol == "http" {
		defaultPort = 8123 // HTTP 协议默认端口
	}

	return &ClickHouseConfig{
		Hosts:    hosts,
		Port:     getEnvInt("CLICKHOUSE_PORT", defaultPort),
		Database: getEnv("CLICKHOUSE_DATABASE", "default"),
		Username: getEnv("CLICKHOUSE_USERNAME", "default"),
		Password: getEnv("CLICKHOUSE_PASSWORD", ""),
		Debug:    getEnvBool("CLICKHOUSE_DEBUG", false),
		Cluster:  getEnv("CLICKHOUSE_CLUSTER", ""),
		Protocol: protocol,
	}
}

func (c *ClickHouseConfig) DSN() string {
	host := "localhost"
	if len(c.Hosts) > 0 {
		host = c.Hosts[0]
	}

	scheme := "clickhouse"
	if c.Protocol == "http" {
		scheme = "http"
	}

	// 对用户名和密码进行 URL 编码
	username := url.QueryEscape(c.Username)
	password := url.QueryEscape(c.Password)

	return fmt.Sprintf("%s://%s:%s@%s:%d/%s",
		scheme, username, password, host, c.Port, c.Database)
}

// GetProtocol 返回协议类型，默认为 native
func (c *ClickHouseConfig) GetProtocol() clickhouse.Protocol {
	if c.Protocol == "http" {
		return clickhouse.HTTP
	}
	return clickhouse.Native
}

func (c *ClickHouseConfig) GetAddresses() []string {
	addresses := make([]string, len(c.Hosts))
	for i, host := range c.Hosts {
		addresses[i] = fmt.Sprintf("%s:%d", host, c.Port)
	}
	return addresses
}

func parseHosts(hostsStr string) []string {
	hosts := make([]string, 0)
	current := ""

	for _, char := range hostsStr {
		if char == ',' {
			if current != "" {
				hosts = append(hosts, trimSpace(current))
				current = ""
			}
		} else {
			current += string(char)
		}
	}

	if current != "" {
		hosts = append(hosts, trimSpace(current))
	}

	return hosts
}

func trimSpace(s string) string {
	start := 0
	end := len(s)

	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}

	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}

	return s[start:end]
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue := parseIntOrDefault(value, defaultValue); intValue != defaultValue {
			return intValue
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return value == "true" || value == "1"
	}
	return defaultValue
}

func parseIntOrDefault(s string, defaultValue int) int {
	if len(s) == 0 {
		return defaultValue
	}

	result := 0
	for _, char := range s {
		if char < '0' || char > '9' {
			return defaultValue
		}
		result = result*10 + int(char-'0')
	}
	return result
}
