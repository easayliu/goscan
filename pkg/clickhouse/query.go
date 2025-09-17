package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// queryExecutor 查询执行器
type queryExecutor struct {
	conn driver.Conn
}

// NewQueryExecutor 创建查询执行器
func NewQueryExecutor(conn driver.Conn) QueryExecutor {
	return &queryExecutor{
		conn: conn,
	}
}

// Exec 执行SQL语句
func (qe *queryExecutor) Exec(ctx context.Context, query string, args ...interface{}) error {
	if qe.conn == nil {
		return WrapConnectionError(fmt.Errorf("connection is nil"))
	}

	if err := qe.conn.Exec(ctx, query, args...); err != nil {
		return WrapError("exec", "", err)
	}
	return nil
}

// Query 执行查询并返回结果集
func (qe *queryExecutor) Query(ctx context.Context, query string, args ...interface{}) (driver.Rows, error) {
	if qe.conn == nil {
		return nil, WrapConnectionError(fmt.Errorf("connection is nil"))
	}

	rows, err := qe.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, WrapError("query", "", err)
	}
	return rows, nil
}

// QueryRow 执行查询并返回单行结果
func (qe *queryExecutor) QueryRow(ctx context.Context, query string, args ...interface{}) driver.Row {
	if qe.conn == nil {
		// 这里不能返回nil，因为driver.Row是接口，需要返回一个错误的实现
		return &errorRow{err: WrapConnectionError(fmt.Errorf("connection is nil"))}
	}

	return qe.conn.QueryRow(ctx, query, args...)
}

// errorRow 实现driver.Row接口，用于返回错误
type errorRow struct {
	err error
}

func (er *errorRow) Scan(dest ...interface{}) error {
	return er.err
}

func (er *errorRow) ScanStruct(dest interface{}) error {
	return er.err
}

func (er *errorRow) Err() error {
	return er.err
}

// QueryWithTimeout 带超时的查询执行
func (qe *queryExecutor) QueryWithTimeout(timeout time.Duration, query string, args ...interface{}) (driver.Rows, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return qe.Query(ctx, query, args...)
}

// ExecWithTimeout 带超时的SQL执行
func (qe *queryExecutor) ExecWithTimeout(timeout time.Duration, query string, args ...interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return qe.Exec(ctx, query, args...)
}

// QueryRowWithTimeout 带超时的单行查询
func (qe *queryExecutor) QueryRowWithTimeout(timeout time.Duration, query string, args ...interface{}) driver.Row {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	return qe.QueryRow(ctx, query, args...)
}

// ValidateQuery 验证SQL查询语句
func ValidateQuery(query string) error {
	if strings.TrimSpace(query) == "" {
		return fmt.Errorf("query cannot be empty")
	}

	// 移除注释和多余空格
	cleanQuery := strings.TrimSpace(strings.ToUpper(query))

	// 检查危险操作
	dangerousOps := []string{
		"DROP DATABASE",
		"DROP TABLE IF EXISTS",
		"TRUNCATE TABLE",
		"DELETE FROM",
	}

	for _, op := range dangerousOps {
		if strings.Contains(cleanQuery, op) {
			return fmt.Errorf("potentially dangerous operation detected: %s", op)
		}
	}

	return nil
}

// BuildInsertQuery 构建插入查询语句
func BuildInsertQuery(tableName string, columns []string) string {
	if len(columns) == 0 {
		return ""
	}

	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = "?"
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		joinStrings(columns, ", "),
		joinStrings(placeholders, ", "))
}

// BuildSelectQuery 构建查询语句
func BuildSelectQuery(tableName string, columns []string, conditions map[string]interface{}, limit int) string {
	query := "SELECT "

	if len(columns) == 0 {
		query += "*"
	} else {
		query += joinStrings(columns, ", ")
	}

	query += " FROM " + tableName

	if len(conditions) > 0 {
		var conditionParts []string
		for key := range conditions {
			conditionParts = append(conditionParts, key+" = ?")
		}
		query += " WHERE " + joinStrings(conditionParts, " AND ")
	}

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}

	return query
}

// BuildUpdateQuery 构建更新语句
func BuildUpdateQuery(tableName string, updates map[string]interface{}, conditions map[string]interface{}) string {
	if len(updates) == 0 {
		return ""
	}

	query := "ALTER TABLE " + tableName + " UPDATE "

	var setParts []string
	for key := range updates {
		setParts = append(setParts, key+" = ?")
	}
	query += joinStrings(setParts, ", ")

	if len(conditions) > 0 {
		var conditionParts []string
		for key := range conditions {
			conditionParts = append(conditionParts, key+" = ?")
		}
		query += " WHERE " + joinStrings(conditionParts, " AND ")
	}

	return query
}

// BuildDeleteQuery 构建删除语句
func BuildDeleteQuery(tableName string, conditions map[string]interface{}) string {
	query := "DELETE FROM " + tableName

	if len(conditions) > 0 {
		var conditionParts []string
		for key := range conditions {
			conditionParts = append(conditionParts, key+" = ?")
		}
		query += " WHERE " + joinStrings(conditionParts, " AND ")
	}

	return query
}

// ExtractColumnsFromData 从数据中提取列名
func ExtractColumnsFromData(data []map[string]interface{}) []string {
	if len(data) == 0 {
		return nil
	}

	columns := make([]string, 0, len(data[0]))
	for column := range data[0] {
		columns = append(columns, column)
	}

	return columns
}

// PrepareValues 准备数据值
func PrepareValues(data map[string]interface{}, columns []string) []interface{} {
	values := make([]interface{}, len(columns))
	for i, column := range columns {
		values[i] = data[column]
	}
	return values
}

// joinStrings 字符串连接工具函数（复用原有逻辑）
func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	if len(strs) == 1 {
		return strs[0]
	}

	totalLen := len(sep) * (len(strs) - 1)
	for _, s := range strs {
		totalLen += len(s)
	}

	result := make([]byte, 0, totalLen)
	result = append(result, strs[0]...)

	for i := 1; i < len(strs); i++ {
		result = append(result, sep...)
		result = append(result, strs[i]...)
	}

	return string(result)
}

// QueryStats 查询统计信息
type QueryStats struct {
	QueryType    string        `json:"query_type"`
	Duration     time.Duration `json:"duration"`
	RowsAffected int64         `json:"rows_affected"`
	RowsReturned int64         `json:"rows_returned"`
	Error        error         `json:"error,omitempty"`
}

// ExecuteWithStats 执行查询并收集统计信息
func (qe *queryExecutor) ExecuteWithStats(ctx context.Context, query string, args ...interface{}) (*QueryStats, error) {
	stats := &QueryStats{
		QueryType: getQueryType(query),
	}

	start := time.Now()
	defer func() {
		stats.Duration = time.Since(start)
	}()

	err := qe.Exec(ctx, query, args...)
	if err != nil {
		stats.Error = err
		return stats, err
	}

	return stats, nil
}

// getQueryType 获取查询类型
func getQueryType(query string) string {
	query = strings.TrimSpace(strings.ToUpper(query))

	if strings.HasPrefix(query, "SELECT") {
		return "SELECT"
	} else if strings.HasPrefix(query, "INSERT") {
		return "INSERT"
	} else if strings.HasPrefix(query, "UPDATE") || strings.HasPrefix(query, "ALTER TABLE") {
		return "UPDATE"
	} else if strings.HasPrefix(query, "DELETE") {
		return "DELETE"
	} else if strings.HasPrefix(query, "CREATE") {
		return "CREATE"
	} else if strings.HasPrefix(query, "DROP") {
		return "DROP"
	} else if strings.HasPrefix(query, "TRUNCATE") {
		return "TRUNCATE"
	} else if strings.HasPrefix(query, "OPTIMIZE") {
		return "OPTIMIZE"
	} else {
		return "OTHER"
	}
}
