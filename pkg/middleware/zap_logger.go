package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"goscan/pkg/logger"
)

// GinZapLogger creates a Gin logging middleware using zap directly
// Use unified logging format for the project
func GinZapLogger(zapLogger *zap.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()

		// Process request
		c.Next()

		// Skip logging for certain paths
		path := c.Request.URL.Path
		if path == "/health" || path == "/metrics" || path == "/ping" || path == "/favicon.ico" {
			return
		}

		// Skip logging for static files in swagger
		if len(path) > 8 && path[:9] == "/swagger/" && path != "/swagger/index.html" {
			return
		}

		// Calculate latency
		latency := time.Since(start)

		// Get request ID
		requestID, _ := c.Get("RequestID")
		reqID := ""
		if requestID != nil {
			reqID = requestID.(string)
		}

		// Log fields - 使用与项目其他地方一致的字段命名
		fields := []zap.Field{
			zap.String("request_id", reqID),
			zap.String("method", c.Request.Method),
			zap.String("path", path),
			zap.String("ip", c.ClientIP()),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", latency),
			zap.Int("response_size", c.Writer.Size()),
		}

		// Add query parameters if present
		if c.Request.URL.RawQuery != "" {
			fields = append(fields, zap.String("query", c.Request.URL.RawQuery))
		}

		// Add user agent for debugging
		if gin.Mode() == gin.DebugMode {
			fields = append(fields, zap.String("user_agent", c.Request.UserAgent()))
		}

		// Add error info if present
		if len(c.Errors) > 0 {
			fields = append(fields, zap.String("error", c.Errors.String()))
		}

		// Select log level based on status code
		statusCode := c.Writer.Status()
		switch {
		case statusCode >= 500:
			logger.Error("Internal server error", fields...)
		case statusCode >= 400:
			logger.Warn("Client request error", fields...)
		case statusCode >= 300:
			logger.Info("Request redirect", fields...)
		default:
			// Use unified message format for normal requests - Debug level for routine operations
			logger.Debug("HTTP request completed", fields...)
		}
	}
}
