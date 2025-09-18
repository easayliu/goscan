package middleware

import (
	"goscan/pkg/logger"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// ErrorHandler handles errors in Gin requests
func ErrorHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()

		// Check if there are any errors
		if len(c.Errors) > 0 {
			err := c.Errors.Last()

			// Log the error with context
			logger.Error("request error",
				zap.String("path", c.Request.URL.Path),
				zap.String("method", c.Request.Method),
				zap.Error(err.Err),
				zap.String("request_id", c.GetString("RequestID")),
				zap.Int("status", c.Writer.Status()),
			)

			// Don't override response if already written
			if !c.Writer.Written() {
				// Determine status code
				status := c.Writer.Status()
				if status == 0 || status == 200 {
					status = 500
				}

				// Return error response
				c.JSON(status, gin.H{
					"error":      true,
					"message":    "Internal Server Error",
					"request_id": c.GetString("RequestID"),
				})
			}
		}
	}
}

// Recovery handles panics and recovers gracefully
func Recovery() gin.HandlerFunc {
	return gin.CustomRecovery(func(c *gin.Context, recovered interface{}) {
		// Log the panic
		logger.Error("panic recovered",
			zap.Any("error", recovered),
			zap.String("path", c.Request.URL.Path),
			zap.String("method", c.Request.Method),
			zap.String("request_id", c.GetString("RequestID")),
			zap.Stack("stack"),
		)

		// Return 500 error
		c.AbortWithStatusJSON(500, gin.H{
			"error":      true,
			"message":    "Internal Server Error",
			"request_id": c.GetString("RequestID"),
		})
	})
}
