package middleware

import (
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// RequestID middleware to generate request ID if not present
func RequestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Check if request ID exists in header
		requestID := c.GetHeader("X-Request-ID")
		if requestID == "" {
			// Generate new request ID if not present
			requestID = generateRequestID()
		}

		// Set request ID in context
		c.Set("RequestID", requestID)

		// Set request ID in response header
		c.Writer.Header().Set("X-Request-ID", requestID)

		c.Next()
	}
}

// generateRequestID generates a unique request ID
func generateRequestID() string {
	// Use UUID for request ID
	return uuid.New().String()
}
