package response

import (
	"encoding/json"
	"goscan/pkg/logger"
	"net/http"

	"go.uber.org/zap"
)

// HTTP response constants
const (
	ContentTypeJSON = "application/json"
)

// Error response field names
const (
	FieldError   = "error"
	FieldMessage = "message"
	FieldCode    = "code"
	FieldDetails = "details"
)

// WriteJSONResponse writes a JSON response with the given status code
// This function is kept for compatibility with existing code that may use it
func WriteJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", ContentTypeJSON)
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		logger.Error("Failed to encode JSON response", zap.Error(err))
	}
}

// WriteErrorResponse writes an error response in JSON format
// This function is kept for compatibility with existing code that may use it
func WriteErrorResponse(w http.ResponseWriter, statusCode int, message string, err error) {
	errorResp := map[string]interface{}{
		FieldError:   true,
		FieldMessage: message,
		FieldCode:    statusCode,
	}

	if err != nil {
		errorResp[FieldDetails] = err.Error()
		logger.Error("API error",
			zap.String("message", message),
			zap.Error(err),
			zap.Int("status_code", statusCode))
	}

	WriteJSONResponse(w, statusCode, errorResp)
}
