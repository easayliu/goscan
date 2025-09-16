package response

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
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
func WriteJSONResponse(w http.ResponseWriter, statusCode int, data interface{}) {
	w.Header().Set("Content-Type", ContentTypeJSON)
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("Failed to encode JSON response", "error", err)
	}
}

// WriteErrorResponse writes an error response in JSON format
func WriteErrorResponse(w http.ResponseWriter, statusCode int, message string, err error) {
	errorResp := map[string]interface{}{
		FieldError:   true,
		FieldMessage: message,
		FieldCode:    statusCode,
	}

	if err != nil {
		errorResp[FieldDetails] = err.Error()
		slog.Error("API error", "message", message, "error", err, "status_code", statusCode)
	}

	WriteJSONResponse(w, statusCode, errorResp)
}

// ParseIntParam parses an integer parameter from URL path
func ParseIntParam(r *http.Request, paramName string) (int, error) {
	vars := mux.Vars(r)
	paramValue, exists := vars[paramName]
	if !exists {
		return 0, fmt.Errorf("parameter %s not found", paramName)
	}

	intValue, err := strconv.Atoi(paramValue)
	if err != nil {
		return 0, fmt.Errorf("invalid integer parameter %s: %v", paramName, err)
	}

	return intValue, nil
}

// ParseStringParam parses a string parameter from URL path
func ParseStringParam(r *http.Request, paramName string) (string, error) {
	vars := mux.Vars(r)
	paramValue, exists := vars[paramName]
	if !exists {
		return "", fmt.Errorf("parameter %s not found", paramName)
	}

	return paramValue, nil
}
