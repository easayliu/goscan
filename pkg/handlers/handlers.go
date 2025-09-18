package handlers

import (
	_ "goscan/pkg/models" // imported for swagger documentation
)

// This file now serves as the unified entry point for the handlers package
// All handler functionality has been split by functional domains into the following files:
// - base.go: Basic handler structure and common methods
// - errors.go: Error definitions and handling
// - middleware.go: Common middleware and helper functions
// - health_handlers.go: Health check and status related APIs
// - sync_handlers.go: Synchronization related API handlers
// - analysis_handlers.go: Analysis related API handlers (including WeChat notifications)
// - schedule_handlers.go: Scheduling related API handlers

// Important Notes:
// 1. HandlerService struct is now defined in base.go
// 2. NewHandlerService constructor is in base.go
// 3. All API handler methods are distributed across files as HandlerService methods
// 4. Unified error handling is in errors.go
// 5. Common helper functions are in middleware.go

// Through Go's package mechanism, all these methods can still be accessed via handlers.HandlerService
// Maintains backward compatibility, existing route registration code requires no modification
