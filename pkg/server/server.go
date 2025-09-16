package server

import (
	"context"
	"encoding/json"
	"fmt"
	"goscan/pkg/config"
	"goscan/pkg/handlers"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/mux"
	httpSwagger "github.com/swaggo/http-swagger"
	_ "goscan/docs" // swagger docs
)

// Server constants
const (
	DefaultReadTimeout  = 30 * time.Second
	DefaultWriteTimeout = 30 * time.Second
	DefaultIdleTimeout  = 120 * time.Second
	DefaultVersion      = "1.0.0"
	ServiceName         = "goscan"
)

// Config holds HTTP server configuration
type Config struct {
	Address string
	Port    int
	Config  *config.Config
}

// HTTPServer represents the HTTP server component
type HTTPServer struct {
	server     *http.Server
	router     *mux.Router
	config     *Config
	ctx        context.Context
	handlerSvc *handlers.HandlerService
}

// NewHTTPServer creates a new HTTP server instance
func NewHTTPServer(ctx context.Context, config *Config) (*HTTPServer, error) {
	slog.Info("Initializing HTTP server", "address", config.Address, "port", config.Port)

	router := mux.NewRouter()

	// Create handler service
	handlerSvc, err := handlers.NewHandlerService(ctx, config.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create handler service: %w", err)
	}

	server := &HTTPServer{
		router:     router,
		config:     config,
		ctx:        ctx,
		handlerSvc: handlerSvc,
	}

	// Setup routes
	server.setupRoutes()

	// Create HTTP server
	addr := fmt.Sprintf("%s:%d", config.Address, config.Port)
	server.server = &http.Server{
		Addr:         addr,
		Handler:      server.router,
		ReadTimeout:  DefaultReadTimeout,
		WriteTimeout: DefaultWriteTimeout,
		IdleTimeout:  DefaultIdleTimeout,
	}

	slog.Info("HTTP server initialized", "listen_addr", addr)
	return server, nil
}

// SetScheduler sets the scheduler reference in the handler service
func (s *HTTPServer) SetScheduler(scheduler interface{}) {
	// Set scheduler in handler service using interface{}
	s.handlerSvc.SetScheduler(scheduler)
}

// setupRoutes configures all HTTP routes
func (s *HTTPServer) setupRoutes() {
	// Add middleware
	s.addMiddleware()

	// Health check endpoint
	s.router.HandleFunc("/health", s.handleHealth).Methods("GET")

	// Swagger documentation
	s.router.PathPrefix("/swagger/").Handler(httpSwagger.WrapHandler)

	// Setup API routes
	s.setupAPIRoutes()

	slog.Info("HTTP routes configured")
}

// Start starts the HTTP server
func (s *HTTPServer) Start() error {
	slog.Info("Starting HTTP server", "addr", s.server.Addr)

	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("HTTP server failed: %w", err)
	}

	return nil
}

// Shutdown gracefully shuts down the HTTP server
func (s *HTTPServer) Shutdown(ctx context.Context) error {
	slog.Info("Shutting down HTTP server")

	if err := s.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("HTTP server shutdown failed: %w", err)
	}

	return nil
}

// handleHealth handles health check requests
// @Summary 健康检查
// @Description 返回服务健康状态
// @Tags System
// @Accept json
// @Produce json
// @Success 200 {object} models.HealthResponse
// @Router /health [get]
func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC(),
		"service":   ServiceName,
		"version":   DefaultVersion,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(health)
}

// loggingMiddleware logs HTTP requests
func (s *HTTPServer) loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(wrapped, r)

		slog.Info("HTTP request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", wrapped.statusCode,
			"duration", time.Since(start),
			"remote_addr", r.RemoteAddr,
			"user_agent", r.UserAgent(),
		)
	})
}

// corsMiddleware handles CORS headers
func (s *HTTPServer) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// responseWriter wraps http.ResponseWriter to capture status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

// WriteHeader captures the status code
func (w *responseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// addMiddleware adds all middleware to the router
func (s *HTTPServer) addMiddleware() {
	s.router.Use(s.loggingMiddleware)
	s.router.Use(s.corsMiddleware)
}

// setupAPIRoutes configures API v1 routes
func (s *HTTPServer) setupAPIRoutes() {
	api := s.router.PathPrefix("/api/v1").Subrouter()

	// Setup route groups
	s.setupSystemRoutes(api)
	s.setupTaskRoutes(api)
	s.setupSyncRoutes(api)
	s.setupSchedulerRoutes(api)
	s.setupNotificationRoutes(api)
}

// setupSystemRoutes configures system endpoints
func (s *HTTPServer) setupSystemRoutes(api *mux.Router) {
	api.HandleFunc("/status", s.handlerSvc.GetStatus).Methods("GET")
	api.HandleFunc("/config", s.handlerSvc.GetAppConfig).Methods("GET")
	api.HandleFunc("/config", s.handlerSvc.UpdateConfig).Methods("PUT")
}

// setupTaskRoutes configures task management endpoints
func (s *HTTPServer) setupTaskRoutes(api *mux.Router) {
	api.HandleFunc("/tasks", s.handlerSvc.GetTasks).Methods("GET")
	api.HandleFunc("/tasks", s.handlerSvc.CreateTask).Methods("POST")
	api.HandleFunc("/tasks/{id}", s.handlerSvc.GetTask).Methods("GET")
	api.HandleFunc("/tasks/{id}", s.handlerSvc.DeleteTask).Methods("DELETE")
}

// setupSyncRoutes configures sync operation endpoints
func (s *HTTPServer) setupSyncRoutes(api *mux.Router) {
	api.HandleFunc("/sync/trigger", s.handlerSvc.TriggerSync).Methods("POST")
	api.HandleFunc("/sync/status", s.handlerSvc.GetSyncStatus).Methods("GET")
	api.HandleFunc("/sync/history", s.handlerSvc.GetSyncHistory).Methods("GET")
}

// setupSchedulerRoutes configures scheduler endpoints
func (s *HTTPServer) setupSchedulerRoutes(api *mux.Router) {
	api.HandleFunc("/scheduler/status", s.handlerSvc.GetSchedulerStatus).Methods("GET")
	api.HandleFunc("/scheduler/jobs", s.handlerSvc.GetScheduledJobs).Methods("GET")
	api.HandleFunc("/scheduler/jobs", s.handlerSvc.CreateScheduledJob).Methods("POST")
	api.HandleFunc("/scheduler/jobs/{id}", s.handlerSvc.DeleteScheduledJob).Methods("DELETE")
}

// setupNotificationRoutes configures WeChat notification endpoints
func (s *HTTPServer) setupNotificationRoutes(api *mux.Router) {
	api.HandleFunc("/notifications/wechat/trigger", s.handlerSvc.TriggerWeChatNotification).Methods("POST")
	api.HandleFunc("/notifications/wechat/status", s.handlerSvc.GetWeChatNotificationStatus).Methods("GET")
	api.HandleFunc("/notifications/wechat/test", s.handlerSvc.TestWeChatWebhook).Methods("POST")
}
