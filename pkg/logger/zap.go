package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type CallerDisplayMode int

const (
	// CallerShort shows only filename:line (intelligent_sync.go:116)
	CallerShort CallerDisplayMode = iota
	// CallerMedium shows package/filename:line (volcengine/intelligent_sync.go:116)
	CallerMedium
	// CallerFull shows full path
	CallerFull
)

var (
	Logger           *zap.Logger
	Sugar            *zap.SugaredLogger
	atomicLevel      zap.AtomicLevel
	callerDisplayMode = CallerShort // Default to most concise
)

// InitLogger initializes the global logger
func InitLogger(isDevelopment bool, logPath string, logLevel ...string) error {
	var logger *zap.Logger
	var err error

	// Determine log level
	level := zap.InfoLevel
	if len(logLevel) > 0 && logLevel[0] != "" {
		switch logLevel[0] {
		case "debug":
			level = zap.DebugLevel
		case "info":
			level = zap.InfoLevel
		case "warn":
			level = zap.WarnLevel
		case "error":
			level = zap.ErrorLevel
		}
	}

	if isDevelopment {
		// Development environment: use preset configuration
		config := zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		config.EncoderConfig.EncodeLevel = func(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
			// Fixed width level formatting for alignment
			enc.AppendString(fmt.Sprintf("%-5s", level.CapitalString()))
		}
		config.EncoderConfig.EncodeDuration = zapcore.MillisDurationEncoder
		config.EncoderConfig.CallerKey = "caller"
		config.EncoderConfig.EncodeCaller = func(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
			// Ultra concise caller path formatting with alignment
			enc.AppendString(formatCallerPath(caller))
		}
		config.Level = zap.NewAtomicLevelAt(level)
		atomicLevel = config.Level
		logger, err = config.Build(
			zap.AddCallerSkip(1), // Skip wrapper function to show actual caller
			zap.AddStacktrace(zapcore.ErrorLevel),
		)
	} else {
		// Production environment: use optimized configuration
		logger, err = NewProductionLogger(logPath, level)
	}

	if err != nil {
		return err
	}

	Logger = logger
	Sugar = logger.Sugar()

	// Replace global logger
	zap.ReplaceGlobals(logger)

	return nil
}

// NewProductionLogger creates a production-ready logger with log rotation
func NewProductionLogger(logPath string, level zapcore.Level) (*zap.Logger, error) {
	// Set default log path
	if logPath == "" {
		logPath = "./logs/app.log"
	}

	// Create log directory if it doesn't exist
	if err := createLogDir(logPath); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	// Log rotation configuration
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    100, // megabytes
		MaxBackups: 5,
		MaxAge:     30, // days
		Compress:   true,
	})

	// Console output for errors
	consoleDebugging := zapcore.Lock(os.Stdout)

	// Production environment encoder configuration
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = func(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
		// Fixed width level formatting for alignment
		enc.AppendString(fmt.Sprintf("%-5s", level.CapitalString()))
	}
	encoderConfig.EncodeDuration = zapcore.MillisDurationEncoder
	encoderConfig.MessageKey = "msg"
	encoderConfig.LevelKey = "level"
	encoderConfig.CallerKey = "caller"
	encoderConfig.EncodeCaller = func(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
		// Ultra concise caller path formatting with alignment
		enc.AppendString(formatCallerPath(caller))
	}

	// Create JSON encoder for file
	jsonEncoder := zapcore.NewJSONEncoder(encoderConfig)

	// Create console encoder for stdout
	consoleEncoder := zapcore.NewConsoleEncoder(encoderConfig)

	// Set up atomic level for dynamic adjustment
	atomicLevel = zap.NewAtomicLevelAt(level)

	// File core: Info level and above (no sampling to avoid losing logs)
	fileCore := zapcore.NewCore(
		jsonEncoder,
		w,
		atomicLevel,
	)

	// Console core: same level as file for consistency
	consoleCore := zapcore.NewCore(
		consoleEncoder,
		consoleDebugging,
		level,
	)

	// Combine cores
	core := zapcore.NewTee(fileCore, consoleCore)

	// Add caller information and stack trace
	logger := zap.New(core,
		zap.AddCaller(),
		zap.AddCallerSkip(1), // Skip wrapper function to show actual caller
		zap.AddStacktrace(zapcore.ErrorLevel),
	)

	return logger, nil
}

// With creates a child logger with additional fields
func With(fields ...zap.Field) *zap.Logger {
	return Logger.With(fields...)
}

// Info logs a message at InfoLevel
func Info(msg string, fields ...zap.Field) {
	Logger.Info(msg, fields...)
}

// Error logs a message at ErrorLevel
func Error(msg string, fields ...zap.Field) {
	Logger.Error(msg, fields...)
}

// Warn logs a message at WarnLevel
func Warn(msg string, fields ...zap.Field) {
	Logger.Warn(msg, fields...)
}

// Debug logs a message at DebugLevel
func Debug(msg string, fields ...zap.Field) {
	Logger.Debug(msg, fields...)
}

// Fatal logs a message at FatalLevel
func Fatal(msg string, fields ...zap.Field) {
	Logger.Fatal(msg, fields...)
}

// Sync flushes any buffered log entries
func Sync() error {
	if Logger != nil {
		return Logger.Sync()
	}
	return nil
}

// SetLevel dynamically changes the log level
func SetLevel(level zapcore.Level) {
	if atomicLevel != (zap.AtomicLevel{}) {
		atomicLevel.SetLevel(level)
	}
}

// GetLevel returns the current log level
func GetLevel() zapcore.Level {
	if atomicLevel != (zap.AtomicLevel{}) {
		return atomicLevel.Level()
	}
	return zapcore.InfoLevel
}

// createLogDir creates log directory if it doesn't exist
func createLogDir(logPath string) error {
	// Extract directory from log file path
	dir := filepath.Dir(logPath)

	// Check if directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// Create directory with proper permissions
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return nil
}

// SetCallerDisplayMode sets the caller path display mode
func SetCallerDisplayMode(mode CallerDisplayMode) {
	callerDisplayMode = mode
}

// UseShortPaths enables ultra-concise logging with only filename:line
func UseShortPaths() {
	SetCallerDisplayMode(CallerShort)
}

// UseMediumPaths enables package/filename:line format
func UseMediumPaths() {
	SetCallerDisplayMode(CallerMedium)
}

// UseFullPaths enables full path display
func UseFullPaths() {
	SetCallerDisplayMode(CallerFull)
}

// formatCallerPath formats caller path based on display mode with alignment
func formatCallerPath(caller zapcore.EntryCaller) string {
	fullPath := caller.TrimmedPath()
	var result string
	
	switch callerDisplayMode {
	case CallerShort:
		// Extract just filename:line (intelligent_sync.go:116)
		parts := strings.Split(fullPath, "/")
		if len(parts) > 0 {
			result = parts[len(parts)-1]
		} else {
			result = fullPath
		}
		
	case CallerMedium:
		// Remove common prefixes, keep package/file.go:line
		shortened := strings.TrimPrefix(fullPath, "pkg/")
		shortened = strings.TrimPrefix(shortened, "cmd/")
		shortened = strings.TrimPrefix(shortened, "internal/")
		
		parts := strings.Split(shortened, "/")
		if len(parts) > 2 {
			result = strings.Join(parts[len(parts)-2:], "/")
		} else {
			result = shortened
		}
		
	case CallerFull:
		result = fullPath
		
	default:
		result = fullPath
	}
	
	// Apply fixed width formatting for alignment
	const callerWidth = 28
	if len(result) > callerWidth {
		// Truncate with ellipsis, keep the end part (filename:line)
		result = "..." + result[len(result)-(callerWidth-3):]
	}
	
	return fmt.Sprintf("%-*s", callerWidth, result)
}
